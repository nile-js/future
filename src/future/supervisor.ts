import { Worker } from "node:worker_threads";
import { Ok, Err } from "../result";
import { safeTry } from "../safe-try";
import type { Result } from "../result";
import { createMemoryPool } from "./memory-pool";
import { createPubSub, createMessageChannel } from "./pubsub";
import { createResourceManager } from "./resource-manager";
import { createActorRef } from "./actor";
import { createGroupManager } from "./group-manager";
import { createDiagnosticsCollector } from "./diagnostics";
import { BOX_CLEAN, BOX_LOCKED, BOX_READY, BOX_READING } from "./types";
import type {
  ActorCallback,
  ActorConfig,
  ActorDiagnostics,
  ActorGroup,
  ActorGroupConfig,
  ActorId,
  ActorRef,
  InternalActorState,
  Lock,
  MainToWorkerMessage,
  Supervisor,
  SupervisorConfig,
  SupervisorDiagnostics,
  WorkerToMainMessage,
} from "./types";

// ============================================================================
// ID generation
// ============================================================================

let idCounter = 0;
function generateId(prefix: string): string {
  return `${prefix}-${Date.now()}-${++idCounter}-${Math.random().toString(36).slice(2, 7)}`;
}

// ============================================================================
// Supervisor factory
// ============================================================================

/**
 * Create a supervisor that manages actor lifecycle, memory pool, resources,
 * lease-based opportunistic cleanup, supervision groups, and diagnostics.
 *
 * @param config - Supervisor configuration
 * @returns Supervisor handle
 */
export function createSupervisor(config: SupervisorConfig): Supervisor {
  const id = generateId("supervisor");
  const memoryPool = createMemoryPool({ poolSize: config.memory.poolSize, boxSize: config.memory.boxSize });
  const resourceManager = createResourceManager(config.resources);
  const pubSub = createPubSub();
  const diagCollector = createDiagnosticsCollector(config.diagnostics);

  const actors = new Map<ActorId, InternalActorState>();
  const lockQueue: Array<{ readonly actorId: ActorId; readonly resolve: (lock: Lock) => void }> = [];
  const readerCounts = new Map<number, number>();

  const defaultLeaseMs = config.timeouts?.defaultLeaseMs ?? 5000;
  const actorTimeouts = config.timeouts?.actorTimeouts ?? {};

  // Derive worker bootstrap path from current module URL
  const workerPath = new URL("./worker-bootstrap.ts", import.meta.url).pathname;

  // ============================================================================
  // Lease cleanup
  // ============================================================================

  /** Opportunistic lease check — called on every actor-supervisor interaction */
  function checkLeases(): void {
    const now = Date.now();
    for (let i = 0; i < memoryPool.poolSize; i++) {
      if (memoryPool.isLeaseExpired(i)) {
        const boxState = memoryPool.stateBoard[i];

        if (boxState === BOX_LOCKED || boxState === BOX_READY) {
          // Writer-owned box — find and terminate the writer
          for (const [actorId, state] of actors) {
            if (state.locks.has(i) && !state.terminated) {
              terminateActor(actorId, "lease_expired");
              break;
            }
          }
        } else if (boxState === BOX_READING) {
          // Reader-owned box — terminate ALL actors reading this box
          for (const [actorId, state] of actors) {
            if (state.reads.has(i) && !state.terminated) {
              terminateActor(actorId, "lease_expired");
            }
          }
          readerCounts.delete(i);
        }

        memoryPool.markClean(i);
        serveLockQueue();
      }
    }
  }

  // ============================================================================
  // Lock queue
  // ============================================================================

  /** Serve queued lock requests after a box becomes available */
  function serveLockQueue(): void {
    while (lockQueue.length > 0) {
      const lock = memoryPool.tryAcquireBox();
      if (!lock) break;

      const next = lockQueue.shift()!;
      const state = actors.get(next.actorId);
      if (state && !state.terminated) {
        state.locks.add(lock.boxIndex);
        const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
        memoryPool.setLease(lock.boxIndex, Date.now() + leaseMs);
        next.resolve(lock);
      } else {
        // Actor died while waiting — recycle box and continue
        memoryPool.markClean(lock.boxIndex);
      }
    }
  }

  /** Queue a lock request for an actor */
  function queueLockRequest(actorId: ActorId, resolve: (lock: Lock) => void): void {
    lockQueue.push({ actorId, resolve });
  }

  /** Remove all queued lock requests for a given actor (on termination) */
  function clearActorLockRequests(actorId: ActorId): void {
    const remaining = lockQueue.filter((req) => req.actorId !== actorId);
    lockQueue.length = 0;
    lockQueue.push(...remaining);
  }

  // ============================================================================
  // Worker message handlers
  // ============================================================================

  function handleSend(actorId: ActorId, msg: unknown): void {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;
    diagCollector.recordActorMessage(actorId);
    state.messagesSent++;
    state.lastHeartbeatAt = Date.now();
    for (const fn of state.subscribers) {
      fn(msg);
    }
  }

  function handleHeartbeat(actorId: ActorId): void {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;
    diagCollector.recordActorHeartbeat(actorId);
    state.heartbeats++;
    state.lastHeartbeatAt = Date.now();
    // Reset leases for all boxes held by this actor
    const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
    for (const boxIndex of state.locks) {
      memoryPool.setLease(boxIndex, Date.now() + leaseMs);
    }
  }

  function handleLockRequest(actorId: ActorId, channel: ReturnType<typeof createMessageChannel>): void {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;

    const lock = memoryPool.tryAcquireBox();
    if (lock) {
      state.locks.add(lock.boxIndex);
      const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
      memoryPool.setLease(lock.boxIndex, Date.now() + leaseMs);
      channel.send({ type: "LOCK_GRANTED", lock } as MainToWorkerMessage);
    } else {
      queueLockRequest(actorId, (grantedLock) => {
        channel.send({ type: "LOCK_GRANTED", lock: grantedLock } as MainToWorkerMessage);
      });
    }
  }

  function handleDeposit(actorId: ActorId): void {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;
    // Implicit heartbeat
    state.lastHeartbeatAt = Date.now();
    const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
    for (const boxIndex of state.locks) {
      memoryPool.setLease(boxIndex, Date.now() + leaseMs);
    }
  }

  function handleDone(actorId: ActorId, lock: Lock): void {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;

    const boxState = memoryPool.stateBoard[lock.boxIndex];

    if (boxState === BOX_LOCKED) {
      // Writer done — mark READY so subscribers can read
      memoryPool.markReady(lock.boxIndex);
      state.lastHeartbeatAt = Date.now();
      const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
      memoryPool.setLease(lock.boxIndex, Date.now() + leaseMs);
      for (const fn of state.subscribers) {
        fn({ type: "DEPOSIT_READY", address: lock });
      }
    } else if (boxState === BOX_READING) {
      // Reader done — decrement reader count (guard against double-done)
      if (!state.reads.has(lock.boxIndex)) return;
      const currentCount = readerCounts.get(lock.boxIndex) ?? 1;
      const newCount = currentCount - 1;
      if (newCount <= 0) {
        // Last reader — clean up box and serve queue
        memoryPool.markClean(lock.boxIndex);
        readerCounts.delete(lock.boxIndex);
        serveLockQueue();
      } else {
        readerCounts.set(lock.boxIndex, newCount);
      }
      state.reads.delete(lock.boxIndex);
    }
    // Else: ignore DONE for other states
  }

  function handleReadStart(actorId: ActorId, lock: Lock, requestId: string, channel: ReturnType<typeof createMessageChannel>): void {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;

    const boxState = memoryPool.stateBoard[lock.boxIndex];

    if (boxState === BOX_READY) {
      // READY → READING: first reader
      memoryPool.markReading(lock.boxIndex);
      readerCounts.set(lock.boxIndex, 1);
      state.reads.add(lock.boxIndex);
      const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
      memoryPool.setLease(lock.boxIndex, Date.now() + leaseMs);
      channel.send({ type: "READ_GRANTED", lock, requestId } as MainToWorkerMessage);
    } else if (boxState === BOX_READING) {
      // READING: additional reader — increment count
      const currentCount = readerCounts.get(lock.boxIndex) ?? 0;
      readerCounts.set(lock.boxIndex, currentCount + 1);
      state.reads.add(lock.boxIndex);
      const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
      memoryPool.setLease(lock.boxIndex, Date.now() + leaseMs);
      channel.send({ type: "READ_GRANTED", lock, requestId } as MainToWorkerMessage);
    } else {
      // LOCKED or CLEAN — cannot read
      channel.send({ type: "READ_ERROR", lock, error: `Box ${lock.boxIndex} is not readable (state=${boxState})`, requestId } as MainToWorkerMessage);
    }
  }

  async function handleResourceRequest(
    actorId: ActorId,
    msg: { resource: string; method: string; args: unknown; id: string },
    channel: ReturnType<typeof createMessageChannel>,
  ): Promise<void> {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;
    state.lastHeartbeatAt = Date.now();

    const result = await safeTry(() => resourceManager.execute(msg.resource, msg.method, msg.args));
    if (result.isOk) {
      channel.send({ type: "RESOURCE_RESPONSE", id: msg.id, result: result.value } as MainToWorkerMessage);
    } else {
      channel.send({ type: "RESOURCE_ERROR", id: msg.id, error: result.error } as MainToWorkerMessage);
    }
  }

  function handleLink(actorId: ActorId, otherId: ActorId): void {
    const state = actors.get(actorId);
    const other = actors.get(otherId);
    if (!state || !other) return;
    state.linkedActors.add(otherId);
    other.linkedActors.add(actorId);
  }

  function handleMonitor(actorId: ActorId, otherId: ActorId): void {
    const state = actors.get(actorId);
    const other = actors.get(otherId);
    if (!state || !other) return;
    state.monitoredActors.add(otherId);
  }

  function handleError(actorId: ActorId, error: string): void {
    terminateActor(actorId, `error: ${error}`);
  }

  /** Message type → handler map for worker-to-main messages */
  const messageHandlers: Record<string, (actorId: ActorId, msg: WorkerToMainMessage, channel: ReturnType<typeof createMessageChannel>) => void | Promise<void>> = {
    SEND: (actorId, msg) => handleSend(actorId, (msg as any).msg),
    HEARTBEAT: (actorId) => handleHeartbeat(actorId),
    LOCK_REQUEST: (actorId, _msg, channel) => handleLockRequest(actorId, channel),
    DEPOSIT: (actorId) => handleDeposit(actorId),
    DONE: (actorId, msg) => handleDone(actorId, (msg as any).lock),
    READ_START: (actorId, msg, channel) => handleReadStart(actorId, (msg as any).lock, (msg as any).requestId, channel),
    RESOURCE_REQUEST: (actorId, msg, channel) => handleResourceRequest(actorId, msg as any, channel),
    LINK: (actorId, msg) => handleLink(actorId, (msg as any).actorId),
    MONITOR: (actorId, msg) => handleMonitor(actorId, (msg as any).actorId),
    ERROR: (actorId, msg) => handleError(actorId, (msg as any).error),
    SPAWN_CHILD: (actorId, msg, channel) => {
      try {
        const callbackString = (msg as any).callback;
        // Validate: reject strings with dangerous patterns to prevent code injection
        const dangerousPatterns = [/\brequire\s*\(/, /\bprocess\b/, /\bglobalThis\b/, /\bimport\s*\(/, /\beval\s*\(/, /\bFunction\s*\(/];
        if (dangerousPatterns.some((p) => p.test(callbackString))) {
          throw new Error("Callback contains potentially dangerous code patterns");
        }

        const callback = new Function("return " + callbackString)() as ActorCallback;
        if (typeof callback !== "function") {
          throw new Error("Invalid callback: not a function");
        }
        const child = spawnActor(callback, (msg as any).config);
        channel.send({ type: "CHILD_SPAWNED", requestId: (msg as any).requestId, childId: child.id } as MainToWorkerMessage);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        channel.send({ type: "CHILD_SPAWN_ERROR", requestId: (msg as any).requestId, error: errorMessage } as MainToWorkerMessage);
      }
    },
  };

  /** Main worker message dispatcher */
  function handleWorkerMessage(
    actorId: ActorId,
    msg: WorkerToMainMessage,
    channel: ReturnType<typeof createMessageChannel>,
  ): void {
    checkLeases();
    const handler = messageHandlers[msg.type];
    if (handler) {
      handler(actorId, msg, channel);
    }
  }

  // ============================================================================
  // Actor lifecycle
  // ============================================================================

  function spawnActor(callback: ActorCallback, options?: ActorConfig): ActorRef {
    if (actors.size >= config.maxActors) {
      throw new Error(`Max actors limit reached: ${config.maxActors}`);
    }

    const actorId = generateId("actor");
    const worker = new Worker(workerPath, {
      workerData: { sab: memoryPool.sab },
    });
    const channel = createMessageChannel(worker);

    // Send INIT
    channel.send({
      type: "INIT",
      callback: callback.toString(),
      config: {
        memory: config.memory,
        timeouts: config.timeouts ?? { defaultLeaseMs: 5000 },
      },
      actorId,
    } as MainToWorkerMessage);

    // Listen for all worker messages
    channel.onMessage((msg) => handleWorkerMessage(actorId, msg, channel));

    const state: InternalActorState = {
      id: actorId,
      worker,
      config: options ?? {},
      createdAt: Date.now(),
      heartbeats: 0,
      lastHeartbeatAt: Date.now(),
      messagesSent: 0,
      subscribers: new Set(),
      locks: new Set(),
      reads: new Set(),
      linkedActors: new Set(),
      monitoredActors: new Set(),
      terminated: false,
    };
    actors.set(actorId, state);

    // Clean up actor if worker crashes or exits unexpectedly
    worker.on("exit", (code) => {
      if (!state.terminated) {
        terminateActor(actorId, `worker_exit_code_${code}`);
      }
    });
    worker.on("error", (err: unknown) => {
      if (!state.terminated) {
        const message = err instanceof Error ? err.message : String(err);
        terminateActor(actorId, `worker_error: ${message}`);
      }
    });

    diagCollector.recordActorStart(actorId);

    return createActorRef({
      id: actorId,
      sendToWorker: (msg) => channel.send(msg),
      subscribeToMessages: (fn) => {
        state.subscribers.add(fn);
        return () => state.subscribers.delete(fn);
      },
      terminate: () => terminateActor(actorId),
      readBox: (lock) => {
        const boxState = memoryPool.stateBoard[lock.boxIndex];
        if (boxState === BOX_READY) {
          // READY → READING: first main-thread reader
          memoryPool.markReading(lock.boxIndex);
          readerCounts.set(lock.boxIndex, 1);
          state.reads.add(lock.boxIndex);
          const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
          memoryPool.setLease(lock.boxIndex, Date.now() + leaseMs);
          const data = memoryPool.readBox(lock.boxIndex);
          return data;
        }
        if (boxState === BOX_READING) {
          // READING: additional main-thread reader — increment count
          const currentCount = readerCounts.get(lock.boxIndex) ?? 0;
          readerCounts.set(lock.boxIndex, currentCount + 1);
          state.reads.add(lock.boxIndex);
          const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
          memoryPool.setLease(lock.boxIndex, Date.now() + leaseMs);
          const data = memoryPool.readBox(lock.boxIndex);
          return data;
        }
        // LOCKED or CLEAN — cannot read
        throw new Error(`Box ${lock.boxIndex} is not readable (state=${boxState})`);
      },
      doneBox: (lock) => {
        const boxState = memoryPool.stateBoard[lock.boxIndex];
        if (boxState === BOX_READING) {
          if (!state.reads.has(lock.boxIndex)) return;
          const currentCount = readerCounts.get(lock.boxIndex) ?? 1;
          const newCount = currentCount - 1;
          if (newCount <= 0) {
            memoryPool.markClean(lock.boxIndex);
            readerCounts.delete(lock.boxIndex);
            serveLockQueue();
          } else {
            readerCounts.set(lock.boxIndex, newCount);
          }
          state.reads.delete(lock.boxIndex);
        }
      },
      getDiagnostics: () => getActorDiagnostics(actorId),
      linkTo: (otherId) => {
        state.linkedActors.add(otherId);
        const other = actors.get(otherId);
        if (other) other.linkedActors.add(actorId);
      },
      monitor: (otherId) => {
        state.monitoredActors.add(otherId);
      },
    });
  }

  function terminateActor(actorId: ActorId, reason?: string): void {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;

    state.terminated = true;
    state.terminationReason = reason;

    // Terminate linked actors
    for (const linkedId of state.linkedActors) {
      if (linkedId !== actorId) {
        terminateActor(linkedId, "linked_actor_died");
      }
    }

    // Notify monitors
    for (const [monitorId, monitorState] of actors) {
      if (monitorState.monitoredActors.has(actorId) && !monitorState.terminated) {
        for (const fn of monitorState.subscribers) {
          fn({ type: "ActorDown", id: actorId, reason: reason ?? "terminated" });
        }
      }
    }

    // Release locks
    for (const boxIndex of state.locks) {
      memoryPool.markClean(boxIndex);
    }
    state.locks.clear();

    // Release reads — decrement reader counts, clean if last reader
    for (const boxIndex of state.reads) {
      const currentCount = readerCounts.get(boxIndex) ?? 1;
      const newCount = currentCount - 1;
      if (newCount <= 0) {
        memoryPool.markClean(boxIndex);
        readerCounts.delete(boxIndex);
      } else {
        readerCounts.set(boxIndex, newCount);
      }
    }
    state.reads.clear();

    clearActorLockRequests(actorId);
    serveLockQueue();

    // Terminate worker thread
    state.worker.terminate();

    // Record diagnostics before removing from map
    diagCollector.recordActorTermination(actorId, reason);

    // Remove from actors map
    actors.delete(actorId);

    // Notify group manager of failure (fire-and-forget for crashes/errors, not shutdown or normal termination)
    if (reason !== undefined && reason !== "shutdown") {
      groupManager.handleActorFailure(actorId, reason).catch(() => {});
    }
  }

  // ============================================================================
  // Group manager (depends on spawnActor and terminateActor closures)
  // ============================================================================

  const groupManager = createGroupManager({ spawnActor, terminateActor });

  // ============================================================================
  // Root supervision group — all standalone actors get supervised here
  // ============================================================================

  const rootGroupState = groupManager.createGroup({
    strategy: config.strategy ?? "one-for-one",
    retry: config.retry,
  });

  // ============================================================================
  // Groups
  // ============================================================================

  function createGroup(groupConfig: ActorGroupConfig): ActorGroup {
    const groupState = groupManager.createGroup(groupConfig);

    return {
      id: groupState.id,
      spawn: (callback, options) => {
        const actor = spawnActor(callback, options);
        groupManager.registerActor(groupState.id, actor.id, callback, options);
        return actor;
      },
      terminateAll: () => {
        for (const actorId of [...groupState.actorIds]) {
          terminateActor(actorId);
        }
        groupManager.destroyGroup(groupState.id);
      },
      read: (lock) => {
        const boxState = memoryPool.stateBoard[lock.boxIndex];
        if (boxState === BOX_READY) {
          memoryPool.markReading(lock.boxIndex);
          readerCounts.set(lock.boxIndex, 1);
          const leaseMs = defaultLeaseMs;
          memoryPool.setLease(lock.boxIndex, Date.now() + leaseMs);
          const data = memoryPool.readBox(lock.boxIndex);
          return data;
        }
        if (boxState === BOX_READING) {
          const currentCount = readerCounts.get(lock.boxIndex) ?? 0;
          readerCounts.set(lock.boxIndex, currentCount + 1);
          const leaseMs = defaultLeaseMs;
          memoryPool.setLease(lock.boxIndex, Date.now() + leaseMs);
          const data = memoryPool.readBox(lock.boxIndex);
          return data;
        }
        throw new Error(`Box ${lock.boxIndex} is not readable (state=${boxState})`);
      },
      done: (lock) => {
        const boxState = memoryPool.stateBoard[lock.boxIndex];
        if (boxState === BOX_READING) {
          if (!readerCounts.has(lock.boxIndex)) return;
          const currentCount = readerCounts.get(lock.boxIndex) ?? 1;
          const newCount = currentCount - 1;
          if (newCount <= 0) {
            memoryPool.markClean(lock.boxIndex);
            readerCounts.delete(lock.boxIndex);
            serveLockQueue();
          } else {
            readerCounts.set(lock.boxIndex, newCount);
          }
        }
      },
    };
  }

  // ============================================================================
  // Diagnostics
  // ============================================================================

  function getActorDiagnostics(actorId: ActorId): Result<ActorDiagnostics, string> {
    const state = actors.get(actorId);
    if (!state) return Err(`Actor not found: ${actorId}`);
    return Ok(diagCollector.buildActorDiagnostics(actorId, state));
  }

  function getSupervisorDiagnostics(): Result<SupervisorDiagnostics, string> {
    const actorDiagnostics: ActorDiagnostics[] = [];
    for (const [actorId, state] of actors) {
      actorDiagnostics.push(diagCollector.buildActorDiagnostics(actorId, state));
    }

    let boxesInUse = 0;
    for (let i = 0; i < memoryPool.poolSize; i++) {
      const boxState = memoryPool.stateBoard[i];
      if (boxState === BOX_LOCKED || boxState === BOX_READY || boxState === BOX_READING) boxesInUse++;
    }

    return Ok(diagCollector.buildSupervisorDiagnostics({
      activeActors: actors.size,
      totalSpawned: idCounter,
      totalTerminated: idCounter - actors.size,
      poolSize: memoryPool.poolSize,
      boxesInUse,
      actors: actorDiagnostics,
    }));
  }

  // ============================================================================
  // Supervisor object
  // ============================================================================

  return {
    id,
    spawn: (callback: ActorCallback, options?: ActorConfig) => {
      const actor = spawnActor(callback, options);
      groupManager.registerActor(rootGroupState.id, actor.id, callback, options);
      return actor;
    },
    createGroup,
    terminateActor,
    shutdown: async () => {
      for (const [actorId] of actors) {
        terminateActor(actorId, "shutdown");
      }
      await resourceManager.releaseAll();
      pubSub.clear();
    },
    subscribe: pubSub.subscribe,
    getDiagnostics: getSupervisorDiagnostics,
  };
}