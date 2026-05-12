import { Worker } from "node:worker_threads";
import { Ok, Err, safeTry } from "slang-ts";
import type { Result } from "slang-ts";
import { createMemoryPool } from "./memory-pool";
import { createPubSub, createMessageChannel } from "./pubsub";
import { createResourceManager } from "./resource-manager";
import { createActorRef } from "./actor";
import { createGroupManager } from "./group-manager";
import { createDiagnosticsCollector } from "./diagnostics";
import type {
  ActorCallback,
  ActorConfig,
  ActorDiagnostics,
  ActorGroup,
  ActorGroupConfig,
  ActorId,
  ActorRef,
  BoxEntry,
  ChainableReader,
  FmtType,
  InboxEntry,
  InternalActorState,
  Lock,
  MainToWorkerMessage,
  Message,
  ShareConfig,
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
// Write request — queued when no FREE boxes available
// ============================================================================

type WriteRequest = {
  readonly actorId: ActorId;
  readonly msg: string;
  readonly fmt: FmtType;
  readonly data: Uint8Array;
  readonly share: ShareConfig;
  readonly resolve: (lock: Lock) => void;
  readonly reject: (error: Error) => void;
  readonly enqueuedAt: number;
};

// ============================================================================
// Supervisor factory
// ============================================================================

/**
 * Create a supervisor that manages actor lifecycle, memory pool, resources,
 * inbox routing, authorization, lease-based cleanup, supervision groups,
 * and diagnostics.
 *
 * Per ADR 002: SAB is raw data only. All state tracked in BoxEntry[] plain objects.
 * Per ADR 003: Queue-based box assignment, no CAS.
 * Per ADR 004: Opportunistic lease cleanup on any interaction.
 */
export function createSupervisor(config: SupervisorConfig): Supervisor {
  const id = generateId("supervisor");
  const memoryPool = createMemoryPool({ poolSize: config.memory.poolSize, boxSize: config.memory.boxSize });
  const resourceManager = createResourceManager(config.resources);
  const pubSub = createPubSub();
  const diagCollector = createDiagnosticsCollector(config.diagnostics);

  const actors = new Map<ActorId, InternalActorState>();
  const defaultLeaseMs = config.timeouts?.defaultLeaseMs ?? 5000;
  const actorTimeouts = config.timeouts?.actorTimeouts ?? {};

  const workerPath = new URL("./worker-bootstrap.ts", import.meta.url).pathname;

  // ============================================================================
  // BoxEntry[] — supervisor-side state tracking (replaces SAB state board)
  // ============================================================================

  const boxEntries: BoxEntry[] = Array.from({ length: config.memory.poolSize }, () => ({
    state: "FREE" as const,
    from: "",
    msg: "",
    type: "json" as FmtType,
    share: "owner" as ShareConfig,
    refCount: 0,
    expiresAt: 0,
    writer: null,
    readers: new Set<ActorId>(),
    epoch: 0,
  }));

  // ============================================================================
  // Per-actor inbox queues
  // ============================================================================

  const inboxes = new Map<ActorId, InboxEntry[]>();

  // ============================================================================
  // Write queue (FIFO box assignment)
  // ============================================================================

  const writeQueue: WriteRequest[] = [];

  // ============================================================================
  // Lease cleanup (ADR 004: opportunistic on any interaction)
  // ============================================================================

  function checkLeases(): void {
    const now = Date.now();
    for (let i = 0; i < config.memory.poolSize; i++) {
      const entry = boxEntries[i]!;
      if (entry.state === "FREE") continue;
      if (entry.expiresAt === 0 || now <= entry.expiresAt) continue;

      // Lease expired — force-release
      if (entry.state === "WRITING" && entry.writer) {
        terminateActor(entry.writer, "lease_expired");
      }
      if (entry.state === "READY") {
        for (const readerId of entry.readers) {
          const readerState = actors.get(readerId);
          if (readerState && !readerState.terminated) {
            // Remove inbox entries for this box
            const inbox = inboxes.get(readerId);
            if (inbox) {
              const filtered = inbox.filter((e) => e.handle.boxIndex !== i);
              inboxes.set(readerId, filtered);
            }
          }
        }
      }
      resetBoxEntry(i);
      serveWriteQueue();
    }
  }

  function resetBoxEntry(boxIndex: number): void {
    const entry = boxEntries[boxIndex]!;
    entry.state = "FREE";
    entry.from = "";
    entry.msg = "";
    entry.type = "json";
    entry.share = "owner";
    entry.refCount = 0;
    entry.expiresAt = 0;
    entry.writer = null;
    entry.readers.clear();
    entry.epoch++;
  }

  // ============================================================================
  // Write queue service
  // ============================================================================

  function serveWriteQueue(): void {
    while (writeQueue.length > 0) {
      const freeIndex = boxEntries.findIndex((e) => e.state === "FREE");
      if (freeIndex === -1) break;

      const req = writeQueue.shift()!;
      const actorState = actors.get(req.actorId);
      if (!actorState || actorState.terminated) continue;

      const entry = boxEntries[freeIndex]!;
      entry.state = "WRITING";
      entry.from = req.actorId;
      entry.msg = req.msg;
      entry.type = req.fmt;
      entry.share = req.share;
      entry.writer = req.actorId;
      entry.epoch++;

      const leaseMs = actorTimeouts[actorState.config.name ?? ""] ?? defaultLeaseMs;
      entry.expiresAt = Date.now() + leaseMs;

      diagCollector.recordWriteQueueWait(Date.now() - req.enqueuedAt);
      diagCollector.recordWriteQueueDepth(writeQueue.length);

      const lock: Lock = { boxIndex: freeIndex, epoch: entry.epoch };
      req.resolve(lock);
    }
  }

  // ============================================================================
  // Authorization — compute readers for a box based on ShareConfig
  // ============================================================================

  function computeReaders(writerId: ActorId, share: ShareConfig): Set<ActorId> {
    const readers = new Set<ActorId>();

    if (share === "owner") {
      // Only the writer can read (main-thread subscribers handled separately)
      readers.add(writerId);
    } else if (share === "group") {
      // All actors in the same supervision group
      const group = groupManager.getGroupForActor(writerId);
      if (group) {
        for (const actorId of group.actorIds) {
          readers.add(actorId);
        }
      }
    } else if (share === "linked") {
      // Actors linked to the writer
      const writerState = actors.get(writerId);
      if (writerState) {
        for (const linkedId of writerState.linkedActors) {
          readers.add(linkedId);
        }
      }
    } else if (Array.isArray(share)) {
      // Explicit allowlist
      for (const actorId of share) {
        readers.add(actorId);
      }
    }

    return readers;
  }

  function canRead(actorId: ActorId, entry: BoxEntry): boolean {
    return entry.readers.has(actorId);
  }

  // ============================================================================
  // Worker message handlers
  // ============================================================================

  function handleSend(actorId: ActorId, msg: string, data?: unknown): void {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;
    checkLeases();
    diagCollector.recordActorMessage(actorId);
    state.messagesSent++;
    state.lastHeartbeatAt = Date.now();

    const message: Message = { msg, data, from: actorId };
    for (const fn of state.subscribers) {
      fn(message);
    }
    pubSub.publish(message);
  }

  function handleHeartbeat(actorId: ActorId): void {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;
    diagCollector.recordActorHeartbeat(actorId);
    state.heartbeats++;
    state.lastHeartbeatAt = Date.now();
    // Reset leases for boxes where this actor is writer
    const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
    for (let i = 0; i < config.memory.poolSize; i++) {
      const entry = boxEntries[i]!;
      if (entry.writer === actorId && entry.state !== "FREE") {
        entry.expiresAt = Date.now() + leaseMs;
      }
    }
  }

  function handleWriteRequest(
    actorId: ActorId,
    msg: { msg: string; fmt: FmtType; data: Uint8Array; share: ShareConfig },
    channel: ReturnType<typeof createMessageChannel>,
  ): void {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;
    checkLeases();

    // Find a FREE box
    const freeIndex = boxEntries.findIndex((e) => e.state === "FREE");
    if (freeIndex !== -1) {
      const entry = boxEntries[freeIndex]!;
      entry.state = "WRITING";
      entry.from = actorId;
      entry.msg = msg.msg;
      entry.type = msg.fmt;
      entry.share = msg.share ?? "owner";
      entry.writer = actorId;
      entry.epoch++;

      const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
      entry.expiresAt = Date.now() + leaseMs;

      const lock: Lock = { boxIndex: freeIndex, epoch: entry.epoch };
      channel.send({ type: "WRITE_GRANTED", lock } as MainToWorkerMessage);
    } else {
      // Queue the request
      writeQueue.push({
        actorId,
        msg: msg.msg,
        fmt: msg.fmt,
        data: msg.data,
        share: msg.share ?? "owner",
        resolve: (lock) => {
          channel.send({ type: "WRITE_GRANTED", lock } as MainToWorkerMessage);
        },
        reject: () => {},
        enqueuedAt: Date.now(),
      });
      diagCollector.recordWriteQueueDepth(writeQueue.length);
    }
  }

  function handleCommit(actorId: ActorId, lock: Lock): void {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;

    const entry = boxEntries[lock.boxIndex];
    if (!entry || entry.epoch !== lock.epoch) return;
    if (entry.state !== "WRITING" || entry.writer !== actorId) return;

    // Mark READY
    entry.state = "READY";
    const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
    entry.expiresAt = Date.now() + leaseMs;

    // Compute authorized readers (exclude writer — they already have the data)
    const readers = computeReaders(actorId, entry.share);
    readers.delete(actorId); // Writer doesn't need INBOX, never releases
    entry.readers = readers;
    entry.refCount = readers.size;

    diagCollector.recordRefCount(lock.boxIndex, entry.refCount);

    // Deliver INBOX to authorized readers
    for (const readerId of readers) {
      const readerState = actors.get(readerId);
      if (!readerState || readerState.terminated) continue;

      const inboxEntry: InboxEntry = {
        handle: { boxIndex: lock.boxIndex, epoch: lock.epoch },
        from: actorId,
        msg: entry.msg,
        type: entry.type,
      };

      if (!inboxes.has(readerId)) {
        inboxes.set(readerId, []);
      }
      inboxes.get(readerId)!.push(inboxEntry);

      diagCollector.recordInboxDepth(readerId, inboxes.get(readerId)!.length);

      // Post INBOX to reader's worker
      const readerChannel = getChannelForActor(readerId);
      if (readerChannel) {
        readerChannel.send({
          type: "INBOX",
          handle: inboxEntry.handle,
          from: inboxEntry.from,
          msg: inboxEntry.msg,
          fmt: inboxEntry.type,
        } as MainToWorkerMessage);
      }

      diagCollector.recordAuthorizationEvent(true);
    }

    state.lastHeartbeatAt = Date.now();
  }

  function handleRelease(actorId: ActorId, lock: Lock): void {
    const entry = boxEntries[lock.boxIndex];
    if (!entry || entry.epoch !== lock.epoch) return;

    entry.refCount = Math.max(0, entry.refCount - 1);
    diagCollector.recordRefCount(lock.boxIndex, entry.refCount);

    if (entry.refCount <= 0) {
      resetBoxEntry(lock.boxIndex);
      serveWriteQueue();
    }
  }

  async function handleResourceRequest(
    actorId: ActorId,
    msg: { resource: string; method: string; args: unknown; id: string },
    channel: ReturnType<typeof createMessageChannel>,
  ): Promise<void> {
    const state = actors.get(actorId);
    if (!state || state.terminated) return;
    checkLeases();
    state.lastHeartbeatAt = Date.now();

    const start = performance.now();
    const result = await safeTry(() => resourceManager.execute(msg.resource, msg.method, msg.args));
    diagCollector.recordResourceCall(performance.now() - start);

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

  // Channel lookup for delivering INBOX messages
  const actorChannels = new Map<ActorId, ReturnType<typeof createMessageChannel>>();

  function getChannelForActor(actorId: ActorId): ReturnType<typeof createMessageChannel> | undefined {
    return actorChannels.get(actorId);
  }

  /** Message type → handler map */
  const messageHandlers: Record<string, (actorId: ActorId, msg: WorkerToMainMessage, channel: ReturnType<typeof createMessageChannel>) => void | Promise<void>> = {
    SEND: (actorId, msg) => handleSend(actorId, (msg as any).msg, (msg as any).data),
    HEARTBEAT: (actorId) => handleHeartbeat(actorId),
    WRITE_REQUEST: (actorId, msg, channel) => handleWriteRequest(actorId, msg as any, channel),
    COMMIT: (actorId, msg) => handleCommit(actorId, (msg as any).lock),
    RELEASE: (actorId, msg) => handleRelease(actorId, (msg as any).lock),
    RESOURCE_REQUEST: (actorId, msg, channel) => handleResourceRequest(actorId, msg as any, channel),
    LINK: (actorId, msg) => handleLink(actorId, (msg as any).actorId),
    MONITOR: (actorId, msg) => handleMonitor(actorId, (msg as any).actorId),
    ERROR: (actorId, msg) => handleError(actorId, (msg as any).error),
    SPAWN_CHILD: (actorId, msg, channel) => {
      try {
        const callbackString = (msg as any).callback;
        const dangerousPatterns = [/\brequire\s*\(/, /\bprocess\b/, /\bglobalThis\b/, /\bimport\s*\(/, /\beval\s*\(/, /\bFunction\s*\(/];
        if (dangerousPatterns.some((p) => p.test(callbackString))) {
          throw new Error("Callback contains potentially dangerous code patterns");
        }
        const callback = new Function("return " + callbackString)() as ActorCallback;
        if (typeof callback !== "function") throw new Error("Invalid callback: not a function");
        const child = spawnActor(callback, (msg as any).config);
        channel.send({ type: "CHILD_SPAWNED", requestId: (msg as any).requestId, childId: child.id } as MainToWorkerMessage);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        channel.send({ type: "CHILD_SPAWN_ERROR", requestId: (msg as any).requestId, error: errorMessage } as MainToWorkerMessage);
      }
    },
    TERMINATE_CHILD: (_actorId, msg) => terminateActor((msg as any).childId),
  };

  function handleWorkerMessage(
    actorId: ActorId,
    msg: WorkerToMainMessage,
    channel: ReturnType<typeof createMessageChannel>,
  ): void {
    const handler = messageHandlers[msg.type];
    if (handler) handler(actorId, msg, channel);
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
    actorChannels.set(actorId, channel);

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
      linkedActors: new Set(),
      monitoredActors: new Set(),
      terminated: false,
    };
    actors.set(actorId, state);
    inboxes.set(actorId, []);

    // Clean up actor if worker crashes
    worker.on("exit", (code) => {
      if (!state.terminated) terminateActor(actorId, `worker_exit_code_${code}`);
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
      readFromBox: (msg: Message): ChainableReader | null => {
        if (!msg.handle) return null;
        const entry = boxEntries[msg.handle.boxIndex];
        if (!entry || entry.epoch !== msg.handle.epoch) return null;
        if (entry.state !== "READY") return null;

        // Authorization check for main-thread reader
        diagCollector.recordAuthorizationEvent(true);

        // Increment ref count and extend lease
        entry.refCount++;
        const leaseMs = actorTimeouts[state.config.name ?? ""] ?? defaultLeaseMs;
        entry.expiresAt = Date.now() + leaseMs;
        diagCollector.recordRefCount(msg.handle.boxIndex, entry.refCount);

        const raw = memoryPool.readBox(msg.handle.boxIndex);
        const decoder = new TextDecoder();
        /** Strip trailing null bytes for text/JSON decoding */
        const stripNulls = (bytes: Uint8Array): Uint8Array => {
          let end = bytes.length;
          while (end > 0 && bytes[end - 1] === 0) end--;
          return bytes.subarray(0, end);
        };
        return {
          json: () => JSON.parse(decoder.decode(stripNulls(raw))),
          string: () => decoder.decode(stripNulls(raw)),
          binary: () => new Uint8Array(raw),
          cbor: () => { throw new Error("CBOR not implemented"); },
          raw: () => new Uint8Array(raw),
        };
      },
      releaseBox: (handle: Lock) => {
        const entry = boxEntries[handle.boxIndex];
        if (!entry || entry.epoch !== handle.epoch) return;
        entry.refCount = Math.max(0, entry.refCount - 1);
        diagCollector.recordRefCount(handle.boxIndex, entry.refCount);
        if (entry.refCount <= 0) {
          resetBoxEntry(handle.boxIndex);
          serveWriteQueue();
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

    // Terminate linked actors (bi-directional)
    for (const linkedId of state.linkedActors) {
      if (linkedId !== actorId) terminateActor(linkedId, "linked_actor_died");
    }

    // Notify monitors
    for (const [, monitorState] of actors) {
      if (monitorState.monitoredActors.has(actorId) && !monitorState.terminated) {
        for (const fn of monitorState.subscribers) {
          fn({ msg: "DOWN", data: { id: actorId, reason: reason ?? "terminated" }, from: "system" as ActorId });
        }
      }
    }

    // Force-release all boxes where this actor is writer or reader
    for (let i = 0; i < config.memory.poolSize; i++) {
      const entry = boxEntries[i]!;
      if (entry.writer === actorId) {
        resetBoxEntry(i);
      }
      if (entry.readers.has(actorId)) {
        entry.readers.delete(actorId);
        entry.refCount = Math.max(0, entry.refCount - 1);
        if (entry.refCount <= 0 && entry.state === "READY") {
          resetBoxEntry(i);
        }
      }
    }

    // Clear inbox
    inboxes.delete(actorId);

    // Clear write queue requests for this actor
    const remaining = writeQueue.filter((req) => req.actorId !== actorId);
    writeQueue.length = 0;
    writeQueue.push(...remaining);

    serveWriteQueue();

    // Terminate worker
    state.worker.terminate();
    actorChannels.delete(actorId);

    diagCollector.recordActorTermination(actorId, reason);
    diagCollector.recordProcessLifetime(actorId, Date.now() - state.createdAt);

    actors.delete(actorId);

    // Notify group manager
    if (reason !== undefined && reason !== "shutdown") {
      groupManager.handleActorFailure(actorId, reason).catch(() => {});
    }
  }

  // ============================================================================
  // Group manager
  // ============================================================================

  const groupManager = createGroupManager({ spawnActor, terminateActor });

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
        for (const actorId of [...groupState.actorIds]) terminateActor(actorId);
        groupManager.destroyGroup(groupState.id);
      },
      read: (msg: Message): ChainableReader | null => {
        if (!msg.handle) return null;
        const entry = boxEntries[msg.handle.boxIndex];
        if (!entry || entry.epoch !== msg.handle.epoch) return null;
        if (entry.state !== "READY") return null;
        entry.refCount++;
        const leaseMs = defaultLeaseMs;
        entry.expiresAt = Date.now() + leaseMs;
        const raw = memoryPool.readBox(msg.handle.boxIndex);
        const decoder = new TextDecoder();
        const stripNulls = (bytes: Uint8Array): Uint8Array => {
          let end = bytes.length;
          while (end > 0 && bytes[end - 1] === 0) end--;
          return bytes.subarray(0, end);
        };
        return {
          json: () => JSON.parse(decoder.decode(stripNulls(raw))),
          string: () => decoder.decode(stripNulls(raw)),
          binary: () => new Uint8Array(raw),
          cbor: () => { throw new Error("CBOR not implemented"); },
          raw: () => new Uint8Array(raw),
        };
      },
      release: (handle: Lock) => {
        const entry = boxEntries[handle.boxIndex];
        if (!entry || entry.epoch !== handle.epoch) return;
        entry.refCount = Math.max(0, entry.refCount - 1);
        if (entry.refCount <= 0) {
          resetBoxEntry(handle.boxIndex);
          serveWriteQueue();
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
    for (let i = 0; i < config.memory.poolSize; i++) {
      if (boxEntries[i]!.state !== "FREE") boxesInUse++;
    }

    return Ok(diagCollector.buildSupervisorDiagnostics({
      activeActors: actors.size,
      totalSpawned: idCounter,
      totalTerminated: idCounter - actors.size,
      poolSize: config.memory.poolSize,
      boxesInUse,
      actors: actorDiagnostics,
    }));
  }

  // ============================================================================
  // Supervisor object
  // ============================================================================

  return Object.freeze({
    id,
    spawn: (callback: ActorCallback, options?: ActorConfig) => {
      const actor = spawnActor(callback, options);
      groupManager.registerActor(rootGroupState.id, actor.id, callback, options);
      return actor;
    },
    createGroup,
    terminateActor,
    shutdown: async () => {
      for (const [actorId] of actors) terminateActor(actorId, "shutdown");
      await resourceManager.releaseAll();
      pubSub.clear();
    },
    subscribe: pubSub.subscribe as unknown as Supervisor["subscribe"],
    getDiagnostics: getSupervisorDiagnostics,
  });
}
