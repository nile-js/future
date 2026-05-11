/**
 * Worker thread entry point for @nilejs/future actors.
 *
 * Receives INIT message with serialized callback, reconstructs it, then
 * listens for SPAWN messages to create ActorSelf + ActorContext and execute
 * the callback. Handles LOCK_GRANTED, RESOURCE_RESPONSE, RESOURCE_ERROR,
 * and TERMINATE messages from the main thread.
 *
 * @module worker-bootstrap
 */

import { parentPort, workerData } from "node:worker_threads";
import { Ok, Err } from "../result";
import { safeTry } from "../safe-try";
import type { Result } from "../result";
import type {
  ActorSelf,
  ActorContext,
  ActorCallback,
  ActorRef,
  FormatUtils,
  Lock,
  WorkerInitPayload,
  WorkerSpawnPayload,
  SupervisorConfig,
  ChildSpawnedMessage,
  ChildSpawnErrorMessage,
  ReadGrantedMessage,
  ReadErrorMessage,
} from "./types";

// ============================================================================
// Guard — must run inside a worker thread
// ============================================================================

if (!parentPort) {
  throw new Error("worker-bootstrap must run inside a worker thread");
}

// ============================================================================
// Internal message types not in shared types
// ============================================================================

/** Main thread confirms lock acquisition */
type LockGrantedMessage = {
  readonly type: "LOCK_GRANTED";
  readonly lock: Lock;
};

/** Discriminated union for all incoming messages we handle */
type IncomingMessage =
  | WorkerInitPayload
  | WorkerSpawnPayload
  | LockGrantedMessage
  | ReadGrantedMessage
  | ReadErrorMessage
  | { readonly type: "TERMINATE" }
  | { readonly type: "RESOURCE_RESPONSE"; readonly id: string; readonly result: unknown }
  | { readonly type: "RESOURCE_ERROR"; readonly id: string; readonly error: string }
  | ChildSpawnedMessage
  | ChildSpawnErrorMessage;

// ============================================================================
// Mutable state — lives for worker lifetime
// ============================================================================

let actorId: string;
let callback: ActorCallback;
let config: Pick<SupervisorConfig, "memory" | "timeouts">;
let isCancelled = false;
let sab: SharedArrayBuffer;

/** Pending resource requests awaiting RESPONSE or ERROR */
const pendingResourceRequests = new Map<
  string,
  {
    resolve: (value: unknown) => void;
    reject: (error: string) => void;
    timer: ReturnType<typeof setTimeout> | null;
  }
>();

/** Pending lock request awaiting LOCK_GRANTED */
let pendingLock: {
  resolve: (lock: Lock) => void;
  reject: (error: string) => void;
} | null = null;

/** Pending read requests awaiting READ_GRANTED or READ_ERROR, keyed by requestId */
const pendingReads = new Map<
  string,
  { resolve: (data: Uint8Array) => void; reject: (error: string) => void }
>();

/** Monotonic counter for unique read request IDs */
let readRequestIdCounter = 0;

/** Pending child spawn requests awaiting CHILD_SPAWNED or CHILD_SPAWN_ERROR */
const pendingChildSpawns = new Map<
  string,
  {
    resolve: (childId: string) => void;
    reject: (err: string) => void;
    timer: ReturnType<typeof setTimeout> | null;
  }
>();

// ============================================================================
// Port helper
// ============================================================================

const port = parentPort!;

/** Post a message to the main thread */
function postToMain(msg: Record<string, unknown>): void {
  port.postMessage(msg);
}

// ============================================================================
// FormatUtils — local utilities, no postMessage needed
// ============================================================================

/** Safely parse JSON string, returning discriminated result instead of throwing */
function safeParseJson(str: string): { readonly ok: true; readonly value: unknown } | { readonly ok: false } {
  try {
    return { ok: true, value: JSON.parse(str) };
  } catch {
    return { ok: false };
  }
}

/** Create FormatUtils instance with TextEncoder/Decoder for encoding ops */
function createFormatUtils(): FormatUtils {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  const encodeString = (str: string): Uint8Array => encoder.encode(str);
  const encodeJson = (obj: unknown): Uint8Array =>
    encoder.encode(JSON.stringify(obj));

  return {
    alloc: (size) => new Uint8Array(size),
    allocU8: (length) => new Uint8Array(length),
    allocI8: (length) => new Int8Array(length),
    allocU16: (length) => new Uint16Array(length),
    allocI16: (length) => new Int16Array(length),
    allocU32: (length) => new Uint32Array(length),
    allocI32: (length) => new Int32Array(length),
    allocU64: (length) => new BigUint64Array(length),
    allocI64: (length) => new BigInt64Array(length),
    allocF32: (length) => new Float32Array(length),
    allocF64: (length) => new Float64Array(length),
    from: (data) => {
      if (data instanceof Uint8Array) return data;
      if (typeof data === "string") return encodeString(data);
      return encodeJson(data);
    },
    encode: (data) => {
      if (data instanceof Uint8Array) return data;
      if (typeof data === "string") return encodeString(data);
      return encodeJson(data);
    },
    decode: (buffer) => {
      const str = decoder.decode(buffer);
      const parsed = safeParseJson(str);
      return parsed.ok ? parsed.value : str;
    },
    json: {
      encode: (obj) => encodeJson(obj),
      decode: (buf) => JSON.parse(decoder.decode(buf)),
    },
    string: {
      encode: (str) => encodeString(str),
      decode: (buf) => decoder.decode(buf),
    },
  };
}

// ============================================================================
// ActorSelf factory
// ============================================================================

/** Create ActorSelf — the actor's identity and send handle */
function createActorSelf(): ActorSelf {
  return {
    id: actorId,
    send: (msg) => postToMain({ type: "SEND", msg }),
  };
}

// ============================================================================
// Resource proxy — intent relay to main thread
// ============================================================================

/**
 * Creates a double-proxy for resource access.
 * `ctx.resources.db.query("SELECT ...")` →
 *   postMessage { type: 'RESOURCE_REQUEST', id, resource: 'db', method: 'query', args: ["SELECT ..."] }
 *   → waits for RESOURCE_RESPONSE or RESOURCE_ERROR
 */
function createResourcesProxy(): ActorContext["resources"] {
  return new Proxy(
    {} as Record<string, Record<string, (...args: unknown[]) => Promise<unknown>>>,
    {
      get: (_target, resource: string) => {
        return new Proxy(
          {} as Record<string, (...args: unknown[]) => Promise<unknown>>,
          {
            get: (_target, method: string) => {
              return (...args: unknown[]) => {
                return new Promise<unknown>((resolve, reject) => {
                  const id = `${resource}.${method}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
                  const timeout = config.timeouts?.defaultLeaseMs ?? 30_000;

                  const timer = setTimeout(() => {
                    pendingResourceRequests.delete(id);
                    reject(`Resource request timeout: ${resource}.${method}`);
                  }, timeout);

                  pendingResourceRequests.set(id, { resolve, reject, timer });

                  postToMain({
                    type: "RESOURCE_REQUEST",
                    id,
                    resource,
                    method,
                    args,
                  });
                });
              };
            },
          },
        );
      },
    },
  );
}

// ============================================================================
// ActorContext factory
// ============================================================================

/** Create ActorContext — the full execution context for an actor callback */
function createActorContext(): ActorContext {
  const fmt = createFormatUtils();
  const resources = createResourcesProxy();

  return {
    heartbeat: () => postToMain({ type: "HEARTBEAT" }),

    terminate: () => {
      postToMain({ type: "TERMINATE" });
      process.exit(0);
    },

    get isCancelled() {
      return isCancelled;
    },

    acquireLock: (): Promise<Result<Lock, string>> => {
      return new Promise<Result<Lock, string>>((resolve) => {
        pendingLock = {
          resolve: (lock) => resolve(Ok(lock)),
          reject: (err) => resolve(Err(err)),
        };
        postToMain({ type: "LOCK_REQUEST" });
      });
    },

    deposit: (lock, data) => {
      const view = new Uint8Array(sab, lock.byteOffset, lock.length);
      view.set(data.subarray(0, lock.length));
      postToMain({ type: "DEPOSIT" });
    },

    done: (lock) => postToMain({ type: "DONE", lock }),

    read: (lock: Lock): Promise<Result<Uint8Array, string>> => {
      return new Promise<Result<Uint8Array, string>>((resolve) => {
        const requestId = `read-${++readRequestIdCounter}-${Date.now()}`;
        pendingReads.set(requestId, {
          resolve: (data) => resolve(Ok(data)),
          reject: (err) => resolve(Err(err)),
        });
        postToMain({ type: "READ_START", lock, requestId });
      });
    },

    resources,

    link: (actor: ActorRef) =>
      postToMain({ type: "LINK", actorId: actor.id }),

    monitor: (actor: ActorRef) =>
      postToMain({ type: "MONITOR", actorId: actor.id }),

    spawn: (callback: ActorCallback): Promise<ActorRef> => {
      return new Promise<ActorRef>((resolve, reject) => {
        const requestId = `child-spawn-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const timeout = config.timeouts?.defaultLeaseMs ?? 5000;

        const timer = setTimeout(() => {
          if (pendingChildSpawns.has(requestId)) {
            pendingChildSpawns.delete(requestId);
            reject("Child spawn timeout");
          }
        }, timeout);

        pendingChildSpawns.set(requestId, {
          resolve: (childId) => resolve(createChildActorRef(childId)),
          reject,
          timer,
        });

        postToMain({
          type: "SPAWN_CHILD",
          requestId,
          callback: callback.toString(),
          config: {},
        });
      });
    },

    fmt,
  };
}

// ============================================================================
// Child ActorRef factory — minimal proxy for spawned children
// ============================================================================

/** Create a minimal ActorRef for a child actor. Full proxy methods require main-thread relay. */
function createChildActorRef(childId: string): ActorRef {
  return {
    id: childId,
    spawn: () => {
      throw new Error("ActorRef.spawn not available in worker context");
    },
    subscribe: () => {
      throw new Error("ActorRef.subscribe not available in worker context");
    },
    terminate: () => postToMain({ type: "TERMINATE_CHILD", childId }),
    read: () => {
      throw new Error("ActorRef.read not available in worker context");
    },
    done: () => {
      throw new Error("ActorRef.done not available in worker context");
    },
    getDiagnostics: () => Err("Diagnostics not available in worker context"),
    link: (other: ActorRef) =>
      postToMain({ type: "LINK", actorId: other.id, fromId: childId }),
    monitor: (other: ActorRef) =>
      postToMain({ type: "MONITOR", actorId: other.id, fromId: childId }),
  };
}

// ============================================================================
// Cleanup — reject all pending requests
// ============================================================================

/** Reject all pending locks, reads, resource requests, and child spawns on termination */
function rejectAllPending(reason: string): void {
  if (pendingLock) {
    pendingLock.reject(reason);
    pendingLock = null;
  }

  for (const [, pending] of pendingReads) {
    pending.reject(reason);
  }
  pendingReads.clear();

  for (const [, pending] of pendingResourceRequests) {
    if (pending.timer) clearTimeout(pending.timer);
    pending.reject(reason);
  }
  pendingResourceRequests.clear();

  for (const [, pending] of pendingChildSpawns) {
    if (pending.timer) clearTimeout(pending.timer);
    pending.reject(reason);
  }
  pendingChildSpawns.clear();
}

// ============================================================================
// Message handler — dispatch by msg.type
// ============================================================================

/** Handle INIT: reconstruct callback, store config */
function handleInit(msg: WorkerInitPayload): void {
  actorId = msg.actorId;
  config = msg.config;
  sab = (workerData as { sab: SharedArrayBuffer }).sab;

  // Validate: reject strings with dangerous patterns to prevent code injection
  const dangerousPatterns = [/\brequire\s*\(/, /\bprocess\b/, /\bglobalThis\b/, /\bimport\s*\(/, /\beval\s*\(/, /\bFunction\s*\(/];
  if (dangerousPatterns.some((p) => p.test(msg.callback))) {
    postToMain({ type: "ERROR", error: "Callback contains potentially dangerous code patterns" });
    process.exit(1);
    return;
  }

  const fn = new Function("return " + msg.callback)() as unknown;
  if (typeof fn !== "function") {
    postToMain({
      type: "ERROR",
      error: "Invalid callback: reconstructed value is not a function",
    });
    process.exit(1);
    return;
  }

  callback = fn as ActorCallback;
}

/** Handle SPAWN: create self + ctx, run callback, catch errors */
async function handleSpawn(msg: WorkerSpawnPayload): Promise<void> {
  const self = createActorSelf();
  const ctx = createActorContext();

  const result = await safeTry(() => callback(self, msg.data, ctx));
  if (result.isErr) {
    postToMain({ type: "ERROR", error: result.error });
  }
}

/** Handle TERMINATE from main: set cancelled flag, clean up, exit */
function handleTerminate(): void {
  isCancelled = true;
  rejectAllPending("Actor terminated by main thread");
  process.exit(0);
}

/** Handle LOCK_GRANTED: resolve pending lock request */
function handleLockGranted(msg: LockGrantedMessage): void {
  if (pendingLock) {
    pendingLock.resolve(msg.lock);
    pendingLock = null;
  }
}

/** Handle READ_GRANTED: resolve pending read request with data from SAB */
function handleReadGranted(msg: ReadGrantedMessage): void {
  const pending = pendingReads.get(msg.requestId);
  if (!pending) return;

  const view = new Uint8Array(sab, msg.lock.byteOffset, msg.lock.length);
  const data = new Uint8Array(view);
  pending.resolve(data);
  pendingReads.delete(msg.requestId);
}

/** Handle READ_ERROR: reject pending read request */
function handleReadError(msg: ReadErrorMessage): void {
  const pending = pendingReads.get(msg.requestId);
  if (!pending) return;

  pending.reject(msg.error);
  pendingReads.delete(msg.requestId);
}

/** Handle RESOURCE_RESPONSE: resolve pending resource request */
function handleResourceResponse(id: string, result: unknown): void {
  const pending = pendingResourceRequests.get(id);
  if (!pending) return;

  if (pending.timer) clearTimeout(pending.timer);
  pending.resolve(result);
  pendingResourceRequests.delete(id);
}

/** Handle RESOURCE_ERROR: reject pending resource request */
function handleResourceError(id: string, error: string): void {
  const pending = pendingResourceRequests.get(id);
  if (!pending) return;

  if (pending.timer) clearTimeout(pending.timer);
  pending.reject(error);
  pendingResourceRequests.delete(id);
}

/** Handle CHILD_SPAWNED: resolve pending child spawn request */
function handleChildSpawned(msg: ChildSpawnedMessage): void {
  const pending = pendingChildSpawns.get(msg.requestId);
  if (!pending) return;

  if (pending.timer) clearTimeout(pending.timer);
  pending.resolve(msg.childId);
  pendingChildSpawns.delete(msg.requestId);
}

/** Handle CHILD_SPAWN_ERROR: reject pending child spawn request */
function handleChildSpawnError(msg: ChildSpawnErrorMessage): void {
  const pending = pendingChildSpawns.get(msg.requestId);
  if (!pending) return;

  if (pending.timer) clearTimeout(pending.timer);
  pending.reject(msg.error);
  pendingChildSpawns.delete(msg.requestId);
}

// ============================================================================
// Main message dispatch
// ============================================================================

/** Message type → handler map for incoming main-to-worker messages */
const incomingHandlers: Record<string, (msg: IncomingMessage) => void | Promise<void>> = {
  INIT: (msg) => handleInit(msg as WorkerInitPayload),
  SPAWN: (msg) => handleSpawn(msg as WorkerSpawnPayload),
  TERMINATE: () => handleTerminate(),
  LOCK_GRANTED: (msg) => handleLockGranted(msg as LockGrantedMessage),
  READ_GRANTED: (msg) => handleReadGranted(msg as ReadGrantedMessage),
  READ_ERROR: (msg) => handleReadError(msg as ReadErrorMessage),
  RESOURCE_RESPONSE: (msg) => handleResourceResponse((msg as any).id, (msg as any).result),
  RESOURCE_ERROR: (msg) => handleResourceError((msg as any).id, (msg as any).error),
  CHILD_SPAWNED: (msg) => handleChildSpawned(msg as ChildSpawnedMessage),
  CHILD_SPAWN_ERROR: (msg) => handleChildSpawnError(msg as ChildSpawnErrorMessage),
};

/**
 * Dispatch incoming messages by type.
 * INIT must arrive before SPAWN. Other messages handled independently.
 */
function handleMessage(raw: unknown): void {
  const msg = raw as IncomingMessage;
  const handler = incomingHandlers[msg.type];
  if (handler) {
    const result = handler(msg);
    if (result instanceof Promise) {
      result.catch(() => {});
    }
  }
}

// ============================================================================
// Bootstrap — attach listener
// ============================================================================

port.on("message", handleMessage);
