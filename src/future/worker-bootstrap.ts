/**
 * Worker thread entry point for @nilejs/future actors.
 * Receives INIT → reconstructs callback. SPAWN → runs callback with self+ctx.
 * Handles WRITE_GRANTED, INBOX, RESOURCE_*, CHILD_*, TERMINATE from main thread.
 * @module worker-bootstrap
 */

import { parentPort, workerData } from "node:worker_threads";
import { Ok, Err, safeTry } from "slang-ts";
import type { Result } from "slang-ts";
import type {
  ActorSelf, ActorContext, ActorCallback, ActorRef, FormatUtils,
  Lock, Message, WorkerInitPayload, WorkerSpawnPayload, SupervisorConfig,
  ChildSpawnedMessage, ChildSpawnErrorMessage, InboxMessage, WriteGrantedMessage,
} from "./types";

if (!parentPort) throw new Error("worker-bootstrap must run inside a worker thread");

// --- Internal types ---

type IncomingMessage =
  | WorkerInitPayload | WorkerSpawnPayload | WriteGrantedMessage | InboxMessage
  | { readonly type: "TERMINATE" }
  | { readonly type: "RESOURCE_RESPONSE"; readonly id: string; readonly result: unknown }
  | { readonly type: "RESOURCE_ERROR"; readonly id: string; readonly error: string }
  | ChildSpawnedMessage | ChildSpawnErrorMessage;

type PendingEntry = {
  resolve: (value: unknown) => void;
  reject: (error: string) => void;
  timer: ReturnType<typeof setTimeout> | null;
};

// --- Mutable state ---

let actorId: string;
let callback: ActorCallback;
let config: Pick<SupervisorConfig, "memory" | "timeouts">;
let isCancelled = false;
let sab: SharedArrayBuffer;

const pendingResourceRequests = new Map<string, PendingEntry>();
let pendingWrite: { resolve: (lock: Lock) => void; reject: (error: string) => void } | null = null;
const pendingChildSpawns = new Map<string, PendingEntry>();

// --- Port helper ---

const port = parentPort!;

function postToMain(msg: Record<string, unknown>): void {
  port.postMessage(msg);
}

// --- FormatUtils ---

function safeParseJson(str: string): { readonly ok: true; readonly value: unknown } | { readonly ok: false } {
  try { return { ok: true, value: JSON.parse(str) }; }
  catch { return { ok: false }; }
}

function createFormatUtils(): FormatUtils {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const encodeString = (str: string): Uint8Array => encoder.encode(str);
  const encodeJson = (obj: unknown): Uint8Array => encoder.encode(JSON.stringify(obj));

  const allocBase = (size: number): Uint8Array => new Uint8Array(size);
  const alloc = Object.assign(allocBase, {
    u8: (length: number) => new Uint8Array(length),
    i8: (length: number) => new Int8Array(length),
    u16: (length: number) => new Uint16Array(length),
    i16: (length: number) => new Int16Array(length),
    u32: (length: number) => new Uint32Array(length),
    i32: (length: number) => new Int32Array(length),
    u64: (length: number) => new BigUint64Array(length),
    i64: (length: number) => new BigInt64Array(length),
    f32: (length: number) => new Float32Array(length),
    f64: (length: number) => new Float64Array(length),
  });

  const coerce = (data: unknown): Uint8Array => {
    if (data instanceof Uint8Array) return data;
    if (typeof data === "string") return encodeString(data);
    return encodeJson(data);
  };

  return {
    alloc,
    from: coerce,
    encode: coerce,
    decode: (buffer) => {
      const str = decoder.decode(buffer);
      const parsed = safeParseJson(str);
      return parsed.ok ? parsed.value : str;
    },
    json: { encode: (obj) => encodeJson(obj), decode: (buf) => JSON.parse(decoder.decode(buf)) },
    string: { encode: (str) => encodeString(str), decode: (buf) => decoder.decode(buf) },
    cbor: {
      encode: () => { throw new Error("CBOR codec not configured"); },
      decode: () => { throw new Error("CBOR codec not configured"); },
    },
  };
}

// --- ActorSelf ---

function createActorSelf(): ActorSelf {
  return {
    id: actorId,
    send: (msg, data) => postToMain({ type: "SEND", msg, data }),
  };
}

// --- Resource proxy ---

function createResourcesProxy(): ActorContext["resources"] {
  return new Proxy(
    {} as Record<string, Record<string, (...args: unknown[]) => Promise<unknown>>>,
    {
      get: (_t, resource: string) => new Proxy(
        {} as Record<string, (...args: unknown[]) => Promise<unknown>>,
        {
          get: (_t, method: string) => (...args: unknown[]) =>
            new Promise<unknown>((resolve, reject) => {
              const id = `${resource}.${method}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
              const timeout = config.timeouts?.defaultLeaseMs ?? 30_000;
              const timer = setTimeout(() => {
                pendingResourceRequests.delete(id);
                reject(`Resource request timeout: ${resource}.${method}`);
              }, timeout);
              pendingResourceRequests.set(id, { resolve, reject, timer });
              postToMain({ type: "RESOURCE_REQUEST", id, resource, method, args: args[0] });
            }),
        },
      ),
    },
  );
}

// --- ActorContext ---

function createActorContext(): ActorContext {
  const fmt = createFormatUtils();
  const resources = createResourcesProxy();
  const decoder = new TextDecoder();

  return {
    heartbeat: () => postToMain({ type: "HEARTBEAT" }),
    terminate: () => { postToMain({ type: "TERMINATE" }); process.exit(0); },
    get isCancelled() { return isCancelled; },

    /** Acquire box → copy data → commit. WRITE_REQUEST → WRITE_GRANTED → COMMIT. */
    write: (params) => new Promise<Result<Lock, string>>((resolve) => {
      const { msg, type, data, share = "owner" } = params;
      pendingWrite = {
        resolve: (lock) => {
          const boxSize = Number(config.memory.boxSize);
          const offset = lock.boxIndex * boxSize;
          const view = new Uint8Array(sab, offset, Math.min(data.length, boxSize));
          view.set(data.subarray(0, boxSize));
          postToMain({ type: "COMMIT", lock });
          resolve(Ok(lock));
        },
        reject: (err) => resolve(Err(err)),
      };
      postToMain({ type: "WRITE_REQUEST", msg, fmt: type, data, share });
    }),

    /** Sync read from SAB. Returns null for Tier 1 (no handle). */
    read: (msg: Message) => {
      if (!msg.handle) return null;
      const boxSize = Number(config.memory.boxSize);
      const offset = msg.handle.boxIndex * boxSize;
      const view = new Uint8Array(sab, offset, boxSize);
      /** Strip trailing null bytes for text/JSON decoding (SAB is zero-initialized) */
      const stripNulls = (bytes: Uint8Array): Uint8Array => {
        let end = bytes.length;
        while (end > 0 && bytes[end - 1] === 0) end--;
        return bytes.subarray(0, end);
      };
      return {
        json: () => JSON.parse(decoder.decode(stripNulls(view))),
        string: () => decoder.decode(stripNulls(view)),
        binary: () => new Uint8Array(view),
        cbor: () => { throw new Error("CBOR not implemented"); },
        raw: () => new Uint8Array(view),
      };
    },

    release: (handle) => postToMain({ type: "RELEASE", lock: handle }),
    resources,
    link: (actor: ActorRef) => postToMain({ type: "LINK", actorId: actor.id }),
    monitor: (actor: ActorRef) => postToMain({ type: "MONITOR", actorId: actor.id }),

    spawn: (childCallback: ActorCallback): Promise<ActorRef> =>
      new Promise<ActorRef>((resolve, reject) => {
        const requestId = `child-spawn-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const timeout = config.timeouts?.defaultLeaseMs ?? 5000;
        const timer = setTimeout(() => {
          if (pendingChildSpawns.has(requestId)) {
            pendingChildSpawns.delete(requestId);
            reject("Child spawn timeout");
          }
        }, timeout);
        pendingChildSpawns.set(requestId, {
          resolve: (childId) => resolve(createChildActorRef(childId as string)),
          reject, timer,
        });
        postToMain({ type: "SPAWN_CHILD", requestId, callback: childCallback.toString(), config: {} });
      }),

    fmt,
  };
}

// --- Child ActorRef ---

function createChildActorRef(childId: string): ActorRef {
  const throwCtx = () => { throw new Error("Not available in worker context"); };
  return {
    id: childId,
    receive: throwCtx,
    subscribe: throwCtx,
    terminate: () => postToMain({ type: "TERMINATE_CHILD", childId }),
    read: throwCtx,
    release: throwCtx,
    getDiagnostics: () => Err("Diagnostics not available in worker context"),
    link: (other: ActorRef) => postToMain({ type: "LINK", actorId: other.id, fromId: childId }),
    monitor: (other: ActorRef) => postToMain({ type: "MONITOR", actorId: other.id, fromId: childId }),
  };
}

// --- Cleanup ---

function rejectAllPending(reason: string): void {
  if (pendingWrite) { pendingWrite.reject(reason); pendingWrite = null; }
  for (const [, p] of pendingResourceRequests) { if (p.timer) clearTimeout(p.timer); p.reject(reason); }
  pendingResourceRequests.clear();
  for (const [, p] of pendingChildSpawns) { if (p.timer) clearTimeout(p.timer); p.reject(reason); }
  pendingChildSpawns.clear();
}

// --- Message handlers ---

function handleInit(msg: WorkerInitPayload): void {
  actorId = msg.actorId;
  config = msg.config;
  sab = (workerData as { sab: SharedArrayBuffer }).sab;

  const dangerous = [/\brequire\s*\(/, /\bprocess\b/, /\bglobalThis\b/, /\bimport\s*\(/, /\beval\s*\(/, /\bFunction\s*\(/];
  if (dangerous.some((p) => p.test(msg.callback))) {
    postToMain({ type: "ERROR", error: "Callback contains potentially dangerous code patterns" });
    process.exit(1);
    return;
  }

  const fn = new Function("return " + msg.callback)() as unknown;
  if (typeof fn !== "function") {
    postToMain({ type: "ERROR", error: "Invalid callback: reconstructed value is not a function" });
    process.exit(1);
    return;
  }
  callback = fn as ActorCallback;
}

async function handleSpawn(msg: WorkerSpawnPayload): Promise<void> {
  const self = createActorSelf();
  const ctx = createActorContext();
  const result = await safeTry(() => callback(self, msg.data, ctx));
  if (result.isErr) postToMain({ type: "ERROR", error: result.error });
}

function handleTerminate(): void {
  isCancelled = true;
  rejectAllPending("Actor terminated by main thread");
  process.exit(0);
}

function handleWriteGranted(msg: WriteGrantedMessage): void {
  if (pendingWrite) { pendingWrite.resolve(msg.lock); pendingWrite = null; }
}

function handleInbox(msg: InboxMessage): void {
  const self = createActorSelf();
  const ctx = createActorContext();
  const message: Message = { msg: msg.msg, type: msg.fmt, handle: msg.handle, from: msg.from };
  safeTry(() => callback(self, message, ctx)).then((result) => {
    if (result.isErr) postToMain({ type: "ERROR", error: result.error });
  });
}

function handleResourceResponse(id: string, result: unknown): void {
  const pending = pendingResourceRequests.get(id);
  if (!pending) return;
  if (pending.timer) clearTimeout(pending.timer);
  pending.resolve(result);
  pendingResourceRequests.delete(id);
}

function handleResourceError(id: string, error: string): void {
  const pending = pendingResourceRequests.get(id);
  if (!pending) return;
  if (pending.timer) clearTimeout(pending.timer);
  pending.reject(error);
  pendingResourceRequests.delete(id);
}

function handleChildSpawned(msg: ChildSpawnedMessage): void {
  const pending = pendingChildSpawns.get(msg.requestId);
  if (!pending) return;
  if (pending.timer) clearTimeout(pending.timer);
  pending.resolve(msg.childId);
  pendingChildSpawns.delete(msg.requestId);
}

function handleChildSpawnError(msg: ChildSpawnErrorMessage): void {
  const pending = pendingChildSpawns.get(msg.requestId);
  if (!pending) return;
  if (pending.timer) clearTimeout(pending.timer);
  pending.reject(msg.error);
  pendingChildSpawns.delete(msg.requestId);
}

// --- Dispatch ---

const incomingHandlers: Record<string, (msg: IncomingMessage) => void | Promise<void>> = {
  INIT: (msg) => handleInit(msg as WorkerInitPayload),
  SPAWN: (msg) => handleSpawn(msg as WorkerSpawnPayload),
  TERMINATE: () => handleTerminate(),
  WRITE_GRANTED: (msg) => handleWriteGranted(msg as WriteGrantedMessage),
  INBOX: (msg) => handleInbox(msg as InboxMessage),
  RESOURCE_RESPONSE: (msg) => {
    const m = msg as { readonly id: string; readonly result: unknown };
    handleResourceResponse(m.id, m.result);
  },
  RESOURCE_ERROR: (msg) => {
    const m = msg as { readonly id: string; readonly error: string };
    handleResourceError(m.id, m.error);
  },
  CHILD_SPAWNED: (msg) => handleChildSpawned(msg as ChildSpawnedMessage),
  CHILD_SPAWN_ERROR: (msg) => handleChildSpawnError(msg as ChildSpawnErrorMessage),
};

function handleMessage(raw: unknown): void {
  const msg = raw as IncomingMessage;
  const handler = incomingHandlers[msg.type];
  if (handler) {
    const result = handler(msg);
    if (result instanceof Promise) result.catch(() => {});
  }
}

port.on("message", handleMessage);
