import type { Result } from "../result";

/** Unique actor identifier — string for debuggability and hash-map keys */
export type ActorId = string;
/** Serialization format for shared-memory messages — determines encode/decode path */
export type FmtType = "json" | "string" | "binary" | "cbor";
/** Lock on a shared-memory box. Epoch prevents use-after-free on recycled boxes. */
export type Lock = { readonly boxIndex: number; readonly epoch: number };
/**
 * Share config — controls which actors can read a written message.
 * "owner" = writer + main-thread subscribers (default). "group" = same supervision group.
 * "linked" = actors linked to writer. Array = explicit allowlist.
 */
export type ShareConfig = "owner" | "group" | "linked" | readonly ActorId[];

/** BoxEntry — supervisor-side metadata per box. State machine: FREE → WRITING → READY → FREE */
export type BoxEntry = {
  state: "FREE" | "WRITING" | "READY";
  from: ActorId; msg: string; type: FmtType; share: ShareConfig;
  refCount: number; expiresAt: number; writer: ActorId | null;
  readers: Set<ActorId>; epoch: number;
};
/** InboxEntry — per-actor inbox item. Handle lets actor read SAB payload via ctx.read(). */
export type InboxEntry = { readonly handle: Lock; readonly from: ActorId; readonly msg: string; readonly type: FmtType };

/**
 * Message — unified envelope for Tier 1 (inline JSON) and Tier 2 (SAB reference).
 * Tier 1: data present, handle absent. Tier 2: handle present, data absent.
 * Supervisor auto-injects `from`; sender never sets it.
 */
export type Message = {
  readonly msg: string; readonly type?: FmtType; readonly data?: unknown;
  readonly handle?: Lock; readonly from: ActorId;
};

/** ActorSelf — minimal self-reference. `send` emits Tier 1 signal to subscribers. */
export type ActorSelf = { readonly send: (msg: string, data?: unknown) => void; readonly id: ActorId };
/** ChainableReader — lazy decoder. `raw()` returns zero-copy SAB view (main thread only). */
export type ChainableReader = {
  readonly json: () => unknown; readonly string: () => string;
  readonly binary: () => Uint8Array; readonly cbor: () => unknown; readonly raw: () => Uint8Array;
};

/** FormatUtils — encoding/decoding on ctx.fmt. Typed alloc sub-methods allocate SAB-backed views. */
export type FormatUtils = {
  readonly from: (data: string | object | Uint8Array) => Uint8Array;
  readonly encode: (data: unknown) => Uint8Array;
  readonly decode: (buffer: Uint8Array) => unknown;
  readonly json: { readonly encode: (obj: unknown) => Uint8Array; readonly decode: (buf: Uint8Array) => unknown };
  readonly string: { readonly encode: (str: string) => Uint8Array; readonly decode: (buf: Uint8Array) => string };
  readonly cbor: { readonly encode: (data: unknown) => Uint8Array; readonly decode: (buf: Uint8Array) => unknown };
  readonly alloc: ((size: number) => Uint8Array) & {
    readonly u8: (length: number) => Uint8Array; readonly i8: (length: number) => Int8Array;
    readonly u16: (length: number) => Uint16Array; readonly i16: (length: number) => Int16Array;
    readonly u32: (length: number) => Uint32Array; readonly i32: (length: number) => Int32Array;
    readonly u64: (length: number) => BigUint64Array; readonly i64: (length: number) => BigInt64Array;
    readonly f32: (length: number) => Float32Array; readonly f64: (length: number) => Float64Array;
  };
};

/**
 * ActorContext — actor's gateway to supervisor. `write` acquires box + encodes data.
 * `read` decodes a Message's handle into ChainableReader (null for Tier 1).
 * `release` frees a box when done reading.
 */
export type ActorContext = {
  readonly write: (params: {
    readonly msg: string; readonly type: FmtType; readonly data: Uint8Array; readonly share?: ShareConfig;
  }) => Promise<Result<Lock, string>>;
  readonly read: (msg: Message) => ChainableReader | null;
  readonly release: (handle: Lock) => void;
  readonly heartbeat: () => void;
  readonly resources: Record<string, Record<string, (...args: unknown[]) => Promise<unknown>>>;
  readonly link: (actor: ActorRef) => void;
  readonly monitor: (actor: ActorRef) => void;
  readonly spawn: (callback: ActorCallback) => Promise<ActorRef>;
  readonly terminate: () => void;
  readonly isCancelled: boolean;
  readonly fmt: FormatUtils;
};

/** Actor callback — serialized to worker, no outer scope. */
export type ActorCallback = (self: ActorSelf, msg: unknown, ctx: ActorContext) => void | Promise<void>;

/** ActorRef — main-thread handle to a running actor. read/release are synchronous. */
export type ActorRef = {
  readonly id: ActorId;
  readonly spawn: (msg: unknown) => void;
  readonly subscribe: (fn: (msg: Message) => void) => () => void;
  readonly terminate: () => void;
  readonly read: (msg: Message) => ChainableReader | null;
  readonly release: (handle: Lock) => void;
  readonly getDiagnostics: () => Result<ActorDiagnostics, string>;
  readonly link: (other: ActorRef) => void;
  readonly monitor: (other: ActorRef) => void;
};

/** Per-actor config — optional overrides for timeout and naming. */
export type ActorConfig = { readonly name?: string; readonly timeoutMs?: number };

/** Strategy — controls failure blast radius: one-for-one, one-for-all, rest-for-one */
export type SupervisionStrategy = "one-for-one" | "one-for-all" | "rest-for-one";
/** Retry backoff — prevents thundering herd on rapid failures. */
export type RetryConfig = { readonly max: number; readonly backoff: "exponential" | "linear" | "fixed"; readonly delayMs?: number };
/** Actor group config — isolates supervision policies per group. */
export type ActorGroupConfig = { readonly strategy: SupervisionStrategy; readonly retry?: RetryConfig };
/** Actor group — scoped spawn/terminate with shared supervision policy. */
export type ActorGroup = {
  readonly id: string;
  readonly spawn: (callback: ActorCallback, options?: ActorConfig) => ActorRef;
  readonly terminateAll: () => void;
  readonly read: (msg: Message) => ChainableReader | null;
  readonly release: (handle: Lock) => void;
};

/** Resource method config — Zod schemas enforce contract at boundaries. */
export type ResourceMethodConfig = {
  readonly input: import("zod").ZodTypeAny;
  readonly output: import("zod").ZodTypeAny;
  readonly handler: (args: unknown) => unknown | Promise<unknown>;
};
/**
 * Resource config — method configs or cleanup functions. `release` is a top-level
 * cleanup hook called on actor termination, distinct from method-level cleanup.
 */
export type ResourceConfig = {
  readonly [method: string]: ResourceMethodConfig | (() => void | Promise<void>) | undefined;
  readonly release?: () => void | Promise<void>;
};
/** Resources config — centralizes resource definitions for the supervisor. */
export type ResourcesConfig = { readonly [resourceName: string]: ResourceConfig };

/** Memory config — poolSize/boxSize tradeoff: concurrency vs memory overhead. */
export type MemoryConfig = { readonly poolSize: number; readonly boxSize: string | number };
/** Timeout config — lease prevents indefinite resource holding. */
export type TimeoutConfig = { readonly defaultLeaseMs: number; readonly actorTimeouts?: Record<string, number> };
/** Diagnostics config — zero-cost when disabled. sampleRate controls overhead. */
export type DiagnosticsConfig = {
  readonly enabled: boolean; readonly sampleRate?: number;
  readonly track?: {
    readonly actorLifetimes?: boolean; readonly startTimes?: boolean;
    readonly processLifetimes?: boolean; readonly writeQueueWait?: boolean;
    readonly writeQueueDepth?: boolean; readonly messageLatency?: boolean;
    readonly bufferUtilization?: boolean; readonly heartbeatIntervals?: boolean;
    readonly resourceCallLatency?: boolean; readonly authorizationEvents?: boolean;
    readonly inboxDepth?: boolean; readonly refCountHistory?: boolean;
  };
};
/** Supervisor config — defaultCodec and codecs allow custom serialization beyond built-ins. */
export type SupervisorConfig = {
  readonly maxActors: number; readonly memory: MemoryConfig;
  readonly resources?: ResourcesConfig; readonly timeouts?: TimeoutConfig;
  readonly strategy?: SupervisionStrategy; readonly retry?: RetryConfig;
  readonly diagnostics?: DiagnosticsConfig;
  readonly defaultCodec?: FmtType;
  readonly codecs?: Record<string, { readonly encode: (data: unknown) => Uint8Array; readonly decode: (buf: Uint8Array) => unknown }>;
};

/** Supervisor — owns all actor refs, manages lifecycle, aggregates diagnostics. */
export type Supervisor = {
  readonly id: string;
  readonly spawn: (callback: ActorCallback, options?: ActorConfig) => ActorRef;
  readonly createGroup: (config: ActorGroupConfig) => ActorGroup;
  readonly terminateActor: (id: ActorId) => void;
  readonly shutdown: () => Promise<void>;
  readonly subscribe: (fn: (msg: Message) => void) => () => void;
  readonly getDiagnostics: () => Result<SupervisorDiagnostics, string>;
};

/** Actor diagnostics — lifetime, heartbeats, message count, termination reason. */
export type ActorDiagnostics = {
  readonly id: ActorId; readonly lifetimeMs: number; readonly heartbeatCount: number;
  readonly lastHeartbeatAt: number; readonly messageCount: number; readonly terminationReason?: string;
};
/** Supervisor diagnostics — aggregate system health, memory pool, and optional detailed metrics. */
export type SupervisorDiagnostics = {
  readonly activeActors: number; readonly totalActorsSpawned: number; readonly totalActorsTerminated: number;
  readonly memoryPool: { readonly poolSize: number; readonly boxesInUse: number; readonly utilization: number };
  readonly actors: readonly ActorDiagnostics[];
  readonly writeQueueWait?: { readonly avgMs: number; readonly maxMs: number; readonly totalWaits: number };
  readonly writeQueueDepth?: { readonly avgDepth: number; readonly maxDepth: number; readonly totalSamples: number };
  readonly authorization?: { readonly granted: number; readonly denied: number };
  readonly inboxDepth?: { readonly maxDepth: number; readonly avgDepth: number; readonly perActor: ReadonlyArray<[ActorId, number]> };
  readonly refCounts?: { readonly avgRefCount: number; readonly maxRefCount: number; readonly samples: ReadonlyArray<{ readonly boxIndex: number; readonly refCount: number }> };
  readonly processLifetimes?: { readonly avgMs: number; readonly maxMs: number; readonly perActor: ReadonlyArray<[ActorId, number]> };
};

// Worker control messages
/** Worker init payload — sent once when worker thread starts. */
export type WorkerInitPayload = {
  readonly type: "INIT"; readonly callback: string;
  readonly config: Pick<SupervisorConfig, "memory" | "timeouts">; readonly actorId: ActorId;
};
/** Worker spawn payload — delivers initial message to actor callback. */
export type WorkerSpawnPayload = { readonly type: "SPAWN"; readonly data: unknown };
/** Worker control messages — lifecycle management (INIT, SPAWN, TERMINATE). */
export type WorkerControlMessage = WorkerInitPayload | WorkerSpawnPayload | { readonly type: "TERMINATE" };

// Worker → Main messages
/** Worker sends Tier 1 signal/status message to subscribers. */
export type WorkerSendMessage = { readonly type: "SEND"; readonly msg: string; readonly data?: unknown };
/** Worker requests shared-memory box for Tier 2 data. `fmt` is the serialization format. */
export type WriteRequestMessage = { readonly type: "WRITE_REQUEST"; readonly msg: string; readonly fmt: FmtType; readonly data: Uint8Array; readonly share: ShareConfig };
/** Worker commits written data — makes box available to readers. */
export type CommitMessage = { readonly type: "COMMIT"; readonly lock: Lock };
/** Worker releases a read lock on a shared-memory box. */
export type ReleaseMessage = { readonly type: "RELEASE"; readonly lock: Lock };
/** Worker invokes a resource method on the main thread. */
export type ResourceRequestMessage = { readonly type: "RESOURCE_REQUEST"; readonly id: string; readonly resource: string; readonly method: string; readonly args: unknown };
/** Worker requests bi-directional link to another actor. */
export type LinkMessage = { readonly type: "LINK"; readonly actorId: ActorId };
/** Worker requests uni-directional monitoring of another actor. */
export type MonitorMessage = { readonly type: "MONITOR"; readonly actorId: ActorId };
/** Worker requests spawning a child actor. */
export type SpawnChildMessage = { readonly type: "SPAWN_CHILD"; readonly requestId: string; readonly callback: string; readonly config?: ActorConfig };
/** Worker requests termination of a child actor. */
export type TerminateChildMessage = { readonly type: "TERMINATE_CHILD"; readonly childId: ActorId };
/** Worker reports an error to the supervisor. */
export type WorkerErrorMessage = { readonly type: "ERROR"; readonly error: string };
/** Worker signals resource release (cleanup hook). */
export type ResourceReleaseMessage = { readonly type: "RESOURCE_RELEASE" };
/** Worker heartbeat — resets lease timer during CPU-intensive loops. */
export type HeartbeatMessage = { readonly type: "HEARTBEAT" };

// Main → Worker messages
/** Supervisor grants write lock for a shared-memory box. */
export type WriteGrantedMessage = { readonly type: "WRITE_GRANTED"; readonly lock: Lock };
/** Supervisor delivers a message to the worker's inbox. `fmt` is the serialization format. */
export type InboxMessage = { readonly type: "INBOX"; readonly handle: Lock; readonly from: ActorId; readonly msg: string; readonly fmt: FmtType };
/** Supervisor returns successful resource call result. */
export type ResourceResponseMessage = { readonly type: "RESOURCE_RESPONSE"; readonly id: string; readonly result: unknown };
/** Supervisor reports resource call failure. */
export type ResourceErrorMessage = { readonly type: "RESOURCE_ERROR"; readonly id: string; readonly error: string };
/** Supervisor confirms child actor was spawned. */
export type ChildSpawnedMessage = { readonly type: "CHILD_SPAWNED"; readonly requestId: string; readonly childId: ActorId };
/** Supervisor reports child actor spawn failure. */
export type ChildSpawnErrorMessage = { readonly type: "CHILD_SPAWN_ERROR"; readonly requestId: string; readonly error: string };

// Protocol unions
/** All messages sent from worker to main thread. */
export type WorkerToMainMessage =
  | WorkerSendMessage | WriteRequestMessage | CommitMessage | ReleaseMessage
  | ResourceRequestMessage | LinkMessage | MonitorMessage | SpawnChildMessage
  | TerminateChildMessage | WorkerErrorMessage | ResourceReleaseMessage | HeartbeatMessage;
/** All messages sent from main thread to worker. */
export type MainToWorkerMessage =
  | WorkerInitPayload | WorkerSpawnPayload | { readonly type: "TERMINATE" }
  | WriteGrantedMessage | InboxMessage | ResourceResponseMessage | ResourceErrorMessage
  | ChildSpawnedMessage | ChildSpawnErrorMessage | HeartbeatMessage;

// Internal state
/** Internal actor state — mutable fields for lifecycle bookkeeping. */
export type InternalActorState = {
  readonly id: ActorId; readonly worker: import("node:worker_threads").Worker;
  readonly config: ActorConfig; readonly createdAt: number;
  heartbeats: number; lastHeartbeatAt: number; messagesSent: number;
  readonly subscribers: Set<(msg: Message) => void>;
  readonly linkedActors: Set<ActorId>; readonly monitoredActors: Set<ActorId>;
  terminated: boolean; terminationReason?: string;
};
/** Pending resource request — bridges async worker request to main-thread handler. */
export type PendingResourceRequest = {
  readonly resolve: (value: unknown) => void; readonly reject: (error: string) => void;
  readonly timer: ReturnType<typeof setTimeout> | null;
};
/** Internal group state tracked by supervisor. */
export type GroupState = {
  readonly id: string; readonly config: ActorGroupConfig; readonly actorIds: ActorId[];
  readonly callbacks: Map<ActorId, { readonly callback: ActorCallback; readonly config?: ActorConfig }>;
  readonly restartState: { restarts: number[] };
};

/** Diagnostics collector — abstracts metric backend (console, Prometheus, OTEL, etc.). */
export type DiagnosticsCollector = {
  /** Record actor start time for lifetime tracking */
  readonly recordActorStart: (id: ActorId) => void;
  /** Record actor heartbeat for interval tracking */
  readonly recordActorHeartbeat: (id: ActorId) => void;
  /** Record actor message send for latency tracking */
  readonly recordActorMessage: (id: ActorId) => void;
  /** Record actor termination with optional reason */
  readonly recordActorTermination: (id: ActorId, reason?: string) => void;
  /** Record write queue wait time — measures how long pending writes waited */
  readonly recordWriteQueueWait: (waitMs: number) => void;
  /** Record current write queue depth — tracks pending write requests */
  readonly recordWriteQueueDepth: (depth: number) => void;
  /** Record authorization event — tracks granted vs denied reads */
  readonly recordAuthorizationEvent: (granted: boolean) => void;
  /** Record per-actor inbox depth — tracks inbox queue sizes */
  readonly recordInboxDepth: (actorId: ActorId, depth: number) => void;
  /** Record box ref count — tracks reference counts for memory boxes */
  readonly recordRefCount: (boxIndex: number, refCount: number) => void;
  /** Record process lifetime — tracks worker thread uptime */
  readonly recordProcessLifetime: (actorId: ActorId, durationMs: number) => void;
  /** Record resource call duration — measures handler execution time */
  readonly recordResourceCall: (durationMs: number) => void;
  /** Build per-actor diagnostics snapshot from tracked data and state fallbacks */
  readonly buildActorDiagnostics: (id: ActorId, state: InternalActorState) => ActorDiagnostics;
  /** Build supervisor-level diagnostics snapshot with aggregate metrics */
  readonly buildSupervisorDiagnostics: (options: {
    readonly activeActors: number; readonly totalSpawned: number; readonly totalTerminated: number;
    readonly poolSize: number; readonly boxesInUse: number; readonly actors: readonly ActorDiagnostics[];
  }) => SupervisorDiagnostics;
};