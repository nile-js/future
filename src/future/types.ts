import type { Result } from "../result";

// ============================================================================
// Core Primitives
// ============================================================================

/** Unique actor identifier */
export type ActorId = string;

/** Memory box lock — represents acquired shared memory */
export type Lock = {
  readonly boxIndex: number;
  readonly byteOffset: number;
  readonly length: number;
};

/** Box state values */
export type BoxState = 0 | 1 | 2 | 3;
/** Box state constant — CLEAN (0). A box ready for acquisition. */
export const BOX_CLEAN: BoxState = 0;
/** Box state constant — LOCKED (1). A box currently being written to. */
export const BOX_LOCKED: BoxState = 1;
/** Box state constant — READY (2). A box with data ready for reading. */
export const BOX_READY: BoxState = 2;
/** Box state constant — READING (3). A box being read by one or more workers. */
export const BOX_READING: BoxState = 3;

// ============================================================================
// Actor Self — control surface available inside actor callback
// ============================================================================

/** Actor self-reference passed into the actor callback */
export type ActorSelf = {
  /** Send a Tier 1 signal/status message to subscribers */
  readonly send: (msg: unknown) => void;
  /** Unique actor id */
  readonly id: ActorId;
};

// ============================================================================
// Context — injected into every actor callback
// ============================================================================

/** Formatting utilities available on ctx.fmt */
export type FormatUtils = {
  /** Allocate a Uint8Array buffer (replaces new Uint8Array) */
  readonly alloc: (size: number) => Uint8Array;
  /** Typed allocations */
  readonly allocU8: (length: number) => Uint8Array;
  readonly allocI8: (length: number) => Int8Array;
  readonly allocU16: (length: number) => Uint16Array;
  readonly allocI16: (length: number) => Int16Array;
  readonly allocU32: (length: number) => Uint32Array;
  readonly allocI32: (length: number) => Int32Array;
  readonly allocU64: (length: number) => BigUint64Array;
  readonly allocI64: (length: number) => BigInt64Array;
  readonly allocF32: (length: number) => Float32Array;
  readonly allocF64: (length: number) => Float64Array;
  /** Create buffer from data with auto-encoding */
  readonly from: (data: string | object | Uint8Array) => Uint8Array;
  /** Auto-detect encode (string→UTF8, object→JSON, TypedArray→passthrough) */
  readonly encode: (data: unknown) => Uint8Array;
  /** Auto-detect decode (JSON→object, string→UTF8, else raw) */
  readonly decode: (buffer: Uint8Array) => unknown;
  /** Explicit JSON encoding */
  readonly json: { readonly encode: (obj: unknown) => Uint8Array; readonly decode: (buf: Uint8Array) => unknown };
  /** Explicit string encoding */
  readonly string: { readonly encode: (str: string) => Uint8Array; readonly decode: (buf: Uint8Array) => string };
};

/** Actor execution context */
export type ActorContext = {
  /** Acquire a memory box lock from the pool */
  readonly acquireLock: () => Promise<Result<Lock, string>>;
  /** Write data to a locked box */
  readonly deposit: (lock: Lock, data: Uint8Array) => void;
  /** Finalize write, notify subscribers, release lock */
  readonly done: (lock: Lock) => void;
  /** Explicit heartbeat — reset lease timer during CPU-intensive loops */
  readonly heartbeat: () => void;
  /** Resources proxy — intent relay to main thread */
  readonly resources: Record<string, Record<string, (...args: unknown[]) => Promise<unknown>>>;
  /** Link to another actor (bi-directional failure propagation) */
  readonly link: (actor: ActorRef) => void;
  /** Monitor another actor (uni-directional notification) */
  readonly monitor: (actor: ActorRef) => void;
  /** Spawn a child actor */
  readonly spawn: (callback: ActorCallback) => Promise<ActorRef>;
  /** Terminate self */
  readonly terminate: () => void;
  /** Read data from a READY box (async — sends READ_START, awaits READ_GRANTED) */
  readonly read: (lock: Lock) => Promise<Result<Uint8Array, string>>;
  /** Check if actor has been terminated */
  readonly isCancelled: boolean;
  /** Formatting utilities */
  readonly fmt: FormatUtils;
};

// ============================================================================
// Actor Callback
// ============================================================================

/** Actor callback signature — serialized, no outer scope */
export type ActorCallback = (
  self: ActorSelf,
  msg: unknown,
  ctx: ActorContext,
) => void | Promise<void>;

// ============================================================================
// Actor Reference — returned by supervisor.spawn()
// ============================================================================

/** Reference to a spawned actor */
export type ActorRef = {
  readonly id: ActorId;
  /** Send a message to the actor (triggers callback execution) */
  readonly spawn: (msg: unknown) => void;
  /** Subscribe to Tier 1 messages from this actor */
  readonly subscribe: (fn: (msg: unknown) => void) => () => void;
  /** Terminate the actor immediately (kills thread) */
  readonly terminate: () => void;
  /** Read data from a deposited box (main-thread synchronous) */
  readonly read: (lock: Lock) => Uint8Array;
  /** Signal that reading is complete — decrements reader count, cleans if last */
  readonly done: (lock: Lock) => void;
  /** Get actor diagnostics */
  readonly getDiagnostics: () => Result<ActorDiagnostics, string>;
  /** Link this actor to another (bi-directional) */
  readonly link: (other: ActorRef) => void;
  /** Monitor another actor (uni-directional) */
  readonly monitor: (other: ActorRef) => void;
};

// ============================================================================
// Supervision
// ============================================================================

/**
 * Supervision strategy — controls failure blast radius across actors.
 * "one-for-one": only the failed actor restarts (isolated failures)
 * "one-for-all": all actors in group restart (cascading failures)
 * "rest-for-one": failed actor and all siblings after it restart (pipeline failures)
 */
export type SupervisionStrategy = "one-for-one" | "one-for-all" | "rest-for-one";

/**
 * Retry backoff config — prevents thundering herd on rapid failures.
 * Exponential backoff spreads restarts over time, linear for predictable cadence.
 */
export type RetryConfig = {
  readonly max: number;
  readonly backoff: "exponential" | "linear" | "fixed";
  readonly delayMs?: number;
};

/**
 * Actor group config — isolates supervision policies per group.
 * Each group has its own strategy and retry policy, independent of other groups.
 */
export type ActorGroupConfig = {
  readonly strategy: SupervisionStrategy;
  readonly retry?: RetryConfig;
};

/** Actor group for supervision */
export type ActorGroup = {
  readonly id: string;
  readonly spawn: (callback: ActorCallback, options?: ActorConfig) => ActorRef;
  readonly terminateAll: () => void;
  readonly read: (lock: Lock) => Uint8Array;
  readonly done: (lock: Lock) => void;
};

// ============================================================================
// Resource Manager
// ============================================================================

/**
 * Resource method config — schemas enforce contract at boundaries.
 * Input/output Zod schemas validate messages before/after handler execution.
 */
export type ResourceMethodConfig = {
  readonly input: import("zod").ZodTypeAny;
  readonly output: import("zod").ZodTypeAny;
  readonly handler: (args: unknown) => unknown | Promise<unknown>;
};

/**
 * Resource config — release hooks guarantee cleanup on actor termination.
 * If a method returns a function, it's called when the actor shuts down.
 */
export type ResourceConfig = {
  readonly [method: string]: ResourceMethodConfig | (() => void | Promise<void>) | undefined;
};

/**
 * Resources config — centralizes resource definitions for the supervisor.
 * Resources are shared across all actors; their methods are invoked via ctx.resources.
 */
export type ResourcesConfig = {
  readonly [resourceName: string]: ResourceConfig;
};

// ============================================================================
// Supervisor Configuration
// ============================================================================

/**
 * Memory config — poolSize/boxSize tradeoff balances concurrency vs memory usage.
 * More boxes per pool = higher concurrency but more memory overhead per slot.
 */
export type MemoryConfig = {
  readonly poolSize: number;
  readonly boxSize: string | number;
};

/**
 * Timeout config — lease prevents indefinite resource holding.
 * If an actor doesn't heartbeat within defaultLeaseMs, its locks are forcibly released.
 */
export type TimeoutConfig = {
  readonly defaultLeaseMs: number;
  readonly actorTimeouts?: Record<string, number>;
};

/**
 * Diagnostics config — zero-cost when disabled (enabled: false).
 * When enabled, sampleRate controls collection overhead; individual flags opt-in to specific metrics.
 */
export type DiagnosticsConfig = {
  readonly enabled: boolean;
  readonly sampleRate?: number;
  readonly track?: {
    readonly actorLifetimes?: boolean;
    readonly startTimes?: boolean;
    readonly processLifetimes?: boolean;
    readonly lockAcquisitionTimes?: boolean;
    readonly messageLatency?: boolean;
    readonly bufferUtilization?: boolean;
    readonly heartbeatIntervals?: boolean;
    readonly resourceCallLatency?: boolean;
  };
};

/**
 * Supervisor config — all fields required except resources/timeouts/strategy/retry/diagnostics.
 * These are the foundational settings; everything else has safe defaults or is optional.
 */
export type SupervisorConfig = {
  readonly maxActors: number;
  readonly memory: MemoryConfig;
  readonly resources?: ResourcesConfig;
  readonly timeouts?: TimeoutConfig;
  readonly strategy?: SupervisionStrategy;
  readonly retry?: RetryConfig;
  readonly diagnostics?: DiagnosticsConfig;
};

// ============================================================================
// Actor Configuration
// ============================================================================

/**
 * Actor config — per-actor overrides for timeout and naming.
 * Optional; defaults are inherited from supervisor config if not specified.
 */
export type ActorConfig = {
  readonly name?: string;
  readonly timeoutMs?: number;
};

// ============================================================================
// Supervisor
// ============================================================================

/**
 * Supervisor — single source of truth for actor lifecycle.
 * Owns all actor refs, manages spawning/termination, and aggregates diagnostics.
 */
export type Supervisor = {
  readonly id: string;
  readonly spawn: (callback: ActorCallback, options?: ActorConfig) => ActorRef;
  readonly createGroup: (config: ActorGroupConfig) => ActorGroup;
  readonly terminateActor: (id: ActorId) => void;
  readonly shutdown: () => Promise<void>;
  readonly subscribe: (fn: (msg: unknown) => void) => () => void;
  readonly getDiagnostics: () => Result<SupervisorDiagnostics, string>;
};

// ============================================================================
// Diagnostics
// ============================================================================

/**
 * Actor diagnostics — metrics help debug production issues.
 * Captures lifetime, heartbeats, message count, and termination reason for observability.
 */
export type ActorDiagnostics = {
  readonly id: ActorId;
  readonly lifetimeMs: number;
  readonly heartbeatCount: number;
  readonly lastHeartbeatAt: number;
  readonly messageCount: number;
  readonly terminationReason?: string;
};

/**
 * Supervisor diagnostics — aggregate view of system health.
 * Shows active/total actors, memory pool utilization, and per-actor metrics.
 */
export type SupervisorDiagnostics = {
  readonly activeActors: number;
  readonly totalActorsSpawned: number;
  readonly totalActorsTerminated: number;
  readonly memoryPool: {
    readonly poolSize: number;
    readonly boxesInUse: number;
    readonly utilization: number;
  };
  readonly actors: readonly ActorDiagnostics[];
};

// ============================================================================
// Internal Types
// ============================================================================

/** Worker initialization payload */
export type WorkerInitPayload = {
  readonly type: "INIT";
  readonly callback: string;
  readonly config: Pick<SupervisorConfig, "memory" | "timeouts">;
  readonly actorId: ActorId;
};

/** Worker spawn payload */
export type WorkerSpawnPayload = {
  readonly type: "SPAWN";
  readonly data: unknown;
};

/**
 * Worker control messages — differ from data messages (SPAWN, SEND).
 * Control messages manage worker lifecycle (INIT, TERMINATE, HEARTBEAT).
 */
export type WorkerControlMessage =
  | WorkerInitPayload
  | WorkerSpawnPayload
  | { readonly type: "TERMINATE" }
  | { readonly type: "HEARTBEAT" };

/** Main-to-worker resource request */
export type ResourceRequestMessage = {
  readonly type: "RESOURCE_REQUEST";
  readonly id: string;
  readonly resource: string;
  readonly method: string;
  readonly args: unknown;
};

/** Worker-to-main send message */
export type WorkerSendMessage = {
  readonly type: "SEND";
  readonly msg: unknown;
};

/** Worker requests a lock from main thread */
export type LockRequestMessage = {
  readonly type: "LOCK_REQUEST";
};

/** Worker notifies main of deposit (implicit heartbeat) */
export type DepositMessage = {
  readonly type: "DEPOSIT";
};

/** Worker signals deposit is done */
export type DoneMessage = {
  readonly type: "DONE";
  readonly lock: Lock;
};

/** Worker requests to read a READY box */
export type ReadStartMessage = {
  readonly type: "READ_START";
  readonly lock: Lock;
  readonly requestId: string;
};

/** Main thread grants read access to a box */
export type ReadGrantedMessage = {
  readonly type: "READ_GRANTED";
  readonly lock: Lock;
  readonly requestId: string;
};

/** Main thread denies read access to a box */
export type ReadErrorMessage = {
  readonly type: "READ_ERROR";
  readonly lock: Lock;
  readonly error: string;
  readonly requestId: string;
};

/** Worker requests link to another actor */
export type LinkMessage = {
  readonly type: "LINK";
  readonly actorId: ActorId;
};

/** Worker requests monitor of another actor */
export type MonitorMessage = {
  readonly type: "MONITOR";
  readonly actorId: ActorId;
};

/** Main grants lock to worker */
export type LockGrantedMessage = {
  readonly type: "LOCK_GRANTED";
  readonly lock: Lock;
};

/** All messages sent from worker to main thread */
export type WorkerToMainMessage =
  | WorkerSendMessage
  | ResourceRequestMessage
  | LockRequestMessage
  | DepositMessage
  | DoneMessage
  | ReadStartMessage
  | LinkMessage
  | MonitorMessage
  | { readonly type: "HEARTBEAT" }
  | { readonly type: "LOCK_RELEASED"; readonly boxIndex: number }
  | { readonly type: "ERROR"; readonly error: string }
  | { readonly type: "RESOURCE_RELEASE" }
  | SpawnChildMessage;

/** All messages sent from main thread to worker */
export type MainToWorkerMessage =
  | WorkerControlMessage
  | LockGrantedMessage
  | ReadGrantedMessage
  | ReadErrorMessage
  | { readonly type: "RESOURCE_RESPONSE"; readonly id: string; readonly result: unknown }
  | { readonly type: "RESOURCE_ERROR"; readonly id: string; readonly error: string }
  | ChildSpawnedMessage
  | ChildSpawnErrorMessage;

/** Internal actor state tracked by supervisor */
export type InternalActorState = {
  readonly id: ActorId;
  readonly worker: import("node:worker_threads").Worker;
  readonly config: ActorConfig;
  readonly createdAt: number;
  heartbeats: number;
  lastHeartbeatAt: number;
  messagesSent: number;
  readonly subscribers: Set<(msg: unknown) => void>;
  readonly locks: Set<number>;
  readonly reads: Set<number>;
  readonly linkedActors: Set<ActorId>;
  readonly monitoredActors: Set<ActorId>;
  terminated: boolean;
  terminationReason?: string;
};

/** Pending resource request */
export type PendingResourceRequest = {
  readonly resolve: (value: unknown) => void;
  readonly reject: (error: string) => void;
  readonly timer: ReturnType<typeof setTimeout> | null;
};

// ============================================================================
// Group State (internal)
// ============================================================================

/** Internal group state tracked by supervisor */
export type GroupState = {
  readonly id: string;
  readonly config: ActorGroupConfig;
  readonly actorIds: ActorId[];
  /** Stored callbacks for restart */
  readonly callbacks: Map<ActorId, { readonly callback: ActorCallback; readonly config?: ActorConfig }>;
  /** Restart tracking */
  readonly restartState: { restarts: number[] };
};

// ============================================================================
// Worker Child Spawn
// ============================================================================

/** Worker requests to spawn a child actor */
export type SpawnChildMessage = {
  readonly type: "SPAWN_CHILD";
  readonly requestId: string;
  readonly callback: string;
  readonly config?: ActorConfig;
};

/** Main thread confirms child spawn */
export type ChildSpawnedMessage = {
  readonly type: "CHILD_SPAWNED";
  readonly requestId: string;
  readonly childId: ActorId;
};

/** Main thread reports child spawn failure */
export type ChildSpawnErrorMessage = {
  readonly type: "CHILD_SPAWN_ERROR";
  readonly requestId: string;
  readonly error: string;
};

// ============================================================================
// Diagnostics Collector
// ============================================================================

/**
 * Diagnostics collector — abstracts metric backend for supervisor.
 * Implementations decide where metrics go (console, Prometheus, OTEL, etc.).
 */
export type DiagnosticsCollector = {
  readonly recordActorStart: (id: ActorId) => void;
  readonly recordActorHeartbeat: (id: ActorId) => void;
  readonly recordActorMessage: (id: ActorId) => void;
  readonly recordActorTermination: (id: ActorId, reason?: string) => void;
  readonly recordLockAcquisition: (waitMs: number) => void;
  readonly recordResourceCall: (durationMs: number) => void;
  readonly buildActorDiagnostics: (id: ActorId, state: InternalActorState) => ActorDiagnostics;
  readonly buildSupervisorDiagnostics: (options: {
    readonly activeActors: number;
    readonly totalSpawned: number;
    readonly totalTerminated: number;
    readonly poolSize: number;
    readonly boxesInUse: number;
    readonly actors: readonly ActorDiagnostics[];
  }) => SupervisorDiagnostics;
};
