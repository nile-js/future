# Technical Specification: @nilejs/future

> Uses `slang-ts` semantics: `match`, `matchAll`, `Result`, `Option`, `safeTry`, `pipe`, `andThen` instead of if/switch statements.

## 1. Vision & Overview

`@nilejs/future` is a high-performance, system-level actor library for **Bun** inspired by Erlang. It facilitates isolated concurrent execution using a **Two-Tier Communication** model: Tier 1 for control signals and small data via `postMessage`, Tier 2 for zero-copy shared memory via `SharedArrayBuffer`.

The design follows Erlang's message-passing model: immutable data, per-actor inboxes, and transparent authorization. Actors never share state. All inter-actor communication goes through the supervisor, which routes messages and manages box lifecycle.

---

## 2. Architecture Decision Records (ADR)

### ADR 001: Callback-Based Spawning

* **Decision:** Actors are spawned via serialized callbacks rather than external files.
* **Why:** Allows the Supervisor to inject Resource Manager proxies and context (`ctx`) automatically. Improves Developer Experience (DX) by keeping logic co-located. Enables safe concurrent execution with automatic cleanup.
* **Trade-off:** Closures are lost, outer scope is not inherited. All state must be passed explicitly via `msg` or accessed through `ctx.resources`. This isolation prevents memory leaks and ensures actors can be terminated safely.
* **Benefits:** Any actor can be terminated on demand. Hanging code is automatically killed. One actor's failure cannot corrupt another's state.

### ADR 002: Two-Tier Communication with Immutable Boxes

* **Decision:** Split messaging into Tier 1 (Control/Signals) and Tier 2 (Data/Zero-copy). Tier 2 uses immutable SharedArrayBuffer boxes with supervisor-managed state, not CAS atomic locking.
* **Why:** `postMessage` is too slow for large buffers due to serialization tax. SharedArrayBuffer alone is too complex for simple status updates. Removing CAS simplifies the state machine and eliminates a class of concurrency bugs.
* **Strategy:** Tier 1 uses native messaging. Tier 2 uses a pre-allocated pool of fixed-size SAB segments. Supervisor tracks box state in plain objects. Messages route via per-actor inbox queues.
* **Usage Pattern:** Tier 1 for coordination and small payloads. Tier 2 for bulk data transfer with zero-copy reads.
* **Immutable After Commit:** Data written to a box becomes immutable after `COMMIT`. Multiple readers can read concurrently without synchronization. No reading state. No atomic contention.

### ADR 003: Queue-Based Box Assignment

* **Decision:** Use a configurable pool of memory boxes with queue-based assignment instead of CAS lock acquisition.
* **Why:** FIFO queues with supervisor-side state tracking eliminate atomic contention entirely. No spin-waiting. No CAS retry loops.
* **Configuration:** `poolSize` (max concurrent boxes) and `boxSize` (size of each box).
* **Trade-off:** No size-classes (SM/MD/LG); single configurable box size. Simpler but less optimized for varied payload sizes. No CAS means all state transitions are deterministic and observable.

### ADR 004: Opportunistic Lease Cleanup on Commit

* **Decision:** Calculate timeout timestamps at commit time (not lock acquisition) and store in supervisor-side `BoxEntry`. Cleanup happens on any actor-supervisor interaction, not via `setTimeout`.
* **Why:** More performant than `setTimeout`; avoids timer overhead. Cleanup is deterministic and happens when there is natural interaction. Leasing on commit (not write) gives writers unrestricted time to prepare data while bounding read access.
* **Mechanism:** Store `expiresAt` in `BoxEntry`. On any `self.send()`, `ctx.write()`, `ctx.release()`, `ctx.heartbeat()`, or resource call, check if any boxes have expired and clean up.

### ADR 005: Context-Based Formatting & Serialization

* **Decision:** Provide buffer allocation and serialization utilities via `ctx.fmt` namespace.
* **Why:** Developers should not think about `Uint8Array`, `TextEncoder`, or manual serialization. Keep low-level details abstracted.
* **Configuration:** Default codec is JSON, configurable per supervisor.
* **Design:**
  - `ctx.fmt.alloc(size)` - Buffer allocation (replaces `new Uint8Array(length)`)
  - `ctx.fmt.from(data)` - Create buffer from data with auto-encoding
  - `ctx.fmt.encode(data)` - Auto-detect encoding (string to UTF8, object to JSON, TypedArray passthrough)
  - `ctx.fmt.decode(buffer)` - Auto-detect decoding (JSON to object, string to UTF8, else raw)
  - `ctx.fmt.json.encode/decode` - Explicit JSON encoding/decoding
  - `ctx.fmt.string.encode/decode` - Explicit string encoding/decoding
  - `ctx.fmt.cbor.encode/decode` - CBOR encoding/decoding (if available)

### ADR 006: Bun-Only Runtime Support

* **Decision:** Target **Bun exclusively** as the runtime. Drop official Node.js support.
* **Why:**
  - Bun has native TypeScript support without transpilation or loaders. Worker threads in Bun can directly resolve `.ts` imports, which is essential for the callback serialization model.
  - Node.js worker threads require `tsx`, `ts-node`, or pre-compilation to resolve TypeScript worker entry points. This adds friction and breaks the zero-config developer experience.
  - Bun's `SharedArrayBuffer` + `Atomics` implementation is fully compatible with the spec.
  - Focusing on one runtime reduces testing surface and lets us leverage Bun-specific optimizations.
* **Trade-off:** Users on Node.js cannot use `@nilejs/future` without Bun.
* **Mitigation:** Document the Bun requirement clearly. If demand is high, Node.js support can be added later via a `tsx` loader or pre-compiled worker bootstrap.
* **Status:** Bun-only for now. Node.js support is a **planned future enhancement**.

### ADR 007: Erlang-Style Message Model

* **Decision:** Messages are immutable, typed packets routed through per-actor inboxes. The `Message` type carries a matching key, optional format hint, optional SAB handle, and auto-injected sender identity.
* **Why:** Erlang's model of immutable data with mailbox routing eliminates shared-state bugs. No actor can mutate another actor's data. `from` is always trustworthy because the supervisor injects it.
* **Strategy:**
  - `msg` field serves as the matching key for `matchAll` dispatch (equivalent to Erlang message tags).
  - `handle` is a supervisor-side `Lock` opaque reference to SAB data, never raw memory addresses.
  - `from` is auto-injected by supervisor at send time, never settable by the sender.
  - Boxes with handles are immutable after commit, mirroring Erlang's "copy on send, read-only on receive."
* **Trade-off:** Passing data through supervisor-mediated inboxes adds one level of indirection compared to direct SAB access. This indirection is the foundation of authorization and lifecycle management.

### ADR 008: Authorization Model

* **Decision:** Implement `ShareConfig` with four levels: `"owner"`, `"group"`, `"linked"`, and explicit `ActorId[]`.
* **Why:** Not all shared data should be visible to all actors. Authorization at the write level prevents data leaks. The model mirrors Unix file permissions in spirit (owner/group/world) adapted to actor topology.
* **Levels:**
  - `"owner"` (default): Only the writer actor can read. Subscribers on the main thread can always read.
  - `"group"`: All actors in the same supervision group can read.
  - `"linked"`: Actors linked to the writer can read (bi-directional and uni-directional links).
  - Explicit list: Only specified actor IDs can read.
* **Enforcement:** Supervisor checks `readers` set in `BoxEntry` on every `INBOX` delivery. Unauthorized read attempts are silently dropped.

---

## 3. Technical Architecture

### Memory Layout (The Shared Bus)

The `SharedArrayBuffer` is divided into **Data Boxes** only. All state tracking moves to supervisor-side plain objects.

| Section | Type | Description |
| --- | --- | --- |
| **Data Boxes** | `Uint8Array` | Fixed-size memory segments (configurable `boxSize`). |

No State Board. No Lease Tracker. No CAS operations on the SAB. The SAB is raw byte storage only.

### Supervisor-Side State Tracking

The supervisor maintains a `BoxEntry[]` array parallel to the data boxes:

```typescript
type BoxEntry = {
  state: "FREE" | "WRITING" | "READY";
  from: ActorId;
  msg: string;
  type: FmtType;
  share: ShareConfig;
  refCount: number;
  expiresAt: number;
  writer: ActorId | null;
  readers: Set<ActorId>;
};
```

Box identity is an index into this array, wrapped in an opaque `Lock` type:

```typescript
type Lock = { boxIndex: number; epoch: number };
```

The `epoch` prevents use-after-free: if a box is recycled and reassigned, old handles with a stale epoch are rejected.

### The Two-Tier System

1. **Tier 1 (Control):** `worker.postMessage` for `self.send()`, `ctx.resources` intents, heartbeats, inbox delivery, and protocol messages (`WRITE_REQUEST`, `COMMIT`, `RELEASE`, etc.).
2. **Tier 2 (Data):** Direct `SharedArrayBuffer` read/write. Writers copy data into a locked box. Readers decode from immutable READY boxes. No atomic operations on data.

### State Machine

Three states. No CAS. Supervisor tracks state in plain `BoxEntry` objects.

```
FREE -> WRITING -> READY -> FREE
```

- **FREE:** Box is available for assignment. No writer. No data.
- **WRITING:** One writer assigned. Writer copies data into the SAB segment. Mutable.
- **READY:** Data committed. Immutable. Ref count tracks active readers. No READING state because immutable data needs no synchronization.
- **Transition to FREE:** When ref count reaches 0 (all readers released), box returns to FREE.

All state transitions are deterministic and managed by the supervisor in response to worker protocol messages.

### Per-Actor Inbox

The supervisor maintains a queue of incoming messages for each actor. Inbox entries hold Lock references, not data copies:

```typescript
type InboxEntry = {
  handle: Lock;
  from: ActorId;
  msg: string;
  type: FmtType;
};

const inboxes: Map<ActorId, InboxEntry[]>;
```

When an actor completes its current message handler, the supervisor delivers the next message from its inbox by posting an `INBOX` protocol message.

---

## 4. Supervisor Configuration

```typescript
const supervisor = createSupervisor({
  // Maximum concurrent actors allowed
  maxActors: 20,

  // Resources available to actors (each can have a release method)
  // All methods must specify Zod input/output schemas
  resources: {
    db: {
      query: {
        input: z.object({ sql: z.string(), params: z.array(z.unknown()).optional() }),
        output: z.any(),
        handler: async ({ sql, params }) => await pool.query(sql, params),
      },
      release: async () => /* cleanup */
    },
    storage: {
      get: {
        input: z.object({ id: z.string() }),
        output: z.any(),
        handler: async ({ id }) => /* fetch */,
      },
      release: async () => /* cleanup */
    }
  },

  // Tier 2 Memory Pool Configuration
  memory: {
    poolSize: 10,      // Max concurrent boxes in the pool
    boxSize: '1mb'     // Size of each box (same for all)
  },

  // Timeout Configuration
  timeouts: {
    // Lease timeout for READY boxes (applies to all actors unless overridden)
    defaultLeaseMs: 5000,

    // Per-actor timeout overrides (optional)
    actorTimeouts: {
      'heavy-compute': 10000,
      'quick-task': 2000
    }
  },

  // Supervision Strategy (default: one-for-one)
  strategy: 'one-for-one',

  // Retry Configuration
  retry: {
    max: 3,
    backoff: 'exponential'
  }
});
```

---

## 5. Actor Context Interface (The DSL)

### Message Shape

```typescript
type FmtType = "json" | "string" | "binary" | "cbor";

type Message = {
  msg: string;           // Matching key for matchAll
  type?: FmtType;        // Required for Tier 2. Absent for Tier 1 (json assumed)
  data?: Uint8Array;     // Raw bytes. Always raw. No auto-decode.
  handle?: Lock;         // Tier 2 SAB reference. Absent for Tier 1
  from: ActorId;         // Auto-injected by supervisor. Sender never sets this.
};
```

### Actor Callback Interface

Actor callbacks are **serialized** and do **not inherit the outer scope**. All state must be passed via `msg` or accessed through `ctx`. This isolation enables safe concurrent execution.

```typescript
// WRONG: Outer scope is NOT available in actor
const config = { timeout: 5000 };
const data = heavyPayload;

const actor = supervisor.spawn(async (self, msg, ctx) => {
  // config === undefined  (outer scope lost on serialization)
  // data === undefined     (outer scope lost on serialization)

  // RIGHT: Pass via msg, access via ctx
  const timeout = msg.timeout;          // From message
  const payload = ctx.resources.db.get(msg.dataId);  // From resources
});

// Spawn with state via msg
actor.spawn({ timeout: 5000, dataId: 'abc123' });
```

```typescript
const actor = supervisor.spawn(async (self, msg, ctx) => {
  // Tier 1: Signal/Status update (small data, JSON)
  self.send("progress", { percent: 0.5 });

  // Tier 2: Zero-copy SAB write. One async call.
  const handle = await ctx.write({
    msg: "result",
    type: "json",
    data: ctx.fmt.encode({ large: "payload" }),
    share: "group",
  });

  // Tier 2: Read SAB data from an incoming message
  const input = ctx.read(msg).json();
  const result = heavyComputation(input);
  ctx.release(msg.handle);

  // Explicit Heartbeat (resets lease during CPU-intensive loops)
  ctx.heartbeat();

  // Resource Manager (Proxy-based intent relay)
  const result = await ctx.resources.db.query({ sql: 'SELECT * FROM users' });

  // Link to another actor (bi-directional failure propagation)
  const worker = await ctx.spawn(workerCallback);
  ctx.link(worker);

  // Monitor another actor (uni-directional notification)
  ctx.monitor(worker);
});

// Main Program: Subscribe to actor messages
actor.subscribe((msg) => {
  // Handles Tier 1 signals and Tier 2 inbox deliveries
  matchAll(msg, {
    result: (m) => {
      const data = actor.read(m).json();
      actor.release(m.handle);
    },
    _: () => {},
  });
});
```

### On-Demand Termination

Actors can be terminated on demand from userland code:

```typescript
// Terminate via actor reference
actor.terminate();

// Terminate via supervisor (by actor id)
supervisor.terminateActor(actor.id);

// Terminate entire group
group.terminateAll();
```

Termination guarantees:
- In-flight write requests are cancelled.
- Locked boxes are force-released to FREE.
- Resource cleanup hooks are called.
- Linked actors receive termination signal.
- Actor reference is marked as dead.

### Termination vs AbortController

Actor termination is NOT the same as AbortController.

| Aspect | AbortController | @nilejs/future Termination |
| --- | --- | --- |
| What happens | Execution continues, results ignored | Thread is killed immediately |
| Resources freed | No | Yes (release methods called) |
| Memory freed | No | Yes (buffers recycled) |
| Can truly stop hung code | No | Yes |

AbortController only sets a flag that code can check. The execution continues running until it naturally completes or checks the flag.

@nilejs/future termination kills the worker thread. The execution stops right there. This is true termination.

Most libraries in TypeScript and JavaScript cannot do this. @nilejs/future and Effect-TS are among the few that support true on-demand termination of running code.

### Context API

| Method | Tier | Description |
|---|---|---|
| `self.send(msg, data)` | 1 | Send signal/status to subscribers. JSON default. Auto-injects `from`. |
| `ctx.write({ msg, type, data, share })` | 2 | Write data to SAB. Zero-copy. Returns `Lock`. Immutable after write. |
| `ctx.read(m)` | 2 | Read SAB data from message. Returns chainable decoder. |
| `ctx.release(handle)` | 2 | Decrement ref count. If 0 then FREE. |
| `ctx.heartbeat()` | 1 | Explicitly reset lease timer during CPU-intensive loops. |
| `ctx.resources.*` | 1 | Access resources via Proxy. Calls go through intent relay. |
| `ctx.link(actor)` | 1 | Create bi-directional link. If either dies, both die. |
| `ctx.monitor(actor)` | 1 | Create uni-directional monitor. Receive notification when actor dies. |
| `ctx.spawn(callback)` | 1 | Spawn a child actor. |
| `ctx.terminate()` | 1 | Terminate the actor gracefully. |
| `ctx.isCancelled` | 1 | Check if actor has been terminated. |
| `ctx.fmt.*` | N/A | Buffer allocation and serialization utilities. |

### ActorRef API

| Method | Description |
|---|---|
| `actor.id` | Unique actor identifier |
| `actor.spawn(msg)` | Send initial message to actor |
| `actor.subscribe(fn)` | Subscribe to actor messages |
| `actor.terminate()` | Terminate actor immediately |
| `actor.read(m)` | Read SAB data from message. Returns chainable decoder. |
| `actor.release(handle)` | Decrement ref count. If 0 then FREE. |
| `actor.getDiagnostics()` | Get actor diagnostics |
| `actor.link(other)` | Bi-directional link |
| `actor.monitor(other)` | Uni-directional monitor |

### Chainable Reader API

`ctx.read(m)` and `actor.read(m)` return a chainable decoder object:

```typescript
const reader = ctx.read(m);
reader.json();     // Decode as JSON (default: m.type === "json")
reader.string();   // Decode as UTF-8 string
reader.binary();   // Return raw Uint8Array
reader.cbor();     // Decode as CBOR (if available)
reader.raw();      // Return Uint8Array from SAB slice (main thread only)
```

The `m.type` field hints the default decode method. `ctx.read(m).json()` is equivalent to `ctx.read(m).decode('json')`.

If `m.handle` is absent (Tier 1 message), `ctx.read(m)` returns `null` for all decoders.

---

## 6. Write Queue (FIFO Box Assignment)

When an actor calls `ctx.write()` and no boxes are FREE:

1. The request is pushed to an internal `Queue<WriteRequest>` in the supervisor.
2. When a box transitions to FREE (via `RELEASE` or lease expiry), the supervisor assigns it to the next queued writer.
3. The writer receives a `WRITE_GRANTED` protocol message with the `Lock`.
4. The writer copies data into the SAB segment and sends `COMMIT { lock }`.

```typescript
type WriteRequest = {
  actorId: ActorId;
  msg: string;
  type: FmtType;
  data: Uint8Array;
  share: ShareConfig;
  resolve: (lock: Lock) => void;
  reject: (error: Error) => void;
};
```

---

## 7. The Resource Manager (Intent Relay)

Inside the worker callback, `ctx.resources` is a **Proxy**.

* **Call:** `ctx.resources.db.query({ sql: 'SELECT 1' })`
* **Action:** Proxy intercepts the call, packages it as an Intent Packet, and sends it via Tier 1 to the Supervisor.
* **Return:** Supervisor executes the real logic and sends the result back to the worker.
* **Release:** Each resource can define a `release()` method called during Supervisor cleanup.
* **Schema:** Each method must specify a Zod input and output schema for type safety and validation.

### Technical Workflow

1. **Definition:** Define resources in `createSupervisor` config with optional `release` method.
2. **Schema:** Each method defines input/output Zod schemas for validation.
3. **Mapping:** Supervisor maps method names to actual functions.
4. **Call:** Actor calls `await ctx.resources.db.query({ sql: 'SELECT 1' })`.
5. **Validation:** Zod validates arguments against input schema before execution.
6. **Serialization:** Proxy converts validated arguments into a serializable Tier 1 message.
7. **Execution:** Main Thread receives message, runs the handler.
8. **Resolution:** Supervisor sends return value back, validating against output schema, resolving worker's await.
9. **Cleanup:** On supervisor shutdown, call each resource's `release()` method.

### Constraints

* **Serialization:** Arguments and return values must be Cloneable (JSON-compatible or `Uint8Array`).
* **Large Data:** For large returns, return a Tier 2 lock address instead of actual data.
* **Scoping:** Supervisor can optionally filter resources per actor.
* **Schema Required:** All resource methods must declare Zod schemas for inputs and outputs.

---

## 8. Heartbeat & Lease System

The Heartbeat is the Dead Man's Switch that prevents data from being held indefinitely in READY boxes.

### Lease Mechanism

* **Decision:** Lease timestamps are computed at COMMIT time and stored in the supervisor-side `BoxEntry.expiresAt`.
* **Reset:** When a new reader acquires a handle to a READY box, `expiresAt` is extended (reset timer).
* **Check:** On any actor-supervisor interaction (`self.send`, `ctx.write`, `ctx.release`, `ctx.heartbeat`, resource call), check all `BoxEntry` entries for expired leases.

### Lease Expiry

If `Date.now() > expiresAt`:
1. Force-release all reader refs on that box.
2. Set box state to FREE.
3. If a writer is still holding the box in WRITING state, terminate the writer actor.
4. If any queued write requests exist, assign the freshly freed box to the next in queue.

### Explicit Heartbeat

`ctx.heartbeat()` resets the lease timer for any boxes the calling actor holds:
- As writer in WRITING state: extends write timeout.
- As reader with active handle: extends read lease.

```typescript
const supervisor = createSupervisor({
  timeouts: {
    defaultLeaseMs: 5000,  // Global default
    actorTimeouts: {       // Per-actor override
      'long-running': 15000
    }
  }
});
```

---

## 9. Supervision Strategies (The Restart Logic)

When defining a Supervisor, specify a `strategy` that determines the fate of siblings when one actor fails.

Three supervision strategies are supported:

* **One-For-One (Default):** If an actor dies, only that actor is restarted. Good for independent tasks.
* **One-For-All:** If one actor dies, kill and restart **all** actors in that group. Use when actors are tightly coupled.
* **Rest-For-One:** If an actor dies, restart only the actors started **after** it in sequence.

```typescript
const billingGroup = supervisor.createGroup({
  strategy: 'one-for-all',
  retry: {
    max: 3,
    backoff: 'exponential'
  }
});
```

---

## 10. Linking vs Monitoring

### Linking (Bi-directional)

Linking creates a suicide pact. If Actor A links to Actor B, and either dies, the other is automatically killed.

```typescript
// Inside Actor A
const workerActor = await ctx.spawn(workerCallback);
ctx.link(workerActor);

// If Actor A crashes, Supervisor automatically kills 'workerActor'
```

### Monitoring (Uni-directional)

Monitoring sends a notification without killing the monitor. Actor A monitors Actor B. If B dies, A receives a Tier 1 message.

```typescript
ctx.monitor(actorB);

// Receive monitor notification via message handler
matchAll(msg, {
  DOWN: (info) => console.log(`Actor ${info.id} died: ${info.reason}`),
  _: () => {}
});
```

---

## 11. Internal State Management

### Dependency Matrix

To keep link propagation fast, the Supervisor tracks links in a **Dependency Matrix**. When an actor is flagged as `DOWN`, the Supervisor scans the matrix to find and terminate linked actors.

### Health Metrics

The Supervisor manages three distinct health metrics for every actor:

1. **The Lease (Memory):** Is a READY box held too long? (Lease expiry check on interaction).
2. **The Exit (Lifecycle):** Did the worker thread finish with a non-zero exit code?
3. **The Link (Topology):** Does this death require killing other actors?

### Box Metadata Table

The supervisor maintains a `BoxEntry[]` array as the single source of truth for box state:

| Field | Type | Description |
|---|---|---|
| `state` | `"FREE" \| "WRITING" \| "READY"` | Current box state |
| `from` | `ActorId` | Writer actor ID |
| `msg` | `string` | Message matching key |
| `type` | `FmtType` | Data format hint |
| `share` | `ShareConfig` | Authorization level |
| `refCount` | `number` | Active reader count |
| `expiresAt` | `number` | Lease expiry timestamp |
| `writer` | `ActorId \| null` | Writer actor (null when FREE) |
| `readers` | `Set<ActorId>` | Set of authorized reader IDs |

### Inbox Data Structure

```typescript
type InboxEntry = {
  handle: Lock;
  from: ActorId;
  msg: string;
  type: FmtType;
};

const inboxes: Map<ActorId, InboxEntry[]>;
```

---

## 12. Error Handling

* Use `slang-ts` `Result` types for all async operations like `ctx.write`.
* Resource Manager errors are serialized, sent back, and returned as `Err` in the actor.
* Supervision strategies handle actor crashes based on configuration.
* Authorization failures at read time are silent: the unauthorized reader simply does not receive the message.
* Lease expiry kills the holding actor if in WRITING state. Readers are silently released, box returns to FREE.

---

## 13. Delivery Milestones

1. **Phase 1:** Tier 1 PubSub with `self.send()` and callback serialization (Bun).
2. **Phase 2:** Tier 2 SAB pool with `ctx.write()`, immutable boxes, `COMMIT`/`RELEASE` protocol.
3. **Phase 3:** Per-actor inbox routing, `INBOX` delivery, and `ctx.read()` chainable decoder.
4. **Phase 4:** Authorization model (`ShareConfig`: owner/group/linked/explicit).
5. **Phase 5:** Resource Manager Proxy, opportunistic lease cleanup, and embedded diagnostics.

---

## 14. Testing Strategy

Every feature in `@nilejs/future` must include comprehensive test coverage:

### Test Categories per Feature

| Category | Description |
| --- | --- |
| **Constraint Tests** | Verify boundary conditions and invariants hold |
| **Happy Path Tests** | Normal expected usage scenarios work correctly |
| **Non-Happy Path Tests** | Error conditions and failure modes are handled |
| **Edge Case Tests** | Boundary values, empty inputs, maximum sizes |

### Test Structure per Feature

```typescript
describe('featureName', () => {
  // Constraint tests - invariants must hold
  it('maintains invariant X under all conditions', () => { /* ... */ });
  it('does not allow invalid state transitions', () => { /* ... */ });

  // Happy path
  it('processes valid input correctly', () => { /* ... */ });
  it('completes lifecycle successfully', () => { /* ... */ });

  // Non-happy path
  it('handles error Y gracefully', () => { /* ... */ });
  it('returns Err for invalid input', () => { /* ... */ });
  it('recovers from partial failures', () => { /* ... */ });

  // Edge cases
  it('works with empty input', () => { /* ... */ });
  it('handles maximum buffer size', () => { /* ... */ });
  it('handles concurrent access correctly', () => { /* ... */ });
});
```

### Testing Approach

* **Unit Tests:** Individual functions and utilities (e.g., `encode`/`decode`, queue management)
* **Integration Tests:** Actor interactions, message passing, Tier 1/Tier 2 coordination
* **Property-Based Tests:** Invariant validation across random inputs
* **Stress Tests:** Memory pool, write contention, high concurrency scenarios
* **Chaos Tests:** Simulated crashes, timeouts, authorization failures

### Key Invariants

- A box in WRITING state has exactly one writer.
- A box in READY state has immutable data.
- `refCount` decrements to 0 before FREE transition.
- `from` is always the supervisor-injected sender identity.
- No message reaches an unauthorized reader.
- `epoch` on Lock prevents use-after-free on recycled boxes.

---

## 15. Embedded Diagnostics

Configurable, low-overhead observability for monitoring system behavior.

### Supervisor Configuration

```typescript
const supervisor = createSupervisor({
  // ... other config

  diagnostics: {
    enabled: true,                    // Master switch
    sampleRate: 1.0,                  // Sample rate (0.0-1.0), 1.0 = 100%

    // Track specific metrics
    track: {
      actorLifetimes: true,           // Track creation to termination duration
      startTimes: true,               // Actor and system initialization times
      processLifetimes: true,         // Worker thread lifetimes
      writeQueueDepth: true,          // Pending write request queue depth
      messageLatency: true,           // Tier 1/Tier 2 message round-trip
      bufferUtilization: true,        // Pool usage percentages (FREE/WRITING/READY)
      heartbeatIntervals: true,       // Time between heartbeats
      resourceCallLatency: true,      // Proxied method call durations
      authorizationEvents: true,      // Authorized vs denied read attempts
      inboxDepth: true,               // Per-actor inbox queue depth
      refCountHistory: true,          // Box ref count over time
    }
  }
});
```

### Available Metrics

| Metric | Description |
| --- | --- |
| `actorLifetimes` | Creation time, last heartbeat, termination reason |
| `startTimes` | Actor spawn time, supervisor init time |
| `processLifetimes` | Worker thread uptime, restart count |
| `writeQueueDepth` | Average/max queue depths, wait times |
| `messageLatency` | Messages/sec by type and tier |
| `bufferUtilization` | Pool state distribution (FREE/WRITING/READY), utilization |
| `heartbeatIntervals` | Intervals, missed heartbeats, timeout events |
| `resourceCallLatency` | Duration of proxied method calls |
| `authorizationEvents` | Count of reads granted vs denied |
| `inboxDepth` | Per-actor inbox queue depth over time |
| `refCountHistory` | Box reference count snapshots |

### Implementation Notes

* Uses `performance.now()` for high-resolution timing.
* Atomic counters for low-overhead increment operations.
* Zero-cost when disabled (conditional compilation where possible).
* Data accessible via `supervisor.getDiagnostics()` or periodic export.

### Usage

```typescript
// Periodic reporting
setInterval(() => {
  const stats = supervisor.getDiagnostics();
  matchAll(stats, {
    Ok: (s) => console.log('System health:', {
      writeQueueDepth: s.writeQueueDepth,
      actorCount: s.activeActors,
      poolDistribution: s.bufferUtilization
    }),
    Err: (e) => console.log('Diagnostics unavailable:', e.error),
    _: () => {}
  });
}, 5000);

// Per-actor diagnostics
actor.getDiagnostics().andThen((d) => {
  console.log(`Actor ${actor.id}: lifetime=${d.lifetimeMs}ms, heartbeats=${d.heartbeatCount}`);
  return Ok(d);
});
```

---

## 16. Slang-ts Semantics Reference

All code uses `slang-ts` patterns instead of imperative control flow:

```typescript
import {
  atom,
  Err,
  match,
  matchAll,
  Ok,
  option,
  panic,
  pipe,
  safeTry,
  type Option,
  type Result
} from "slang-ts";

// match - for Result/Option types
match(someResult, {
  Ok: (v) => console.log("Success:", v.value),
  Err: (e) => console.log("Failed:", e.error),
});

// matchAll - for tagged unions and arbitrary values
matchAll(msg, {
  progress: (m) => console.log(`Progress: ${m.data.percent * 100}%`),
  result: (m) => {
    const data = ctx.read(m).json();
    console.log("Data received from:", m.from);
  },
  _: () => console.log("Unknown message"),
});

// matchAll with atoms
const status = atom("ready");
matchAll(status, {
  ready: (v) => console.log("Ready!", v),
  pending: () => console.log("Still pending..."),
  _: () => console.log("Unknown status"),
});

// Result/Option creation
const success = Ok(42);
const failure = Err("something went wrong");
const maybe = option(someValue);    // Some(value) or None
const empty = option(null);         // None

// safeTry - for try/catch that returns Result
const result = await safeTry(() => fetchData());
match(result, {
  Ok: (v) => process(v),
  Err: (e) => handleError(e),
});

// panic - for unrecoverable errors
if (!config) panic("Configuration required");

// pipe - function composition over Results
const pipeline = await pipe(
  initialValue,
  (r) => r.isOk ? Ok(transform(r.value)) : r,
  (r) => r.isOk ? Ok(validate(r.value)) : r,
).run();
match(pipeline, {
  Ok: (v) => console.log("Pipeline result:", v.value),
  Err: (e) => console.log("Pipeline failed:", e.error),
});

// andThen - chainable transformations
const chained = Ok(10)
  .andThen((x) => x * 2)
  .andThen((x) => x + 5)
  .andThen((x) => ({ value: x }));
console.log("Chained result:", chained.value); // 25
```

---

## 17. Buffer & Serialization Utilities (`ctx.fmt`)

All buffer allocation and serialization utilities are available via `ctx.fmt` namespace. Developers never need to think about `Uint8Array`, `TextEncoder`, or manual serialization.

### Supervisor Configuration

```typescript
const supervisor = createSupervisor({
  defaultCodec: 'json',  // json, string, cbor, or custom codec

  // Custom codecs (optional)
  codecs: {
    myType: {
      encode: (data) => /* Uint8Array */,
      decode: (buffer) => /* parsed value */
    }
  }
});
```

### Buffer Allocation

```typescript
// Allocate a buffer (replaces new Uint8Array(length))
const buffer = ctx.fmt.alloc(size);
const buffer = ctx.fmt.alloc(1024);  // 1KB buffer

// Typed allocations
const u8 = ctx.fmt.alloc.u8(length);    // Uint8Array
const i32 = ctx.fmt.alloc.i32(length);  // Int32Array
const f64 = ctx.fmt.alloc.f64(length);  // Float64Array

// Create buffer from data
const buffer = ctx.fmt.from("hello");              // UTF-8 encoded
const buffer = ctx.fmt.from({ name: "kizz" });     // JSON encoded
const buffer = ctx.fmt.from([1, 2, 3]);           // Array encoded
const buffer = ctx.fmt.from(existingUint8Array);   // Pass-through (zero-copy)
```

### Encode/Decode (Auto-detect with Explicit Escape Hatches)

```typescript
// Auto-detect encoding
ctx.fmt.encode(data)          // string to UTF8, object to JSON, TypedArray passthrough
ctx.fmt.decode(buffer)        // Try JSON to object, then string, else raw buffer

// Explicit JSON encoding/decoding
ctx.fmt.json.encode(obj)      // Uint8Array
ctx.fmt.json.decode(buf)      // parsed object

// Explicit string encoding/decoding
ctx.fmt.string.encode(str)    // UTF-8 Uint8Array
ctx.fmt.string.decode(buf)    // string

// CBOR encoding/decoding (if available)
ctx.fmt.cbor.encode(data)     // Uint8Array
ctx.fmt.cbor.decode(buf)      // parsed object
```

### Usage with ctx.write / ctx.read

```typescript
// Writing structured data to Tier 2
const handle = await ctx.write({
  msg: "result",
  type: "json",
  data: ctx.fmt.encode({ transactions: [...], metadata: {...} }),
  share: "group",
});

// Reading structured data from Tier 2
const data = ctx.read(m).json();       // Auto JSON decoding via m.type hint
const text = ctx.read(m).string();     // Explicit string decoding
const raw = ctx.read(m).binary();      // Raw Uint8Array

// Working with strings
const buffer = ctx.fmt.encode("hello world");  // UTF-8 bytes
const decoded = ctx.fmt.decode(buffer);        // Back to string
```

### Typed Variants for Common Types

```typescript
ctx.fmt.alloc.u8(length)    // Uint8Array (alias for base alloc)
ctx.fmt.alloc.i8(length)    // Int8Array
ctx.fmt.alloc.u16(length)   // Uint16Array
ctx.fmt.alloc.i16(length)   // Int16Array
ctx.fmt.alloc.u32(length)   // Uint32Array
ctx.fmt.alloc.i32(length)   // Int32Array
ctx.fmt.alloc.u64(length)   // BigUint64Array
ctx.fmt.alloc.i64(length)   // BigInt64Array
ctx.fmt.alloc.f32(length)   // Float32Array
ctx.fmt.alloc.f64(length)   // Float64Array
```

---

## 18. End-User Scenarios

### Scenario 1: Real-Time Financial Processor

```typescript
import { createSupervisor } from "@nilejs/future";
import { Ok, matchAll } from "slang-ts";

const supervisor = createSupervisor({
  resources: {
    bankApi: {
      verify: {
        input: z.object({ id: z.string() }),
        output: z.boolean(),
        handler: async ({ id }) => /* ... */,
      },
      release: async () => /* cleanup */
    }
  },
  memory: { poolSize: 8, boxSize: '2mb' },
  timeouts: { defaultLeaseMs: 10000 }
});

const reconciler = supervisor.spawn(async (self, msg, ctx) => {
  const transactions = msg.batch;
  const results = ctx.fmt.alloc(transactions.length);

  for (let i = 0; i < transactions.length; i++) {
    const isValid = await ctx.resources.bankApi.verify(transactions[i].id);
    results[i] = isValid ? 1 : 0;

    // Reset lease during long loop
    if (i % 100 === 0) ctx.heartbeat();

    // Tier 1 progress update
    self.send("progress", { percent: i / transactions.length });
  }

  // Tier 2: Write the result as immutable box
  const handle = await ctx.write({
    msg: "reconciled",
    type: "binary",
    data: results,
    share: "owner",
  });
});

reconciler.subscribe((msg) => {
  matchAll(msg, {
    progress: (m) => console.log(`Progress: ${m.data.percent * 100}%`),
    reconciled: (m) => {
      const data = reconciler.read(m).binary();
      console.log("Batch reconciled.");
      reconciler.release(m.handle);
    },
    _: () => {},
  });
});
```

### Scenario 2: AI Media Transcoder

Actors are perfect for burst memory tasks. Here, an actor converts an image, resetting the heartbeat during intensive pixel manipulation.

```typescript
const supervisor = createSupervisor({
  resources: {
    storage: {
      get: {
        input: z.object({ fileId: z.string() }),
        output: z.any(),
        handler: async ({ fileId }) => /* fetch image */,
      },
      release: async () => /* cleanup */
    }
  },
  memory: { poolSize: 5, boxSize: '5mb' },
  timeouts: { defaultLeaseMs: 30000 }
});

const transcoder = supervisor.spawn(async (self, msg, ctx) => {
  const rawImage = await ctx.resources.storage.get(msg.fileId);

  // Pixel-by-pixel transformation with heartbeat
  for (let row = 0; row < rawImage.height; row++) {
    processRow(rawImage, row);
    ctx.heartbeat();  // Explicit heartbeat during CPU-intensive loop
  }

  // Write transcoded data to shared memory
  const handle = await ctx.write({
    msg: "transcoded",
    type: "binary",
    data: rawImage.buffer,
    share: "owner",
  });

  self.send("done", { fileId: msg.fileId });
});

transcoder.subscribe((msg) => {
  matchAll(msg, {
    done: (m) => console.log(`Transcoded: ${m.data.fileId}`),
    _: () => {}
  });
});
```

### Scenario 3: Intent-Based Tool Calling (Agentic)

In an agentic workflow, the Resource Manager acts as the Tool Belt for the AI.

```typescript
const supervisor = createSupervisor({
  resources: {
    searchTool: {
      find: {
        input: z.object({ q: z.string() }),
        output: z.any(),
        handler: async ({ q }) => google.search(q),
      },
      release: async () => { /* cleanup */ }
    },
    calculator: {
      compute: {
        input: z.object({ expr: z.string() }),
        output: z.number(),
        handler: async ({ expr }) => evaluate(expr),
      },
      release: async () => {}
    }
  }
});

const agent = supervisor.spawn(async (self, msg, ctx) => {
  // Agent decides it needs to search
  const searchResult = await ctx.resources.searchTool.find(msg.query);

  // Then compute something
  const calcResult = await ctx.resources.calculator.compute('2+2');

  matchAll(calcResult, {
    Ok: (v) => self.send("result", { search: searchResult, calc: v.value }),
    Err: (e) => self.send("error", { reason: e.error }),
    _: () => {}
  });
});
```

### Scenario 4: Fault-Tolerant Pipeline with Immutable Data

Using supervision strategies to build a reliable processing pipeline. Data flows through immutable Tier 2 boxes with group authorization.

```typescript
const pipeline = supervisor.createGroup({
  strategy: 'rest-for-one'  // Restart downstream on upstream failure
});

// Stage 1: Data Ingest
const ingest = pipeline.spawn(async (self, msg, ctx) => {
  const rawData = await ctx.resources.storage.fetch(msg.url);

  // Write to shared memory, authorize group members
  const handle = await ctx.write({
    msg: "ingested",
    type: "binary",
    data: ctx.fmt.encode(rawData),
    share: "group",
  });
  self.send("ready");
});

// Stage 2: Transformer
const transform = pipeline.spawn(async (self, msg, ctx) => {
  matchAll(msg, {
    ingested: (m) => {
      // Read the immutable data
      const data = ctx.read(m).json();
      const result = data.map((x: number) => x * 2).reduce((a: number, b: number) => a + b, 0);
      ctx.release(m.handle);

      // Write transformed result
      ctx.write({
        msg: "transformed",
        type: "json",
        data: ctx.fmt.encode({ result }),
        share: "group",
      }).andThen((handle) => {
        self.send("ready");
      });
    },
    _: () => {}
  });
});

// Stage 3: Output
const output = pipeline.spawn(async (self, msg, ctx) => {
  matchAll(msg, {
    transformed: async (m) => {
      const result = ctx.read(m).json();
      await ctx.resources.storage.save(msg.outputPath, result);
      ctx.release(m.handle);
      self.send("complete", { outputPath: msg.outputPath });
    },
    _: () => {}
  });
});
```

### Scenario 5: AI Swarm (Multi-Agent)

Agent A (Searcher) scrapes websites, Agent B (Analyst) processes the text. Linked so failure cascades.

```typescript
const supervisor = createSupervisor({
  resources: {
    webScraper: {
      fetch: {
        input: z.object({ url: z.string() }),
        output: z.string(),
        handler: async ({ url }) => /* ... */,
      },
      release: async () => {}
    },
    llm: {
      analyze: {
        input: z.object({ text: z.string() }),
        output: z.any(),
        handler: async ({ text }) => /* ... */,
      },
      release: async () => {}
    }
  },
  memory: { poolSize: 10, boxSize: '4mb' },
  timeouts: {
    defaultLeaseMs: 60000,
    actorTimeouts: {
      'searcher': 30000,
      'analyst': 120000
    }
  }
});

// Searcher Agent
const searcher = supervisor.spawn(
  async (self, msg, ctx) => {
    const urls = msg.urls;
    let htmlData = '';

    for (const url of urls) {
      const html = await ctx.resources.webScraper.fetch(url);
      htmlData += html;
      ctx.heartbeat();  // Keep write lease alive during scraping

      if (ctx.isCancelled) break;
    }

    // Write scraped data to shared memory, share with linked actors
    const handle = await ctx.write({
      msg: "scraped",
      type: "string",
      data: ctx.fmt.encode(htmlData),
      share: "linked",
    });
    self.send("search_done");
  },
  { name: 'searcher' }
);

// Analyst Agent (Linked to Searcher)
const analyst = supervisor.spawn(
  async (self, msg, ctx) => {
    matchAll(msg, {
      scraped: async (m) => {
        // Read the immutable scraped data
        const htmlData = ctx.read(m).string();
        const analysis = await ctx.resources.llm.analyze(htmlData);
        ctx.release(m.handle);

        self.send("analysis", { result: analysis });
      },
      _: () => {}
    });
  },
  { name: 'analyst' }
);

// Link: If Searcher dies, Analyst dies too
await searcher.link(analyst);

// Monitor the swarm from main thread
supervisor.subscribe((msg) => {
  matchAll(msg, {
    search_done: () => console.log("Search complete"),
    analysis: (m) => console.log("Swarm result:", m.data.result),
    DOWN: (info) => console.log(`Agent ${info.data.id} died: ${info.data.reason}`),
    _: () => {}
  });
});
```

### Scenario 6: AI Agent with Resource Cleanup

```typescript
const supervisor = createSupervisor({
  resources: {
    vectorDb: {
      search: {
        input: z.object({ query: z.string() }),
        output: z.any(),
        handler: async ({ query }) => /* ... */,
      },
      release: async () => { /* close connections */ }
    },
    llm: {
      generate: {
        input: z.object({ prompt: z.string() }),
        output: z.any(),
        handler: async ({ prompt }) => /* ... */,
      },
      release: async () => { /* cleanup model */ }
    }
  },
  memory: { poolSize: 5, boxSize: '2mb' },
  timeouts: { defaultLeaseMs: 30000 }
});

const agent = supervisor.spawn(async (self, msg, ctx) => {
  // Search vector database
  const context = await ctx.resources.vectorDb.search(msg.query);

  // Generate with LLM and write result to shared memory
  const response = await ctx.resources.llm.generate(context);

  const handle = await ctx.write({
    msg: "generated",
    type: "json",
    data: ctx.fmt.encode(response),
    share: "owner",
  });

  self.send("complete");
});

// Main thread subscriber reads the generated content
agent.subscribe((msg) => {
  matchAll(msg, {
    generated: (m) => {
      const data = agent.read(m).json();
      console.log("Generation result:", data);
      agent.release(m.handle);
    },
    complete: () => console.log("Agent finished"),
    _: () => {}
  });
});

// Graceful shutdown triggers resource.release() calls
process.on('SIGTERM', () => supervisor.shutdown());
```

### Scenario 7: Worker Protocol in Action

Demonstrates the full protocol flow between worker and supervisor.

```typescript
// Inside actor callback:
const handle = await ctx.write({
  msg: "processed",
  type: "json",
  data: ctx.fmt.encode({ result: 42 }),
  share: "group",
});
// Protocol flow:
// 1. Worker sends: WRITE_REQUEST { msg: "processed", type: "json", data: ..., share: "group" }
// 2. Supervisor assigns box, replies: WRITE_GRANTED { lock: { boxIndex: 3, epoch: 1 } }
// 3. Worker copies data to SAB box[3], sends: COMMIT { lock: { boxIndex: 3, epoch: 1 } }
// 4. Supervisor marks box READY, distributes INBOX messages to authorized readers
// 5. Reader receives: INBOX { handle: { boxIndex: 3, epoch: 1 }, from: "actor-1", msg: "processed", type: "json" }

// Inside reader callback:
const result = ctx.read(m).json();
// No async. Data already in SAB. Box is READY (immutable).
ctx.release(m.handle);
// Protocol flow:
// 1. Worker sends: RELEASE { lock: { boxIndex: 3, epoch: 1 } }
// 2. Supervisor decrements refCount. If 0: marks box FREE.
```

### Scenario 8: Tier 1 Only (Small Data, No SAB)

For small payloads, use `self.send()` exclusively. No `handle` in the message, no `release` needed.

```typescript
const notifier = supervisor.spawn(async (self, msg, ctx) => {
  // Tier 1 only - no shared memory needed
  self.send("progress", { percent: 0.5 });

  const result = await someAsyncWork();
  self.send("done", { value: result });
});

notifier.subscribe((msg) => {
  matchAll(msg, {
    progress: (m) => {
      // No handle - direct data access
      const { percent } = m.data;
      console.log(`Progress: ${percent * 100}%`);
      // No release needed - Tier 1 messages have no handle
    },
    done: (m) => {
      console.log("Result:", m.data.value);
    },
    _: () => {},
  });
});
```

---

## 19. Summary Table

| Feature | Description |
| --- | --- |
| **Actor Isolation** | Callbacks are serialized, outer scope not inherited. All state via `msg` or `ctx`. Enables safe termination. |
| **On-Demand Termination** | Any actor can be terminated from userland via `actor.terminate()` or `supervisor.terminateActor(id)`. |
| **Two-Tier Communication** | Tier 1: PubSub (signals, heartbeats, resource intents). Tier 2: Immutable SAB boxes (zero-copy data). |
| **Memory Pool** | Fixed-size boxes (configurable `poolSize` and `boxSize`). Queue-based assignment. No CAS. |
| **Immutable Data** | Boxes are READY (immutable) after commit. No READING state. Safe concurrent reads without synchronization. |
| **Per-Actor Inbox** | Supervisor routes messages to actor inboxes. Inbox entries hold Lock references, not data copies. |
| **Authorization** | `ShareConfig`: owner (default), group, linked, or explicit ActorId list. Enforced at inbox delivery. |
| **Opportunistic Cleanup** | Calculate `expiresAt` on commit. Check and cleanup on any interaction. No setTimeout overhead. |
| **Heartbeats** | Implicit on `self.send()`, `ctx.write()`, `ctx.release()`. Explicit via `ctx.heartbeat()`. |
| **Resource Manager** | Proxy-based intent relay. Each resource has `release()` for cleanup. Zod schemas for input/output validation. |
| **Supervision** | one-for-one, one-for-all, rest-for-one strategies with retry configuration. |
| **Linking** | Bi-directional (`ctx.link`) or uni-directional (`ctx.monitor`). |
| **Error Handling** | slang-ts Result types. Errors serialized across Tier 1. |
| **Buffer & Serialization** | `ctx.fmt.alloc()`, `ctx.fmt.from()`, `ctx.fmt.encode/decode()` for zero-abstract buffer and serialization DX. |
| **Testing** | Constraint, happy path, non-happy path, and edge case tests per feature. Key invariants enforced. |
| **Diagnostics** | Configurable embedded metrics: actor lifetimes, write queue depth, buffer utilization, inbox depth, authorization events. |

---

## 20. Pitch

> @nilejs/future turns your Bun runtime into an Erlang-style actor system. It combines **immutable shared memory** with **Erlang-style supervision**, letting you build AI agent swarms and high-throughput systems with deterministic message routing, transparent authorization, and zero-copy data transfer. It is not just a library, it is a runtime upgrade for the Agentic Era.

### The Problem It Solves

What happens when your function hangs forever? When your agent enters an infinite loop? When your code crashes your entire backend?

Standard JavaScript has no built-in protection against:
- Infinite loops that hang your event loop
- Functions that consume all memory
- Unreleased resources from crashed code
- Cascading failures that bring down your entire system
- Accidental data sharing and corruption between concurrent tasks

### The Solution

@nilejs/future isolates execution in separate threads with automatic cleanup:
- Any actor can be terminated on demand
- Hanging code is automatically killed via heartbeat timeout
- Resource cleanup is guaranteed on termination
- One actor's failure cannot corrupt others
- Data is immutable after commit; safe concurrent reads without locks
- Authorization is enforced at the supervisor level; actors cannot impersonate each other

### For Agentic Workflows

* **The Thinking Sandbox:** Wrap AI agents in actors. If they enter infinite loops, the Lease and Heartbeat system kills and recovers automatically.
* **Streaming High-Volume Context:** Use Tier 2 writes for zero-copy sharing of large context windows between agents.
* **Supervision Trees:** Use rest-for-one to automatically reset downstream agents when upstream fails.
* **Multi-Agent Authorization:** Use `ShareConfig` to control which agents see which data. No accidental context leaks between agents.

---

*This specification defines the Two-Tier actor model with immutable data, supervisor-mediated inbox routing, and transparent authorization. The architecture balances performance (zero-copy SAB, no CAS contention), reliability (supervision strategies, lease system), and developer experience (chainable readers, proxy-based resources, intuitive DSL).*
