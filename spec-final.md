# Technical Specification: @nilejs/future

> Uses `slang-ts` semantics: `match`, `matchAll`, `Result`, `Option`, `safeTry`, `pipe`, `andThen` instead of if/switch statements.

## 1. Vision & Overview

`@nilejs/future` is a high-performance, system-level actor and promise primitives for Bun and Node.js inspired by Erlang. It facilitates isolated concurrent execution using a **Two-Tier Communication** model, balancing ease of use with zero-copy data transfer.

---

## 2. Architecture Decision Records (ADR)

### ADR 001: Callback-Based Spawning

* **Decision:** Actors are spawned via serialized callbacks rather than external files.
* **Why:** Allows the Supervisor to inject "Resource Manager" proxies and context (`ctx`) automatically. It improves Developer Experience (DX) by keeping logic co-located. Enables safe concurrent execution with automatic cleanup.
* **Trade-off:** Closures are lost, outer scope is not inherited. All state must be passed explicitly via `msg` or accessed through `ctx.resources`. This isolation prevents memory leaks and ensures actors can be terminated safely.
* **Benefits:** Any actor can be terminated on demand. Hanging code is automatically killed. One actor's failure cannot corrupt another's state.

### ADR 002: Two-Tier Communication (Hybrid Model)

* **Decision:** Split messaging into Tier 1 (Control/Signals) and Tier 2 (Data/Zero-copy).
* **Why:** `postMessage` is too slow for large buffers due to serialization tax. SharedArrayBuffer alone is too complex for simple status updates.
* **Strategy:** Tier 1 uses native messaging; Tier 2 uses atomic-locked shared memory.
* **Usage Pattern:** Tier 1 is used more frequently for coordination; Tier 2 reserved for bulk data transfer.

### ADR 003: Tier 2 Memory Pool Strategy

* **Decision:** Use a configurable pool of memory boxes with FIFO queuing instead of N+1 dedicated lanes.
* **Why:** FIFO queues with atomic lock dynamics prevent contention effectively. Pool strategy is simpler and scales better with varying workloads.
* **Configuration:** `poolSize` (max concurrent boxes) and `boxSize` (size of each box).
* **Trade-off:** No size-classes (SM/MD/LG); single configurable box size. Simpler but less optimized for varied payload sizes.

### ADR 004: Opportunistic Lease Cleanup

* **Decision:** Calculate timeout timestamps at acquisition time and store in header. Cleanup happens on any actor-supervisor interaction, not via setTimeout.
* **Why:** More performant than setTimeout; avoids timer overhead. Cleanup is deterministic and happens when there's natural interaction.
* **Mechanism:** Store `expiresAt` timestamp. On any `self.send()`, `ctx.deposit()`, `ctx.heartbeat()`, or resource call, check if any leases have expired and clean up.

### ADR 005: Context-Based Formatting & Serialization

* **Decision:** Provide buffer allocation and serialization utilities via `ctx.fmt` namespace.
* **Why:** Developers shouldn't think about `Uint8Array`, `TextEncoder`, or manual serialization. Keep low-level details abstracted.
* **Configuration:** Default codec is JSON, but configurable per supervisor.
* **Design:**
  - `ctx.fmt.alloc(size)` - Buffer allocation (replaces `new Uint8Array(length)`)
  - `ctx.fmt.from(data)` - Create buffer from data with auto-encoding
  - `ctx.fmt.encode(data)` - Auto-detect encoding (string→UTF8, object→JSON, TypedArray→passthrough)
  - `ctx.fmt.decode(buffer)` - Auto-detect decoding (JSON→object, string→UTF8, else raw)
  - `ctx.fmt.json.encode/decode` - Explicit JSON encoding/decoding
  - `ctx.fmt.string.encode/decode` - Explicit string encoding/decoding
  - `ctx.fmt.cbor.encode/decode` - CBOR encoding/decoding (if available)

---

## 3. Technical Architecture

### Memory Layout (The Shared Bus)

The `SharedArrayBuffer` is divided into a **Control Header** and a **Data Region**.

| Section | Type | Description |
| --- | --- | --- |
| **State Board** | `Int32Array` | Indices tracking Box state: `0` (Clean), `1` (Locked/Writing), `2` (Ready). |
| **Lease Tracker** | `BigInt64Array` | Timestamps (`expiresAt`) of when each lock expires. `0` = no lease. |
| **Data Boxes** | `Uint8Array` | Fixed-size memory segments (configurable `boxSize`). |

### The Two-Tier System

1. **Tier 1 (Control):** A PubSub implementation over `worker.postMessage`. Used for `self.send()`, `ctx.resources` (Resource Manager) intents, and heartbeats.
2. **Tier 2 (Data):** Atomic-locked memory access. Used for `ctx.deposit()`. Uses `Atomics.compareExchange` for non-blocking lock acquisition.

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
    // Global lease timeout (applies to all actors unless overridden)
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
  // Tier 1: Signal/Status update
  self.send({ status: 'active' });

  // Tier 2: Acquire lock, deposit data, mark done
  const lock = await ctx.acquireLock();
  ctx.deposit(lock, uint8Data);
  ctx.done(lock);

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
actor.subscribe(msg => {
  // Handles Tier 1 signals and Tier 2 'Done' notifications
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
- Buffer state reset to Clean (returned to pool)
- Resource cleanup hooks called
- Linked actors receive termination signal
- Actor reference marked as dead

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
| --- | --- | --- |
| `self.send(msg)` | 1 | Send signal/status to subscribers. Triggers implicit heartbeat. |
| `ctx.acquireLock()` | 2 | Acquire a box from the pool. Returns lock with `byteOffset` and `length`. |
| `ctx.deposit(lock, data)` | 2 | Write data to the box. Triggers implicit heartbeat. |
| `ctx.done(lock)` | 2 | Finalize write, notify subscribers, release lock. |
| `ctx.heartbeat()` | 1 | Explicitly reset lease timer during CPU-intensive loops. |
| `ctx.resources.*` | 1 | Access resources via Proxy. Calls go through intent relay. |
| `ctx.link(actor)` | 1 | Create bi-directional link. If either dies, both die. |
| `ctx.monitor(actor)` | 1 | Create uni-directional monitor. Receive notification when actor dies. |
| `ctx.terminate()` | 1 | Terminate the actor gracefully. |
| `ctx.isCancelled` | 1 | Check if actor has been terminated. |
| `ctx.fmt.alloc(size)` | N/A | Allocate a buffer (replaces `new Uint8Array`). |
| `ctx.fmt.from(data)` | N/A | Create buffer from data with auto-encoding. |
| `ctx.fmt.encode(data)` | N/A | Encode data (auto-detect or explicit: json, string, cbor). |
| `ctx.fmt.decode(buffer)` | N/A | Decode buffer to typed data. |

---

## 6. Lock Acquisition (FIFO)

If an actor requests a lock and no boxes are available:

1. The request is pushed to an internal `Queue<Resolver>` in the Main Process.
2. When a box is marked `0` (Clean) by a retriever, the next resolver is triggered.
3. The actor receives the `lock` object containing the `byteOffset` and `length`.
4. Uses `Atomics.compareExchange` for non-blocking lock acquisition.

---

## 7. The Resource Manager (Intent Relay)

Inside the worker callback, `ctx.resources` is a **Proxy**.

* **Call:** `ctx.resources.db.query(sql)`
* **Action:** Proxy intercepts the call, packages it as an "Intent Packet," and sends it via Tier 1 to the Supervisor.
* **Return:** Supervisor executes the real logic and sends the result back to the worker.
* **Release:** Each resource can define a `release()` method called during Supervisor cleanup.
* **Schema:** Each method must specify a Zod input and output schema for type safety and validation.

### Technical Workflow

1. **Definition:** Define resources in `createSupervisor` config with optional `release` method.
2. **Schema:** Each method defines input/output Zod schemas for validation.
3. **Mapping:** Supervisor maps method names to actual functions.
4. **Call:** Actor calls `await ctx.resources.db.query(sql)`.
5. **Validation:** Zod validates arguments against input schema before execution.
6. **Serialization:** Proxy converts validated arguments into a serializable Tier 1 message.
7. **Execution:** Main Thread receives message, runs the handler.
8. **Resolution:** Supervisor sends return value back, validating against output schema, resolving worker's await.
9. **Cleanup:** On supervisor shutdown, call each resource's `release()` method.

### Constraints

* **Serialization:** Arguments and return values must be Cloneable (JSON-compatible or Uint8Array).
* **Large Data:** For large returns, return a Tier 2 address instead of actual data.
* **Scoping:** Supervisor can optionally filter resources per actor.
* **Schema Required:** All resource methods must declare Zod schemas for inputs and outputs.

---

## 8. Heartbeat & Lease System

The Heartbeat is the "Dead Man's Switch" that prevents an actor from holding a memory box indefinitely.

### ADR 005: Explicit & Implicit Heartbeats

* **Decision:** Implement a `Lease` system where a lock expires after `defaultLeaseMs` unless a heartbeat is received.
* **Mechanism:**
  1. **Implicit:** Every Tier 1 `self.send()` or Tier 2 `ctx.deposit()` automatically updates the `Lease Tracker` in the SAB Header.
  2. **Explicit:** `ctx.heartbeat()` allows manual timestamp reset during high-CPU loops that don't perform I/O.
  3. **Opportunistic Cleanup:** On ANY actor-supervisor interaction, check for expired leases and recycle boxes.

### Supervisor Action

On any interaction (self.send, ctx.deposit, ctx.heartbeat, resource call):
1. Check `Lease Tracker` for expired leases.
2. If `Date.now() > expiresAt`:
   - **Panic:** Terminate the actor thread.
   - **Recycle:** Set Box state to `0` (Clean) and return to pool.

### Lease Timeout Configuration

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

## 9. Supervision Strategies (The "Restart" Logic)

When defining a Supervisor, specify a `strategy` that determines the fate of "siblings" when one actor fails.

### ADR 006: Supervision Strategies

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

Linking creates a "suicide pact." If Actor A links to Actor B, and either dies, the other is automatically killed.

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

self.subscribe((msg) => {
  match(msg, {
    ActorDown: (info) => println(`Actor ${info.id} died of ${info.reason}`),
    _: () => {}
  });
});
```

---

## 11. Internal State Management

### Dependency Matrix

To keep link propagation fast, the Supervisor tracks links in a **Dependency Matrix**. When an actor is flagged as `DOWN`, the Supervisor scans the matrix to find and terminate linked actors.

### Health Metrics

The Supervisor manages three distinct "Health" metrics for every actor:

1. **The Lease (Memory):** Is the actor holding a Tier 2 box too long? (Lease expiry check on interaction).
2. **The Exit (Lifecycle):** Did the process/thread finish with a non-zero exit code?
3. **The Link (Topology):** Does this death require me to kill others?

---

## 12. Error Handling

* Use `slang-ts` `Result` types for all async operations like `acquireLock`.
* Resource Manager errors are serialized, sent back, and re-thrown (or returned as `Err`) in the actor.
* Supervision strategies handle actor crashes based on configuration.

---

## 13. Delivery Milestones

1. **Phase 1:** Tier 1 PubSub and Callback Serialization (Bun/Node).
2. **Phase 2:** SAB implementation with Atomic locking, FIFO queue, and pool strategy.
3. **Phase 3:** Resource Manager Proxy implementation with release methods and Zod schemas.
4. **Phase 4:** Opportunistic lease cleanup, backpressure logic, and diagnostics.

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

* **Unit Tests:** Individual functions/utilities (e.g., `encode/decode`, lock acquisition)
* **Integration Tests:** Actor interactions, message passing, Tier 1/Tier 2 coordination
* **Property-Based Tests:** Invariant validation across random inputs
* **Stress Tests:** Memory pool, lock contention, high concurrency scenarios
* **Chaos Tests:** Simulated crashes, timeouts, network failures

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
      actorLifetimes: true,           // Track creation→termination duration
      startTimes: true,               // Actor and system initialization times
      processLifetimes: true,         // Worker thread lifetimes
      lockAcquisitionTimes: true,     // Time spent waiting for locks
      messageLatency: true,           // Tier 1/Tier 2 message round-trip
      bufferUtilization: true,        // Pool usage percentages
      heartbeatIntervals: true,       // Time between heartbeats
      resourceCallLatency: true,      // Proxied method call durations
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
| `lockAcquisitionTimes` | Average/max wait times, queue depths |
| `messageLatency` | Messages/sec by type and tier |
| `bufferUtilization` | Pool allocation/free rates, fragmentation |
| `heartbeatIntervals` | Intervals, missed heartbeats, timeout events |
| `resourceCallLatency` | Duration of proxied method calls |

### Implementation Notes

* Uses `performance.now()` for high-resolution timing
* Atomic counters for low-overhead increment operations
* Zero-cost when disabled (conditional compilation where possible)
* Data accessible via `supervisor.getDiagnostics()` or periodic export

### Usage

```typescript
// Periodic reporting
setInterval(() => {
  const stats = supervisor.getDiagnostics();
  matchAll(stats, {
    Ok: (s) => println('System health:', {
      avgLockWait: s.lockAcquisition.mean,
      actorCount: s.activeActors,
      poolUsage: s.memoryPool.utilization
    }),
    Err: (e) => println('Diagnostics unavailable:', e.error),
    _: () => {}
  });
}, 5000);

// Per-actor diagnostics
actor.getDiagnostics().andThen((d) => {
  println(`Actor ${actor.id}: lifetime=${d.lifetimeMs}ms, heartbeats=${d.heartbeatCount}`);
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
  Ok: (v) => println("Success:", v.value),
  Err: (e) => println("Failed:", e.error),
});

// matchAll - for tagged unions and arbitrary values
matchAll(msg, {
  PROGRESS: (m) => println(`Progress: ${m.value * 100}%`),
  DEPOSIT_READY: (m) => {
    const data = actor.read(m.address);
    println("Data received");
  },
  _: () => println("Unknown message"),
});

// matchAll with atoms
const status = atom("ready");
matchAll(status, {
  ready: (v) => println("Ready!", v),
  pending: () => println("Still pending..."),
  _: () => println("Unknown status"),
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
  Ok: (v) => println("Pipeline result:", v.value),
  Err: (e) => println("Pipeline failed:", e.error),
});

// andThen - chainable transformations
const chained = Ok(10)
  .andThen((x) => x * 2)
  .andThen((x) => x + 5)
  .andThen((x) => ({ value: x }));
println("Chained result:", chained.value); // 25
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
ctx.fmt.encode(data)          // string→UTF8, object→JSON, TypedArray→passthrough
ctx.fmt.decode(buffer)        // Try JSON→object, then string, else raw buffer

// Explicit JSON encoding/decoding
ctx.fmt.json.encode(obj)      // → Uint8Array
ctx.fmt.json.decode(buf)      // → parsed object

// Explicit string encoding/decoding
ctx.fmt.string.encode(str)    // → UTF-8 Uint8Array
ctx.fmt.string.decode(buf)    // → string

// CBOR encoding/decoding (if available)
ctx.fmt.cbor.encode(data)     // → Uint8Array
ctx.fmt.cbor.decode(buf)     // → parsed object
```

### Usage Examples

```typescript
// Writing structured data to Tier 2
const lock = await ctx.acquireLock();
const data = { transactions: [...], metadata: {...} };
ctx.deposit(lock, ctx.fmt.encode(data));  // Auto JSON encoding
ctx.done(lock);

// Reading structured data from Tier 2
const lock = await ctx.acquireLock();
const buffer = ctx.read(lock);
const data = ctx.fmt.decode(buffer);      // Auto JSON decoding
ctx.done(lock);

// Working with strings
const text = ctx.fmt.encode("hello world");  // UTF-8 bytes
const decoded = ctx.fmt.decode(text);        // Back to string

// Manual buffer work (no more new Uint8Array!)
const results = ctx.fmt.alloc(100);
for (let i = 0; i < results.length; i++) {
  results[i] = i % 2;
}
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
import { Ok, match } from "slang-ts";

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
    self.send({ type: 'PROGRESS', value: i / transactions.length });
  }

  // Tier 2: Deposit the result
  const lock = await ctx.acquireLock();
  ctx.deposit(lock, results);
  ctx.done(lock);
});

reconciler.subscribe((msg) => {
  matchAll(msg, {
    PROGRESS: (m) => println(`Progress: ${m.value * 100}%`),
    DEPOSIT_READY: (m) => {
      const data = reconciler.read(m.address);
      println("Batch reconciled.");
    },
    _: () => {},
  });
});
```

### Scenario 2: AI Media Transcoder

Actors are perfect for "Burst Memory" tasks. Here, an actor converts an image, resetting the heartbeat during intensive pixel manipulation.

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
  
  // Start high-gear work
  const lock = await ctx.acquireLock();
  
  // Pixel-by-pixel transformation with heartbeat
  for (let row = 0; row < rawImage.height; row++) {
    processRow(rawImage, row);
    ctx.heartbeat();  // Explicit heartbeat during CPU-intensive loop
  }

  ctx.deposit(lock, rawImage.buffer);
  ctx.done(lock);
  
  self.send({ type: 'TRANSCODED', fileId: msg.fileId });
});

transcoder.subscribe((msg) => {
  match(msg, {
    TRANSCODED: (m) => println(`Transcoded: ${m.fileId}`),
    _: () => {}
  });
});
```

### Scenario 3: Intent-Based Tool Calling (Agentic)

In an agentic workflow, the Resource Manager acts as the "Tool Belt" for the AI.

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
    Ok: (v) => self.send({ type: 'RESULT', data: { search: searchResult, calc: v.value } }),
    Err: (e) => self.send({ type: 'ERROR', reason: e.error }),
    _: () => {}
  });
});
```

### Scenario 4: Fault-Tolerant Pipeline

Using supervision strategies to build a reliable processing pipeline.

```typescript
const pipeline = supervisor.createGroup({ 
  strategy: 'rest-for-one'  // Restart downstream on upstream failure
});

// Stage 1: Data Ingest
const ingest = pipeline.spawn(async (self, msg, ctx) => {
  const rawData = await ctx.resources.storage.fetch(msg.url);
  const lock = await ctx.acquireLock();
  ctx.deposit(lock, rawData);
  ctx.done(lock);
  self.send({ type: 'INGESTED' });
});

// Stage 2: Transformer (Depends on Ingest)
const transform = pipeline.spawn(async (self, msg, ctx) => {
  match(msg, {
    INGESTED: async () => {
      const data = pipeline.read(msg.address);
      const transformed = processData(data);
      const lock = await ctx.acquireLock();
      ctx.deposit(lock, transformed);
      ctx.done(lock);
    },
    _: () => {}
  });
});

// Stage 3: Output (Depends on Transform)
const output = pipeline.spawn(async (self, msg, ctx) => {
  match(msg, {
    TRANSFORMED: () => {
      const data = pipeline.read(msg.address);
      await ctx.resources.storage.save(msg.outputPath, data);
      self.send({ type: 'COMPLETE' });
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
    const urls = msg.urls;  // Array of 50 URLs
    let htmlData = '';

    for (const url of urls) {
      const html = await ctx.resources.webScraper.fetch(url);
      htmlData += html;
      ctx.heartbeat();  // Keep lease alive during scraping

      if (ctx.isCancelled) break;
    }

    const lock = await ctx.acquireLock();
    ctx.deposit(lock, ctx.fmt.encode(htmlData));
    ctx.done(lock);
    self.send({ type: 'SEARCH_COMPLETE' });
  },
  { name: 'searcher' }
);

// Analyst Agent (Linked to Searcher)
const analyst = supervisor.spawn(
  async (self, msg, ctx) => {
    match(msg, {
      SEARCH_COMPLETE: async () => {
        const lock = msg.address;
        const rawHtml = analyst.read(lock);
        const analysis = await ctx.resources.llm.analyze(rawHtml);
        self.send({ type: 'ANALYSIS_COMPLETE', result: analysis });
      },
      _: () => {}
    });
  },
  { name: 'analyst' }
);

// Link: If Searcher dies, Analyst dies too
await searcher.link(analyst);

// Monitor the swarm
supervisor.subscribe((msg) => {
  match(msg, {
    ActorDown: (info) => println(`Agent ${info.name} died: ${info.reason}`),
    ANALYSIS_COMPLETE: (r) => println("Swarm result:", r.result),
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
  
  // Generate with LLM
  const lock = await ctx.acquireLock();
  const response = await ctx.resources.llm.generate(context);
  
  ctx.deposit(lock, response);
  ctx.done(lock);
  
  self.send({ type: 'COMPLETE' });
});

// Graceful shutdown triggers resource.release() calls
process.on('SIGTERM', () => supervisor.shutdown());
```

---

## 19. Summary Table

| Feature | Description |
| --- | --- |
| **Actor Isolation** | Callbacks are serialized, outer scope not inherited. All state via `msg` or `ctx`. Enables safe termination. |
| **On-Demand Termination** | Any actor can be terminated from userland via `actor.terminate()` or `supervisor.terminateActor(id)`. |
| **Two-Tier Communication** | Tier 1: PubSub (signals, heartbeats, resource intents). Tier 2: Atomic-locked shared memory (zero-copy data). |
| **Memory Pool** | Fixed-size boxes (configurable `poolSize` and `boxSize`). FIFO queuing for lock acquisition. |
| **Opportunistic Cleanup** | Calculate `expiresAt` on lock acquisition. Check and cleanup on any interaction. No setTimeout overhead. |
| **Heartbeats** | Implicit on `self.send()`, `ctx.deposit()`. Explicit via `ctx.heartbeat()`. |
| **Resource Manager** | Proxy-based intent relay. Each resource has `release()` for cleanup. Zod schemas for input/output validation. |
| **Supervision** | one-for-one, one-for-all, rest-for-one strategies with retry configuration. |
| **Linking** | Bi-directional (ctx.link) or uni-directional (ctx.monitor). |
| **Error Handling** | slang-ts Result types. Errors serialized across Tier 1. |
| **Buffer & Serialization** | `ctx.fmt.alloc()`, `ctx.fmt.from()`, `ctx.fmt.encode/decode()` for zero-abstract buffer/serialization DX. |
| **Testing** | Constraint, happy path, non-happy path, and edge case tests per feature. |
| **Diagnostics** | Configurable embedded metrics: actor lifetimes, start times, process lifetimes, lock times, buffer utilization. |

---

## 20. Pitch

> "@nilejs/future turns your Bun/Node runtime into a multi-lane highway. It combines **Atomic Shared Memory** with **Erlang-style Supervision**, letting you build AI agent swarms and high-throughput systems that are physically impossible to build with standard JS. It's not just a library, it is a runtime upgrade for the Agentic Era."

### The Problem It Solves

What happens when your function hangs forever? When your agent enters an infinite loop? When your code crashes your entire backend?

Standard JavaScript has no built-in protection against:
- Infinite loops that hang your event loop
- Functions that consume all memory
- Unreleased resources from crashed code
- Cascading failures that bring down your entire system

### The Solution

@nilejs/future isolates execution in separate threads with automatic cleanup:
- Any actor can be terminated on demand
- Hanging code is automatically killed via heartbeat timeout
- Resource cleanup is guaranteed on termination
- One actor's failure cannot corrupt others

### For Agentic Workflows

* **The "Thinking" Sandbox:** Wrap AI agents in actors. If they enter infinite loops, the Lease and Heartbeat system kills and recovers automatically.
* **Streaming High-Volume Context:** Use Tier 2 Deposits for zero-copy sharing of large context windows between agents.
* **Supervision Trees:** Use rest-for-one to automatically reset downstream agents when upstream fails.

---

*This spec generalizes the core patterns while preserving all implementation details. The architecture balances performance (atomic operations, zero-copy), reliability (supervision strategies, lease system), and developer experience (proxy-based resources, intuitive DSL).*