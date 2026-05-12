# Future

High-performance system-level actor and promise primitives for **Bun** inspired by Erlang.

## Install

```
npm install @nilejs/future
```

**Requirements: Bun v1.0+**, TypeScript v4.5+ (for full type safety)

> **Note:** `@nilejs/future` is designed exclusively for the **Bun runtime**. While the underlying APIs (`worker_threads`, `SharedArrayBuffer`) exist in Node.js, the library relies on Bun's native TypeScript support and worker thread resolution for actor callback serialization. Node.js support is not currently available but may be explored in future releases.

## Quick Example

```typescript
import { createSupervisor, println } from "@nilejs/future";

const supervisor = createSupervisor({
  maxActors: 10,
  memory: { poolSize: 5, boxSize: "1mb" },
});

const actor = supervisor.spawn(async (self, msg, ctx) => {
  // Actor callbacks are serialized — all logic must be inline or via ctx.resources
  const result = msg.input.toUpperCase();

  // Tier 1: lightweight signal via postMessage (string key + optional data)
  self.send("progress", { percent: 0.5 });

  // Tier 2: zero-copy shared memory write — returns Result<Lock, string>
  const writeResult = await ctx.write({
    msg: "result",
    type: "json",
    data: ctx.fmt.encode({ text: result }),
    share: "owner",
  });

  // Send handle to subscribers so they can read the Tier 2 data
  if (writeResult.isOk) {
    self.send("done", { handle: writeResult.value });
  }
});

// Send initial message to the actor
actor.spawn({ input: "hello future" });

actor.subscribe((msg) => {
  // matchAll dispatches on msg.msg field for message objects
  matchAll(msg, {
    progress: (m) => println("Progress:", m.data.percent),
    done: (m) => {
      // Chainable reader API — pass the full message, not just the handle
      const reader = actor.read(m);
      if (reader) {
        println("Result:", reader.json());
        actor.release(m.handle);
      }
    },
    _: () => {},
  });
});
```

## The Problem

What happens when your function hangs forever? When your agent enters an infinite loop? When your code crashes your entire backend?

Standard JavaScript has no built-in protection against:
- Infinite loops that hang your event loop
- Functions that consume all memory
- Unreleased resources from crashed code
- Cascading failures that bring down your entire system

## The Solution

future isolates execution in separate threads with automatic cleanup:
- Any actor can be terminated on demand
- Hanging code is automatically killed via heartbeat timeout
- Resource cleanup is guaranteed on termination
- One actor's failure cannot corrupt others

## Core Concepts

### Supervisor

A supervisor is the orchestrator that manages actor lifecycle. It creates actors, monitors their health, and handles failures according to configured strategies.

```typescript
const supervisor = createSupervisor({
  maxActors: 10,
  memory: { poolSize: 5, boxSize: "1mb" },
  timeouts: { defaultLeaseMs: 5000 },
  resources: { /* resource definitions */ },
  strategy: "one-for-one",
  retry: { max: 3, backoff: "exponential" },
});
```

The supervisor provides:
- Actor creation and lifecycle management
- Heartbeat and timeout monitoring
- Shared memory pool management with BoxEntry[] state tracking
- Per-actor inbox routing with authorization
- Failure handling and supervision strategies

### What is an Actor?

An actor is an isolated execution context running in a separate thread. Actors communicate via messages and can share data through shared memory.

```typescript
const actor = supervisor.spawn(async (self, msg, ctx) => {
  // self: actor control (send messages, terminate)
  // msg: incoming message data
  // ctx: execution context (resources, shared memory, formatting)
});
```

Each actor has:
- Isolated memory and event loop
- Lifecycle tied to supervisor
- Ability to send messages and spawn children
- Access to resources via proxy

### Context (ctx)

The context is injected into every actor callback. It provides everything the actor needs to function:

```typescript
const actor = supervisor.spawn(async (self, msg, ctx) => {
  // Resources (proxy to main-thread services)
  const users = await ctx.resources.db.query({ sql: "SELECT * FROM users" });

  // Shared memory write — single async call, returns Result<Lock, string>
  const result = await ctx.write({
    msg: "result",
    type: "json",
    data: ctx.fmt.encode({ key: "value" }),
    share: "group",
  });

  // Formatting utilities (no need to import)
  const buffer = ctx.fmt.alloc(1024);
  const encoded = ctx.fmt.encode({ key: "value" });

  // Actor control
  self.send("progress", { percent: 0.5 }); // Tier 1 signal
  ctx.heartbeat();      // Keep lease alive during long operations
  ctx.terminate();      // Terminate self
  ctx.isCancelled;      // Check if terminated

  // Spawn child actor
  const child = await ctx.spawn(childCallback);
  ctx.link(child);      // Bi-directional failure propagation
  ctx.monitor(child);   // Uni-directional notification
});
```

### Communication: Two-Tier System

Communication happens in two tiers:

**Tier 1 (Control)**: Lightweight signals via postMessage
- Small messages, progress updates, heartbeats
- `self.send("eventName", data)` — string key matching with auto-encoded JSON data
- `from` field auto-injected by supervisor (sender never sets it)
- Resource method calls (intent relay)

**Tier 2 (Data)**: Zero-copy shared memory
- Large data transfers without serialization
- `ctx.write()` — single call for write/commit, returns `Result<Lock, string>`
- `ctx.read(msg)` / `actor.read(msg)` — sync, returns `ChainableReader | null`
- `ctx.release(handle)` — ref-counted cleanup, box becomes FREE when count reaches 0
- Immutable data after commit — no READING state, no CAS, no atomic contention

```typescript
// Tier 1: Send a signal (string key + optional data)
self.send("progress", { percent: 0.5 });

// Tier 2: Share large data — single call
const result = await ctx.write({
  msg: "result",
  type: "json",
  data: ctx.fmt.encode({ large: "payload" }),
  share: "owner",
});
if (result.isOk) {
  self.send("done", { handle: result.value });
}
```

### Message Shape

All messages follow a standardized format:

```typescript
type Message = {
  readonly msg: string;           // Message key (e.g., "result", "progress")
  readonly type?: FmtType;        // "json" | "string" | "binary" | "cbor"
  readonly data?: unknown;        // Tier 1: deserialized JSON data
  readonly handle?: Lock;         // Tier 2: reference to shared memory box
  readonly from: ActorId;         // Auto-injected by supervisor (always present)
};
```

- `msg` is always required — it identifies the message kind
- `from` is always injected by the supervisor, never set by the sender
- `handle` is present when the message refers to Tier 2 data (read via `actor.read(msg)`)
- `data` is present for Tier 1 messages (small JSON payloads)

### Lock

A lock is a lightweight reference to a shared memory box:

```typescript
type Lock = {
  readonly boxIndex: number;  // Index into the supervisor's BoxEntry array
  readonly epoch: number;     // Monotonically increasing — prevents use-after-free
};
```

The epoch ensures that stale handles cannot access recycled boxes. When a box is freed and reallocated, its epoch increments, invalidating any old references.

### Chainable Reader API

Reading from shared memory is synchronous and chainable:

```typescript
// On the main thread (actor.read takes the full message)
const reader = actor.read(msg);
if (reader) {
  const json = reader.json();
  const str = reader.string();
  const bin = reader.binary();
  const cbor = reader.cbor();
  const raw = reader.raw(); // Raw Uint8Array
}

// Inside an actor callback (ctx.read takes the full message)
const reader = ctx.read(msg);
if (reader) {
  const value = reader.json();
}
```

The reader API returns a decoder object with chainable format methods. All reads are zero-copy — they return views into the shared `Uint8Array` without allocating new buffers.

If `msg.handle` is absent (Tier 1 message), `ctx.read(msg)` returns `null`.

### Send API

```typescript
// self.send(msg, data?) — inside actor callback
//   msg: string — message key for matchAll dispatch
//   data: optional — automatically structured-cloned via postMessage
self.send("progress", { percent: 0.5 });
self.send("done");

// actor.spawn(msg) — send initial message from main thread
actor.spawn({ input: "process this" });
```

For Tier 2 transfers, use `ctx.write()` and the supervisor routes the handle via INBOX to authorized readers.

### Message Dispatch with matchAll

Use `matchAll` to dispatch on message types. It works with both primitives and message objects:

```typescript
import { matchAll } from "slang-ts";

// Message objects — dispatches on msg.msg field
actor.subscribe((msg) => {
  matchAll(msg, {
    progress: (m) => println("Progress:", m.data.percent),
    done: (m) => {
      const reader = actor.read(m);
      if (reader) println("Result:", reader.json());
      actor.release(m.handle);
    },
    _: () => {},
  });
});

// Inside actor callback — dispatch on incoming message
matchAll(msg, {
  ingested: (m) => {
    const data = ctx.read(m).json();
    ctx.release(m.handle);
  },
  _: () => {},
});
```

## Key Features

**Fault Tolerance and Safety**
- Automatic lease system that terminates stalled actors
- Configurable heartbeat timeouts to detect hung code
- On-demand actor termination from userland code
- Isolation prevents one actor crash from affecting others
- Resource cleanup guarantees on actor termination

**Concurrency and Parallelism**
- Two-tier communication: lightweight signals and zero-copy data transfer
- Memory pool with immutable-after-commit semantics — no READING state, no CAS contention
- Ref-counted box lifecycle (FREE → WRITING → READY → FREE)
- Per-actor inbox routing with authorization

**Execution Safety**
- Actor callbacks are serialized, outer scope is not inherited
- All state must be passed via message or accessed through context
- This isolation enables safe concurrent execution and termination

**Formatting Utilities (ctx.fmt)**
- Buffer allocation without thinking about Uint8Array
- Automatic encoding and decoding (JSON, string, binary, CBOR)
- Available directly on context, no separate imports
- Typed allocations for common data types

## Erlang Inspired

future draws from Erlang's proven concurrency model:

**Let It Crash**: Rather than defensive programming, actors can fail and the supervisor handles recovery. Code does not need to handle every possible error state.

**Supervision Trees**: Actors are organized hierarchically. When a parent fails, children are handled according to the supervision strategy.

**Actor Isolation**: Each actor has its own memory and cannot corrupt others. Failures are contained.

**Linking**: Actors can be linked so that when one dies, others are terminated. This creates "suicide pacts" for dependent actors.

**Monitoring**: Actors can monitor others without linking. When a monitored actor dies, the watcher receives a notification.

Supervision strategies:
- **one-for-one**: Restart only the failed actor
- **one-for-all**: Restart all actors in the group
- **rest-for-one**: Restart actors started after the failed one

## Authorization (ShareConfig)

Access to shared memory boxes is controlled by `ShareConfig`:

```typescript
type ShareConfig =
  | "owner"       // Only the writing actor can read (default)
  | "group"       // All actors in the same supervision group can read
  | "linked"      // Only actors linked to the writer can read
  | ActorId[];    // Explicit allowlist of actor IDs
```

```typescript
// Owner-only access (default)
const result = await ctx.write({
  msg: "private",
  type: "json",
  data: ctx.fmt.encode(sensitiveData),
  share: "owner",
});

// Group-wide access
const result = await ctx.write({
  msg: "shared",
  type: "binary",
  data: teamData,
  share: "group",
});

// Explicit allowlist
const result = await ctx.write({
  msg: "targeted",
  type: "cbor",
  data: targetedData,
  share: [actorA.id, actorB.id],
});
```

The supervisor enforces authorization at the inbox routing layer. If an actor attempts to read a box it is not authorized for, the read is silently dropped.

## slang-ts Integration

[slang-ts](https://github.com/Hussseinkizz/slang) is an external TypeScript library that provides functional patterns. All code uses slang-ts patterns for cleaner, safer code:

```typescript
import { match, matchAll } from "slang-ts";

// match instead of if/switch — for Result/Option types
match(result, {
  Ok: (v) => println("Success:", v.value),
  Err: (e) => println("Error:", e.error),
});

// matchAll for message objects — dispatches on msg.msg field
matchAll(msg, {
  progress: (m) => println("Progress:", m.data.percent),
  done: (m) => println("Done"),
  _: () => {},
});

// Result types for explicit error handling
const writeResult = await ctx.write({ msg: "r", type: "json", data });
match(writeResult, {
  Ok: (l) => { /* use lock */ },
  Err: (e) => println("Pool exhausted"),
});
```

This replaces imperative control flow with declarative pattern matching.

## Resource Manager

Resources are services available to actors but running on the main thread. The Resource Manager provides safe access through intent relay.

```typescript
import { z } from "zod";

const supervisor = createSupervisor({
  resources: {
    database: {
      query: {
        input: z.object({ sql: z.string() }),
        output: z.array(z.unknown()),
        handler: async ({ sql }) => await db.query(sql),
      },
      release: async () => await db.close(),
    },
  },
});

// Inside an actor callback:
const results = await ctx.resources.database.query({ sql: "SELECT 1" });
```

How it works:
1. Actor calls `ctx.resources.db.query(...)`
2. Proxy intercepts and sends intent to main thread
3. Main thread validates with Zod, executes the handler
4. Result is sent back to actor

Benefits:
- Database connections stay on main thread
- Resource access is validated via Zod schemas
- Resources are cleaned up on shutdown via release hooks
- Actors cannot directly access shared state

## How It Works

```typescript
import { createSupervisor, println } from "@nilejs/future";
import { matchAll } from "slang-ts";

// Actor callbacks are serialized, outer scope is NOT available
const config = { timeout: 5000 };

// WRONG: outer scope is lost on serialization
const badActor = supervisor.spawn(async (self, msg, ctx) => {
  // config === undefined (outer scope lost)
});

// RIGHT: pass state via msg, use inline logic or resources
const actor = supervisor.spawn(async (self, msg, ctx) => {
  const timeout = msg.timeout;  // From message
  const data = await ctx.resources.storage.get(msg.dataId);  // From resources

  // Heavy processing with heartbeat — all logic must be inline
  for (let i = 0; i < data.length; i++) {
    data[i] = data[i] * 2;  // inline transform, no outer scope
    if (i % 1000 === 0) ctx.heartbeat();
  }

  // Tier 1: lightweight notification
  self.send("progress", { percent: 0.5 });

  // Tier 2: commit result to shared memory
  const result = await ctx.write({
    msg: "result",
    type: "json",
    data: ctx.fmt.encode({ processed: true }),
    share: "owner",
  });

  if (result.isOk) {
    self.send("done", { handle: result.value });
  }
});

// Spawn with state
actor.spawn({ timeout: 5000, dataId: "abc123" });

// Subscribe to messages
actor.subscribe((msg) => {
  matchAll(msg, {
    progress: (m) => println("Progress:", m.data.percent),
    done: (m) => {
      const reader = actor.read(m);
      if (reader) {
        println("Result:", reader.json());
        actor.release(m.handle);
      }
    },
    _: () => {},
  });
});
```

## Termination vs AbortController

This is NOT the same as AbortController.

AbortController just ignores results, the execution continues running in the background until completion. It does not stop anything.

Actor termination in future kills the thread immediately. The execution stops right there. Resources are cleaned up. This is true termination.

Most libraries in TypeScript and JavaScript cannot do this. future and Effect-TS are among the few that support true on-demand termination of running code.

## Termination Guarantees

When an actor terminates, the following happens in order:

1. **Worker thread killed** -- `worker.terminate()` stops the thread immediately. No cleanup hooks run inside the worker. Pending async operations are abandoned.
2. **Boxes force-released** -- All shared memory boxes where the terminated actor is writer or reader are reset to FREE. Writer boxes are reclaimed immediately. Reader refs are decremented; boxes reaching zero refs are freed.
3. **Inboxes cleared** -- The actor's per-actor inbox queue is removed. Undelivered messages are discarded.
4. **Linked actors terminated** -- All actors linked via `ctx.link()` are recursively terminated with reason `linked_actor_died`. Termination cascades through the link graph.
5. **Monitors notified** -- All actors monitoring the terminated actor receive a `DOWN` message with the terminated actor's ID and reason.
6. **Write queue entries removed** -- Pending write queue requests from the terminated actor are filtered out. Remaining queued writes are served.
7. **Group manager notified** -- If termination was not a clean shutdown, the group manager applies the supervision strategy (restart or cascade failure).

```typescript
// Termination triggered from anywhere:
actor.terminate();                    // Main thread
ctx.terminate();                      // Self-terminate inside actor
supervisor.terminateActor(actor.id);  // Supervisor-level

// Monitors receive a DOWN message:
actor.subscribe((msg) => {
  matchAll(msg, {
    DOWN: (m) => console.log(`Actor ${m.data.id} died: ${m.data.reason}`),
    _: () => {},
  });
});
```

## Shared Memory

Zero-copy data transfer between actors using write/read/release:

```typescript
const pipeline = supervisor.createGroup({ strategy: "one-for-one" });

const producer = pipeline.spawn(async (self, msg, ctx) => {
  const buffer = ctx.fmt.alloc(msg.size);

  // Write data directly to shared buffer — inline computation only
  for (let i = 0; i < msg.size; i++) {
    buffer[i] = i % 256;
  }

  // Single call: write, commit, and get a Lock reference
  const result = await ctx.write({
    msg: "data",
    type: "binary",
    data: buffer,
    share: "group",  // Share with group members
  });

  if (result.isOk) {
    self.send("ready", { handle: result.value });
  }
});

const consumer = pipeline.spawn(async (self, msg, ctx) => {
  // When we receive a message with a handle, read synchronously
  matchAll(msg, {
    data: () => {
      const reader = ctx.read(msg);
      if (reader) {
        const bytes = reader.raw();
        println("Received", bytes.length, "bytes");
        ctx.release(msg.handle);
      }
    },
    _: () => {},
  });
});
```

Box lifecycle (3 states):
- **FREE**: Box is available for allocation
- **WRITING**: Actor is writing data to the box
- **READY**: Data is committed and available for authorized readers

Immutable data after commit: once a box enters READY state, the data is never modified. No READING state, no CAS operations, no atomic contention. The supervisor tracks state in plain `BoxEntry[]` objects (no SAB state board).

## Supervision

Resilient actor hierarchies:

```typescript
import { matchAll } from "slang-ts";

const pipeline = supervisor.createGroup({
  strategy: "rest-for-one",  // Restart downstream on upstream failure
  retry: { max: 3, backoff: "exponential" },
});

// Stage 1: Fetch data and write to Tier 2
const ingest = pipeline.spawn(async (self, msg, ctx) => {
  const data = await ctx.resources.http.get({ url: msg.url });
  const result = await ctx.write({
    msg: "ingested",
    type: "binary",
    data: data,
    share: "group",
  });
  if (result.isOk) {
    self.send("stage1_done", { handle: result.value });
  }
});

// Stage 2: Read from shared memory and transform
const transform = pipeline.spawn(async (self, msg, ctx) => {
  matchAll(msg, {
    ingested: (m) => {
      const reader = ctx.read(m);
      if (!reader) return;
      const raw = reader.raw();
      const transformed = raw.map((b) => b * 2);
      ctx.release(m.handle);

      ctx.write({
        msg: "transformed",
        type: "binary",
        data: transformed,
        share: "group",
      }).then((writeResult) => {
        if (writeResult.isOk) {
          self.send("stage2_done", { handle: writeResult.value });
        }
      });
    },
    _: () => {},
  });
});
```

## Supervisor API

```typescript
// Spawn an actor
const actor = supervisor.spawn(callback, { name: "my-actor" });

// Create a supervision group
const group = supervisor.createGroup({
  strategy: "one-for-all",
  retry: { max: 3, backoff: "exponential" },
});

// Terminate a specific actor by ID
supervisor.terminateActor(actor.id);

// Subscribe to all actor messages (system-wide)
const unsub = supervisor.subscribe((msg) => { ... });

// Graceful shutdown — terminates all actors, calls resource release hooks
await supervisor.shutdown();

// Get supervisor-level diagnostics
const diag = supervisor.getDiagnostics();
if (diag.isOk) {
  console.log("Active actors:", diag.value.activeActors);
  console.log("Pool utilization:", diag.value.memoryPool.utilization);
}
```

## ActorRef API

```typescript
// Send initial message to actor
actor.spawn({ input: "process this" });

// Subscribe to actor messages
const unsub = actor.subscribe((msg) => { ... });

// Terminate actor immediately
actor.terminate();

// Read Tier 2 data from message — returns ChainableReader | null
const reader = actor.read(msg);
if (reader) {
  const data = reader.json();
  actor.release(msg.handle);
}

// Get per-actor diagnostics
const diag = actor.getDiagnostics();

// Link (bi-directional) or monitor (uni-directional)
actor.link(otherActor);
actor.monitor(otherActor);
```

## Context API (inside actor callbacks)

| Method | Tier | Description |
|---|---|---|
| `self.send(msg, data?)` | 1 | Send signal/status to subscribers. Auto-injects `from`. |
| `ctx.write({ msg, type, data, share? })` | 2 | Write data to SAB. Returns `Result<Lock, string>`. |
| `ctx.read(msg)` | 2 | Read SAB data from message. Returns `ChainableReader \| null`. |
| `ctx.release(handle)` | 2 | Decrement ref count. If 0 then FREE. |
| `ctx.heartbeat()` | 1 | Explicitly reset lease timer during CPU-intensive loops. |
| `ctx.resources.*` | 1 | Access resources via Proxy. Calls go through intent relay. |
| `ctx.link(actor)` | 1 | Create bi-directional link. If either dies, both die. |
| `ctx.monitor(actor)` | 1 | Create uni-directional monitor. Receive notification when actor dies. |
| `ctx.spawn(callback)` | 1 | Spawn a child actor. |
| `ctx.terminate()` | 1 | Terminate the actor gracefully. |
| `ctx.isCancelled` | 1 | Check if actor has been terminated. |
| `ctx.fmt.*` | N/A | Buffer allocation and serialization utilities. |

### Child Actor Operations

Inside an actor callback, `ctx.spawn()` creates a child actor in a new worker thread:

```typescript
const child = await ctx.spawn(async (selfChild, msgChild, ctxChild) => {
  matchAll(msgChild, {
    task: (m) => {
      const result = (m.data as number) * 2;
      selfChild.send("done", { value: result });
    },
    _: () => {},
  });
});

self.send("child_spawned", { childId: child.id });
child.spawn({ value: 42 });  // Send initial message to child
```

The child `ActorRef` from `ctx.spawn()` has limited API in worker context -- `spawn()`, `terminate()`, `link()`, and `monitor()` work; `subscribe()`, `read()`, and `release()` throw `Not available in worker context`.

**Linking** creates a bi-directional suicide pact -- when either linked actor dies, the other is terminated immediately:

```typescript
ctx.link(child);  // Each dies if the other dies
```

**Monitoring** is uni-directional notification -- the monitor receives `DOWN` when the monitored actor terminates, but the monitored actor is unaffected:

```typescript
ctx.monitor(child);  // Receive DOWN when child dies

// Subscribe handler receives:
matchAll(msg, {
  DOWN: (m) => console.log(`Actor ${m.data.id} died: ${m.data.reason}`),
  _: () => {},
});
```

Main-thread equivalents via `ActorRef`:

```typescript
actorA.link(actorB);      // Bi-directional termination propagation
actorA.monitor(actorB);   // Uni-directional monitoring
```

Under the hood, `handleLink` adds each actor ID to the other's `linkedActors` set. `handleMonitor` only adds to the monitor's `monitoredActors` set. On termination, all linked actors are recursively terminated, and every subscriber of monitoring actors receives a `DOWN` message.

## Buffer & Serialization (ctx.fmt)

```typescript
// Allocate buffers
const buf = ctx.fmt.alloc(1024);
const u8 = ctx.fmt.alloc.u8(256);
const i32 = ctx.fmt.alloc.i32(64);
const f64 = ctx.fmt.alloc.f64(32);

// Auto-detect encoding
const encoded = ctx.fmt.encode({ key: "value" });  // object → JSON bytes
const decoded = ctx.fmt.decode(encoded);             // bytes → object

// Create buffer from data
const buf = ctx.fmt.from("hello");           // string → UTF-8 bytes
const buf = ctx.fmt.from({ name: "kizz" });  // object → JSON bytes

// Explicit codecs
const json = ctx.fmt.json.encode({ a: 1 });
const obj = ctx.fmt.json.decode(json);
const str = ctx.fmt.string.encode("hello");
const text = ctx.fmt.string.decode(str);
// CBOR is available if configured (throws by default)
```

## Diagnostics

```typescript
const supervisor = createSupervisor({
  diagnostics: {
    enabled: true,
    sampleRate: 1.0,  // 0.0-1.0, 1.0 = 100%
    track: {
      actorLifetimes: true,
      writeQueueDepth: true,
      bufferUtilization: true,
      authorizationEvents: true,
      inboxDepth: true,
      resourceCallLatency: true,
    },
  },
});

// Periodic reporting
setInterval(() => {
  const stats = supervisor.getDiagnostics();
  if (stats.isOk) {
    console.log("Active:", stats.value.activeActors);
    console.log("Pool:", stats.value.memoryPool);
  }
}, 5000);

// Per-actor diagnostics
const diag = actor.getDiagnostics();
if (diag.isOk) {
  console.log(`Lifetime: ${diag.value.lifetimeMs}ms`);
  console.log(`Heartbeats: ${diag.value.heartbeatCount}`);
}
```

### Diagnostics Configuration Reference

All available per-metric toggles:

```typescript
const supervisor = createSupervisor({
  diagnostics: {
    enabled: true,
    sampleRate: 0.5,  // 0.0-1.0, gates tracking via Math.random() < sampleRate
    track: {
      actorLifetimes: true,      // Actor start/stop timestamps
      startTimes: true,           // Actor creation time
      processLifetimes: true,     // Worker thread uptime
      writeQueueWait: true,       // Box allocation wait time
      writeQueueDepth: true,      // Pending write queue length
      messageLatency: true,       // Send-to-receive latency
      bufferUtilization: true,    // Memory pool usage
      heartbeatIntervals: true,   // Time between heartbeats
      resourceCallLatency: true,  // Resource handler duration
      authorizationEvents: true,  // Granted vs denied reads
      inboxDepth: true,           // Per-actor inbox queue size
      refCountHistory: true,      // Box reference count history
    },
  },
});
```

Diagnostics are zero-cost when disabled (`enabled: false` produces a no-op collector). The `sampleRate` gates tracking via `Math.random() < sampleRate`, reducing overhead at scale by sampling a fraction of events. When a metric is omitted from `track`, it is not collected regardless of `sampleRate`.

## Constraints

When using future, keep these constraints in mind:

**Data Transfer**
- Arguments and return values must be cloneable (JSON-compatible or Uint8Array)
- Large data should use Tier 2 (shared memory) rather than serialization
- Resource methods must declare input and output validation schemas

**Execution Model**
- Actor callbacks are serialized, outer scope is not available
- All state must be passed via message or accessed through context
- `self.send(msg, data?)` — msg is a string key, data is optional
- Heartbeat timeout defaults to 5000 milliseconds (configurable)

**Shared Memory**
- `ctx.write()` returns `Promise<Result<Lock, string>>` — handle with `result.isOk` check
- `ctx.read(msg)` takes the full message, returns `ChainableReader | null` — sync, chainable
- `ctx.release(handle)` decrements ref count; box becomes FREE when count reaches 0
- Data is immutable after commit — no in-place mutations

**Configuration Required**
- Memory pool requires explicit poolSize and boxSize configuration
- Resources must define input and output schemas
- Supervision strategy must be chosen for actor groups

## Performance Characteristics

**Concurrency Model**
- N isolated threads with their own event loops
- Write-once shared memory removes atomic contention entirely
- Ref-counted box lifecycle (FREE → WRITING → READY → FREE) — no CAS operations
- Box state tracked in plain `BoxEntry[]` objects on supervisor side — no SAB overhead
- Heartbeat system enables automatic recovery from stalled execution

**Fault Tolerance**
- Lease expiration prevents indefinite resource holding
- Supervision strategies (one-for-one, one-for-all, rest-for-one) control failure blast radius
- Linking enables cascading termination for dependent actors
- Monitoring provides failure notification without forced termination
- Resource isolation prevents main-thread crashes from affecting actors

**Isolation Guarantees**
- Each actor runs in separate thread with isolated memory space
- Resource access mediated through serialized intent packets
- Immutable shared memory after commit — no concurrent modification possible
- Actor termination includes guaranteed buffer cleanup
- Failure domains bounded by supervision tree structure

## Use Cases

future is ideal for:

**AI Agent Systems**
- Running agents that can hang or enter infinite loops
- Sharing large context windows between agents
- Managing multi-agent workflows with dependencies
- Streaming token generation that must be interruptible

**Data Processing Pipelines**
- Parallel transformation stages that should not block each other
- ETL workflows requiring resilience to individual stage failures
- Media processing (image, video, audio) with isolated actors
- Real-time data enrichment with automatic cleanup

**Background Job Processing**
- Jobs that must be cancellable on demand
- Queue systems where job failures should not halt processing
- Long-running tasks with configurable timeout and retry
- Resource cleanup guarantees when jobs complete or fail

**Safe Code Execution**
- Running untrusted or unpredictable code
- Plugins or extensions with limited trust
- Sandboxed execution environments
- Any scenario where hanging code must be killed safely

---

## Contributing

Contributions are welcome. This is an open source project.

## License

MIT License

## Status

This project is work in progress. APIs may change as the library evolves.
