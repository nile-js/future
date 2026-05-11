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

  // Tier 2: zero-copy shared memory write
  const handle = await ctx.write({
    msg: "result",
    type: "json",
    data: ctx.fmt.encode({ text: result }),
    share: "owner",
  });

  // Send the handle via Tier 1 so subscribers can read the Tier 2 data
  self.send("done", { handle });
});

// Send initial message to the actor
actor.spawn({ input: "hello future" });

actor.subscribe((msg) => {
  // Chainable reader API — sync, zero-copy read from shared memory
  const reader = actor.read(msg);
  if (reader) {
    const value = reader.json();
    println("Result:", value);
    actor.release(msg.handle);
  }
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
  resources: { /* resource definitions */ },
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
// Resources (proxy to main-thread services)
ctx.resources.db.query({ sql: "SELECT * FROM users" });

// Shared memory write — single async call, returns Lock
const lock = await ctx.write({
  msg: "result",
  type: "json",
  data: encodedData,
  share: "group",
});

// Shared memory read — sync, returns chainable decoder (pass full message, not lock)
const reader = ctx.read(m);
if (reader) {
  const value = reader.json();
  ctx.release(m.handle); // Decrements ref count, frees box at 0
}

// Formatting utilities (no need to import)
const buffer = ctx.fmt.alloc(1024);
const encoded = ctx.fmt.encode({ key: "value" });

// Actor control
self.send("progress", { percent: 0.5 }); // Tier 1 signal
ctx.heartbeat();      // Keep lease alive during long operations
ctx.terminate();      // Terminate self
ctx.isCancelled;      // Check if terminated
```

### Communication: Two-Tier System

Communication happens in two tiers:

**Tier 1 (Control)**: Lightweight signals via postMessage
- Small messages, progress updates, heartbeats
- `self.send("eventName", data)` — string key matching with auto-encoded JSON data
- `from` field auto-injected by supervisor (sender never sets it)
- Resource method calls (intent relay)
- PubSub pattern for subscribers
- Per-actor inbox routing with authorization

**Tier 2 (Data)**: Zero-copy shared memory
- Large data transfers without serialization
- `ctx.write()` — single call for write/commit, returns `Lock`
- `ctx.read()` / `actor.read()` — sync, returns chainable decoder
- `ctx.release()` — ref-counted cleanup, box becomes FREE when count reaches 0
- Immutable data after commit — no READING state, no CAS, no atomic contention

```typescript
// Tier 1: Send a signal (string key + optional data)
self.send("progress", { percent: 0.5 });

// Tier 2: Share large data — single call
const lock = await ctx.write({
  msg: "result",
  type: "json",
  data: encodedLargeData,
  share: "owner",
});
```

### Message Shape

All messages follow a standardized format:

```typescript
type Message = {
  msg: string;                // Message key (e.g., "result", "progress")
  type?: FmtType;             // "json" | "string" | "binary" | "cbor"
  data?: unknown;             // Tier 1: deserialized JSON data
  handle?: Lock;              // Tier 2: reference to shared memory box
  from: ActorId;              // Auto-injected by supervisor (always present)
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
  boxIndex: number;  // Index into the supervisor's BoxEntry array
  epoch: number;     // Monotonically increasing — prevents use-after-free
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
  const raw = reader.raw(); // Raw Uint8Array
}

// Inside an actor callback (ctx.read takes the full message)
const reader = ctx.read(m);
if (reader) {
  const value = reader.json();
}
```

The reader API returns a decoder object with chainable format methods. All reads are zero-copy — they return views into the shared `Uint8Array` without allocating new buffers.

### Send API

```typescript
// self.send(msg, data?) — inside actor callback
//   msg: string — message key for inbox routing
//   data: optional — automatically JSON-encoded for Tier 1 delivery
self.send("progress", { percent: 0.5 });
self.send("done");

// actor.spawn(msg) — send initial message from main thread
actor.spawn({ input: "process this" });
```

When `data` is provided, it is auto-encoded as JSON and delivered via Tier 1 (postMessage). For Tier 2 transfers, use `ctx.write()` and the supervisor routes the handle via INBOX to authorized readers.

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
const lock = await ctx.write({
  msg: "private",
  type: "json",
  data: sensitiveData,
  share: "owner",
});

// Group-wide access
const lock = await ctx.write({
  msg: "shared",
  type: "binary",
  data: teamData,
  share: "group",
});

// Explicit allowlist
const lock = await ctx.write({
  msg: "targeted",
  type: "cbor",
  data: targetedData,
  share: [actorA.id, actorB.id],
});
```

The supervisor enforces authorization at the inbox routing layer. If an actor attempts to read a box it is not authorized for, the read is denied.

## slang-ts Integration

[slang-ts](https://github.com/Hussseinkizz/slang) is an external TypeScript library that provides functional patterns. All code uses slang-ts patterns for cleaner, safer code:

```typescript
import { match, matchAll } from "slang-ts";
import { println } from "@nilejs/future";

// match instead of if/switch — for Result/Option types
match(result, {
  Ok: (v) => println("Success:", v.value),
  Err: (e) => println("Error:", e.error),
});

// matchAll for tagged unions — for plain message objects
matchAll(msg, {
  progress: (m) => println("Progress:", m.data.percent),
  done: (m) => println("Done, handle:", m.handle),
  _: () => {},  // Default case
});

// Result types for explicit error handling
const lockResult = await ctx.write({ msg: "r", type: "json", data });
match(lockResult, {
  Ok: (l) => { /* use lock */ },
  Err: (e) => println("Pool exhausted"),
});
```

This replaces imperative control flow with declarative pattern matching.

## Resource Manager

Resources are services available to actors but running on the main thread. The Resource Manager provides safe access through intent relay.

```typescript
import { z } from "zod";

// Assume `db` is your real database client (e.g., pg, drizzle, etc.)
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
3. Main thread executes the handler
4. Result is sent back to actor

Benefits:
- Database connections stay on main thread
- Resource access is validated via Zod schemas
- Resources are cleaned up on shutdown via release hooks
- Actors cannot directly access shared state

## How It Works

```typescript
import { createSupervisor, println } from "@nilejs/future";

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
  const handle = await ctx.write({
    msg: "result",
    type: "json",
    data: ctx.fmt.encode({ processed: true }),
    share: "owner",
  });

  self.send("done", { handle });
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
        const result = reader.json();
        println("Result:", result);
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

## Shared Memory

Zero-copy data transfer between actors using write/read/release:

```typescript
const producer = supervisor.spawn(async (self, msg, ctx) => {
  const buffer = ctx.fmt.alloc(msg.size);

  // Write data directly to shared buffer — inline computation only
  for (let i = 0; i < msg.size; i++) {
    buffer[i] = i % 256;
  }

  // Single call: write, commit, and get a Lock reference
  const handle = await ctx.write({
    msg: "data",
    type: "binary",
    data: buffer,
    share: "owner",
  });

  self.send("ready", { handle });
});

const consumer = supervisor.spawn(async (self, msg, ctx) => {
  // When we receive a message with a handle, read synchronously
  const reader = ctx.read(msg);
  if (reader) {
    const data = reader.raw();
    println("Received", data.length, "bytes");
    ctx.release(msg.handle);  // Release our reference
  }
});

producer.spawn({ size: 1000000 });
```

Box lifecycle (3 states):
- **FREE**: Box is available for allocation
- **WRITING**: Actor is writing data to the box
- **READY**: Data is committed and available for authorized readers

Immutable data after commit: once a box enters READY state, the data is never modified. No READING state, no CAS operations, no atomic contention. The supervisor tracks state in plain `BoxEntry[]` objects (no SAB state board).

## Supervision

Resilient actor hierarchies:

```typescript
import { matchAll } from "@nilejs/future";

const pipeline = supervisor.createGroup({
  strategy: "rest-for-one",  // Restart downstream on upstream failure
  retry: { max: 3, backoff: "exponential" },
});

// Stage 1: Fetch data and write to Tier 2
const ingest = pipeline.spawn(async (self, msg, ctx) => {
  const data = await ctx.resources.http.get({ url: msg.url });
  const handle = await ctx.write({
    msg: "ingested",
    type: "binary",
    data: data,
    share: "group",
  });
  self.send("stage1_done", { handle });
});

// Stage 2: Read from shared memory and transform
const transform = pipeline.spawn(async (self, msg, ctx) => {
  const reader = ctx.read(msg);
  if (!reader) return;
  const raw = reader.raw();
  const transformed = raw.map((b) => b * 2);
  ctx.release(msg.handle);  // Release input reference

  const handle = await ctx.write({
    msg: "transformed",
    type: "binary",
    data: transformed,
    share: "group",
  });
  self.send("stage2_done", { handle });
});

// Main thread subscriber reads Tier 2 data
ingest.subscribe((msg) => {
  matchAll(msg, {
    stage1_done: (m) => {
      // Forward the message to the next stage
      transform.spawn(m);
    },
    _: () => {},
  });
});
```

## Constraints

When using future, keep these constraints in mind:

**Data Transfer**
- Arguments and return values must be cloneable (JSON-compatible or Uint8Array)
- Large data should use Tier 2 (shared memory) rather than serialization
- Resource methods must declare input and output validation schemas

**Execution Model**
- Actor callbacks are serialized, outer scope is not available
- All state must be passed via message or accessed through context
- `self.send(msg, data?)` — msg is a string key, data is optional and auto-encoded as JSON
- Heartbeat timeout defaults to 5000 milliseconds (configurable)

**Shared Memory**
- `ctx.write()` returns `Promise<Result<Lock, string>>` — single call for write/commit
- `ctx.read(m)` takes the full message, returns `ChainableReader | null` — sync, chainable
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
