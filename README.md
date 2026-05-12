# Future

High-performance, system-level actor and tasks runtime for TypeScript, inspired by Erlang.

## Briefing

You have seen a function hang forever. The event loop is blocked. Your server freezes. No request gets through. There is nothing you can do but wait for the process manager to kill the whole thing.

`AbortController` does not help here. It ignores results while code keeps running. The thread is still stuck. Memory is still held. Cleanup never runs.

Future gives you true termination. When you kill an actor, the worker thread dies. All resources are released. Other actors keep running. No shared state to corrupt. No cascading failure. The problem stops exactly when you say so.

## Core Concepts

A **supervisor** manages the entire actor system. It creates actors, monitors their health, handles failures, and owns the shared memory pool. You start here.

An **actor** is an isolated execution context running in its own thread. Actors do not share memory. They communicate via messages. Each actor has a unique ID, a message inbox, and lifecycle controlled by the supervisor.

Actors can:
- Send signals to subscribers via `self.send(msg, data?)` (Tier 1, lightweight)
- Share large data via `ctx.write()` and `ctx.read()` (Tier 2, zero-copy shared memory)
- Access main-thread resources through `ctx.resources` (proxy with schema validation)
- Spawn child actors, link to other actors, monitor failures

The supervisor enforces boundaries. Actors cannot access each other's memory directly. All communication goes through the supervisor's message routing and authorization layer.

## Featured Example

An actor performs computation that might hang. The supervisor spawns it, you send it work, and if it takes too long you terminate it. Resources clean up. Other actors are unaffected.

```typescript
import { createSupervisor, println, matchAll } from "@nilejs/future";

const supervisor = createSupervisor({
  maxActors: 10,
  memory: { poolSize: 5, boxSize: "1mb" },
  timeouts: { defaultLeaseMs: 5000 },
});

// Spawn an actor that processes data
const actor = supervisor.spawn(async (self, msg, ctx) => {
  self.send("started", { input: msg.input });

  // Heavy computation (or external call that might hang)
  const result = await fragileComputation(msg.input);
  const encoded = ctx.fmt.encode({ text: result });

  // Write result to shared memory
  const writeResult = await ctx.write({
    msg: "done",
    type: "json",
    data: encoded,
    share: "owner",
  });

  if (writeResult.isOk) {
    self.send("result", { handle: writeResult.value });
  }
});

// Send work to the actor
actor.receive({ input: "process me" });

// Subscribe to results
actor.subscribe((msg) => {
  matchAll(msg, {
    started: (m) => println("Started:", m.data.input),
    result: (m) => {
      const reader = actor.read(m);
      if (reader) {
        println("Result:", reader.json());
        actor.release(m.handle);
      }
    },
    _: () => {},
  });
});

// If it hangs, terminate on demand. Thread dies. Memory freed.
setTimeout(() => actor.terminate(), 3000);
```

Three guarantees hold: the actor stops immediately, its resources are freed, and all other actors continue unaffected.

## Quick Features

- **True termination** via worker thread kill, not AbortController
- **Two-tier communication**: lightweight Tier 1 signals plus zero-copy Tier 2 shared memory
- **Supervision strategies**: one-for-one, one-for-all, rest-for-one
- **Automatic lease expiry** for stalled actors
- **Actor linking** (bi-directional) and **monitoring** (uni-directional)
- **Schema-validated resource access** from workers via proxy
- **Child actor spawning** from within worker callbacks
- **Configurable diagnostics** with sampling and per-metric toggles
- **No CAS, no atomic contention** on shared memory

## Erlang Inspiration

Future draws from Erlang's proven concurrency model.

**Let It Crash.** Actors fail independently. The supervisor handles recovery. Code does not guard every edge case.

**Supervision Trees.** Actors form hierarchies. A parent failure triggers strategy-driven cleanup of children.

**Actor Isolation.** Each actor runs in its own thread. No shared memory corruption. No cross-actor state leaks.

**Linking.** Two linked actors share a failure bond. When one dies, the other terminates too.

**Monitoring.** One actor watches another without linking. Receives a notification on death.

Supervision strategies:

- `one-for-one`: restart only the failed actor
- `one-for-all`: restart all actors in the group
- `rest-for-one`: restart the failed actor and all actors started after it

## Install

```
npm install @nilejs/future
```

**Requirements:** Bun v1.0+, TypeScript v4.5+

Future is designed for the Bun runtime. The underlying APIs (worker threads, SharedArrayBuffer) exist in Node.js, but the library relies on Bun native TypeScript support and worker thread resolution for actor callback serialization. Node.js support is planned but not yet available.

## Core Concepts

### Supervisor

Manages actor lifecycle, health monitoring, failure handling, memory pool, inbox routing, and supervision strategies.

```typescript
const supervisor = createSupervisor({
  maxActors: 10,
  memory: { poolSize: 5, boxSize: "1mb" },
  timeouts: { defaultLeaseMs: 5000 },
  strategy: "one-for-one",
  retry: { max: 3, backoff: "exponential" },
});
```

### Actor

An isolated execution context in a separate thread. Each actor has its own memory and event loop.

```typescript
const actor = supervisor.spawn(async (self, msg, ctx) => {
  // self: actor control (send, terminate, receive)
  // msg: incoming message
  // ctx: execution context (resources, shared memory, formatting)
});
```

### Context

Injected into every actor callback. Provides all runtime capabilities.

| Method | Tier | Description |
|---|---|---|
| `self.send(msg, data?)` | 1 | Send signal to subscribers |
| `ctx.write({ msg, type, data, share? })` | 2 | Write data to shared memory, returns `Result<Lock, string>` |
| `ctx.read(msg)` | 2 | Read shared memory data, returns `ChainableReader` or `null` |
| `ctx.release(handle)` | 2 | Decrement reference count, box freed at zero |
| `ctx.heartbeat()` | 1 | Reset lease timer during long operations |
| `ctx.resources.*` | 1 | Access main-thread resources via proxy |
| `ctx.link(actor)` | 1 | Bi-directional failure propagation |
| `ctx.monitor(actor)` | 1 | Uni-directional notification on death |
| `ctx.spawn(callback)` | 1 | Spawn a child actor from the worker |
| `ctx.terminate()` | 1 | Terminate the actor |
| `ctx.isCancelled` | 1 | Check if the actor was terminated |
| `ctx.fmt.*` | N/A | Buffer allocation and serialization |

### Two-Tier Communication

- **Tier 1 (Control):** lightweight signals via `postMessage`. Small messages, progress updates, heartbeats. `self.send("event", data)` with string key matching. `from` is auto-injected by the supervisor.
- **Tier 2 (Data):** zero-copy shared memory via `SharedArrayBuffer`. Large data transfers without serialization. `ctx.write()` returns `Result<Lock, string>`. Data is immutable after commit.

### Message Shape

```typescript
type Message = {
  readonly msg: string;       // Message key for dispatch
  readonly type?: FmtType;    // "json" | "string" | "binary" | "cbor"
  readonly data?: unknown;    // Tier 1: deserialized JSON data
  readonly handle?: Lock;     // Tier 2: shared memory box reference
  readonly from: ActorId;     // Auto-injected by supervisor
};
```

## API Reference

### Supervisor

```typescript
const actor = supervisor.spawn(callback, { name?: string, timeoutMs?: number });
const group = supervisor.createGroup({ strategy, retry?: { max, backoff, delayMs? } });
supervisor.terminateActor(actor.id);
const unsub = supervisor.subscribe((msg) => { ... });
await supervisor.shutdown();
const diag = supervisor.getDiagnostics();
```

### ActorRef

```typescript
actor.receive(msg);                                // Send a message to the actor
const unsub = actor.subscribe((msg) => { ... }); // Subscribe to messages
actor.terminate();                                // Kill the actor thread
const reader = actor.read(msg);                  // Read Tier 2 data
actor.release(handle);                           // Free shared memory box
const diag = actor.getDiagnostics();
actor.link(other);                               // Bi-directional link
actor.monitor(other);                            // Uni-directional monitor
```

### Message Dispatch

```typescript
matchAll(msg, {
  progress: (m) => println("Progress:", m.data.percent),
  result: (m) => handleResult(m),
  _: () => {},  // Default handler for unmatched messages
});
```

## Constraints

**Data Transfer:**
- Arguments and return values must be cloneable (JSON-compatible or Uint8Array)
- Large data should use Tier 2 shared memory instead of serialization
- Resource methods must declare input and output validation schemas

**Execution Model:**
- Actor callbacks are serialized; outer scope is not available
- All state must be passed via message or accessed through context
- Heartbeat timeout defaults to 5000 milliseconds (configurable via `timeouts.defaultLeaseMs`)

**Shared Memory:**
- `ctx.write()` returns `Promise<Result<Lock, string>>`; handle with `result.isOk`
- `ctx.read(msg)` takes the full message, returns `ChainableReader` or `null`
- `ctx.release(handle)` decrements reference count; box becomes free when count reaches zero
- Data is immutable after commit

**Configuration Required:**
- Memory pool requires explicit `poolSize` and `boxSize` configuration
- Resources must define input and output schemas
- Supervision strategy must be chosen for actor groups

## Use Cases

**AI Agent Systems:**
- Running agents that can hang or enter infinite loops
- Sharing large context windows between agents
- Managing multi-agent workflows with dependencies
- Streaming token generation that must be interruptible

**Data Processing Pipelines:**
- Parallel transformation stages that should not block each other
- ETL workflows requiring resilience to individual stage failures
- Media processing (image, video, audio) with isolated actors
- Real-time data enrichment with automatic cleanup

**Background Job Processing:**
- Jobs that can be cancelled on demand
- Queue systems where job failures should not halt processing
- Long-running tasks with configurable timeout and retry
- Resource cleanup guarantees when jobs complete or fail

**Safe Code Execution:**
- Running untrusted or unpredictable code
- Plugins or extensions with limited trust
- Sandboxed execution environments
- Any scenario where hanging code must be killed safely

## Deep Dives

- [Architecture](https://github.com/nile-js/future/blob/main/docs/architecture.md) -- Worker model, memory pool, state machine, performance
- [Shared Memory](https://github.com/nile-js/future/blob/main/docs/shared-memory.md) -- Tier 2, write/read/release, authorization, lease system
- [Supervision](https://github.com/nile-js/future/blob/main/docs/supervision.md) -- Strategies, groups, linking, monitoring, termination guarantees
- [Diagnostics](https://github.com/nile-js/future/blob/main/docs/diagnostics.md) -- Configuration reference, sampling, metrics
- [Resources](https://github.com/nile-js/future/blob/main/docs/resources.md) -- Intent relay, schema validation, cleanup hooks

## Contributing

Contributions are welcome. This is an open source project.

## License

MIT License

## Status

This project is work in progress. APIs may change as the library evolves.