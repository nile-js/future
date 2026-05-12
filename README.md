# Future

High-performance actor primitives for Bun, inspired by Erlang.

## Install

```
npm install @nilejs/future
```

**Requirements:** Bun v1.0+, TypeScript v4.5+

Note: `@nilejs/future` is designed exclusively for the Bun runtime. The underlying APIs (worker_threads, SharedArrayBuffer) exist in Node.js, but the library relies on Bun's native TypeScript support and worker thread resolution for actor callback serialization. Node.js support is not available.

## Quick Example

```typescript
import { createSupervisor, println } from "@nilejs/future";

const supervisor = createSupervisor({
  maxActors: 10,
  memory: { poolSize: 5, boxSize: "1mb" },
});

const actor = supervisor.spawn(async (self, msg, ctx) => {
  const result = msg.input.toUpperCase();

  // Tier 1: lightweight signal via postMessage
  self.send("progress", { percent: 0.5 });

  // Tier 2: zero-copy shared memory write
  const writeResult = await ctx.write({
    msg: "result",
    type: "json",
    data: ctx.fmt.encode({ text: result }),
    share: "owner",
  });

  if (writeResult.isOk) {
    self.send("done", { handle: writeResult.value });
  }
});

actor.spawn({ input: "hello future" });

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

## The Problem

Standard JavaScript has no built-in protection against:
- Infinite loops that hang the event loop
- Functions that consume all memory
- Unreleased resources from crashed code
- Cascading failures that bring down the entire system

## The Solution

future isolates execution in separate threads with automatic cleanup:
- Any actor can be terminated on demand
- Hanging code is killed via heartbeat timeout
- Resource cleanup is guaranteed on termination
- One actor failure cannot corrupt others

## Core Concepts

### Supervisor

A supervisor manages actor lifecycle, monitors health, and handles failures. It owns the memory pool, inbox routing, and supervision strategies.

```typescript
const supervisor = createSupervisor({
  maxActors: 10,
  memory: { poolSize: 5, boxSize: "1mb" },
  strategy: "one-for-one",
  retry: { max: 3, backoff: "exponential" },
});
```

See [Architecture](https://github.com/nile-js/future/blob/main/docs/architecture.md) for the worker model, memory pool, and state machine.

### Actor

An actor is an isolated execution context running in a separate thread. Actors communicate via messages and can share data through shared memory.

```typescript
const actor = supervisor.spawn(async (self, msg, ctx) => {
  // self: actor control (send, terminate)
  // msg: incoming message
  // ctx: execution context (resources, shared memory, formatting)
});
```

Each actor has isolated memory and event loop, lifecycle tied to the supervisor, and access to resources via proxy.

### Context (ctx)

The context is injected into every actor callback and provides all actor capabilities:

| Method | Tier | Description |
|---|---|---|
| `self.send(msg, data?)` | 1 | Send signal to subscribers. Auto-injects `from`. |
| `ctx.write({ msg, type, data, share? })` | 2 | Write data to shared memory. Returns `Result<Lock, string>`. |
| `ctx.read(msg)` | 2 | Read shared memory data. Returns `ChainableReader \| null`. |
| `ctx.release(handle)` | 2 | Decrement ref count. Box freed at 0. |
| `ctx.heartbeat()` | 1 | Reset lease timer during long operations. |
| `ctx.resources.*` | 1 | Access main-thread resources via proxy. |
| `ctx.link(actor)` | 1 | Bi-directional failure propagation. |
| `ctx.monitor(actor)` | 1 | Uni-directional notification on death. |
| `ctx.spawn(callback)` | 1 | Spawn a child actor. |
| `ctx.terminate()` | 1 | Terminate the actor. |
| `ctx.isCancelled` | 1 | Check if actor was terminated. |
| `ctx.fmt.*` | N/A | Buffer allocation and serialization. |

### Two-Tier Communication

Communication uses two tiers:

- **Tier 1 (Control):** Lightweight signals via postMessage. Small messages, progress updates, heartbeats. `self.send("eventName", data)` with string key matching. `from` is auto-injected by the supervisor.
- **Tier 2 (Data):** Zero-copy shared memory. Large data transfers without serialization. `ctx.write()` returns `Result<Lock, string>`. `ctx.read(msg)` returns `ChainableReader | null`. `ctx.release(handle)` frees the box.

### Message Shape

```typescript
type Message = {
  readonly msg: string;            // Message key for dispatch
  readonly type?: FmtType;         // "json" | "string" | "binary" | "cbor"
  readonly data?: unknown;         // Tier 1: deserialized JSON data
  readonly handle?: Lock;          // Tier 2: shared memory box reference
  readonly from: ActorId;          // Auto-injected by supervisor
};
```

- `msg` is always required
- `from` is always injected by the supervisor, never set by the sender
- `handle` is present for Tier 2 data
- `data` is present for Tier 1 messages

## Erlang Inspired

future draws from Erlang's proven concurrency model:

**Let It Crash:** Actors can fail and the supervisor handles recovery. Code does not need to handle every possible error state.

**Supervision Trees:** Actors are organized hierarchically. When a parent fails, children are handled according to the supervision strategy.

**Actor Isolation:** Each actor has its own memory and cannot corrupt others. Failures are contained.

**Linking:** Actors can be linked so that when one dies, the other is terminated. Creates dependency chains for related actors.

**Monitoring:** Actors can monitor others without linking. When a monitored actor dies, the watcher receives a notification.

Supervision strategies:
- **one-for-one:** Restart only the failed actor
- **one-for-all:** Restart all actors in the group
- **rest-for-one:** Restart the failed actor and all actors started after it

## API Reference

### Supervisor API

```typescript
const actor = supervisor.spawn(callback, { name?: string, timeoutMs?: number });
const group = supervisor.createGroup({ strategy, retry?: { max, backoff, delayMs? } });
supervisor.terminateActor(actor.id);
const unsub = supervisor.subscribe((msg) => { ... });
await supervisor.shutdown();
const diag = supervisor.getDiagnostics(); // Result<SupervisorDiagnostics, string>
```

### ActorRef API

```typescript
actor.spawn(msg);                              // Send initial message
const unsub = actor.subscribe((msg) => { ... }); // Subscribe to messages
actor.terminate();                              // Terminate actor
const reader = actor.read(msg);                // Read Tier 2 data, ChainableReader | null
actor.release(handle);                         // Release shared memory box
const diag = actor.getDiagnostics();           // Result<ActorDiagnostics, string>
actor.link(otherActor);                        // Bi-directional link
actor.monitor(otherActor);                     // Uni-directional monitor
```

### Context API

See the [Context table](#context-ctx) above for the full API.

## Constraints

**Data Transfer:**
- Arguments and return values must be cloneable (JSON-compatible or Uint8Array)
- Large data should use Tier 2 (shared memory) rather than serialization
- Resource methods must declare input and output validation schemas

**Execution Model:**
- Actor callbacks are serialized, outer scope is not available
- All state must be passed via message or accessed through context
- Heartbeat timeout defaults to 5000 milliseconds (configurable)

**Shared Memory:**
- `ctx.write()` returns `Promise<Result<Lock, string>>`, handle with `result.isOk` check
- `ctx.read(msg)` takes the full message, returns `ChainableReader | null`
- `ctx.release(handle)` decrements ref count; box becomes FREE when count reaches 0
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
