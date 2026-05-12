# Future

High-performance actor and task primitives for Typescript, inspired by Erlang

## Why Future?

Imagine you have a function that processes a file. It works. Then one day it hangs, not crashes, not throws, just sits there consuming memory and holding connections. The event loop is frozen. Your server stops responding. `AbortController` will not save you; it just stops you from waiting while the code keeps running in the background.

In a standard JavaScript runtime, you have no way to stop it. No way to reclaim the memory. No way to know what went wrong. You restart the process and hope it does not happen again.

Future changes this. Every piece of work runs in its own thread with its own memory. If it hangs, you kill it. Not politely, the thread dies, memory is freed, and the rest of your system keeps running. You can do this from anywhere: a timeout, a user action, a monitoring signal. The actor stops, resources clean up, and nothing else is affected.

That is the core promise: **run anything, kill it anytime, clean up automatically**.

## What You Can Build

Future is not just actors. It is a complete concurrency toolkit:

- **Parallel workloads**: Run hundreds of tasks simultaneously, each in its own thread. One slow task never blocks another.
- **Shared memory at zero cost**: Pass large data between threads without copying. Write once, read from anywhere, no serialization overhead.
- **Self-healing systems**: Actors fail, the supervisor restarts them. Link dependent actors so they recover together. Monitor what matters and react to failures.
- **Safe resource access**: Workers call databases, APIs, and filesystems through a proxy that validates every input and output. Nothing touches the main thread directly.
- **Dynamic actor trees**: Spawn child actors from inside workers. Build pipelines, fan-out/fan-in patterns, and supervision hierarchies that grow with your workload.

## Featured Example

An actor processes data. If it hangs, you terminate it. Resources clean up. Other actors are untouched.

```typescript
import { createSupervisor, println, matchAll } from "@nilejs/future";

const supervisor = createSupervisor({
  maxActors: 10,
  memory: { poolSize: 5, boxSize: "1mb" },
  timeouts: { defaultLeaseMs: 5000 },
});

// Spawn an actor that does real work
const actor = supervisor.spawn(async (self, msg, ctx) => {
  self.send("started", { input: msg.input });

  // Heavy computation or external call that might hang
  const result = await fragileComputation(msg.input);
  const encoded = ctx.fmt.encode({ text: result });

  // Write result to shared memory, zero-copy, single call
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

// Read results from the main thread
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

// Hung after 3 seconds? Kill it. Everything else keeps running.
setTimeout(() => actor.terminate(), 3000);
```

## How It Works

Future uses two tiers of communication:

**Tier 1, Signals.** Lightweight messages via `postMessage`. Progress updates, heartbeats, task dispatch. Small, fast, JSON-encoded.

**Tier 2, Shared memory.** Large data lives in `SharedArrayBuffer` segments. Workers write directly to their assigned box. The supervisor routes a handle to authorized readers. No serialization, no copying, no contention.

When an actor finishes writing, it commits. The supervisor marks the box as ready and delivers a handle to every reader that is authorized to see it. Readers decode in place. When everyone is done, the box returns to the pool.

Every box has a lease. If an actor holds a box too long (crashed, hung, forgot to release), the supervisor forces it free. You can reset the lease with `ctx.heartbeat()` during long operations, or let the system handle it automatically on any interaction.

## Inspired by Erlang

Future draws from three decades of battle-tested concurrency:

**Let it crash.** Do not wrap every call in try-catch. Let actors fail and let the supervisor decide what to do. Your code stays focused on the happy path.

**Supervision trees.** Actors form hierarchies. When one fails, the supervisor restarts it, restarts its siblings, or tears down the whole group, depending on the strategy you chose.

**Isolation by default.** Actors share nothing. No shared mutable state, no race conditions, no locks. Communication happens through messages and immutable shared memory.

**Linking and monitoring.** Link two actors and they die together, useful for tightly coupled dependencies. Monitor an actor and get a `DOWN` notification when it fails, useful for cleanup and logging.

## Install

```
npm install @nilejs/future
```

**Requirements:** Bun v1.0+, TypeScript v4.5+

Future is designed for the Bun runtime. Node.js support is planned but not yet available.

## Core Concepts

### Supervisor

The orchestrator. It creates actors, monitors health, manages the memory pool, routes messages, and handles failures.

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

An isolated execution context in its own thread. Actors communicate through messages and shared memory, never through shared state.

```typescript
const actor = supervisor.spawn(async (self, msg, ctx) => {
  // self, send messages, terminate
  // msg,  incoming message
  // ctx,  shared memory, resources, formatting
});
```

### Context

Injected into every actor callback. Everything the actor needs:

| Method | Tier | What it does |
|---|---|---|
| `self.send(msg, data?)` | 1 | Send a signal to subscribers |
| `ctx.write({ msg, type, data, share? })` | 2 | Write to shared memory, returns `Result<Lock, string>` |
| `ctx.read(msg)` | 2 | Read shared memory, returns `ChainableReader` or `null` |
| `ctx.release(handle)` | 2 | Free a shared memory box |
| `ctx.heartbeat()` | 1 | Reset the lease timer during long work |
| `ctx.resources.*` | 1 | Call main-thread services through a proxy |
| `ctx.link(actor)` | 1 | Bi-directional failure propagation |
| `ctx.monitor(actor)` | 1 | Get a `DOWN` notification when an actor dies |
| `ctx.spawn(callback)` | 1 | Create a child actor from inside a worker |
| `ctx.terminate()` | 1 | Stop the actor |
| `ctx.isCancelled` | 1 | Check if the actor has been terminated |
| `ctx.fmt.*` | n/a | Buffer allocation and serialization utilities |

### Communication

- **Tier 1 (Control):** `self.send("event", data)`, lightweight, JSON-encoded, auto-injects `from`
- **Tier 2 (Data):** `ctx.write()` → `ctx.read()` → `ctx.release()`, zero-copy, ref-counted, immutable after commit

### Message Shape

```typescript
type Message = {
  readonly msg: string;       // Dispatch key
  readonly type?: FmtType;    // "json" | "string" | "binary" | "cbor"
  readonly data?: unknown;    // Tier 1 payload
  readonly handle?: Lock;     // Tier 2 shared memory reference
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
const unsub = actor.subscribe((msg) => { ... });  // Subscribe to messages
actor.terminate();                                 // Kill the actor thread
const reader = actor.read(msg);                   // Read Tier 2 data
actor.release(handle);                            // Free shared memory box
const diag = actor.getDiagnostics();
actor.link(other);                                // Bi-directional link
actor.monitor(other);                             // Uni-directional monitor
```

### Message Dispatch

```typescript
matchAll(msg, {
  progress: (m) => println("Progress:", m.data.percent),
  result: (m) => handleResult(m),
  _: () => {},
});
```

## Constraints

**Data Transfer:**
- Arguments and return values must be cloneable (JSON-compatible or `Uint8Array`)
- Large data should use Tier 2 shared memory instead of serialization
- Resource methods must declare input and output validation schemas

**Execution Model:**
- Actor callbacks are serialized, outer scope is not available
- All state must be passed via message or accessed through context
- Heartbeat timeout defaults to 5000 milliseconds (configurable)

**Shared Memory:**
- `ctx.write()` returns `Promise<Result<Lock, string>>`, check `result.isOk`
- `ctx.read(msg)` returns `ChainableReader | null`, sync, zero-copy
- `ctx.release(handle)` decrements the reference count; box frees at zero
- Data is immutable after commit

## Use Cases

**AI Agent Systems:**
- Run agents that can hang or loop infinitely, kill them on demand
- Share large context windows between agents with zero-copy memory
- Manage multi-agent workflows where one failure should not cascade

**Data Processing Pipelines:**
- Parallel transformation stages that cannot block each other
- ETL workflows that survive individual stage failures
- Media processing with isolated actors and automatic cleanup

**Background Jobs:**
- Cancellable jobs with guaranteed resource cleanup
- Queue systems where one failure does not halt the pipeline
- Long-running tasks with configurable timeout and retry

**Sandboxed Execution:**
- Run untrusted or unpredictable code safely
- Plugins and extensions with limited trust
- Any scenario where hanging code must be killed, not ignored

## Deep Dives

- [Architecture](https://github.com/nile-js/future/blob/main/docs/architecture.md) — Worker model, memory pool, state machine, performance
- [Shared Memory](https://github.com/nile-js/future/blob/main/docs/shared-memory.md) — Tier 2, write/read/release, authorization, lease system
- [Supervision](https://github.com/nile-js/future/blob/main/docs/supervision.md) — Strategies, groups, linking, monitoring, termination guarantees
- [Diagnostics](https://github.com/nile-js/future/blob/main/docs/diagnostics.md) — Configuration reference, sampling, metrics
- [Resources](https://github.com/nile-js/future/blob/main/docs/resources.md) — Intent relay, schema validation, cleanup hooks

## Contributing

Contributions are welcome. This is an open source project.

## License

MIT License

## Status

This project is still work in progress and may not be production ready yet. APIs may change as the library evolves.
