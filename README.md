# @nilejs/future

High-performance system-level actor and promise primitives for Bun and Node.js inspired by Erlang.

## Install

```
npm install @nilejs/future
```

Requirements: Bun v1.0+ or Node.js v18+, TypeScript v4.5+ (for full type safety)

## Quick Example

```typescript
import { createSupervisor } from "@nilejs/future";

const supervisor = createSupervisor({
  maxActors: 10,
  memory: { poolSize: 5, boxSize: '1mb' }
});

const actor = supervisor.spawn(async (self, msg, ctx) => {
  const result = await processData(msg.input);
  self.send({ type: 'DONE', result });
});

actor.spawn({ input: 'test' });
actor.subscribe((msg) => match(msg, {
  DONE: (m) => println('Result:', m.result),
  _: () => {}
}));
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
  memory: { poolSize: 5, boxSize: '1mb' },
  resources: { /* resource definitions */ }
});
```

The supervisor provides:
- Actor creation and lifecycle management
- Heartbeat and lease monitoring
- Resource allocation and cleanup
- Failure handling and supervision strategies

### What is an Actor?

An actor is an isolated execution context running in a separate thread. Actors communicate via messages and can share data through shared memory.

```typescript
const actor = supervisor.spawn(async (self, msg, ctx) => {
  // self: actor control (send messages)
  // msg: incoming message data
  // ctx: execution context (resources, locks, formatting)
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
ctx.resources.db.query({ sql: 'SELECT * FROM users' });

// Lock acquisition for shared memory
const lock = await ctx.acquireLock();
ctx.deposit(lock, data);
ctx.done(lock);

// Formatting utilities (no need to import)
const buffer = ctx.fmt.alloc(1024);
const encoded = ctx.fmt.encode({ key: 'value' });

// Actor control
ctx.heartbeat();      // Keep lease alive during long operations
ctx.terminate();      // Terminate self
ctx.isCancelled;      // Check if terminated
```

### Communication: Two-Tier System

Communication happens in two tiers:

**Tier 1 (Control)**: Lightweight signals via postMessage
- Small messages, status updates, heartbeats
- Resource method calls (intent relay)
- PubSub pattern for subscribers

**Tier 2 (Data)**: Zero-copy shared memory
- Large data transfers without serialization
- Atomic operations for lock-free access
- Memory pool with fixed-size boxes

```typescript
// Tier 1: Send a signal
self.send({ type: 'PROGRESS', value: 0.5 });

// Tier 2: Share large data
const lock = await ctx.acquireLock();
ctx.deposit(lock, largeDataArray);
ctx.done(lock);
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
- Memory pool with FIFO queuing prevents lock contention
- Shared memory enables efficient large data sharing between actors

**Execution Safety**
- Actor callbacks are serialized, outer scope is not inherited
- All state must be passed via message or accessed through context
- This isolation enables safe concurrent execution and termination

**Formatting Utilities (ctx.fmt)**
- Buffer allocation without thinking about Uint8Array
- Automatic encoding and decoding (JSON, string, binary)
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

## slang-ts Integration

[slang-ts](https://github.com/Hussseinkizz/slang) is an external TypeScript library that provides functional patterns. All code uses slang-ts patterns for cleaner, safer code:

```typescript
// match instead of if/switch
match(result, {
  Ok: (v) => process(v),
  Err: (e) => handle(e),
});

// matchAll for tagged unions
matchAll(msg, {
  PROGRESS: (m) => println(m.value),
  ERROR: (e) => println(e.message),
  _: () => {}  // Default case
});

// Result types for explicit error handling
const lock = await ctx.acquireLock();
match(lock, {
  Ok: (l) => { /* use lock */ },
  Err: (e) => println('Pool exhausted'),
});
```

This replaces imperative control flow with declarative pattern matching.

## Resource Manager

Resources are services available to actors but running on the main thread. The Resource Manager provides safe access through intent relay.

```typescript
const supervisor = createSupervisor({
  resources: {
    database: {
      query: {
        input: z.object({ sql: z.string() }),
        output: z.array(z.unknown()),
        handler: async ({ sql }) => await db.query(sql)
      },
      release: async () => await db.close()
    }
  }
});

// Actor uses resource
const results = await ctx.resources.database.query({ sql: 'SELECT 1' });
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
import { createSupervisor } from "@nilejs/future";

// Actor callbacks are serialized, outer scope is NOT available
const config = { timeout: 5000 };
const sharedData = heavyPayload;

// WRONG: outer scope is lost on serialization
const actor = supervisor.spawn(async (self, msg, ctx) => {
  // config === undefined
  // sharedData === undefined
});

// RIGHT: pass state via msg
const actor = supervisor.spawn(async (self, msg, ctx) => {
  const timeout = msg.timeout;  // From message
  const data = await ctx.resources.storage.get(msg.dataId);  // From resources

  // Heavy processing with heartbeat
  for (let i = 0; i < data.length; i++) {
    process(data[i]);
    if (i % 1000 === 0) ctx.heartbeat();
  }

  self.send({ type: 'COMPLETE' });
});

// Spawn with state
actor.spawn({ timeout: 5000, dataId: 'abc123' });

// Subscribe to messages
actor.subscribe((msg) => {
  match(msg, {
    COMPLETE: () => println('Done'),
    _: () => {}
  });
});

// Terminate on demand - CODE STOPS IMMEDIATELY
// This is NOT AbortController which just ignores results
// This is TRUE termination, the thread is killed
actor.terminate();
```

## Termination vs AbortController

This is NOT the same as AbortController.

AbortController just ignores results, the execution continues running in the background until completion. It does not stop anything.

Actor termination in future kills the thread immediately. The execution stops right there. Resources are cleaned up. This is true termination.

Most libraries in TypeScript and JavaScript cannot do this. future and Effect-TS are among the few that support true on-demand termination of running code.

## Shared Memory

Zero-copy data transfer between actors:

```typescript
const producer = supervisor.spawn(async (self, msg, ctx) => {
  const lock = await ctx.acquireLock();
  const buffer = ctx.fmt.alloc(msg.size);

  // Write data directly to shared buffer
  for (let i = 0; i < msg.size; i++) {
    buffer[i] = computeValue(i);
  }

  ctx.deposit(lock, buffer);
  ctx.done(lock);
  self.send({ type: 'READY' });
});

producer.spawn({ size: 1000000 });
```

## Supervision

Resilient actor hierarchies:

```typescript
const pipeline = supervisor.createGroup({
  strategy: 'rest-for-one'  // Restart downstream on upstream failure
});

const ingest = pipeline.spawn(async (self, msg, ctx) => {
  const data = await ctx.resources.http.get(msg.url);
  const lock = await ctx.acquireLock();
  ctx.deposit(lock, data);
  ctx.done(lock);
  self.send({ type: 'INGESTED' });
});

const transform = pipeline.spawn(async (self, msg, ctx) => {
  match(msg, {
    INGESTED: async () => {
      const data = pipeline.read(msg.address);
      const result = transformData(data);
      self.send({ type: 'TRANSFORMED', data: result });
    },
    _: () => {}
  });
});
```

## Constraints

When using future, keep these constraints in mind:

**Data Transfer**
- Arguments and return values must be cloneable (JSON-compatible or Uint8Array)
- Large data should use Tier 2 addresses rather than serialization
- Resource methods must declare input and output validation schemas

**Execution Model**
- Actor callbacks are serialized, outer scope is not available
- All state must be passed via message or accessed through context
- Heartbeat timeout defaults to 5000 milliseconds (configurable)

**Configuration Required**
- Memory pool requires explicit poolSize and boxSize configuration
- Resources must define input and output schemas
- Supervision strategy must be chosen for actor groups

## Performance Characteristics

**Concurrency Model**
- N isolated threads with their own event loops
- Lock-free memory allocation using atomic compare-exchange operations
- FIFO queuing ensures fair resource access without starvation
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
- Memory access controlled through atomic operations only
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