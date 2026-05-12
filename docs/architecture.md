# Architecture

**Category:** Concept

## Intent

Document the internal architecture of future: the worker execution model, memory pool design, state machine, performance characteristics, and integration with slang-ts.

## Responsibilities

- Worker thread execution and callback serialization
- Shared memory pool management (data boxes, no state board)
- State machine (FREE to WRITING to READY to FREE)
- Performance characteristics and isolation guarantees
- Integration with slang-ts for functional patterns

## Non-Goals

- Does not cover Tier 1 and Tier 2 communication details (see [Shared Memory](https://github.com/nile-js/future/blob/main/docs/shared-memory.md))
- Does not cover supervision strategies (see [Supervision](https://github.com/nile-js/future/blob/main/docs/supervision.md))
- Does not cover diagnostics configuration (see [Diagnostics](https://github.com/nile-js/future/blob/main/docs/diagnostics.md))

## Worker Model

Actors run in dedicated worker threads. The supervisor serializes each actor callback string via `fn.toString()` and sends it to a worker bootstrap file. The worker reconstructs the callback using `new Function('return ' + serialized)()`.

Key properties:
- Outer scope is lost by design. All state must be passed via message or accessed through context.
- The worker bootstrap file is at `src/future/worker-bootstrap.ts`
- Callback validation: both the initial worker init and child spawn requests validate callback strings against dangerous patterns (`require(`, `process`, `globalThis`, `import(`, `eval(`, `Function(`)) before evaluation. This prevents code injection.
- Worker crash detection: `worker.on("exit")` and `worker.on("error")` handlers in the spawn path automatically terminate the actor and clean up resources if the worker thread crashes (uncaught exception, OOM, segfault).

## Memory Pool

Each supervisor has a fixed-size memory pool of SharedArrayBuffer data boxes. There is no state board, no Int32Array for box states, no BigInt64Array for leases. The SAB is raw byte storage only.

All state tracking uses supervisor-side `BoxEntry[]` plain objects:

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
  epoch: number;
};
```

Box identity is an index into `BoxEntry[]`, wrapped in an opaque `Lock` type:

```typescript
type Lock = { readonly boxIndex: number; readonly epoch: number };
```

The epoch prevents use-after-free: when a box is recycled and reassigned, old handles with a stale epoch are rejected.

## State Machine

```
FREE -> WRITING -> READY -> FREE
```

- **FREE:** Box available for assignment. No writer. No data.
- **WRITING:** One writer assigned. Worker copies data into the SAB segment. Mutable.
- **READY:** Data committed. Immutable. `refCount` tracks active readers.
- **Transition to FREE:** When `refCount` reaches 0 (all readers released), the box returns to FREE.

All transitions are deterministic and managed by the supervisor in response to worker protocol messages. No CAS operations. No atomic contention.

## Performance Characteristics

**Concurrency Model:**
- N isolated threads with their own event loops
- Write-once shared memory removes atomic contention entirely
- No CAS operations on the SAB
- Box state tracked in plain `BoxEntry[]` objects on the supervisor side
- Heartbeat system enables automatic recovery from stalled execution

**Fault Tolerance:**
- Lease expiration prevents indefinite resource holding
- Supervision strategies control failure blast radius
- Linking enables cascading termination for dependent actors
- Monitoring provides failure notification without forced termination
- Resource isolation prevents main-thread crashes from affecting actors

**Isolation Guarantees:**
- Each actor runs in a separate thread with isolated memory space
- Resource access mediated through serialized intent packets
- Immutable shared memory after commit
- Actor termination includes guaranteed buffer cleanup
- Failure domains bounded by supervision tree structure

## slang-ts Integration

future re-exports [slang-ts](https://github.com/Hussseinkizz/slang), an external TypeScript library that provides functional patterns:

- `Result` and `Option` types for explicit error handling
- `match()` for pattern matching on `Result` types (replaces if/switch)
- `matchAll()` for message dispatch on `msg.msg` field
- `safeTry()` for error propagation without try/catch
- `println()` for structured logging
- `pipe()` for function composition

All `ctx.write()` calls return `Result<Lock, string>`, which is handled with `match()`:

```typescript
import { match } from "slang-ts";

const writeResult = await ctx.write({ msg: "r", type: "json", data });
match(writeResult, {
  Ok: (lock) => { /* use lock */ },
  Err: (e) => println("Pool exhausted:", e),
});
```

Message dispatch uses `matchAll()` which dispatches on the `msg.msg` field:

```typescript
import { matchAll } from "slang-ts";

actor.subscribe((msg) => {
  matchAll(msg, {
    progress: (m) => println("Progress:", m.data.percent),
    done: (m) => { /* handle done */ },
    _: () => {},
  });
});
```
