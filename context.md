# Project Context: @nilejs/future

## Overview
High-performance actor/promise primitives for **Bun**, Erlang-inspired. Isolates execution in worker threads with automatic cleanup, two-tier communication, supervision trees, auto-restart, and configurable diagnostics. Actors communicate via immutable messages routed through per-actor inboxes. Tier 2 uses zero-copy SAB boxes with supervisor-managed state — no CAS, no READING state, no atomic contention. Key features: epoch-based Lock prevents use-after-free, FIFO write queue when boxes exhausted, ShareConfig authorization (owner/group/linked/explicit), per-actor inbox queues with deterministic delivery.

## Key Files
| File | Purpose |
|------|---------|
| `src/future/types.ts` | All type contracts. Import from here. |
| `src/future/memory-pool.ts` | SharedArrayBuffer data boxes (no state board, no CAS) |
| `src/future/pubsub.ts` | Tier 1 message bus + Worker message channel |
| `src/future/worker-bootstrap.ts` | Worker thread entry point |
| `src/future/actor.ts` | ActorRef factory |
| `src/future/supervisor.ts` | createSupervisor factory (core orchestrator, inbox routing, auth) |
| `src/future/resource-manager.ts` | Proxy-based intent relay |
| `src/future/strategies.ts` | Supervision restart strategy types + selectors |
| `src/future/restart.ts` | Backoff calculation + retry orchestration |
| `src/future/group-manager.ts` | Group state tracking + auto-restart |
| `src/future/diagnostics.ts` | Configurable metrics collector with sampling |
| `src/future/index.ts` | Barrel export for future domain |
| `src/index.ts` | Barrel export for slang utilities |
| `index.ts` | Root entry — exports future + slang |

## Conventions
- **No classes**. Factory functions only. `createSupervisor()` returns plain object with methods.
- **Named params**: `{ name, email }` not `(name, email)`.
- **Max 400 LOC/file** (supervisor.ts may exceed as core orchestrator — acceptable exception).
- **`type` over `interface`**. Ban `enum`.
- **JSDoc** for all public APIs. Explain WHY not what.
- **`safeTry`** for error handling. No raw try/catch.
- **`.filter().map()`** over for loops where possible.
- **Explicit return types** on public functions.
- **Domain folders with barrel `index.ts`**.

## Dependencies
- `zod` — resource schema validation
- `node:worker_threads` — worker threads (Bun has full compatibility)
- Existing slang utilities in `src/` — Result, Option, match, safeTry, pipe, atom

## Architecture

### Worker Model
- `supervisor.spawn(callback)` serializes callback via `fn.toString()`
- Worker reconstructs with `new Function('return ' + serialized)()`
- Outer scope lost by design — all state via `msg` or `ctx.resources`
- Worker bootstrap file: `src/future/worker-bootstrap.ts`
- **ctx.spawn**: Worker can request main thread to spawn child actors via `SPAWN_CHILD` message
- **Callback validation**: Both `handleInit` (worker) and `SPAWN_CHILD` (main) validate callback strings against dangerous patterns (`require(`, `process`, `globalThis`, `import(`, `eval(`, `Function(`) before `new Function()` evaluation. Prevents code injection.
- **Worker crash detection**: `worker.on("exit")` and `worker.on("error")` handlers in `spawnActor` automatically terminate the actor and clean up resources if the worker thread crashes (uncaught exception, OOM, segfault).

### Two-Tier Communication
- **Tier 1 (Control)**: `postMessage` — signals, status updates, heartbeats, resource intents, pubsub, child spawn, inbox delivery
- **Tier 2 (Data)**: `SharedArrayBuffer` — zero-copy large data transfer. Workers write to assigned boxes, data is immutable after commit. No CAS. No atomic contention.

### Message Shape
```typescript
type FmtType = "json" | "string" | "binary" | "cbor";

type Message = {
  msg: string;           // Matching key for matchAll dispatch
  type?: FmtType;        // Required for Tier 2. Absent for Tier 1 (json assumed)
  data?: unknown;        // Tier 1: deserialized JSON. Tier 2: absent (use handle + ctx.read)
  handle?: Lock;         // Tier 2 SAB reference. Absent for Tier 1
  from: ActorId;         // Auto-injected by supervisor. Sender never sets this.
};
```

### Memory Pool Layout (per supervisor)
- **Data Boxes only**: `Uint8Array[poolSize * boxSize]` — fixed-size segments
- **No State Board**. No `Int32Array` for box states. No `BigInt64Array` for leases.
- All state tracking moves to supervisor-side `BoxEntry[]` plain objects.
- No CAS operations on the SAB. The SAB is raw byte storage only.

### Supervisor-Side State Tracking
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

Box identity is an index into `BoxEntry[]`, wrapped in an opaque `Lock` type with epoch:
```typescript
type Lock = { boxIndex: number; epoch: number };
```
The `epoch` prevents use-after-free: if a box is recycled and reassigned, old handles with a stale epoch are rejected.

### State Machine
```
FREE → WRITING → READY → FREE
```
- **FREE**: Box available for assignment. No writer. No data.
- **WRITING**: One writer assigned. Worker copies data into SAB segment. Mutable.
- **READY**: Data committed. Immutable. `refCount` tracks active readers. No READING state.
- **Transition to FREE**: When `refCount` reaches 0 (all readers released), box returns to FREE.

All state transitions are deterministic and managed by the supervisor in response to worker protocol messages. No CAS. No atomic operations on data.

### Write Flow (Tier 2)
1. Worker calls `ctx.write({ msg, type, data, share })` → sends `WRITE_REQUEST` to main thread
2. Main thread finds FREE box (or queues in FIFO write queue if none available), sets `BoxEntry.state = WRITING`, assigns writer, increments epoch
3. Main thread sends `WRITE_GRANTED { lock: { boxIndex, epoch } }` to worker
4. Worker copies data into SAB segment at `lock.boxIndex`, strips trailing null bytes
5. Worker sends `COMMIT { lock }` → main marks `BoxEntry.state = READY`, computes `expiresAt`, routes to authorized inboxes
6. Main thread delivers `INBOX { handle, from, msg, type }` to authorized readers

### Read Flow (Tier 2)
1. Reader receives `INBOX` message with `handle`, `from`, `msg`, `type`
2. Reader calls `ctx.read(m)` → returns chainable decoder `{ json(), string(), binary(), cbor(), raw() }` or `null` if no handle
3. Data is read directly from SAB. Zero-copy. No async round-trip. No state transition.
4. Reader calls `ctx.release(handle)` → sends `RELEASE { lock }` to main
5. Main decrements `refCount`. If 0 → box FREE, serve write queue if pending.

### Per-Actor Inbox
```typescript
type InboxEntry = {
  handle: Lock;
  from: ActorId;
  msg: string;
  type: FmtType;
};

const inboxes: Map<ActorId, InboxEntry[]>;
```
- Supervisor routes messages to actor inboxes based on `share` authorization
- When actor completes current message handler, supervisor delivers next inbox entry via `INBOX` protocol message
- Inbox entries hold Lock references, not data copies

### Authorization Model (ShareConfig)
- `"owner"` (default): Only the writer actor can read. Main-thread subscribers can always read.
- `"group"`: All actors in the same supervision group can read.
- `"linked"`: Actors linked to the writer can read (bi-directional and uni-directional links).
- Explicit `ActorId[]`: Only specified actor IDs can read.
- Enforcement: Supervisor checks `readers` set in `BoxEntry` on every `INBOX` delivery. Unauthorized read attempts are silently dropped.

### Resource Manager
- `ctx.resources.db.query()` → Proxy intercepts → `RESOURCE_REQUEST` → main thread validates (Zod) → executes handler → `RESOURCE_RESPONSE`
- Each resource can define `release()` for cleanup on shutdown

### Lease System
- `expiresAt` computed at COMMIT time and stored in `BoxEntry`
- Reset when a new reader acquires a handle to a READY box
- Opportunistic cleanup: on ANY actor-supervisor interaction (`self.send`, `ctx.write`, `ctx.release`, `ctx.heartbeat`, resource call), check all `BoxEntry` entries for expired leases
- Lease expiry: force-release all reader refs, set box FREE, terminate writer if in WRITING state, serve write queue
- No `setTimeout` overhead. Cleanup is deterministic and interaction-driven.

### Supervision & Auto-Restart
- **Root group**: Every supervisor has an implicit root supervision group. `supervisor.spawn()` registers actors here automatically.
- **Groups**: `supervisor.createGroup({ strategy, retry })` creates additional supervision groups with override configs.
- **Strategy enforcement**: On actor crash (not user shutdown), supervisor applies strategy:
  - `one-for-one`: Restart only failed actor
  - `one-for-all`: Restart ALL actors in group
  - `rest-for-one`: Restart failed actor + all actors spawned after it
- **Retry with backoff**: Configurable `max` retries, `backoff` type (`exponential`, `linear`, `fixed`), `delayMs`
- **Unlimited restarts**: When retry is not configured, actors are always restarted (no budget limit).
- **Cascade failure**: If retry budget is exhausted, terminate entire group.
- **Callback storage**: Group manager stores original callbacks + configs for respawn

### Diagnostics
- Configurable via `diagnostics: { enabled, sampleRate, track: {...} }`
- Per-metric toggles: actorLifetimes, startTimes, processLifetimes, writeQueueDepth, messageLatency, bufferUtilization, heartbeatIntervals, resourceCallLatency, authorizationEvents, inboxDepth, refCountHistory
- Sampling: `Math.random() < sampleRate` gates tracking
- Uses `performance.now()` for timing
- Zero-cost when disabled (no-op collector)

## Important Patterns
- Actor callbacks are **serialized**. Cannot capture outer scope.
- Termination = **true thread kill** via `worker.terminate()`. Not AbortController.
- Supervision strategies: `one-for-one` (default), `one-for-all`, `rest-for-one`.
- Linking = bi-directional suicide pact. Monitoring = uni-directional notification.
- **Max actors**: Enforced at spawn time.
- **`self.send(msg, data)`**: Tier 1. `msg` is string matching key. `data` auto-encoded as JSON. `from` auto-injected by supervisor.
- **`ctx.write({ msg, type, data, share })`**: Tier 2. Single async call. Returns `Promise<Lock>`. Data is `Uint8Array`. Immutable after commit.
- **`ctx.read(m)`**: Returns chainable decoder `{ json(), string(), binary(), cbor(), raw() }` or `null`. Zero-copy from SAB. No async.
- **`ctx.release(handle)`**: Decrements ref count. If 0 → FREE.
- **`from` is always auto-injected** by supervisor. Sender never sets it.

## API Changes (Old → New)
| Old API | New API | Notes |
|---------|---------|-------|
| `ctx.acquireLock()` | `ctx.write({ msg, type, data, share })` | Write replaces acquire+deposit+done |
| `ctx.deposit(lock, data)` | (merged into `ctx.write`) | Data passed in write call |
| `ctx.done(lock)` | (merged into `ctx.write`) | Commit is implicit after SAB copy |
| `ctx.read(lock)` (async) | `ctx.read(m)` (sync, chainable) | Returns decoder, no async round-trip |
| `actor.read(lock)` (sync) | `actor.read(m)` (sync, chainable) | Returns decoder or null |
| `actor.done(lock)` | `actor.release(handle)` / `ctx.release(handle)` | Decrements ref count |
| `LOCK_REQUEST/LOCK_GRANTED` | `WRITE_REQUEST/WRITE_GRANTED` | Protocol rename |
| `DEPOSIT` | (removed) | Merged into write flow |
| `DONE` | `COMMIT` | Protocol rename |
| `READ_START/READ_GRANTED/READ_ERROR` | (removed) | No async read. Data is immutable in SAB. |
| `READING` state | (removed) | No READING state. READY is immutable. |
| State Board (`Int32Array`) | (removed) | State tracked in `BoxEntry[]` plain objects |
| Lease Tracker (`BigInt64Array`) | (removed) | `expiresAt` in `BoxEntry` |
| `Lock { boxIndex, byteOffset, length }` | `Lock { boxIndex, epoch }` | Epoch prevents use-after-free |
| `self.send(msg)` | `self.send(msg, data?)` | Now takes optional data param |
| Open read model | `ShareConfig` authorization | owner/group/linked/explicit list |
| No inbox | Per-actor inbox queue | Supervisor routes to authorized readers |

## Testing
- **Use `bun test`**. Bun has native TypeScript support in worker threads.
- Tests in `tests/future/*.spec.ts` for future features.
- Keep existing `tests/*.spec.ts` for slang utilities.
- **355 tests pass, 0 fail.** (Memory pool: 27 tests. Supervisor: 46 tests. Slang utilities: unchanged.)

## Runtime Requirement
- **Bun v1.0+ only**. Node.js is not supported.
- Worker threads rely on Bun's native `.ts` resolution.
- If Node.js support is needed, it requires a pre-compiled worker bootstrap or `tsx` loader — not currently implemented.

## Documentation Status
- `README.md` — Needs update for new API
- `spec-final.md` — Current (1400 lines, 20 sections, 8 ADRs). Rewrite complete.
- `spec.md` — Deprecated (superseded by spec-final.md)
- `docs/ADR-006-bun-only-runtime.md` — Current
- `context.md` — Current (this file)

## Known Limitations
- `ctx.read(m)` returns `null` for all decoders if `m.handle` is absent (Tier 1 message). Callers should check.
- `actor.read(m)` on main thread returns zero-copy SAB view. Caller must call `actor.release(handle)` after reading. Enables concurrent readers.
- Epoch-based Lock prevents use-after-free but adds small overhead on box recycling.
- Write queue blocks requesting worker until a FREE box available. If all boxes busy, writer stalls. Mitigate with sufficient `poolSize`.
- No bounds validation on `lock.boxIndex` in RELEASE handler. Out-of-range indices should be guarded.
- `(msg as any)` casts in message handlers bypass TypeScript strictness. Technical debt, not a runtime bug.

## Boundaries (DO NOT CROSS)
- `/backend`, `/nile`, `frontend/api` — never touch
- DB commands — ask user first
- Git commands — ask user first
- `.env` — never read/edit
- Servers — never start
