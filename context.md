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
| `src/index.ts` | Re-exports slang-ts |
| `index.ts` | Root entry — exports future + slang-ts |

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
- `slang-ts` — Result, Option, match, matchAll, safeTry, pipe, atom, println, panic
- `zod` — resource schema validation
- `node:worker_threads` — worker threads (Bun has full compatibility)

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
  from: ActorId; msg: string; type: FmtType; share: ShareConfig;
  refCount: number; expiresAt: number; writer: ActorId | null;
  readers: Set<ActorId>; epoch: number;
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
  handle: Lock; from: ActorId; msg: string; type: FmtType;
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
- **`ctx.write({ msg, type, data, share })`**: Tier 2. Single async call. Returns `Result<Lock, string>`. Data is `Uint8Array`. Immutable after commit.
- **`ctx.read(m)`**: Returns chainable decoder `{ json(), string(), binary(), cbor(), raw() }` or `null`. Zero-copy from SAB. No async.
- **`ctx.release(handle)`**: Decrements ref count. If 0 → FREE.
- **`from` is always auto-injected** by supervisor. Sender never sets it.
- **`matchAll(msg, { key: (m) => ... })`**: Works on message objects — dispatches on `msg.msg` field, passes full message to handler.

## Testing
- **Use `bun test`**. Bun has native TypeScript support in worker threads.
- Tests in `tests/future/*.spec.ts` for future features.
- Tests in `tests/*.spec.ts` for slang-ts utilities (re-exported).
- **739 tests pass, 0 fail.**

## Runtime Requirement
- **Bun v1.0+ only**. Node.js is not supported.
- Worker threads rely on Bun's native `.ts` resolution.
- If Node.js support is needed, it requires a pre-compiled worker bootstrap or `tsx` loader — not currently implemented.

## Documentation Status
- `README.md` — Simplified overview (273 lines). Motivational briefing, featured example, quick features, Erlang inspiration. Deep dives linked.
- `docs/architecture.md` — Worker model, memory pool, state machine, performance, slang-ts
- `docs/shared-memory.md` — Tier 2, write/read/release, authorization, lease, buffer, lock
- `docs/supervision.md` — Strategies, groups, linking, monitoring, child actors, termination, backoff
- `docs/diagnostics.md` — Configuration reference, sampling, per-actor and supervisor metrics
- `docs/resources.md` — Intent relay, schema validation, cleanup hooks
- `spec.md` — Canonical spec (1400 lines, 20 sections, 8 ADRs)
- `docs/ADR-006-bun-only-runtime.md` — Bun-only runtime decision
- `context.md` — Project context (this file)

## Known Limitations
- `ctx.read(m)` returns `null` for all decoders if `m.handle` is absent (Tier 1 message). Callers should check.
- `actor.read(m)` on main thread returns zero-copy SAB view. Caller must call `actor.release(handle)` after reading. Enables concurrent readers.
- Epoch-based Lock prevents use-after-free but adds small overhead on box recycling.
- Write queue blocks requesting worker until a FREE box available. If all boxes busy, writer stalls. Mitigate with sufficient `poolSize`.
- CBOR codec not implemented — throws on use. Can be configured via `codecs` in supervisor config.
- `ctx.fmt.alloc()` creates heap buffers (not SAB-backed). Data is copied into SAB on `ctx.write()`. JSDoc corrected.
- Resource proxy sends `args[0]` (first argument) to `resourceManager.execute`, not the rest array. Fixed from original `args` spread.

## Recent Changes
- Fixed owner-share refCount leak: `readers.delete(actorId)` in handleCommit excludes writer from readers set
- Fixed resource proxy args bug: `args` → `args[0]` in worker-bootstrap.ts RESOURCE_REQUEST
- Fixed misleading fmt.alloc JSDoc: "SAB-backed" → "heap buffers (copied into SAB on write)"
- Added integration tests: explicit heartbeat, implicit heartbeat, ctx.resources, ctx.spawn, callback validation, linked authorization, per-actor timeouts, resource cleanup on shutdown
- Added README sections: Termination Guarantees, Child Actor Operations (ctx.spawn/link/monitor), Diagnostics Configuration Reference
- Added Teamwork Philosophy and Delegation Protocol to AGENTS.md

## Boundaries (DO NOT CROSS)
- `/backend`, `/nile`, `frontend/api` — never touch
- DB commands — ask user first
- Git commands — ask user first
- `.env` — never read/edit
- Servers — never start
