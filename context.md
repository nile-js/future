# Project Context: @nilejs/future

## Overview
High-performance actor/promise primitives for **Bun**, Erlang-inspired. Isolates execution in worker threads with automatic cleanup, two-tier communication, supervision trees, auto-restart, and configurable diagnostics.

## Key Files
| File | Purpose |
|------|---------|
| `src/future/types.ts` | All type contracts. Import from here. |
| `src/future/memory-pool.ts` | SharedArrayBuffer layout + atomic lock ops |
| `src/future/pubsub.ts` | Tier 1 message bus + Worker message channel |
| `src/future/worker-bootstrap.ts` | Worker thread entry point |
| `src/future/actor.ts` | ActorRef factory |
| `src/future/supervisor.ts` | createSupervisor factory (core orchestrator) |
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
- **Max 400 LOC/file** (supervisor.ts is ~500 LOC as the core orchestrator — acceptable exception).
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
- **Tier 1 (Control)**: `postMessage` — signals, heartbeats, resource intents, pubsub, child spawn
- **Tier 2 (Data)**: `SharedArrayBuffer` — zero-copy large data transfer

### Memory Pool Layout (per supervisor)
- State Board: `Int32Array[poolSize]` — 0=CLEAN, 1=LOCKED, 2=READY, 3=READING
- Lease Tracker: `BigInt64Array[poolSize]` — `expiresAt` timestamp ms, 0n = no lease
- Data Boxes: `Uint8Array[poolSize * boxSize]` — fixed-size segments

### State Machine
```
CLEAN(0) --acquireLock()--> LOCKED(1) --ctx.done()--> READY(2) --read()--> READING(3) --done()--> CLEAN(0)
```
- **READING(3)**: One or more active readers. Reference count tracked by supervisor. Last reader triggers `READING→CLEAN`.
- All state transitions are main-thread serialized.

### Lock Flow
1. Worker sends `LOCK_REQUEST` → main thread
2. Main thread finds CLEAN box, sets LOCKED, sets lease
3. Main thread sends `LOCK_GRANTED` with `{ boxIndex, byteOffset, length }`
4. Worker writes data via `ctx.deposit(lock, data)`
5. Worker calls `ctx.done(lock)` → main marks READY, notifies subscribers
6. **Reader** calls `actor.read(lock)` (main thread) or `ctx.read(lock)` (worker) → main transitions READY→READING, increments ref count
7. **Reader** calls `actor.done(lock)` or `ctx.done(lock)` → main verifies actor actually has box in `state.reads` before decrementing. If 0: READING→CLEAN, serves queue. Guard prevents double-decrement from buggy/malicious actors.

> **Worker reads:** `ctx.read(lock)` is async. Worker sends `READ_START` with unique `requestId` → main approves with `READ_GRANTED` (echoes `requestId`) → worker creates SAB view → worker calls `ctx.done(lock)` → main decrements count. `requestId` prevents promise collision when same worker reads same box concurrently. Safe because all state transitions are main-thread serialized.

### Resource Manager
- `ctx.resources.db.query()` → Proxy intercepts → `RESOURCE_REQUEST` → main thread validates (Zod) → executes handler → `RESOURCE_RESPONSE`
- Each resource can define `release()` for cleanup on shutdown

### Lease System
- `acquireLock()` sets `expiresAt = now + leaseMs`
- Implicit heartbeat on `self.send()` and `ctx.deposit()`
- Explicit heartbeat via `ctx.heartbeat()`
- Opportunistic cleanup: on ANY actor-supervisor interaction, scan leases. Expired → terminate actor, recycle box.
- **READING boxes**: `checkLeases()` handles READING state — terminates all actors reading the box, deletes `readerCounts` entry, then marks CLEAN.

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
- Per-metric toggles: actorLifetimes, startTimes, processLifetimes, lockAcquisitionTimes, messageLatency, bufferUtilization, heartbeatIntervals, resourceCallLatency
- Sampling: `Math.random() < sampleRate` gates tracking
- Uses `performance.now()` for timing
- Zero-cost when disabled (no-op collector)

## Important Patterns
- Actor callbacks are **serialized**. Cannot capture outer scope.
- Termination = **true thread kill** via `worker.terminate()`. Not AbortController.
- Supervision strategies: `one-for-one` (default), `one-for-all`, `rest-for-one`.
- Linking = bi-directional suicide pact. Monitoring = uni-directional notification.
- **Max actors**: Enforced at spawn time.

## Testing
- **Use `bun test`**. Bun has native TypeScript support in worker threads.
- Tests in `tests/future/*.spec.ts` for future features.
- Keep existing `tests/*.spec.ts` for slang utilities.
- **357 tests pass, 0 fail.**

## Runtime Requirement
- **Bun v1.0+ only**. Node.js is not supported.
- Worker threads rely on Bun's native `.ts` resolution.
- If Node.js support is needed, it requires a pre-compiled worker bootstrap or `tsx` loader — not currently implemented.

## Documentation Status
- `README.md` — Updated with copy-paste runnable examples
- `spec-final.md` — Updated with named params for resource calls
- `spec.md` — Marked as deprecated (superseded by spec-final.md)
- `docs/ADR-006-bun-only-runtime.md` — Current
- `task.md` — Current
- `context.md` — Current

## Known Limitations
- Worker `ctx.read()` requires async round-trip to main thread for state transition. Not as fast as main-thread `actor.read()` but enables safe concurrent reads.
- `actor.read()` on main thread does NOT auto-clean. Caller must call `actor.done(lock)` after reading. This enables concurrent readers.
- `readBox()` returns zero-copy SAB view. Caller must call `done()` before the view is used after box cleanup — documented contract.
- No bounds validation on `lock.boxIndex` in `READ_START` handler. Out-of-range indices fall through to READ_ERROR safely but without explicit guard.
- No ownership validation for reads. Any actor can read any READY box — by design (open read model).
- `handleDone` silently ignores DONE for non-matching states (LOCKED/CLEAN/READY when expecting READING). By design — no-op on mismatched state.
- `(msg as any)` casts in message handlers bypass TypeScript strictness. Technical debt, not a runtime bug.
- 100% spec compliance achieved.

## Boundaries (DO NOT CROSS)
- `/backend`, `/nile`, `frontend/api` — never touch
- DB commands — ask user first
- Git commands — ask user first
- `.env` — never read/edit
- Servers — never start
