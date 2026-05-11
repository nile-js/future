# Task: Align Codebase with spec-final.md

## Goal
Rewrite the actor system implementation and tests to match the canonical spec-final.md architecture.

## Current State vs Spec
The codebase uses an older API:
- `Lock = { boxIndex, byteOffset, length }` — no epoch, use-after-free risk
- Memory pool stores state board + lease tracker inside SAB with Atomics CAS
- `acquireLock()` → `deposit()` → `done()` — low-level 3-step write
- `ctx.read(lock)` returns `Result<Uint8Array, string>` — async, no chainable decoder
- No authorization model — any actor can read any box
- No per-actor inbox queues — messages delivered synchronously to subscribers
- `actor.done(lock)` instead of `actor.release(handle)`

The spec requires:
- `Lock = { boxIndex, epoch }` — prevents use-after-free on recycled boxes
- SAB is raw byte storage only — all state in supervisor-side `BoxEntry[]`
- `ctx.write({ msg, type, data, share })` — single async call, returns Lock
- `ctx.read(m)` / `actor.read(m)` — sync, returns chainable decoder `{ json(), string(), binary(), cbor(), raw() }`
- `ctx.release(handle)` / `actor.release(handle)` — ref-counted cleanup
- `ShareConfig` authorization — owner/group/linked/explicit list
- Per-actor inbox queues (`Map<ActorId, InboxEntry[]>`) with `INBOX` protocol
- FIFO write queue when no FREE boxes available
- Opportunistic lease cleanup (no setTimeout)
- Proper diagnostics with actual timing measurements

## Files to Modify

### Critical Rewrites
1. `src/future/types.ts` — Update all type contracts
2. `src/future/memory-pool.ts` — SAB raw data only, remove state board/lease tracker/atomics
3. `src/future/supervisor.ts` — BoxEntry[], inbox queues, write queue, auth, epoch, INBOX protocol
4. `src/future/worker-bootstrap.ts` — New ctx.write(), ctx.read() chainable, ctx.release(), self.send(msg, data)
5. `src/future/actor.ts` — actor.read(m) chainable, actor.release(handle)

### Moderate Changes
6. `src/future/diagnostics.ts` — Add missing metrics, measure actual durations
7. `src/future/group-manager.ts` — Update for group-level share authorization
8. `src/future/resource-manager.ts` — Track actual call latency

### Test Rewrites
9. `tests/future/supervisor.spec.ts` — Complete rewrite for new APIs
10. `tests/future/memory-pool.spec.ts` — Rewrite for new memory pool
11. `tests/future/diagnostics.spec.ts` — Update for new metrics
12. `tests/future/group-manager.spec.ts` — Update if needed
13. `tests/future/restart.spec.ts` — Verify still passes
14. `tests/future/resource-manager.spec.ts` — Verify still passes

### Documentation
15. `context.md` — Update with new patterns and decisions

## Implementation Order
1. Types (foundation)
2. Memory pool (data layer)
3. Diagnostics (lightweight, no deps)
4. Supervisor (core orchestrator, depends on types + memory pool + diagnostics)
5. Worker bootstrap (depends on supervisor protocol)
6. ActorRef (depends on supervisor + types)
7. Group manager (depends on supervisor)
8. Resource manager (timing updates)
9. Tests (integration)
10. Context update

## Key Invariants to Test
- A box in WRITING state has exactly one writer
- A box in READY state has immutable data
- refCount decrements to 0 before FREE transition
- from is always supervisor-injected sender identity
- No message reaches unauthorized reader
- epoch on Lock prevents use-after-free on recycled boxes
- SAB contains no state metadata — only raw data

## Risk Mitigation
- Copy all modified files to /backup before changes
- Create intent.md with detailed change plan
- Run tests after each major file change
- Keep slang utilities (Result, Option, etc.) untouched
