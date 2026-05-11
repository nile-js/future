# Intent: Align Codebase with spec-final.md Architecture

## What We're Changing

The current codebase implements an older actor system API. We are aligning it with the canonical spec-final.md which defines a Two-Tier actor model with immutable SAB boxes, supervisor-side state tracking, per-actor inbox queues, authorization, and a cleaner DSL.

## Files Being Modified

### Core Rewrites
- `src/future/types.ts` — New type contracts: `Lock` with epoch, `BoxEntry`, `InboxEntry`, `ShareConfig`, new `Message` shape, new `ActorContext` API, new `ActorRef` API
- `src/future/memory-pool.ts` — Remove SAB state board and lease tracker. SAB becomes raw byte storage only. Remove all Atomics/CAS operations.
- `src/future/supervisor.ts` — Major rewrite: maintain `BoxEntry[]` parallel to data boxes, per-actor inbox queues (`Map<ActorId, InboxEntry[]>`), FIFO write queue, authorization checks, epoch validation, opportunistic lease cleanup, INBOX protocol delivery, proper TERMINATE_CHILD handling
- `src/future/worker-bootstrap.ts` — New APIs: `ctx.write({ msg, type, data, share })` single call, `ctx.read(m)` returning chainable decoder, `ctx.release(handle)`, `self.send(msg, data?)`, proper `from` injection
- `src/future/actor.ts` — `actor.read(m)` returning chainable decoder, `actor.release(handle)` replacing `actor.done(lock)`

### Supporting Changes
- `src/future/diagnostics.ts` — Add `writeQueueDepth`, `authorizationEvents`, `inboxDepth`, `refCountHistory`, `processLifetimes` tracking. Measure actual lock acquisition wait times and resource call durations with `performance.now()`.
- `src/future/group-manager.ts` — Ensure group membership is available for `ShareConfig` "group" authorization
- `src/future/resource-manager.ts` — Measure actual handler execution duration for `recordResourceCall`

### Test Rewrites
- `tests/future/supervisor.spec.ts` — Complete rewrite using new APIs
- `tests/future/memory-pool.spec.ts` — Rewrite for raw-data SAB pool
- `tests/future/diagnostics.spec.ts` — Update for new metrics
- `tests/future/group-manager.spec.ts` — Verify group auth works
- `tests/future/restart.spec.ts` — Verify still passes
- `tests/future/resource-manager.spec.ts` — Verify latency tracking

## How

1. Rewrite types first (foundation)
2. Rewrite memory pool (no state in SAB)
3. Rewrite supervisor (BoxEntry[], inbox, write queue, auth)
4. Rewrite worker bootstrap (new ctx API)
5. Update actor ref
6. Update diagnostics, group manager, resource manager
7. Rewrite tests
8. Run full suite

## Expected Impact

- Breaking API changes: `acquireLock`/`deposit`/`done` → `ctx.write`. `ctx.read(lock)` async → `ctx.read(m)` sync chainable. `actor.done(lock)` → `actor.release(handle)`.
- All existing future tests will need rewriting. Slang tests should be unaffected.
- SAB memory layout changes: state board and lease tracker removed. Box indices remain the same but state is tracked off-SAB.
- Lock shape changes: `{ boxIndex, epoch }` instead of `{ boxIndex, byteOffset, length }`.

## Invariants to Preserve
- A box in WRITING state has exactly one writer
- A box in READY state has immutable data
- refCount must reach 0 before FREE transition
- `from` is always auto-injected by supervisor
- No unauthorized message delivery
- Epoch prevents use-after-free on recycled boxes
- SAB is raw data only — no state metadata
