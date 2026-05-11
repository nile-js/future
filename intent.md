# Intent: Fix AGENTS.md Audit Violations

## What
Fix all code style violations found in the audit of `src/future/`.

## Why
AGENTS.md mandates: no `any`, no raw try/catch, no `switch`, named params, max 400 LOC, JSDoc for public APIs.

## How
1. Fix `any` → `unknown` in types.ts + remove `as any` in strategies.ts
2. Convert raw `try/catch` → `safeTry` in 4 locations
3. Convert `switch` → object lookup in 3 locations
4. Convert positional params → named params in 4 exported functions
5. Add JSDoc to 16 missing types in types.ts
6. Update all callers/tests for breaking changes
7. Typecheck + run full test suite

## Expected Impact
- All source files pass AGENTS.md style rules
- Tests updated to match new signatures
- 344 tests continue to pass
- No functional behavior changes (pure refactoring)

## Files to Modify
- `src/future/types.ts` — `any` → `unknown`, add JSDoc
- `src/future/strategies.ts` — remove `as any`, switch→lookup, positional→named
- `src/future/restart.ts` — positional→named
- `src/future/supervisor.ts` — try/catch→safeTry (2), switch→lookup
- `src/future/worker-bootstrap.ts` — try/catch→safeTry (1), switch→lookup
- `src/future/resource-manager.ts` — try/catch→safeTry (1)
- `src/future/group-manager.ts` — update callers for named params
- `tests/future/*.spec.ts` — update for breaking changes
