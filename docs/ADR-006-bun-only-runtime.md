# ADR 006: Bun-Only Runtime Support

## Status
Accepted

## Context
`@nilejs/future` uses worker threads for actor isolation. The worker bootstrap file (`src/future/worker-bootstrap.ts`) is written in TypeScript and imported dynamically when spawning actors. The callback serialization model (`fn.toString()` → `new Function()` in worker) requires the worker to import TypeScript modules directly.

## Decision
Target **Bun exclusively** as the runtime. Drop official Node.js support.

## Consequences

### Positive
- **Native TypeScript in workers**: Bun resolves `.ts` imports in worker threads without any loader or pre-compilation. This is critical for our architecture.
- **Simpler testing**: `bun test` works out of the box. No need for `tsx`, `ts-node`, or `vitest` workarounds.
- **Reduced maintenance**: One runtime to test, one set of edge cases, one set of optimizations.
- **Better DX**: Users don't need to configure loaders or pre-build worker files.

### Negative
- **Node.js ecosystem excluded**: Users on Node.js cannot use `@nilejs/future` without switching to Bun.
- **Perception risk**: Some teams may see "Bun-only" as immature or risky.

### Mitigation
- Document the Bun requirement clearly in README and installation docs.
- If demand is high, Node.js support can be added later via:
  - Pre-compiled worker bootstrap (dist/worker-bootstrap.js)
  - `tsx` loader integration
  - Separate `@nilejs/future-node` package

## Related Decisions
- ADR 001 (Callback-Based Spawning): Relies on Bun's native TS support.
- ADR 002 (Two-Tier Communication): Uses `SharedArrayBuffer` and `Atomics`, which work in both runtimes — but the worker entry point is the blocker.

## Planned Future Enhancement
**Node.js support** is tracked as a future milestone. It would require:
1. Pre-compiling `worker-bootstrap.ts` to JS during build.
2. Updating `supervisor.ts` to use the JS path in Node.js environments.
3. Testing with `tsx` or `ts-node` loader.
4. CI matrix for both Bun and Node.js.

This is **not prioritized** for the initial release.
