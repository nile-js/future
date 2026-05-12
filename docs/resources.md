# Resources

**Category:** Guide

## Intent

Document the resource manager in future: how actors access main-thread services, the intent relay pattern, schema validation, and cleanup hooks.

## Responsibilities

- Resource manager concept and architecture
- Intent relay pattern (actor to main thread via proxy)
- Schema validation for resource method inputs and outputs
- Cleanup hooks for graceful resource release

## Non-Goals

- Does not cover shared memory or Tier 2 (see [Shared Memory](https://github.com/nile-js/future/blob/main/docs/shared-memory.md))
- Does not cover supervision (see [Supervision](https://github.com/nile-js/future/blob/main/docs/supervision.md))

## Resource Manager Concept

Resources are services that run on the main thread but are accessible to actors. The resource manager provides safe, validated access through an intent relay pattern. Actors cannot directly access shared state or main-thread objects; all access is mediated through the resource proxy.

## Intent Relay Pattern

1. Actor calls `ctx.resources.db.query(...)`.
2. The Proxy intercepts the call and sends a `RESOURCE_REQUEST` message to the main thread.
3. The main thread validates the input using its Zod schema, executes the handler, and sends a `RESOURCE_RESPONSE` back.
4. The actor receives the result.

```typescript
import { z } from "zod";

const supervisor = createSupervisor({
  resources: {
    database: {
      query: {
        input: z.object({ sql: z.string() }),
        output: z.array(z.unknown()),
        handler: async ({ sql }) => await db.query(sql),
      },
      release: async () => await db.close(),
    },
  },
});

// Inside actor callback
const results = await ctx.resources.database.query({ sql: "SELECT 1" });
```

## Schema Validation

Each resource method declares input and output schemas using Zod. The supervisor validates all data at the boundary:

- Input schema validates arguments before the handler runs
- Output schema validates the return value before sending it to the actor
- Validation failures are returned as errors, not thrown

```typescript
type ResourceMethodConfig = {
  readonly input: ZodTypeAny;
  readonly output: ZodTypeAny;
  readonly handler: (args: unknown) => unknown | Promise<unknown>;
};
```

## Cleanup Hooks

Each resource can define a `release` function that is called during supervisor shutdown. This enables graceful cleanup of connections, file handles, or other resources.

```typescript
const supervisor = createSupervisor({
  resources: {
    database: {
      query: {
        input: z.object({ sql: z.string() }),
        output: z.array(z.unknown()),
        handler: async ({ sql }) => await db.query(sql),
      },
      release: async () => {
        await db.close();
        await pool.end();
      },
    },
  },
});

// Calls all resource release hooks, then terminates all actors
await supervisor.shutdown();
```

The release function is a top-level cleanup hook distinct from method-level operations. It runs once on shutdown and is not called per-actor termination.
