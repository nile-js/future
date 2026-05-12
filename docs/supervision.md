# Supervision

**Category:** Guide

## Intent

Document the supervision system in future: strategies, groups, linking, monitoring, child actor operations, termination guarantees, and auto-restart with backoff.

## Responsibilities

- Supervision strategies (one-for-one, one-for-all, rest-for-one)
- Actor groups for scoped supervision policies
- Bi-directional linking for dependent actors
- Uni-directional monitoring for death notification
- Child actor creation via `ctx.spawn()`
- Termination guarantees and cleanup ordering
- Auto-restart with configurable retry and backoff

## Non-Goals

- Does not cover shared memory or Tier 2 (see [Shared Memory](https://github.com/nile-js/future/blob/main/docs/shared-memory.md))
- Does not cover worker model (see [Architecture](https://github.com/nile-js/future/blob/main/docs/architecture.md))

## Supervision Strategies

When an actor crashes (not a clean shutdown), the supervisor applies the group's strategy:

- **one-for-one:** Restart only the failed actor. Other actors in the group continue running. This is the default strategy.
- **one-for-all:** Restart all actors in the group. The entire group is recreated from stored callbacks.
- **rest-for-one:** Restart the failed actor and all actors that were spawned after it within the same group. Actors spawned before the failed actor continue running.

```typescript
const group = supervisor.createGroup({
  strategy: "rest-for-one",
  retry: { max: 3, backoff: "exponential" },
});
```

## Groups

Every supervisor has an implicit root supervision group. Actors spawned directly with `supervisor.spawn()` register in this group.

Additional groups are created with `supervisor.createGroup()`:

```typescript
const pipeline = supervisor.createGroup({
  strategy: "one-for-one",
  retry: { max: 5, backoff: "exponential", delayMs: 1000 },
});
```

Groups store original callbacks and configs for respawn. They provide scoped spawn and terminate operations:

```typescript
const actor = pipeline.spawn(callback, { name: "stage-1" });
pipeline.terminateAll();
```

## Auto-Restart and Backoff

When retry is configured, the group manager tracks restart attempts per actor:

- `retry.max`: Maximum retry attempts before cascade failure
- `retry.backoff`: Type of backoff, "exponential", "linear", or "fixed"
- `retry.delayMs`: Base delay in milliseconds (default is implementation-specific)

If the retry budget is exhausted, the entire group is terminated (cascade failure). If retry is not configured, actors are always restarted with no budget limit.

```typescript
const group = supervisor.createGroup({
  strategy: "one-for-all",
  retry: { max: 3, backoff: "exponential" },
});
```

## Linking

Linking creates a bi-directional suicide pact. When either linked actor dies, the other is terminated immediately. Termination cascades through the link graph.

```typescript
// Inside actor callback
ctx.link(child);

// On main thread
actorA.link(actorB);
```

Under the hood, `ctx.link()` adds each actor ID to the other's `linkedActors` set. On termination, all linked actors are recursively terminated with reason `linked_actor_died`.

## Monitoring

Monitoring is uni-directional. The monitor receives a `DOWN` message when the monitored actor terminates, but the monitored actor is unaffected.

```typescript
// Inside actor callback
ctx.monitor(child);

// On main thread
actorA.monitor(actorB);
```

Subscribers receive a `DOWN` message:

```typescript
actor.subscribe((msg) => {
  matchAll(msg, {
    DOWN: (m) => console.log(`Actor ${m.data.id} died: ${m.data.reason}`),
    _: () => {},
  });
});
```

## Child Actors

Inside an actor callback, `ctx.spawn()` creates a child actor in a new worker thread. The child receives a parent-child relationship with the spawning actor.

```typescript
const child = await ctx.spawn(async (selfChild, msgChild, ctxChild) => {
  matchAll(msgChild, {
    task: (m) => {
      const result = (m.data as number) * 2;
      selfChild.send("done", { value: result });
    },
    _: () => {},
  });
});

child.spawn({ value: 42 });  // Send initial message
```

The child `ActorRef` from `ctx.spawn()` has a limited API in the worker context: `spawn()`, `terminate()`, `link()`, and `monitor()` work. `subscribe()`, `read()`, and `release()` throw `Not available in worker context`.

## Termination Guarantees

When an actor terminates, the following happens in order:

1. **Worker thread killed:** `worker.terminate()` stops the thread immediately. No cleanup hooks run inside the worker. Pending async operations are abandoned.
2. **Boxes force-released:** All shared memory boxes where the terminated actor is writer or reader are reset to FREE. Writer boxes are reclaimed immediately. Reader refs are decremented; boxes reaching zero refs are freed.
3. **Inboxes cleared:** The actor's inbox queue is removed. Undelivered messages are discarded.
4. **Linked actors terminated:** All actors linked via `ctx.link()` are recursively terminated with reason `linked_actor_died`. Termination cascades through the link graph.
5. **Monitors notified:** All actors monitoring the terminated actor receive a `DOWN` message with the terminated actor's ID and reason.
6. **Write queue entries removed:** Pending write queue requests from the terminated actor are filtered out. Remaining queued writes are served.
7. **Group manager notified:** If termination was not a clean shutdown, the group manager applies the supervision strategy (restart or cascade failure).

```typescript
actor.terminate();                    // Main thread
ctx.terminate();                      // Self-terminate inside actor
supervisor.terminateActor(actor.id);  // Supervisor-level
```

This is true thread termination, not AbortController-style cancellation. AbortController ignores results while the execution continues in the background. Actor termination kills the thread immediately and cleans up resources.
