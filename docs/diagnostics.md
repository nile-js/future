# Diagnostics

**Category:** Reference

## Intent

Document the diagnostics system in future: configuration options, available metrics, sampling, and zero-cost behavior when disabled.

## Responsibilities

- Configuration reference for all diagnostic options
- Per-metric tracking toggles
- Sampling via sampleRate
- Zero-cost behavior when disabled
- Per-actor diagnostics access
- Supervisor-level diagnostics access

## Non-Goals

- Does not cover architecture or internals (see [Architecture](https://github.com/nile-js/future/blob/main/docs/architecture.md))
- Does not cover supervision (see [Supervision](https://github.com/nile-js/future/blob/main/docs/supervision.md))

## Configuration

Diagnostics are configured when creating the supervisor:

```typescript
const supervisor = createSupervisor({
  diagnostics: {
    enabled: true,
    sampleRate: 1.0,
    track: {
      actorLifetimes: true,
      writeQueueDepth: true,
      bufferUtilization: true,
      authorizationEvents: true,
      inboxDepth: true,
      resourceCallLatency: true,
    },
  },
});
```

### Configuration Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | `boolean` | `false` | Master switch. When false, produces a no-op collector. |
| `sampleRate` | `number` | `1.0` | Gates tracking via `Math.random() < sampleRate`. Range 0.0 to 1.0. |
| `track` | `object` | `{}` | Per-metric toggles. Omitted metrics are not collected. |

### Track Options

| Option | Type | Description |
|---|---|---|
| `actorLifetimes` | `boolean` | Actor start and stop timestamps |
| `startTimes` | `boolean` | Actor creation time |
| `processLifetimes` | `boolean` | Worker thread uptime |
| `writeQueueWait` | `boolean` | Box allocation wait time |
| `writeQueueDepth` | `boolean` | Pending write queue length |
| `messageLatency` | `boolean` | Send-to-receive latency |
| `bufferUtilization` | `boolean` | Memory pool usage |
| `heartbeatIntervals` | `boolean` | Time between heartbeats |
| `resourceCallLatency` | `boolean` | Resource handler duration |
| `authorizationEvents` | `boolean` | Granted versus denied reads |
| `inboxDepth` | `boolean` | Per-actor inbox queue size |
| `refCountHistory` | `boolean` | Box reference count history |

## Sampling

The `sampleRate` field reduces diagnostic overhead at scale by sampling a fraction of events. It uses `Math.random() < sampleRate` to gate tracking. When a metric is omitted from `track`, it is not collected regardless of `sampleRate`.

## Zero-Cost When Disabled

When `diagnostics.enabled` is `false`, the diagnostics collector is a no-op. All method calls return immediately with no work performed. There is no memory allocation or performance impact.

## Per-Actor Diagnostics

Each actor exposes diagnostics via `actor.getDiagnostics()`:

```typescript
const diag = actor.getDiagnostics();
if (diag.isOk) {
  console.log(diag.value.lifetimeMs);
  console.log(diag.value.heartbeatCount);
}
```

The `ActorDiagnostics` type:

```typescript
type ActorDiagnostics = {
  readonly id: ActorId;
  readonly lifetimeMs: number;
  readonly heartbeatCount: number;
  readonly lastHeartbeatAt: number;
  readonly messageCount: number;
  readonly terminationReason?: string;
};
```

## Supervisor-Level Diagnostics

The supervisor exposes aggregate diagnostics via `supervisor.getDiagnostics()`:

```typescript
const diag = supervisor.getDiagnostics();
if (diag.isOk) {
  console.log("Active actors:", diag.value.activeActors);
  console.log("Pool utilization:", diag.value.memoryPool.utilization);
}
```

The `SupervisorDiagnostics` type:

```typescript
type SupervisorDiagnostics = {
  readonly activeActors: number;
  readonly totalActorsSpawned: number;
  readonly totalActorsTerminated: number;
  readonly memoryPool: {
    readonly poolSize: number;
    readonly boxesInUse: number;
    readonly utilization: number;
  };
  readonly actors: readonly ActorDiagnostics[];
  readonly writeQueueWait?: { readonly avgMs: number; readonly maxMs: number; readonly totalWaits: number };
  readonly writeQueueDepth?: { readonly avgDepth: number; readonly maxDepth: number; readonly totalSamples: number };
  readonly authorization?: { readonly granted: number; readonly denied: number };
  readonly inboxDepth?: { readonly maxDepth: number; readonly avgDepth: number; readonly perActor: ReadonlyArray<[ActorId, number]> };
  readonly refCounts?: { readonly avgRefCount: number; readonly maxRefCount: number; readonly samples: ReadonlyArray<{ readonly boxIndex: number; readonly refCount: number }> };
  readonly processLifetimes?: { readonly avgMs: number; readonly maxMs: number; readonly perActor: ReadonlyArray<[ActorId, number]> };
};
```

Optional fields are populated only when the corresponding `track` option is enabled.
