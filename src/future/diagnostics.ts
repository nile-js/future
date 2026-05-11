import type {
  ActorId,
  ActorDiagnostics,
  DiagnosticsConfig,
  DiagnosticsCollector,
  InternalActorState,
  SupervisorDiagnostics,
} from "./types";

// ============================================================================
// No-op collector
// ============================================================================

/** Creates a no-op collector — all record methods are empty, build methods use state fallbacks */
function createNoOpCollector(): DiagnosticsCollector {
  return {
    recordActorStart: () => {},
    recordActorHeartbeat: () => {},
    recordActorMessage: () => {},
    recordActorTermination: () => {},
    recordWriteQueueWait: () => {},
    recordWriteQueueDepth: () => {},
    recordAuthorizationEvent: () => {},
    recordInboxDepth: () => {},
    recordRefCount: () => {},
    recordProcessLifetime: () => {},
    recordResourceCall: () => {},
    buildActorDiagnostics: (id, state) => ({
      id,
      lifetimeMs: Date.now() - state.createdAt,
      heartbeatCount: state.heartbeats,
      lastHeartbeatAt: state.lastHeartbeatAt,
      messageCount: state.messagesSent,
      terminationReason: state.terminationReason,
    }),
    buildSupervisorDiagnostics: (options) => ({
      activeActors: options.activeActors,
      totalActorsSpawned: options.totalSpawned,
      totalActorsTerminated: options.totalTerminated,
      memoryPool: {
        poolSize: options.poolSize,
        boxesInUse: options.boxesInUse,
        utilization: options.poolSize > 0 ? options.boxesInUse / options.poolSize : 0,
      },
      actors: options.actors,
    }),
  };
}

// ============================================================================
// Helpers
// ============================================================================

/** Average of an array, returns 0 for empty arrays */
function average(values: readonly number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, v) => sum + v, 0) / values.length;
}

/** Maximum of an array, returns 0 for empty arrays */
function maxOf(values: readonly number[]): number {
  if (values.length === 0) return 0;
  return Math.max(...values);
}

// ============================================================================
// Active collector factory
// ============================================================================

/**
 * Creates a diagnostics collector that respects DiagnosticsConfig.
 * Only tracks metrics when enabled and sampled — returns a no-op collector
 * when config is undefined or disabled. Uses performance.now() for timing.
 */
export function createDiagnosticsCollector(config: DiagnosticsConfig | undefined): DiagnosticsCollector {
  if (!config || !config.enabled) {
    return createNoOpCollector();
  }

  const track = config.track ?? {};
  const sampleRate = config.sampleRate ?? 1.0;

  // Allocate storage only for enabled track flags — null means tracking disabled

  // Actor lifecycle tracking
  const actorStartTimes = (track.actorLifetimes || track.startTimes)
    ? new Map<ActorId, number>()
    : null;

  const actorHeartbeats = (track.heartbeatIntervals || track.actorLifetimes)
    ? new Map<ActorId, number>()
    : null;

  const actorMessages = track.messageLatency
    ? new Map<ActorId, number>()
    : null;

  const actorTerminationReasons = track.actorLifetimes
    ? new Map<ActorId, string>()
    : null;

  // Write queue tracking
  const writeQueueWaitTimes: number[] | null = track.writeQueueWait ? [] : null;
  const writeQueueDepthSamples: number[] | null = track.writeQueueDepth ? [] : null;

  // Authorization tracking
  const authorizationCounts = track.authorizationEvents
    ? { granted: 0, denied: 0 }
    : null;

  // Per-actor inbox depth tracking
  const inboxDepths = track.inboxDepth
    ? new Map<ActorId, number>()
    : null;

  // Ref count history tracking
  const refCountSamples: Array<{ readonly boxIndex: number; readonly refCount: number }> | null =
    track.refCountHistory ? [] : null;

  // Process lifetime tracking
  const processLifetimes = track.processLifetimes
    ? new Map<ActorId, number>()
    : null;

  // Resource call latency tracking
  const resourceCallDurations: number[] | null = track.resourceCallLatency ? [] : null;

  const shouldSample = (): boolean => Math.random() < sampleRate;

  return {
    recordActorStart: (id: ActorId): void => {
      if (!actorStartTimes) return;
      if (!shouldSample()) return;
      actorStartTimes.set(id, performance.now());
    },

    recordActorHeartbeat: (id: ActorId): void => {
      if (!actorHeartbeats) return;
      if (!shouldSample()) return;
      actorHeartbeats.set(id, (actorHeartbeats.get(id) ?? 0) + 1);
    },

    recordActorMessage: (id: ActorId): void => {
      if (!actorMessages) return;
      if (!shouldSample()) return;
      actorMessages.set(id, (actorMessages.get(id) ?? 0) + 1);
    },

    recordActorTermination: (id: ActorId, reason?: string): void => {
      if (!actorTerminationReasons) return;
      if (!shouldSample()) return;
      if (reason !== undefined) {
        actorTerminationReasons.set(id, reason);
      }
    },

    /** Record write queue wait time — how long a pending write request waited */
    recordWriteQueueWait: (waitMs: number): void => {
      if (!writeQueueWaitTimes) return;
      if (!shouldSample()) return;
      writeQueueWaitTimes.push(waitMs);
    },

    /** Record current write queue depth — number of pending write requests */
    recordWriteQueueDepth: (depth: number): void => {
      if (!writeQueueDepthSamples) return;
      if (!shouldSample()) return;
      writeQueueDepthSamples.push(depth);
    },

    /** Record authorization event — whether a read was granted or denied */
    recordAuthorizationEvent: (granted: boolean): void => {
      if (!authorizationCounts) return;
      if (!shouldSample()) return;
      if (granted) {
        authorizationCounts.granted++;
      } else {
        authorizationCounts.denied++;
      }
    },

    /** Record per-actor inbox depth — tracks inbox queue sizes */
    recordInboxDepth: (actorId: ActorId, depth: number): void => {
      if (!inboxDepths) return;
      if (!shouldSample()) return;
      inboxDepths.set(actorId, depth);
    },

    /** Record box ref count — tracks reference counts for memory boxes */
    recordRefCount: (boxIndex: number, refCount: number): void => {
      if (!refCountSamples) return;
      if (!shouldSample()) return;
      refCountSamples.push({ boxIndex, refCount });
    },

    /** Record process lifetime — tracks worker thread uptime in milliseconds */
    recordProcessLifetime: (actorId: ActorId, durationMs: number): void => {
      if (!processLifetimes) return;
      if (!shouldSample()) return;
      processLifetimes.set(actorId, durationMs);
    },

    /** Record resource call duration — measures handler execution time */
    recordResourceCall: (durationMs: number): void => {
      if (!resourceCallDurations) return;
      if (!shouldSample()) return;
      resourceCallDurations.push(durationMs);
    },

    buildActorDiagnostics: (id: ActorId, state: InternalActorState): ActorDiagnostics => {
      const trackedStart = actorStartTimes?.get(id);
      const lifetimeMs = trackedStart !== undefined
        ? performance.now() - trackedStart
        : Date.now() - state.createdAt;

      return {
        id,
        lifetimeMs,
        heartbeatCount: actorHeartbeats?.get(id) ?? state.heartbeats,
        lastHeartbeatAt: state.lastHeartbeatAt,
        messageCount: actorMessages?.get(id) ?? state.messagesSent,
        terminationReason: actorTerminationReasons?.get(id) ?? state.terminationReason,
      };
    },

    buildSupervisorDiagnostics: (options: {
      readonly activeActors: number;
      readonly totalSpawned: number;
      readonly totalTerminated: number;
      readonly poolSize: number;
      readonly boxesInUse: number;
      readonly actors: readonly ActorDiagnostics[];
    }): SupervisorDiagnostics => {
      const result: SupervisorDiagnostics = {
        activeActors: options.activeActors,
        totalActorsSpawned: options.totalSpawned,
        totalActorsTerminated: options.totalTerminated,
        memoryPool: {
          poolSize: options.poolSize,
          boxesInUse: options.boxesInUse,
          utilization: options.poolSize > 0 ? options.boxesInUse / options.poolSize : 0,
        },
        actors: options.actors,
      };

      // Conditionally include write queue wait stats when tracked
      if (writeQueueWaitTimes && writeQueueWaitTimes.length > 0) {
        (result as { writeQueueWait?: SupervisorDiagnostics["writeQueueWait"] }).writeQueueWait = {
          avgMs: average(writeQueueWaitTimes),
          maxMs: maxOf(writeQueueWaitTimes),
          totalWaits: writeQueueWaitTimes.length,
        };
      }

      // Conditionally include write queue depth stats when tracked
      if (writeQueueDepthSamples && writeQueueDepthSamples.length > 0) {
        (result as { writeQueueDepth?: SupervisorDiagnostics["writeQueueDepth"] }).writeQueueDepth = {
          avgDepth: average(writeQueueDepthSamples),
          maxDepth: maxOf(writeQueueDepthSamples),
          totalSamples: writeQueueDepthSamples.length,
        };
      }

      // Conditionally include authorization stats when tracked
      if (authorizationCounts && (authorizationCounts.granted > 0 || authorizationCounts.denied > 0)) {
        (result as { authorization?: SupervisorDiagnostics["authorization"] }).authorization = {
          granted: authorizationCounts.granted,
          denied: authorizationCounts.denied,
        };
      }

      // Conditionally include inbox depth stats when tracked
      if (inboxDepths && inboxDepths.size > 0) {
        const depths = Array.from(inboxDepths.values());
        (result as { inboxDepth?: SupervisorDiagnostics["inboxDepth"] }).inboxDepth = {
          maxDepth: maxOf(depths),
          avgDepth: average(depths),
          perActor: Array.from(inboxDepths.entries()),
        };
      }

      // Conditionally include ref count stats when tracked
      if (refCountSamples && refCountSamples.length > 0) {
        const refCounts = refCountSamples.map((s) => s.refCount);
        (result as { refCounts?: SupervisorDiagnostics["refCounts"] }).refCounts = {
          avgRefCount: average(refCounts),
          maxRefCount: maxOf(refCounts),
          samples: refCountSamples,
        };
      }

      // Conditionally include process lifetime stats when tracked
      if (processLifetimes && processLifetimes.size > 0) {
        const durations = Array.from(processLifetimes.values());
        (result as { processLifetimes?: SupervisorDiagnostics["processLifetimes"] }).processLifetimes = {
          avgMs: average(durations),
          maxMs: maxOf(durations),
          perActor: Array.from(processLifetimes.entries()),
        };
      }

      return result;
    },
  };
}