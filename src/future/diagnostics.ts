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
    recordLockAcquisition: () => {},
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
  const actorStartTimes = (track.actorLifetimes || track.startTimes)
    ? new Map<ActorId, number>()
    : null;

  const actorHeartbeats = (track.heartbeatIntervals || track.actorLifetimes)
    ? new Map<ActorId, number>()
    : null;

  const actorMessages = track.messageLatency
    ? new Map<ActorId, number>()
    : null;

  const actorTerminationReasons = (track.actorLifetimes || track.processLifetimes)
    ? new Map<ActorId, string>()
    : null;

  const lockWaitTimes: number[] | null = track.lockAcquisitionTimes ? [] : null;
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

    recordLockAcquisition: (waitMs: number): void => {
      if (!lockWaitTimes) return;
      if (!shouldSample()) return;
      lockWaitTimes.push(waitMs);
    },

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
    }): SupervisorDiagnostics => ({
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