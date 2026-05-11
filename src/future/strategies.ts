import type { ActorCallback, ActorConfig, ActorId, ActorRef, SupervisionStrategy } from "./types";

/**
 * Restart policy for a supervision group.
 */
export type RestartPolicy = {
  readonly strategy: SupervisionStrategy;
  readonly maxRestarts: number;
  readonly restartWindowMs: number;
};

/**
 * Tracks restart history for an actor or group.
 */
export type RestartState = {
  restarts: number[];
};

/**
 * Create a restart state tracker.
 */
export function createRestartState(): RestartState {
  return { restarts: [] };
}

/**
 * Check if restart is allowed under the policy.
 * Removes old restart timestamps outside the window.
 */
export function isRestartAllowed(options: { readonly state: RestartState; readonly policy: RestartPolicy }): boolean {
  const { state, policy } = options;
  const now = Date.now();
  const windowStart = now - policy.restartWindowMs;
  const recentRestarts = state.restarts.filter((t) => t > windowStart);
  state.restarts = recentRestarts;
  return recentRestarts.length < policy.maxRestarts;
}

/**
 * Record a restart attempt.
 */
export function recordRestart(state: RestartState): void {
  state.restarts.push(Date.now());
}

/**
 * Strategy → restart selector map. Each strategy decides which actors
 * to restart when one fails.
 */
const strategySelectors: Record<SupervisionStrategy, (failedId: ActorId, allIds: readonly ActorId[]) => readonly ActorId[]> = {
  "one-for-one": (failedId) => [failedId],
  "one-for-all": (_, allIds) => [...allIds],
  "rest-for-one": (failedId, allIds) => {
    const failedIndex = allIds.indexOf(failedId);
    if (failedIndex === -1) return [failedId];
    return allIds.slice(failedIndex);
  },
};

/**
 * Determine which actors to restart based on strategy.
 *
 * @param options.failedId — actor that died
 * @param options.allIds — ordered list of all actor ids in the group
 * @param options.strategy — supervision strategy
 * @returns array of actor ids that should be restarted
 */
export function selectActorsToRestart(options: {
  readonly failedId: ActorId;
  readonly allIds: readonly ActorId[];
  readonly strategy: SupervisionStrategy;
}): readonly ActorId[] {
  return strategySelectors[options.strategy](options.failedId, options.allIds);
}
