import type {
  ActorCallback,
  ActorConfig,
  ActorId,
  ActorGroupConfig,
  GroupState,
} from "./types";
import { safeTry } from "../safe-try";
import {
  selectActorsToRestart,
  isRestartAllowed,
  recordRestart,
} from "./strategies";
import type { RestartPolicy } from "./strategies";

/** Default time window for restart rate limiting (60 seconds) */
const DEFAULT_RESTART_WINDOW_MS = 60_000;

/**
 * Create a group manager that handles supervision groups with auto-restart.
 *
 * Tracks group membership, spawn order, callbacks, and orchestrates
 * restarts on actor failure using the configured supervision strategy.
 *
 * @param options - dependencies for actor lifecycle management
 * @param options.spawnActor - function to spawn a new actor with callback and optional config
 * @param options.terminateActor - function to terminate an actor by id with optional reason
 * @returns group manager API for creating groups, registering actors, and handling failures
 */
export function createGroupManager(options: {
  readonly spawnActor: (callback: ActorCallback, config?: ActorConfig) => import("./types").ActorRef;
  readonly terminateActor: (id: ActorId, reason?: string) => void;
}): {
  readonly createGroup: (config: ActorGroupConfig) => GroupState;
  readonly registerActor: (groupId: string, actorId: ActorId, callback: ActorCallback, config?: ActorConfig) => void;
  readonly handleActorFailure: (actorId: ActorId, reason: string) => Promise<void>;
  readonly getGroupForActor: (actorId: ActorId) => GroupState | undefined;
  readonly destroyGroup: (groupId: string) => void;
} {
  const groups = new Map<string, GroupState>();
  const actorToGroup = new Map<ActorId, string>();

  /** Create a new supervision group with empty state. */
  function createGroup(config: ActorGroupConfig): GroupState {
    const id = `group-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    const state: GroupState = {
      id,
      config,
      actorIds: [],
      callbacks: new Map(),
      restartState: { restarts: [] },
    };
    groups.set(id, state);
    return state;
  }

  /**
   * Register an actor into a supervision group.
   * Maintains spawn order and stores callback+config for potential restart.
   */
  function registerActor(
    groupId: string,
    actorId: ActorId,
    callback: ActorCallback,
    config?: ActorConfig,
  ): void {
    const group = groups.get(groupId);
    if (!group) return;

    group.actorIds.push(actorId);
    group.callbacks.set(actorId, { callback, config });
    actorToGroup.set(actorId, groupId);
  }

  /**
   * Handle an actor failure within its supervision group.
   *
   * If retry is configured and restarts are allowed, restarts the appropriate
   * actors based on the group's strategy. Otherwise cascades failure to all
   * group members by terminating them.
   */
  async function handleActorFailure(actorId: ActorId, reason: string): Promise<void> {
    const groupId = actorToGroup.get(actorId);
    if (!groupId) return;

    const group = groups.get(groupId);
    if (!group) return;

    const { retry } = group.config;

    // If retry is configured, enforce restart budget. Otherwise unlimited restarts.
    if (retry) {
      const policy: RestartPolicy = {
        strategy: group.config.strategy,
        maxRestarts: retry.max,
        restartWindowMs: DEFAULT_RESTART_WINDOW_MS,
      };

      // Restart budget exhausted — cascade failure
      if (!isRestartAllowed({ state: group.restartState, policy })) {
        cascadeTerminate(group, reason);
        return;
      }
    }

    // Determine which actors to restart based on strategy
    const targetIds = selectActorsToRestart({
      failedId: actorId,
      allIds: group.actorIds,
      strategy: group.config.strategy,
    });

    for (const targetId of targetIds) {
      const stored = group.callbacks.get(targetId);
      if (!stored) continue;

      // Terminate the actor if still alive
      options.terminateActor(targetId, `restarting: ${reason}`);

      // Let event loop clear before re-spawn
      await new Promise<void>((resolve) => setTimeout(resolve, 0));

      // Re-spawn with stored callback + config
      const spawnResult = await safeTry(() =>
        options.spawnActor(stored.callback, stored.config),
      );

      if (!spawnResult.isOk) continue;

      const newId = spawnResult.value.id;

      // Record the restart attempt (only when retry budget is enforced)
      if (retry) recordRestart(group.restartState);

      // Replace old id with new id in actorIds (preserves spawn order)
      const idx = group.actorIds.indexOf(targetId);
      if (idx !== -1) {
        group.actorIds[idx] = newId;
      }

      // Update callbacks: remove old entry, add new with same callback+config
      group.callbacks.delete(targetId);
      group.callbacks.set(newId, { callback: stored.callback, config: stored.config });

      // Update actor-to-group mapping
      actorToGroup.delete(targetId);
      actorToGroup.set(newId, groupId);
    }
  }

  /** Terminate all actors in a group and clean up mappings. */
  function cascadeTerminate(group: GroupState, reason: string): void {
    for (const id of group.actorIds) {
      options.terminateActor(id, `cascade: ${reason}`);
      actorToGroup.delete(id);
    }
    group.actorIds.length = 0;
    group.callbacks.clear();
  }

  /** Look up which group an actor belongs to. Returns undefined if not in any group. */
  function getGroupForActor(actorId: ActorId): GroupState | undefined {
    const groupId = actorToGroup.get(actorId);
    if (!groupId) return undefined;
    return groups.get(groupId);
  }

  /**
   * Remove a group from tracking. Does not terminate actors —
   * call terminateAll separately if cleanup is needed.
   */
  function destroyGroup(groupId: string): void {
    const group = groups.get(groupId);
    if (!group) return;

    // Clean up actor-to-group mappings for all current members
    for (const actorId of Array.from(group.callbacks.keys())) {
      actorToGroup.delete(actorId);
    }

    groups.delete(groupId);
  }

  return {
    createGroup,
    registerActor,
    handleActorFailure,
    getGroupForActor,
    destroyGroup,
  };
}