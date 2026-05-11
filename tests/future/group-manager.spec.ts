import { describe, it, expect, vi, beforeEach } from "vitest";
import { createGroupManager } from "../../src/future/group-manager";
import type { ActorCallback, ActorConfig, ActorRef } from "../../src/future/types";

const mockSpawnActor = vi.fn((callback: ActorCallback, config?: ActorConfig) => {
  return { id: `actor-${mockSpawnActor.mock.calls.length}` } as ActorRef;
});

const mockTerminateActor = vi.fn();

const groupManager = createGroupManager({
  spawnActor: mockSpawnActor,
  terminateActor: mockTerminateActor,
});

describe("groupManager", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // ==========================================================================
  // Constraint tests
  // ==========================================================================

  it("creates group with unique id", () => {
    const g1 = groupManager.createGroup({ strategy: "one-for-one" });
    const g2 = groupManager.createGroup({ strategy: "one-for-one" });

    expect(g1.id).toBeDefined();
    expect(g2.id).toBeDefined();
    expect(g1.id).not.toBe(g2.id);
    expect(g1.id).toMatch(/^group-/);
    expect(g2.id).toMatch(/^group-/);
  });

  it("registers actors in spawn order", () => {
    const group = groupManager.createGroup({ strategy: "one-for-one" });

    groupManager.registerActor(group.id, "a-1", vi.fn());
    groupManager.registerActor(group.id, "a-2", vi.fn());
    groupManager.registerActor(group.id, "a-3", vi.fn());

    expect(group.actorIds).toEqual(["a-1", "a-2", "a-3"]);
  });

  // ==========================================================================
  // Happy path - one-for-one
  // ==========================================================================

  it("one-for-one: restarts only failed actor", async () => {
    const group = groupManager.createGroup({
      strategy: "one-for-one",
      retry: { max: 3, backoff: "fixed" },
    });

    const cb1 = vi.fn();
    const cb2 = vi.fn();
    const cb3 = vi.fn();

    groupManager.registerActor(group.id, "a-1", cb1);
    groupManager.registerActor(group.id, "a-2", cb2);
    groupManager.registerActor(group.id, "a-3", cb3);

    await groupManager.handleActorFailure("a-2", "crashed");

    // Only a-2 should have been terminated
    expect(mockTerminateActor).toHaveBeenCalledTimes(1);
    expect(mockTerminateActor).toHaveBeenCalledWith("a-2", "restarting: crashed");

    // One new actor should have been spawned
    expect(mockSpawnActor).toHaveBeenCalledTimes(1);
    expect(mockSpawnActor).toHaveBeenCalledWith(cb2, undefined);

    // Group should now contain the new actor id in place of a-2
    expect(group.actorIds).toContain("a-1");
    expect(group.actorIds).toContain("a-3");
    expect(group.actorIds).not.toContain("a-2");
    expect(group.actorIds).toHaveLength(3);
  });

  // ==========================================================================
  // Happy path - one-for-all
  // ==========================================================================

  it("one-for-all: restarts all actors on failure", async () => {
    const group = groupManager.createGroup({
      strategy: "one-for-all",
      retry: { max: 5, backoff: "fixed" },
    });

    const cb1 = vi.fn();
    const cb2 = vi.fn();
    const cb3 = vi.fn();

    groupManager.registerActor(group.id, "a-1", cb1);
    groupManager.registerActor(group.id, "a-2", cb2);
    groupManager.registerActor(group.id, "a-3", cb3);

    await groupManager.handleActorFailure("a-2", "crashed");

    // All three actors should have been terminated
    expect(mockTerminateActor).toHaveBeenCalledTimes(3);
    expect(mockTerminateActor).toHaveBeenCalledWith("a-1", "restarting: crashed");
    expect(mockTerminateActor).toHaveBeenCalledWith("a-2", "restarting: crashed");
    expect(mockTerminateActor).toHaveBeenCalledWith("a-3", "restarting: crashed");

    // All three should have been respawned
    expect(mockSpawnActor).toHaveBeenCalledTimes(3);
    expect(mockSpawnActor).toHaveBeenCalledWith(cb1, undefined);
    expect(mockSpawnActor).toHaveBeenCalledWith(cb2, undefined);
    expect(mockSpawnActor).toHaveBeenCalledWith(cb3, undefined);

    // All original ids replaced
    expect(group.actorIds).not.toContain("a-1");
    expect(group.actorIds).not.toContain("a-2");
    expect(group.actorIds).not.toContain("a-3");
    expect(group.actorIds).toHaveLength(3);
  });

  // ==========================================================================
  // Happy path - rest-for-one
  // ==========================================================================

  it("rest-for-one: restarts failed and subsequent", async () => {
    const group = groupManager.createGroup({
      strategy: "rest-for-one",
      retry: { max: 5, backoff: "fixed" },
    });

    const cb1 = vi.fn();
    const cb2 = vi.fn();
    const cb3 = vi.fn();

    groupManager.registerActor(group.id, "a-1", cb1);
    groupManager.registerActor(group.id, "a-2", cb2);
    groupManager.registerActor(group.id, "a-3", cb3);

    await groupManager.handleActorFailure("a-2", "crashed");

    // a-2 and a-3 should be terminated (a-1 survives)
    expect(mockTerminateActor).toHaveBeenCalledTimes(2);
    expect(mockTerminateActor).toHaveBeenCalledWith("a-2", "restarting: crashed");
    expect(mockTerminateActor).toHaveBeenCalledWith("a-3", "restarting: crashed");

    // a-2 and a-3 should be respawned
    expect(mockSpawnActor).toHaveBeenCalledTimes(2);
    expect(mockSpawnActor).toHaveBeenCalledWith(cb2, undefined);
    expect(mockSpawnActor).toHaveBeenCalledWith(cb3, undefined);

    // a-1 remains, a-2 and a-3 replaced
    expect(group.actorIds).toContain("a-1");
    expect(group.actorIds).not.toContain("a-2");
    expect(group.actorIds).not.toContain("a-3");
    expect(group.actorIds).toHaveLength(3);
  });

  // ==========================================================================
  // Non-happy path
  // ==========================================================================

  it("cascades failure when retry exhausted", async () => {
    const group = groupManager.createGroup({
      strategy: "one-for-one",
      retry: { max: 0, backoff: "fixed" },
    });

    groupManager.registerActor(group.id, "a-1", vi.fn());
    groupManager.registerActor(group.id, "a-2", vi.fn());

    // Record a restart to exhaust the budget (max: 0 means no restarts allowed)
    groupManager.registerActor(group.id, "a-3", vi.fn());

    await groupManager.handleActorFailure("a-2", "crashed");

    // All actors should be cascade-terminated
    expect(mockTerminateActor).toHaveBeenCalledTimes(3);
    expect(mockTerminateActor).toHaveBeenCalledWith("a-1", "cascade: crashed");
    expect(mockTerminateActor).toHaveBeenCalledWith("a-2", "cascade: crashed");
    expect(mockTerminateActor).toHaveBeenCalledWith("a-3", "cascade: crashed");

    // No respawn should occur
    expect(mockSpawnActor).not.toHaveBeenCalled();

    // Group should be emptied
    expect(group.actorIds).toHaveLength(0);
    expect(group.callbacks.size).toBe(0);
  });

  it("restarts actor when no retry config (unlimited restarts)", async () => {
    const group = groupManager.createGroup({
      strategy: "one-for-one",
      // no retry — unlimited restarts
    });

    groupManager.registerActor(group.id, "a-1", vi.fn());
    groupManager.registerActor(group.id, "a-2", vi.fn());

    await groupManager.handleActorFailure("a-1", "crashed");

    // Only failed actor terminated (one-for-one)
    expect(mockTerminateActor).toHaveBeenCalledTimes(1);
    expect(mockTerminateActor).toHaveBeenCalledWith("a-1", "restarting: crashed");

    // Respawned
    expect(mockSpawnActor).toHaveBeenCalledTimes(1);

    // Group still has 2 actors (a-2 + respawned a-1)
    expect(group.actorIds).toHaveLength(2);
  });

  // ==========================================================================
  // Edge cases
  // ==========================================================================

  it("ignores standalone actor failure", async () => {
    // Actor not registered in any group
    await groupManager.handleActorFailure("standalone-actor", "crashed");

    expect(mockTerminateActor).not.toHaveBeenCalled();
    expect(mockSpawnActor).not.toHaveBeenCalled();
  });

  it("getGroupForActor returns correct group", () => {
    const g1 = groupManager.createGroup({ strategy: "one-for-one" });
    const g2 = groupManager.createGroup({ strategy: "one-for-all" });

    groupManager.registerActor(g1.id, "g1-a", vi.fn());
    groupManager.registerActor(g2.id, "g2-a", vi.fn());

    expect(groupManager.getGroupForActor("g1-a")).toBe(g1);
    expect(groupManager.getGroupForActor("g2-a")).toBe(g2);
    expect(groupManager.getGroupForActor("unknown")).toBeUndefined();
  });

  it("destroyGroup cleans up mappings", () => {
    const group = groupManager.createGroup({ strategy: "one-for-one" });

    groupManager.registerActor(group.id, "a-1", vi.fn());
    groupManager.registerActor(group.id, "a-2", vi.fn());

    // Verify mappings exist
    expect(groupManager.getGroupForActor("a-1")).toBe(group);
    expect(groupManager.getGroupForActor("a-2")).toBe(group);

    groupManager.destroyGroup(group.id);

    // Mappings should be cleaned up
    expect(groupManager.getGroupForActor("a-1")).toBeUndefined();
    expect(groupManager.getGroupForActor("a-2")).toBeUndefined();
    expect(groupManager.getGroupForActor(group.id)).toBeUndefined();
  });
});
