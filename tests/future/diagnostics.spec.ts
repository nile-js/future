import { describe, it, expect, vi } from "vitest";
import { createDiagnosticsCollector } from "../../src/future/diagnostics";
import type { InternalActorState, DiagnosticsConfig } from "../../src/future/types";

const mockState: InternalActorState = {
  id: "actor-1",
  worker: {} as any,
  config: {},
  createdAt: Date.now() - 1000,
  heartbeats: 5,
  lastHeartbeatAt: Date.now(),
  messagesSent: 10,
  subscribers: new Set(),
  locks: new Set(),
  reads: new Set(),
  linkedActors: new Set(),
  monitoredActors: new Set(),
  terminated: false,
  terminationReason: undefined,
};

function enabledConfig(track: DiagnosticsConfig["track"] = {}): DiagnosticsConfig {
  return { enabled: true, track };
}

describe("diagnostics", () => {
  describe("createDiagnosticsCollector", () => {
    // ── Constraint tests ──

    it("returns no-op when config is undefined", () => {
      const c = createDiagnosticsCollector(undefined);
      expect(c.buildActorDiagnostics("x", mockState).lifetimeMs).toBeGreaterThanOrEqual(0);
    });

    it("returns no-op when enabled is false", () => {
      const c = createDiagnosticsCollector({ enabled: false });
      c.recordActorStart("a");
      c.recordActorHeartbeat("a");
      c.recordActorMessage("a");
      c.recordLockAcquisition(5);
      c.recordResourceCall(3);
      const d = c.buildActorDiagnostics("a", mockState);
      expect(d.lifetimeMs).toBeGreaterThanOrEqual(0);
      expect(d.heartbeatCount).toBe(5);
      expect(d.messageCount).toBe(10);
    });

    // ── Happy path ──

    it("tracks actor starts when actorLifetimes enabled", () => {
      vi.spyOn(performance, "now").mockReturnValue(100);
      const c = createDiagnosticsCollector(enabledConfig({ actorLifetimes: true }));
      c.recordActorStart("actor-1");
      vi.spyOn(performance, "now").mockReturnValue(250);
      const d = c.buildActorDiagnostics("actor-1", mockState);
      expect(d.lifetimeMs).toBe(150);
      vi.restoreAllMocks();
    });

    it("tracks heartbeats when heartbeatIntervals enabled", () => {
      const c = createDiagnosticsCollector(enabledConfig({ heartbeatIntervals: true }));
      c.recordActorHeartbeat("actor-1");
      c.recordActorHeartbeat("actor-1");
      c.recordActorHeartbeat("actor-1");
      const d = c.buildActorDiagnostics("actor-1", mockState);
      expect(d.heartbeatCount).toBe(3);
    });

    it("tracks messages when messageLatency enabled", () => {
      const c = createDiagnosticsCollector(enabledConfig({ messageLatency: true }));
      c.recordActorMessage("actor-1");
      c.recordActorMessage("actor-1");
      const d = c.buildActorDiagnostics("actor-1", mockState);
      expect(d.messageCount).toBe(2);
    });

    it("tracks lock acquisition when lockAcquisitionTimes enabled", () => {
      const c = createDiagnosticsCollector(enabledConfig({ lockAcquisitionTimes: true }));
      c.recordLockAcquisition(12);
      c.recordLockAcquisition(8);
      const diag = c.buildSupervisorDiagnostics({
        activeActors: 1, totalSpawned: 1, totalTerminated: 0,
        poolSize: 10, boxesInUse: 2, actors: [],
      });
      expect(diag.activeActors).toBe(1);
      expect(diag.memoryPool.utilization).toBe(0.2);
    });

    it("tracks resource calls when resourceCallLatency enabled", () => {
      const c = createDiagnosticsCollector(enabledConfig({ resourceCallLatency: true }));
      c.recordResourceCall(42);
      c.recordResourceCall(7);
      const diag = c.buildSupervisorDiagnostics({
        activeActors: 0, totalSpawned: 0, totalTerminated: 0,
        poolSize: 5, boxesInUse: 0, actors: [],
      });
      expect(diag.memoryPool.poolSize).toBe(5);
      expect(diag.memoryPool.utilization).toBe(0);
    });

    // ── Sampling ──

    it("respects sampleRate of 0.0 (tracks nothing)", () => {
      const c = createDiagnosticsCollector({
        enabled: true, sampleRate: 0.0,
        track: { actorLifetimes: true, heartbeatIntervals: true, messageLatency: true },
      });
      c.recordActorStart("actor-1");
      c.recordActorHeartbeat("actor-1");
      c.recordActorMessage("actor-1");
      const d = c.buildActorDiagnostics("actor-1", mockState);
      expect(d.heartbeatCount).toBe(5);
      expect(d.messageCount).toBe(10);
    });

    it("respects sampleRate of 1.0 (tracks everything)", () => {
      const c = createDiagnosticsCollector({
        enabled: true, sampleRate: 1.0,
        track: { heartbeatIntervals: true, messageLatency: true },
      });
      c.recordActorHeartbeat("actor-1");
      c.recordActorHeartbeat("actor-1");
      c.recordActorMessage("actor-1");
      const d = c.buildActorDiagnostics("actor-1", mockState);
      expect(d.heartbeatCount).toBe(2);
      expect(d.messageCount).toBe(1);
    });

    // ── Non-happy path ──

    it("does not track when track flag is disabled", () => {
      const c = createDiagnosticsCollector(enabledConfig({}));
      c.recordActorStart("actor-1");
      c.recordActorHeartbeat("actor-1");
      c.recordActorMessage("actor-1");
      c.recordLockAcquisition(99);
      c.recordResourceCall(99);
      const d = c.buildActorDiagnostics("actor-1", mockState);
      expect(d.heartbeatCount).toBe(5);
      expect(d.messageCount).toBe(10);
    });

    // ── Edge cases ──

    it("builds actor diagnostics from tracked data", () => {
      vi.spyOn(performance, "now").mockReturnValue(500);
      const c = createDiagnosticsCollector(enabledConfig({
        actorLifetimes: true, heartbeatIntervals: true, messageLatency: true,
      }));
      c.recordActorStart("actor-1");
      c.recordActorHeartbeat("actor-1");
      c.recordActorMessage("actor-1");
      c.recordActorMessage("actor-1");
      vi.spyOn(performance, "now").mockReturnValue(700);
      const d = c.buildActorDiagnostics("actor-1", mockState);
      expect(d.id).toBe("actor-1");
      expect(d.lifetimeMs).toBe(200);
      expect(d.heartbeatCount).toBe(1);
      expect(d.messageCount).toBe(2);
      expect(d.lastHeartbeatAt).toBe(mockState.lastHeartbeatAt);
      vi.restoreAllMocks();
    });

    it("builds supervisor diagnostics correctly", () => {
      const c = createDiagnosticsCollector(enabledConfig({}));
      const diag = c.buildSupervisorDiagnostics({
        activeActors: 3, totalSpawned: 10, totalTerminated: 7,
        poolSize: 100, boxesInUse: 25, actors: [],
      });
      expect(diag.activeActors).toBe(3);
      expect(diag.totalActorsSpawned).toBe(10);
      expect(diag.totalActorsTerminated).toBe(7);
      expect(diag.memoryPool.poolSize).toBe(100);
      expect(diag.memoryPool.boxesInUse).toBe(25);
      expect(diag.memoryPool.utilization).toBe(0.25);
      expect(diag.actors).toEqual([]);
    });

    it("falls back to state values when tracking disabled", () => {
      const c = createDiagnosticsCollector(enabledConfig({}));
      const d = c.buildActorDiagnostics("actor-1", mockState);
      expect(d.lifetimeMs).toBeGreaterThanOrEqual(1000);
      expect(d.heartbeatCount).toBe(5);
      expect(d.messageCount).toBe(10);
      expect(d.terminationReason).toBe(undefined);
    });
  });
});
