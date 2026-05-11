import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { calculateBackoff, retrySpawn } from "../../src/future/restart";
import {
  selectActorsToRestart,
  isRestartAllowed,
  createRestartState,
} from "../../src/future/strategies";
import type { ActorRef } from "../../src/future/types";

async function advanceTime(ms: number) {
  vi.advanceTimersByTime(ms);
  await Promise.resolve();
}

const makeRef = (id: string): ActorRef =>
  ({ id, spawn: vi.fn(), subscribe: vi.fn(), terminate: vi.fn(), read: vi.fn(), getDiagnostics: vi.fn(), link: vi.fn(), monitor: vi.fn() }) as unknown as ActorRef;

// ============================================================================
// calculateBackoff
// ============================================================================

describe("calculateBackoff", () => {
  describe("constraints", () => {
    it("never returns negative or zero delay", () => {
      const strategies: Array<"exponential" | "linear" | "fixed"> = [
        "exponential",
        "linear",
        "fixed",
      ];
      for (const s of strategies) {
        for (let attempt = 0; attempt < 10; attempt++) {
          const delay = calculateBackoff({ attempt, backoff: s, baseDelayMs: 100 });
          expect(delay).toBeGreaterThan(0);
        }
      }
    });
  });

  describe("happy path", () => {
    it("exponential backoff doubles each attempt", () => {
      expect(calculateBackoff({ attempt: 0, backoff: "exponential" })).toBe(100);
      expect(calculateBackoff({ attempt: 1, backoff: "exponential" })).toBe(200);
      expect(calculateBackoff({ attempt: 2, backoff: "exponential" })).toBe(400);
      expect(calculateBackoff({ attempt: 3, backoff: "exponential" })).toBe(800);
    });

    it("linear backoff increases linearly", () => {
      expect(calculateBackoff({ attempt: 0, backoff: "linear" })).toBe(100);
      expect(calculateBackoff({ attempt: 1, backoff: "linear" })).toBe(200);
      expect(calculateBackoff({ attempt: 2, backoff: "linear" })).toBe(300);
      expect(calculateBackoff({ attempt: 3, backoff: "linear" })).toBe(400);
    });

    it("fixed backoff stays constant", () => {
      expect(calculateBackoff({ attempt: 0, backoff: "fixed" })).toBe(100);
      expect(calculateBackoff({ attempt: 5, backoff: "fixed" })).toBe(100);
      expect(calculateBackoff({ attempt: 100, backoff: "fixed" })).toBe(100);
    });
  });

  describe("edge cases", () => {
    it("defaults baseDelay to 100ms", () => {
      expect(calculateBackoff({ attempt: 0, backoff: "exponential" })).toBe(100);
      expect(calculateBackoff({ attempt: 0, backoff: "linear" })).toBe(100);
      expect(calculateBackoff({ attempt: 0, backoff: "fixed" })).toBe(100);
    });

    it("clamps baseDelay to at least 1ms", () => {
      expect(calculateBackoff({ attempt: 0, backoff: "exponential", baseDelayMs: 0 })).toBe(1);
      expect(calculateBackoff({ attempt: 0, backoff: "exponential", baseDelayMs: -50 })).toBe(1);
      expect(calculateBackoff({ attempt: 0, backoff: "linear", baseDelayMs: -10 })).toBe(1);
    });

    it("respects custom baseDelay", () => {
      expect(calculateBackoff({ attempt: 0, backoff: "exponential", baseDelayMs: 50 })).toBe(50);
      expect(calculateBackoff({ attempt: 1, backoff: "exponential", baseDelayMs: 50 })).toBe(100);
      expect(calculateBackoff({ attempt: 2, backoff: "linear", baseDelayMs: 25 })).toBe(75);
    });
  });
});

// ============================================================================
// retrySpawn
// ============================================================================

describe("retrySpawn", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("happy path", () => {
    it("succeeds on first attempt", async () => {
      const spawnFn = vi.fn(() => makeRef("actor-1"));
      const result = retrySpawn({ spawnFn, policy: { max: 3, backoff: "exponential" } });
      await advanceTime(0);
      const resolved = await result;
      expect(resolved.isOk).toBe(true);
      if (resolved.isOk) expect(resolved.value.id).toBe("actor-1");
      expect(spawnFn).toHaveBeenCalledTimes(1);
    });

    it("retries and succeeds on second attempt", async () => {
      let attempts = 0;
      const spawnFn = () => {
        attempts++;
        if (attempts < 2) throw new Error(`attempt ${attempts}`);
        return makeRef("actor-1");
      };
      const result = retrySpawn({ spawnFn, policy: { max: 3, backoff: "fixed", delayMs: 50 } });
      await advanceTime(0);
      await advanceTime(50);
      const resolved = await result;
      expect(resolved.isOk).toBe(true);
      expect(attempts).toBe(2);
    });
  });

  describe("non-happy path", () => {
    it("returns Err after max retries exhausted", async () => {
      const spawnFn = vi.fn(() => {
        throw new Error("always fails");
      });
      const result = retrySpawn({ spawnFn, policy: { max: 2, backoff: "fixed", delayMs: 10 } });
      await advanceTime(0);
      await advanceTime(10);
      await advanceTime(10);
      const resolved = await result;
      expect(resolved.isErr).toBe(true);
      expect(spawnFn).toHaveBeenCalledTimes(3);
      if (resolved.isErr) {
        expect(resolved.error).toContain("3 attempts");
      }
    });

    it("waits between retries with backoff", async () => {
      let attempts = 0;
      const spawnFn = () => {
        attempts++;
        if (attempts < 3) throw new Error(`attempt ${attempts}`);
        return makeRef("actor-1");
      };
      const result = retrySpawn({ spawnFn, policy: { max: 3, backoff: "fixed", delayMs: 100 } });
      await advanceTime(0);
      expect(attempts).toBe(1);
      await advanceTime(99);
      expect(attempts).toBe(1);
      await advanceTime(1);
      expect(attempts).toBe(2);
      await advanceTime(100);
      expect(attempts).toBe(3);
      const resolved = await result;
      expect(resolved.isOk).toBe(true);
    });
  });

  describe("edge cases", () => {
    it("handles spawnFn that throws", async () => {
      const spawnFn = vi.fn(() => {
        throw new Error("boom");
      });
      const result = retrySpawn({ spawnFn, policy: { max: 0, backoff: "fixed" } });
      await advanceTime(0);
      const resolved = await result;
      expect(resolved.isErr).toBe(true);
      if (resolved.isErr) {
        expect(resolved.error).toContain("boom");
      }
    });

    it("handles spawnFn that returns invalid ref", async () => {
      const spawnFn = vi.fn(() => ({}) as unknown as ActorRef);
      const result = retrySpawn({ spawnFn, policy: { max: 1, backoff: "fixed", delayMs: 10 } });
      await advanceTime(0);
      await advanceTime(10);
      const resolved = await result;
      expect(resolved.isErr).toBe(true);
      if (resolved.isErr) {
        expect(resolved.error).toContain("invalid ActorRef");
      }
      expect(spawnFn).toHaveBeenCalledTimes(2);
    });
  });
});

// ============================================================================
// selectActorsToRestart
// ============================================================================

describe("selectActorsToRestart", () => {
  const allIds = ["a", "b", "c", "d"] as const;

  it("one-for-one: only failed actor", () => {
    const result = selectActorsToRestart({ failedId: "b", allIds, strategy: "one-for-one" });
    expect(result).toEqual(["b"]);
  });

  it("one-for-all: all actors", () => {
    const result = selectActorsToRestart({ failedId: "b", allIds, strategy: "one-for-all" });
    expect(result).toEqual(["a", "b", "c", "d"]);
  });

  it("rest-for-one: failed + subsequent", () => {
    const result = selectActorsToRestart({ failedId: "b", allIds, strategy: "rest-for-one" });
    expect(result).toEqual(["b", "c", "d"]);
  });

  it("rest-for-one: last actor returns only itself", () => {
    const result = selectActorsToRestart({ failedId: "d", allIds, strategy: "rest-for-one" });
    expect(result).toEqual(["d"]);
  });

  it("rest-for-one: unknown actor returns only that actor", () => {
    const result = selectActorsToRestart({ failedId: "z", allIds, strategy: "rest-for-one" });
    expect(result).toEqual(["z"]);
  });
});

// ============================================================================
// isRestartAllowed
// ============================================================================

describe("isRestartAllowed", () => {
  const policy = { strategy: "one-for-one" as const, maxRestarts: 2, restartWindowMs: 1000 };

  it("allows restart within budget", () => {
    const state = createRestartState();
    expect(isRestartAllowed({ state, policy })).toBe(true);
  });

  it("denies restart when budget exhausted", () => {
    const now = Date.now();
    const state = createRestartState();
    state.restarts.push(now - 500, now - 200);
    expect(isRestartAllowed({ state, policy })).toBe(false);
  });

  it("prunes old restarts outside window", () => {
    const now = Date.now();
    const state = createRestartState();
    state.restarts.push(now - 2000, now - 1500, now - 100);
    const allowed = isRestartAllowed({ state, policy });
    expect(allowed).toBe(true);
    expect(state.restarts.length).toBe(1);
    expect(state.restarts[0]).toBe(now - 100);
  });

  it("allows restart after old entries are pruned", () => {
    const now = Date.now();
    const state = createRestartState();
    state.restarts.push(now - 2000, now - 1500);
    expect(isRestartAllowed({ state, policy })).toBe(true);
  });
});
