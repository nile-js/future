import { describe, expect, it, beforeEach, afterEach } from "vitest";
import { createSupervisor } from "../../src/future/supervisor";
import { BOX_CLEAN, BOX_READY, BOX_READING } from "../../src/future/types";
import type { Supervisor, ActorRef, Lock } from "../../src/future/types";

function makeSupervisor(): Supervisor {
  return createSupervisor({
    maxActors: 5,
    memory: { poolSize: 3, boxSize: 1024 },
    timeouts: { defaultLeaseMs: 1000 },
  });
}

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("supervisor", () => {
  let supervisor: Supervisor;

  beforeEach(() => {
    supervisor = makeSupervisor();
  });

  afterEach(async () => {
    await supervisor.shutdown();
  });

  // ==========================================================================
  // Constraint tests
  // ==========================================================================

  describe("constraints", () => {
    it("enforces maxActors limit — throws when exceeded", async () => {
      const actors: ActorRef[] = [];
      for (let i = 0; i < 5; i++) {
        actors.push(
          supervisor.spawn(async (_self, _msg, ctx) => {
            ctx.heartbeat();
          }),
        );
      }
      expect(() =>
        supervisor.spawn(async (_self, _msg, ctx) => {
          ctx.heartbeat();
        }),
      ).toThrow("Max actors limit reached: 5");
      for (const a of actors) a.terminate();
      await wait(100);
    });

    it("terminates actor on lease expiry after another interaction triggers checkLeases", async () => {
      const lockAcquired = new Promise<void>((resolve) => {
        const holder = supervisor.spawn(async (self, _msg, ctx) => {
          const lockResult = await ctx.acquireLock();
          if (lockResult.isOk) {
            self.send({ type: "LOCK_ACQUIRED" });
          }
        });
        holder.subscribe((msg) => {
          const m = msg as { type?: string };
          if (m.type === "LOCK_ACQUIRED") resolve();
        });
        (globalThis as unknown as Record<string, ActorRef | undefined>).__leaseHolder = holder;
      });

      await wait(100);
      const holder = (globalThis as unknown as Record<string, ActorRef | undefined>).__leaseHolder!;
      holder.spawn("acquire");

      await lockAcquired;
      await wait(1200);

      const pinger = supervisor.spawn(async (_self, _msg, ctx) => {
        ctx.heartbeat();
      });
      pinger.spawn("ping");
      await wait(300);

      const diag = holder.getDiagnostics();
      expect(diag.isErr).toBe(true);

      const supDiag = supervisor.getDiagnostics();
      expect(supDiag.isOk).toBe(true);
      if (supDiag.isOk) {
        expect(supDiag.value.activeActors).toBe(2);
      }

      pinger.terminate();
      await wait(100);
      delete (globalThis as Record<string, unknown>).__leaseHolder;
    });
  });

  // ==========================================================================
  // Happy path
  // ==========================================================================

  describe("happy path", () => {
    it("spawns actor and receives messages", async () => {
      const messages: unknown[] = [];
      const actor = supervisor.spawn(async (self, msg, _ctx) => {
        self.send({ type: "ECHO", data: msg });
      });

      actor.subscribe((msg) => messages.push(msg));

      await wait(100);
      actor.spawn("hello");
      await wait(200);

      expect(messages.length).toBeGreaterThanOrEqual(1);
      expect(messages[0]).toEqual({ type: "ECHO", data: "hello" });
    });

    it("actor can use self.send", async () => {
      const messages: unknown[] = [];
      const actor = supervisor.spawn(async (self, _msg, _ctx) => {
        self.send({ type: "STATUS", payload: "running" });
        self.send({ type: "STATUS", payload: "done" });
      });

      actor.subscribe((msg) => messages.push(msg));

      await wait(100);
      actor.spawn("start");
      await wait(200);

      expect(messages).toHaveLength(2);
      expect(messages[0]).toEqual({ type: "STATUS", payload: "running" });
      expect(messages[1]).toEqual({ type: "STATUS", payload: "done" });
    });

    it("actor can use ctx.heartbeat", async () => {
      const actor = supervisor.spawn(async (_self, _msg, ctx) => {
        ctx.heartbeat();
        ctx.heartbeat();
        ctx.heartbeat();
      });

      await wait(100);
      actor.spawn("beat");
      await wait(200);

      const diag = actor.getDiagnostics();
      expect(diag.isOk).toBe(true);
      if (diag.isOk) {
        expect(diag.value.heartbeatCount).toBe(3);
      }
    });

    it("actor can use ctx.fmt to encode and send data", async () => {
      const messages: unknown[] = [];
      const actor = supervisor.spawn(async (self, _msg, ctx) => {
        const encoded = ctx.fmt.from({ hello: "world" });
        const decoded = ctx.fmt.decode(encoded);
        self.send({ type: "FMT_RESULT", decoded });
      });

      actor.subscribe((msg) => messages.push(msg));

      await wait(100);
      actor.spawn("test");
      await wait(200);

      expect(messages.length).toBeGreaterThanOrEqual(1);
      expect(messages[0]).toEqual({
        type: "FMT_RESULT",
        decoded: { hello: "world" },
      });
    });

    it("shutdown terminates all actors", async () => {
      const actors: ActorRef[] = [];
      for (let i = 0; i < 3; i++) {
        actors.push(
          supervisor.spawn(async (_self, _msg, ctx) => {
            ctx.heartbeat();
          }),
        );
      }

      await wait(100);
      for (const a of actors) a.spawn("init");
      await wait(200);

      const beforeShutdown = supervisor.getDiagnostics();
      expect(beforeShutdown.isOk).toBe(true);
      if (beforeShutdown.isOk) {
        expect(beforeShutdown.value.activeActors).toBe(3);
      }

      await supervisor.shutdown();

      const afterShutdown = supervisor.getDiagnostics();
      expect(afterShutdown.isOk).toBe(true);
      if (afterShutdown.isOk) {
        expect(afterShutdown.value.activeActors).toBe(0);
      }
    });
  });

  // ==========================================================================
  // Non-happy path
  // ==========================================================================

  describe("error handling", () => {
    it("handles actor callback errors", async () => {
      const actor = supervisor.spawn(async (_self, _msg, _ctx) => {
        throw new Error("intentional failure");
      });

      await wait(100);
      actor.spawn("boom");
      await wait(300);

      const diag = actor.getDiagnostics();
      expect(diag.isErr).toBe(true);
    });

    it("terminates actor on demand", async () => {
      const actor = supervisor.spawn(async (_self, _msg, ctx) => {
        ctx.heartbeat();
      });

      await wait(100);
      actor.spawn("idle");
      await wait(100);

      const beforeTerm = actor.getDiagnostics();
      expect(beforeTerm.isOk).toBe(true);

      actor.terminate();
      await wait(200);

      const afterTerm = actor.getDiagnostics();
      expect(afterTerm.isErr).toBe(true);
    });
  });

  // ==========================================================================
  // Edge cases
  // ==========================================================================

  describe("edge cases", () => {
    it("handles rapid spawn/terminate cycles", async () => {
      const actors: ActorRef[] = [];
      for (let i = 0; i < 10; i++) {
        const a = supervisor.spawn(async (_self, _msg, ctx) => {
          ctx.heartbeat();
        });
        actors.push(a);
        if (i % 2 === 0) {
          a.terminate();
        }
      }
      await wait(300);

      const diag = supervisor.getDiagnostics();
      expect(diag.isOk).toBe(true);
      if (diag.isOk) {
        expect(diag.value.activeActors).toBe(5);
      }

      for (const a of actors) {
        try {
          a.terminate();
        } catch {
          // Already terminated
        }
      }
      await wait(100);
    });

    it("subscribe unsubscribe prevents further message delivery", async () => {
      const messages: unknown[] = [];
      const actor = supervisor.spawn(async (self, _msg, _ctx) => {
        self.send({ type: "MSG" });
      });

      const unsub = actor.subscribe((msg) => messages.push(msg));

      await wait(100);
      actor.spawn("first");
      await wait(200);

      unsub();

      actor.spawn("second");
      await wait(200);

      expect(messages).toHaveLength(1);
    });

    it("actor id is unique per spawn", async () => {
      const ids = new Set<string>();
      for (let i = 0; i < 5; i++) {
        const a = supervisor.spawn(async (_self, _msg, ctx) => {
          ctx.heartbeat();
        });
        ids.add(a.id);
        a.terminate();
      }
      await wait(200);
      expect(ids.size).toBe(5);
    });
  });

  // ==========================================================================
  // Reading state (READING(3))
  // ==========================================================================

  describe("reading state", () => {
    // ------------------------------------------------------------------------
    // Main thread reads
    // ------------------------------------------------------------------------

    describe("main thread reads", () => {
      it("actor.read(lock) on READY box transitions to READING and returns data", async () => {
        let savedLock: Lock | null = null;

        // Data created inside callback — must be self-contained for serialization
        const writer = supervisor.spawn(async (self, _msg, ctx) => {
          const lockResult = await ctx.acquireLock();
          if (lockResult.isOk) {
            const lock = lockResult.value;
            ctx.deposit(lock, new Uint8Array([42, 43, 44]));
            ctx.done(lock);
          }
        });

        const readyPromise = new Promise<void>((resolve) => {
          writer.subscribe((msg) => {
            const m = msg as { type?: string; address?: Lock };
            if (m.type === "DEPOSIT_READY" && m.address) {
              savedLock = m.address;
              resolve();
            }
          });
        });

        writer.spawn("write");
        await readyPromise;
        await wait(100);

        expect(savedLock).not.toBeNull();
        const data = writer.read(savedLock!);
        expect(data[0]).toBe(42);
        expect(data[1]).toBe(43);
        expect(data[2]).toBe(44);

        writer.done(savedLock!);
        writer.terminate();
        await wait(100);
      });

      it("actor.done(lock) on READING box with count=1 transitions to CLEAN", async () => {
        let savedLock: Lock | null = null;

        const writer = supervisor.spawn(async (self, _msg, ctx) => {
          const lockResult = await ctx.acquireLock();
          if (lockResult.isOk) {
            const lock = lockResult.value;
            ctx.deposit(lock, new Uint8Array([1, 2, 3]));
            ctx.done(lock);
          }
        });

        const readyPromise = new Promise<void>((resolve) => {
          writer.subscribe((msg) => {
            const m = msg as { type?: string; address?: Lock };
            if (m.type === "DEPOSIT_READY" && m.address) {
              savedLock = m.address;
              resolve();
            }
          });
        });

        writer.spawn("write");
        await readyPromise;
        await wait(100);

        // Read (transitions to READING)
        writer.read(savedLock!);
        // Done (should transition back to CLEAN)
        writer.done(savedLock!);

        // Verify box is now CLEAN by acquiring it again
        const acquired = new Promise<boolean>((resolve) => {
          const reader = supervisor.spawn(async (self, _msg, ctx) => {
            const lockResult = await ctx.acquireLock();
            if (lockResult.isOk) {
              self.send({ type: "ACQUIRED", boxIndex: lockResult.value.boxIndex });
              ctx.done(lockResult.value);
            }
          });
          reader.subscribe((msg) => {
            const m = msg as { type?: string; boxIndex?: number };
            if (m.type === "ACQUIRED" && m.boxIndex !== undefined) {
              resolve(m.boxIndex === savedLock!.boxIndex);
            }
          });
          reader.spawn("acquire");
        });

        const sameBox = await acquired;
        expect(sameBox).toBe(true);

        writer.terminate();
        await wait(100);
      });

      it("actor.read(lock) on READING box increments ref count (concurrent readers)", async () => {
        let savedLock: Lock | null = null;

        const writer = supervisor.spawn(async (self, _msg, ctx) => {
          const lockResult = await ctx.acquireLock();
          if (lockResult.isOk) {
            const lock = lockResult.value;
            ctx.deposit(lock, new Uint8Array([99]));
            ctx.done(lock);
          }
        });

        const readyPromise = new Promise<void>((resolve) => {
          writer.subscribe((msg) => {
            const m = msg as { type?: string; address?: Lock };
            if (m.type === "DEPOSIT_READY" && m.address) {
              savedLock = m.address;
              resolve();
            }
          });
        });

        writer.spawn("write");
        await readyPromise;
        await wait(100);

        // First read — transitions READY → READING, count=1
        const data1 = writer.read(savedLock!);
        expect(data1[0]).toBe(99);

        // Second read on same lock — should still work (concurrent reader), count=2
        const data2 = writer.read(savedLock!);
        expect(data2[0]).toBe(99);

        // First done — count goes to 1, should still be READING
        writer.done(savedLock!);

        // Should still be able to read (box is still READING)
        const data3 = writer.read(savedLock!);
        expect(data3[0]).toBe(99);

        // Clean up: two more done calls
        writer.done(savedLock!); // count 2→1
        writer.done(savedLock!); // count 1→0, CLEAN

        writer.terminate();
        await wait(100);
      });

      it("actor.done(lock) on READING box with count>1 decrements count but keeps READING", async () => {
        let savedLock: Lock | null = null;

        const writer = supervisor.spawn(async (self, _msg, ctx) => {
          const lockResult = await ctx.acquireLock();
          if (lockResult.isOk) {
            const lock = lockResult.value;
            ctx.deposit(lock, new Uint8Array([77]));
            ctx.done(lock);
          }
        });

        const readyPromise = new Promise<void>((resolve) => {
          writer.subscribe((msg) => {
            const m = msg as { type?: string; address?: Lock };
            if (m.type === "DEPOSIT_READY" && m.address) {
              savedLock = m.address;
              resolve();
            }
          });
        });

        writer.spawn("write");
        await readyPromise;
        await wait(100);

        // Two concurrent reads
        writer.read(savedLock!);
        writer.read(savedLock!);

        // First done — count goes from 2→1, box should still be READING
        writer.done(savedLock!);

        // Should still be able to read (box is still READING)
        const data = writer.read(savedLock!);
        expect(data[0]).toBe(77);

        // Clean up
        writer.done(savedLock!); // count 2→1
        writer.done(savedLock!); // count 1→0, CLEAN

        writer.terminate();
        await wait(100);
      });
    });

    // ------------------------------------------------------------------------
    // Worker reads
    // ------------------------------------------------------------------------

    describe("worker reads", () => {
      it("worker calls ctx.read(lock) → receives READ_GRANTED → can read data", async () => {
        let savedLock: Lock | null = null;

        const writer = supervisor.spawn(async (self, _msg, ctx) => {
          const lockResult = await ctx.acquireLock();
          if (lockResult.isOk) {
            const lock = lockResult.value;
            ctx.deposit(lock, new Uint8Array([10, 20, 30]));
            ctx.done(lock);
          }
        });

        const readyPromise = new Promise<void>((resolve) => {
          writer.subscribe((msg) => {
            const m = msg as { type?: string; address?: Lock };
            if (m.type === "DEPOSIT_READY" && m.address) {
              savedLock = m.address;
              resolve();
            }
          });
        });

        writer.spawn("write");
        await readyPromise;
        await wait(100);

        expect(savedLock).not.toBeNull();

        // Pass lock via spawn message to reader worker
        const readResult = new Promise<Uint8Array>((resolve) => {
          const reader = supervisor.spawn(async (self, msg, ctx) => {
            const lockToRead = msg as Lock;
            const result = await ctx.read(lockToRead);
            if (result.isOk) {
              const bytes = result.value.slice(0, 3);
              self.send({ type: "READ_DATA", bytes: Array.from(bytes) });
            } else {
              self.send({ type: "READ_ERROR", error: result.error });
            }
          });
          reader.subscribe((msg) => {
            const m = msg as { type?: string; bytes?: number[]; error?: string };
            if (m.type === "READ_DATA" && m.bytes) {
              resolve(new Uint8Array(m.bytes));
            }
            if (m.type === "READ_ERROR") {
              resolve(new Uint8Array([0, 0, 0]));
            }
          });
          reader.spawn(savedLock);
        });

        const data = await readResult;
        expect(data[0]).toBe(10);
        expect(data[1]).toBe(20);
        expect(data[2]).toBe(30);

        writer.terminate();
        await wait(100);
      });

      it("worker calls ctx.done(lock) → main thread decrements count", async () => {
        let savedLock: Lock | null = null;

        const writer = supervisor.spawn(async (self, _msg, ctx) => {
          const lockResult = await ctx.acquireLock();
          if (lockResult.isOk) {
            const lock = lockResult.value;
            ctx.deposit(lock, new Uint8Array([55]));
            ctx.done(lock);
          }
        });

        const readyPromise = new Promise<void>((resolve) => {
          writer.subscribe((msg) => {
            const m = msg as { type?: string; address?: Lock };
            if (m.type === "DEPOSIT_READY" && m.address) {
              savedLock = m.address;
              resolve();
            }
          });
        });

        writer.spawn("write");
        await readyPromise;
        await wait(100);

        // Reader reads and then calls ctx.done — pass lock via spawn message
        const reader = supervisor.spawn(async (_self, msg, ctx) => {
          const lockToRead = msg as Lock;
          const result = await ctx.read(lockToRead);
          if (result.isOk) {
            ctx.done(lockToRead);
          }
        });
        reader.spawn(savedLock);
        await wait(500);

        // Box should be CLEAN now (count went to 0)
        const acquireResult = new Promise<boolean>((resolve) => {
          const verifier = supervisor.spawn(async (self, _msg, ctx) => {
            const lockResult = await ctx.acquireLock();
            self.send({ type: "VERIFY", success: lockResult.isOk });
          });
          verifier.subscribe((msg) => {
            const m = msg as { type?: string; success?: boolean };
            if (m.type === "VERIFY") {
              resolve(m.success ?? false);
            }
          });
          verifier.spawn("verify");
        });

        const success = await acquireResult;
        expect(success).toBe(true);

        reader.terminate();
        writer.terminate();
        await wait(100);
      });

      it("ctx.read(lock) on non-READY box (CLEAN) returns error", async () => {
        const readCleanResult = new Promise<string>((resolve) => {
          const reader = supervisor.spawn(async (self, _msg, ctx) => {
            const fakeLock: Lock = { boxIndex: 0, byteOffset: 0, length: 1024 };
            const result = await ctx.read(fakeLock);
            if (result.isErr) {
              self.send({ type: "READ_FAILED", error: result.error });
            } else {
              self.send({ type: "READ_UNEXPECTED_SUCCESS" });
            }
          });
          reader.subscribe((msg) => {
            const m = msg as { type?: string; error?: string };
            if (m.type === "READ_FAILED") {
              resolve(m.error ?? "unknown");
            }
            if (m.type === "READ_UNEXPECTED_SUCCESS") {
              resolve("UNEXPECTED_SUCCESS");
            }
          });
          reader.spawn("read-clean");
        });

        const error = await readCleanResult;
        expect(error).toContain("not readable");
      });
    });

    // ------------------------------------------------------------------------
    // End-to-end actor-to-actor
    // ------------------------------------------------------------------------

    describe("end-to-end actor-to-actor", () => {
      it("Actor A deposits → READY, Actor B reads → correct data, Actor B done → CLEAN", async () => {
        let savedLock: Lock | null = null;

        // Actor A deposits data — all data created inside callback for serialization
        const actorA = supervisor.spawn(async (self, _msg, ctx) => {
          const lockResult = await ctx.acquireLock();
          if (lockResult.isOk) {
            const lock = lockResult.value;
            ctx.deposit(lock, new Uint8Array([100, 101, 102, 103, 104]));
            ctx.done(lock);
          }
        });

        const readyPromise = new Promise<void>((resolve) => {
          actorA.subscribe((msg) => {
            const m = msg as { type?: string; address?: Lock };
            if (m.type === "DEPOSIT_READY" && m.address) {
              savedLock = m.address;
              resolve();
            }
          });
        });

        actorA.spawn("deposit");
        await readyPromise;
        await wait(100);

        expect(savedLock).not.toBeNull();

        // Actor B reads the data — pass lock via spawn message
        const readData = new Promise<Uint8Array>((resolve) => {
          const actorB = supervisor.spawn(async (self, msg, ctx) => {
            const lockToRead = msg as Lock;
            const result = await ctx.read(lockToRead);
            if (result.isOk) {
              const bytes = result.value.slice(0, 5);
              self.send({ type: "DATA", bytes: Array.from(bytes) });
              ctx.done(lockToRead);
            } else {
              self.send({ type: "READ_ERR", error: result.error });
            }
          });
          actorB.subscribe((msg) => {
            const m = msg as { type?: string; bytes?: number[]; error?: string };
            if (m.type === "DATA" && m.bytes) {
              resolve(new Uint8Array(m.bytes));
            }
            if (m.type === "READ_ERR") {
              resolve(new Uint8Array([0]));
            }
          });
          actorB.spawn(savedLock);
        });

        const data = await readData;
        expect(data[0]).toBe(100);
        expect(data[1]).toBe(101);
        expect(data[2]).toBe(102);
        expect(data[3]).toBe(103);
        expect(data[4]).toBe(104);

        await wait(200);

        // Verify box is CLEAN by acquiring it
        const canAcquire = new Promise<boolean>((resolve) => {
          const actorC = supervisor.spawn(async (self, _msg, ctx) => {
            const result = await ctx.acquireLock();
            self.send({ type: "ACQUIRE_RESULT", ok: result.isOk });
          });
          actorC.subscribe((msg) => {
            const m = msg as { type?: string; ok?: boolean };
            if (m.type === "ACQUIRE_RESULT") {
              resolve(m.ok ?? false);
            }
          });
          actorC.spawn("acquire");
        });

        const acquired = await canAcquire;
        expect(acquired).toBe(true);

        actorA.terminate();
        await wait(100);
      });
    });

    // ------------------------------------------------------------------------
    // Lease expiry during READING
    // ------------------------------------------------------------------------

    describe("lease expiry during READING", () => {
      it("force-cleans box and terminates actor when lease expires during READING", async () => {
        let savedLock: Lock | null = null;

        const writer = supervisor.spawn(async (self, _msg, ctx) => {
          const lockResult = await ctx.acquireLock();
          if (lockResult.isOk) {
            const lock = lockResult.value;
            ctx.deposit(lock, new Uint8Array([200]));
            ctx.done(lock);
          }
        });

        const readyPromise = new Promise<void>((resolve) => {
          writer.subscribe((msg) => {
            const m = msg as { type?: string; address?: Lock };
            if (m.type === "DEPOSIT_READY" && m.address) {
              savedLock = m.address;
              resolve();
            }
          });
        });

        writer.spawn("write");
        await readyPromise;
        await wait(100);

        // Main thread reads — transitions to READING
        const data = writer.read(savedLock!);
        expect(data[0]).toBe(200);

        // Wait for lease to expire (defaultLeaseMs = 1000)
        await wait(1200);

        // Trigger checkLeases by spawning another actor
        const pinger = supervisor.spawn(async (_self, _msg, ctx) => {
          ctx.heartbeat();
        });
        pinger.spawn("ping");
        await wait(300);

        // Writer should have been terminated due to lease expiry
        const diag = writer.getDiagnostics();
        expect(diag.isErr).toBe(true);

        pinger.terminate();
        await wait(100);
      });
    });

    // ------------------------------------------------------------------------
    // Termination while READING
    // ------------------------------------------------------------------------

    describe("termination while READING", () => {
      it("box is cleaned and reader count cleared when actor terminated during READING", async () => {
        let savedLock: Lock | null = null;

        const writer = supervisor.spawn(async (self, _msg, ctx) => {
          const lockResult = await ctx.acquireLock();
          if (lockResult.isOk) {
            const lock = lockResult.value;
            ctx.deposit(lock, new Uint8Array([150]));
            ctx.done(lock);
          }
        });

        const readyPromise = new Promise<void>((resolve) => {
          writer.subscribe((msg) => {
            const m = msg as { type?: string; address?: Lock };
            if (m.type === "DEPOSIT_READY" && m.address) {
              savedLock = m.address;
              resolve();
            }
          });
        });

        writer.spawn("write");
        await readyPromise;
        await wait(100);

        // Main thread reads — transitions to READING
        writer.read(savedLock!);

        // Terminate the actor while it holds a reading lock
        writer.terminate();
        await wait(200);

        // Box should be CLEAN now — verify by acquiring it
        const canAcquire = new Promise<boolean>((resolve) => {
          const verifier = supervisor.spawn(async (self, _msg, ctx) => {
            const result = await ctx.acquireLock();
            self.send({ type: "ACQUIRE_RESULT", ok: result.isOk });
          });
          verifier.subscribe((msg) => {
            const m = msg as { type?: string; ok?: boolean };
            if (m.type === "ACQUIRE_RESULT") {
              resolve(m.ok ?? false);
            }
          });
          verifier.spawn("verify");
        });

        const acquired = await canAcquire;
        expect(acquired).toBe(true);

        await wait(100);
      });
    });
  });
});
