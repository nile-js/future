import { describe, expect, it } from "vitest";
import { parseBoxSize, createMemoryPool } from "../../src/future/memory-pool";
import { BOX_CLEAN, BOX_LOCKED, BOX_READY, BOX_READING, type BoxState } from "../../src/future/types";

// ============================================================================
// parseBoxSize
// ============================================================================

describe("parseBoxSize", () => {
  describe("numeric input", () => {
    it("returns floored value for positive number", () => {
      expect(parseBoxSize(1024)).toBe(1024);
      expect(parseBoxSize(1024.7)).toBe(1024);
    });

    it("throws on zero, negative, Infinity, NaN", () => {
      expect(() => parseBoxSize(0)).toThrow("Invalid box size");
      expect(() => parseBoxSize(-100)).toThrow("Invalid box size");
      expect(() => parseBoxSize(Infinity)).toThrow("Invalid box size");
      expect(() => parseBoxSize(NaN)).toThrow("Invalid box size");
    });
  });

  describe("string input with units", () => {
    it("parses KB (case-insensitive)", () => {
      expect(parseBoxSize("1KB")).toBe(1024);
      expect(parseBoxSize("2kb")).toBe(2048);
      expect(parseBoxSize("1KiB")).toBe(1024);
    });

    it("parses MB (case-insensitive)", () => {
      expect(parseBoxSize("1MB")).toBe(1024 * 1024);
      expect(parseBoxSize("2mb")).toBe(2 * 1024 * 1024);
      expect(parseBoxSize("1MiB")).toBe(1024 * 1024);
    });

    it("parses GB (case-insensitive)", () => {
      expect(parseBoxSize("1GB")).toBe(1024 * 1024 * 1024);
      expect(parseBoxSize("1GiB")).toBe(1024 * 1024 * 1024);
    });

    it("parses bytes", () => {
      expect(parseBoxSize("64b")).toBe(64);
      expect(parseBoxSize("100byte")).toBe(100);
      expect(parseBoxSize("256bytes")).toBe(256);
    });

    it("handles decimal values and whitespace", () => {
      expect(parseBoxSize("1.5KB")).toBe(1536);
      expect(parseBoxSize("  1KB  ")).toBe(1024);
      expect(parseBoxSize("2 MB")).toBe(2 * 1024 * 1024);
    });

    it("throws on unknown unit", () => {
      expect(() => parseBoxSize("5TB")).toThrow("Unknown size unit");
    });
  });

  describe("string input without unit", () => {
    it("parses plain number string", () => {
      expect(parseBoxSize("512")).toBe(512);
      expect(parseBoxSize("1024")).toBe(1024);
    });

    it("throws on invalid or zero string", () => {
      expect(() => parseBoxSize("abc")).toThrow("Invalid box size string");
      expect(() => parseBoxSize("0")).toThrow("Invalid box size string");
    });
  });
});

// ============================================================================
// createMemoryPool
// ============================================================================

describe("memoryPool", () => {
  // --------------------------------------------------------------------------
  // Constraint tests
  // --------------------------------------------------------------------------

  describe("invariants", () => {
    it("initializes all boxes to CLEAN", () => {
      const pool = createMemoryPool({ poolSize: 4, boxSize: 64 });
      for (let i = 0; i < 4; i++) {
        expect(pool.stateBoard[i]).toBe(BOX_CLEAN);
      }
    });

    it("maintains invariant: box state is CLEAN, LOCKED, READY, or READING", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: 64 });
      const validStates = new Set([BOX_CLEAN, BOX_LOCKED, BOX_READY, BOX_READING]);

      pool.markReady(0);
      expect(validStates.has(pool.stateBoard[0] as BoxState)).toBe(true);

      pool.tryAcquireBox(); // acquires box 1 → LOCKED
      expect(validStates.has(pool.stateBoard[1] as BoxState)).toBe(true);

      pool.markClean(0);
      expect(validStates.has(pool.stateBoard[0] as BoxState)).toBe(true);

      pool.markReading(0);
      expect(validStates.has(pool.stateBoard[0] as BoxState)).toBe(true);
    });

    it("public API only produces valid state transitions", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.markReady(0);
      expect(pool.stateBoard[0]).toBe(BOX_READY);
      pool.markReading(0);
      expect(pool.stateBoard[0]).toBe(BOX_READING);
      pool.markClean(0);
      expect(pool.stateBoard[0]).toBe(BOX_CLEAN);
    });
  });

  // --------------------------------------------------------------------------
  // Happy path
  // --------------------------------------------------------------------------

  describe("pool creation", () => {
    it("creates pool with correct layout", () => {
      const pool = createMemoryPool({ poolSize: 4, boxSize: 128 });

      expect(pool.poolSize).toBe(4);
      expect(pool.boxSize).toBe(128);
      expect(pool.sab).toBeInstanceOf(SharedArrayBuffer);
      expect(pool.stateBoard).toBeInstanceOf(Int32Array);
      expect(pool.stateBoard.length).toBe(4);
      expect(pool.leaseTracker).toBeInstanceOf(BigInt64Array);
      expect(pool.leaseTracker.length).toBe(4);
      expect(pool.dataRegion).toBeInstanceOf(Uint8Array);
      expect(pool.dataRegion.length).toBe(4 * 128);
    });

    it("accepts string boxSize", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: "1KB" });
      expect(pool.boxSize).toBe(1024);
      expect(pool.dataRegion.length).toBe(2 * 1024);
    });

    it("returns frozen object", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      expect(Object.isFrozen(pool)).toBe(true);
    });
  });

  describe("tryAcquireBox", () => {
    it("acquires first CLEAN box and returns Lock", () => {
      const pool = createMemoryPool({ poolSize: 3, boxSize: 256 });
      const lock = pool.tryAcquireBox();

      expect(lock).not.toBeNull();
      expect(lock!.boxIndex).toBe(0);
      expect(lock!.length).toBe(256);
      expect(lock!.byteOffset).toBe(pool.getBoxOffset(0));
      expect(pool.stateBoard[0]).toBe(BOX_LOCKED);
    });

    it("skips LOCKED boxes", () => {
      const pool = createMemoryPool({ poolSize: 3, boxSize: 64 });
      pool.tryAcquireBox(); // locks box 0
      const lock = pool.tryAcquireBox();
      expect(lock).not.toBeNull();
      expect(lock!.boxIndex).toBe(1);
    });

    it("returns null when pool exhausted", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: 64 });
      pool.tryAcquireBox();
      pool.tryAcquireBox();
      expect(pool.tryAcquireBox()).toBeNull();
    });

    it("returns frozen Lock object", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      const lock = pool.tryAcquireBox();
      expect(Object.isFrozen(lock)).toBe(true);
    });
  });

  describe("readBox and writeBox", () => {
    it("writes data and reads it back", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: 64 });
      pool.writeBox(0, new Uint8Array([1, 2, 3, 4, 5]));
      const view = pool.readBox(0);
      expect(view[0]).toBe(1);
      expect(view[1]).toBe(2);
      expect(view[2]).toBe(3);
      expect(view[3]).toBe(4);
      expect(view[4]).toBe(5);
    });

    it("readBox returns zero-copy view (subarray, not copy)", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      const view1 = pool.readBox(0);
      const view2 = pool.readBox(0);

      expect(view1.buffer).toBe(view2.buffer);
      expect(view1.buffer).toBe(pool.sab);

      view1[0] = 99;
      expect(view2[0]).toBe(99);
    });

    it("truncates data larger than boxSize", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 8 });
      pool.writeBox(0, new Uint8Array(16).fill(0xAA));
      const view = pool.readBox(0);
      expect(view.length).toBe(8);
      expect(view[0]).toBe(0xAA);
      expect(view[7]).toBe(0xAA);
    });

    it("handles empty data write", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.writeBox(0, new Uint8Array(0));
      expect(pool.readBox(0)[0]).toBe(0);
    });

    it("isolates writes between boxes", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: 32 });
      pool.writeBox(0, new Uint8Array([1, 2, 3]));
      pool.writeBox(1, new Uint8Array([4, 5, 6]));
      expect(pool.readBox(0)[0]).toBe(1);
      expect(pool.readBox(1)[0]).toBe(4);
    });
  });

  describe("getBoxOffset", () => {
    it("returns correct offset for box 0", () => {
      const pool = createMemoryPool({ poolSize: 4, boxSize: 128 });
      expect(pool.getBoxOffset(0)).toBe(pool.dataRegion.byteOffset);
    });

    it("returns sequential offsets", () => {
      const pool = createMemoryPool({ poolSize: 4, boxSize: 128 });
      const off0 = pool.getBoxOffset(0);
      const off1 = pool.getBoxOffset(1);
      const off2 = pool.getBoxOffset(2);
      expect(off1 - off0).toBe(128);
      expect(off2 - off1).toBe(128);
    });

    it("throws on negative or out-of-range index", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: 64 });
      expect(() => pool.getBoxOffset(-1)).toThrow(RangeError);
      expect(() => pool.getBoxOffset(2)).toThrow(RangeError);
      expect(() => pool.getBoxOffset(999)).toThrow(RangeError);
    });
  });

  // --------------------------------------------------------------------------
  // State transitions
  // --------------------------------------------------------------------------

  describe("markReady", () => {
    it("sets box state to READY", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.markReady(0);
      expect(pool.stateBoard[0]).toBe(BOX_READY);
    });

    it("throws on out-of-range index", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      expect(() => pool.markReady(1)).toThrow(RangeError);
    });
  });

  describe("markReading", () => {
    it("sets box state to READING(3)", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.markReading(0);
      expect(pool.stateBoard[0]).toBe(BOX_READING);
    });

    it("throws on out-of-range index", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      expect(() => pool.markReading(1)).toThrow(RangeError);
      expect(() => pool.markReading(-1)).toThrow(RangeError);
    });
  });

  describe("markClean", () => {
    it("sets box state to CLEAN", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.markReady(0);
      pool.markClean(0);
      expect(pool.stateBoard[0]).toBe(BOX_CLEAN);
    });

    it("clears lease when marking clean", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.setLease(0, Date.now() + 60_000);
      expect(pool.isLeaseExpired(0)).toBe(false);
      pool.markClean(0);
      expect(pool.isLeaseExpired(0)).toBe(true);
    });

    it("throws on out-of-range index", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      expect(() => pool.markClean(1)).toThrow(RangeError);
    });
  });

  describe("full state transition cycle", () => {
    it("CLEAN → LOCKED → READY → READING → CLEAN", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });

      // Start: CLEAN
      expect(pool.stateBoard[0]).toBe(BOX_CLEAN);

      // CLEAN → LOCKED (tryAcquireBox)
      const lock = pool.tryAcquireBox();
      expect(lock).not.toBeNull();
      expect(pool.stateBoard[0]).toBe(BOX_LOCKED);

      // LOCKED → READY (markReady)
      pool.markReady(0);
      expect(pool.stateBoard[0]).toBe(BOX_READY);

      // READY → READING (markReading)
      pool.markReading(0);
      expect(pool.stateBoard[0]).toBe(BOX_READING);

      // READING → CLEAN (markClean)
      pool.markClean(0);
      expect(pool.stateBoard[0]).toBe(BOX_CLEAN);
    });
  });

  // --------------------------------------------------------------------------
  // Lease tracking
  // --------------------------------------------------------------------------

  describe("setLease / isLeaseExpired", () => {
    it("returns true when no lease set (0n)", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      expect(pool.isLeaseExpired(0)).toBe(true);
    });

    it("returns false for future expiry", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.setLease(0, Date.now() + 60_000);
      expect(pool.isLeaseExpired(0)).toBe(false);
    });

    it("returns true for past expiry", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.setLease(0, Date.now() - 1000);
      expect(pool.isLeaseExpired(0)).toBe(true);
    });

    it("clearLease sets to 0n", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.setLease(0, Date.now() + 60_000);
      pool.clearLease(0);
      expect(pool.isLeaseExpired(0)).toBe(true);
    });

    it("throws on out-of-range index", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      expect(() => pool.setLease(1, Date.now())).toThrow(RangeError);
      expect(() => pool.clearLease(1)).toThrow(RangeError);
      expect(() => pool.isLeaseExpired(1)).toThrow(RangeError);
    });
  });

  // --------------------------------------------------------------------------
  // Edge cases
  // --------------------------------------------------------------------------

  describe("edge cases", () => {
    it("works with poolSize 1", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 32 });
      expect(pool.poolSize).toBe(1);
      expect(pool.stateBoard.length).toBe(1);

      const lock = pool.tryAcquireBox();
      expect(lock).not.toBeNull();
      expect(lock!.boxIndex).toBe(0);
      expect(pool.tryAcquireBox()).toBeNull();

      pool.markClean(0);
      expect(pool.tryAcquireBox()).not.toBeNull();
    });

    it("works with boxSize 1", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: 1 });
      expect(pool.boxSize).toBe(1);
      expect(pool.dataRegion.length).toBe(2);
      pool.writeBox(0, new Uint8Array([42]));
      expect(pool.readBox(0)[0]).toBe(42);
    });

    it("throws on invalid poolSize", () => {
      expect(() => createMemoryPool({ poolSize: 0, boxSize: 64 })).toThrow("Invalid poolSize");
      expect(() => createMemoryPool({ poolSize: -1, boxSize: 64 })).toThrow("Invalid poolSize");
      expect(() => createMemoryPool({ poolSize: NaN, boxSize: 64 })).toThrow("Invalid poolSize");
    });

    it("writeBox with data exactly boxSize fills completely", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 4 });
      pool.writeBox(0, new Uint8Array([10, 20, 30, 40]));
      const view = pool.readBox(0);
      expect(view[0]).toBe(10);
      expect(view[1]).toBe(20);
      expect(view[2]).toBe(30);
      expect(view[3]).toBe(40);
    });

    it("readBox view length equals boxSize", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 256 });
      expect(pool.readBox(0).length).toBe(256);
    });
  });
});
