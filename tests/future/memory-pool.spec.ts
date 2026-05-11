import { describe, expect, it } from "bun:test";
import { parseBoxSize, createMemoryPool } from "../../src/future/memory-pool";

// ============================================================================
// parseBoxSize
// ============================================================================

describe("parseBoxSize", () => {
  describe("happy path", () => {
    it("parses KB strings correctly", () => {
      expect(parseBoxSize("1KB")).toBe(1024);
      expect(parseBoxSize("2kb")).toBe(2048);
      expect(parseBoxSize("1KiB")).toBe(1024);
    });

    it("parses MB strings correctly", () => {
      expect(parseBoxSize("1MB")).toBe(1024 * 1024);
      expect(parseBoxSize("2mb")).toBe(2 * 1024 * 1024);
      expect(parseBoxSize("1MiB")).toBe(1024 * 1024);
    });

    it("parses GB strings correctly", () => {
      expect(parseBoxSize("1GB")).toBe(1024 * 1024 * 1024);
      expect(parseBoxSize("1GiB")).toBe(1024 * 1024 * 1024);
    });

    it("parses byte strings correctly", () => {
      expect(parseBoxSize("64b")).toBe(64);
      expect(parseBoxSize("100byte")).toBe(100);
      expect(parseBoxSize("256bytes")).toBe(256);
    });

    it("parses numeric values", () => {
      expect(parseBoxSize(1024)).toBe(1024);
      expect(parseBoxSize(1024.7)).toBe(1024);
      expect(parseBoxSize("512")).toBe(512);
    });

    it("handles decimal values and whitespace", () => {
      expect(parseBoxSize("1.5KB")).toBe(1536);
      expect(parseBoxSize("  1KB  ")).toBe(1024);
      expect(parseBoxSize("2 MB")).toBe(2 * 1024 * 1024);
    });
  });

  describe("error cases", () => {
    it("throws on non-positive numeric values", () => {
      expect(() => parseBoxSize(0)).toThrow("Invalid box size");
      expect(() => parseBoxSize(-100)).toThrow("Invalid box size");
    });

    it("throws on non-finite numeric values", () => {
      expect(() => parseBoxSize(Infinity)).toThrow("Invalid box size");
      expect(() => parseBoxSize(NaN)).toThrow("Invalid box size");
    });

    it("throws on unknown unit", () => {
      expect(() => parseBoxSize("5TB")).toThrow("Unknown size unit");
      expect(() => parseBoxSize("10PB")).toThrow("Unknown size unit");
    });

    it("throws on invalid string format", () => {
      expect(() => parseBoxSize("abc")).toThrow("Invalid box size string");
      expect(() => parseBoxSize("")).toThrow("Invalid box size string");
    });

    it("throws on zero string", () => {
      expect(() => parseBoxSize("0")).toThrow("Invalid box size string");
    });
  });
});

// ============================================================================
// createMemoryPool
// ============================================================================

describe("createMemoryPool", () => {
  // --------------------------------------------------------------------------
  // Constraint tests — invariants
  // --------------------------------------------------------------------------

  describe("invariants", () => {
    it("SAB byteLength equals poolSize * boxSize", () => {
      const pool = createMemoryPool({ poolSize: 4, boxSize: 128 });
      expect(pool.sab.byteLength).toBe(4 * 128);
    });

    it("SAB byteLength is correct with string boxSize", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: "1KB" });
      expect(pool.sab.byteLength).toBe(2 * 1024);
      expect(pool.boxSize).toBe(1024);
    });

    it("returned object is frozen", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      expect(Object.isFrozen(pool)).toBe(true);
    });

    it("readBox returns consistent data after writeBox", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.writeBox(0, new Uint8Array([10, 20, 30]));
      const view = pool.readBox(0);
      expect(view[0]).toBe(10);
      expect(view[1]).toBe(20);
      expect(view[2]).toBe(30);
    });

    it("multiple readBox calls return views of same underlying data", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.writeBox(0, new Uint8Array([42]));
      const view1 = pool.readBox(0);
      const view2 = pool.readBox(0);
      expect(view1.buffer).toBe(view2.buffer);
      expect(view1.buffer).toBe(pool.sab);
    });

    it("box isolation: writing to one box does not affect others", () => {
      const pool = createMemoryPool({ poolSize: 3, boxSize: 32 });
      pool.writeBox(0, new Uint8Array([1, 2, 3]));
      pool.writeBox(1, new Uint8Array([4, 5, 6]));
      pool.writeBox(2, new Uint8Array([7, 8, 9]));

      const b0 = pool.readBox(0);
      const b1 = pool.readBox(1);
      const b2 = pool.readBox(2);

      expect(b0[0]).toBe(1);
      expect(b1[0]).toBe(4);
      expect(b2[0]).toBe(7);

      // Verify no cross-contamination
      expect(b0[3]).toBe(0); // box 0 byte 3 untouched
      expect(b1[3]).toBe(0);
      expect(b2[3]).toBe(0);
    });
  });

  // --------------------------------------------------------------------------
  // Happy path
  // --------------------------------------------------------------------------

  describe("pool creation", () => {
    it("creates pool with correct dimensions", () => {
      const pool = createMemoryPool({ poolSize: 4, boxSize: 128 });
      expect(pool.poolSize).toBe(4);
      expect(pool.boxSize).toBe(128);
      expect(pool.sab).toBeInstanceOf(SharedArrayBuffer);
    });

    it("accepts string boxSize and resolves it", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: "1KB" });
      expect(pool.boxSize).toBe(1024);
    });

    it("exposes readBox and writeBox as functions", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      expect(typeof pool.readBox).toBe("function");
      expect(typeof pool.writeBox).toBe("function");
    });
  });

  describe("readBox", () => {
    it("returns Uint8Array view of correct size", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 256 });
      const view = pool.readBox(0);
      expect(view).toBeInstanceOf(Uint8Array);
      expect(view.length).toBe(256);
    });

    it("returns zero-initialized view on fresh pool", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      const view = pool.readBox(0);
      for (let i = 0; i < view.length; i++) {
        expect(view[i]).toBe(0);
      }
    });

    it("returns zero-copy view — modifications reflect in SAB", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      const view = pool.readBox(0);
      view[0] = 99;
      const view2 = pool.readBox(0);
      expect(view2[0]).toBe(99);
    });
  });

  describe("writeBox", () => {
    it("copies data into correct box position", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: 64 });
      pool.writeBox(0, new Uint8Array([1, 2, 3, 4, 5]));
      const view = pool.readBox(0);
      expect(view[0]).toBe(1);
      expect(view[1]).toBe(2);
      expect(view[2]).toBe(3);
      expect(view[3]).toBe(4);
      expect(view[4]).toBe(5);
    });

    it("truncates data larger than boxSize", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 8 });
      pool.writeBox(0, new Uint8Array(16).fill(0xAA));
      const view = pool.readBox(0);
      expect(view.length).toBe(8);
      expect(view[0]).toBe(0xAA);
      expect(view[7]).toBe(0xAA);
    });

    it("leaves trailing zeros for data smaller than boxSize", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 16 });
      pool.writeBox(0, new Uint8Array([1, 2, 3]));
      const view = pool.readBox(0);
      expect(view[0]).toBe(1);
      expect(view[1]).toBe(2);
      expect(view[2]).toBe(3);
      expect(view[3]).toBe(0);
      expect(view[15]).toBe(0);
    });

    it("multiple writes to different boxes work independently", () => {
      const pool = createMemoryPool({ poolSize: 3, boxSize: 16 });
      pool.writeBox(0, new Uint8Array([10]));
      pool.writeBox(1, new Uint8Array([20]));
      pool.writeBox(2, new Uint8Array([30]));

      expect(pool.readBox(0)[0]).toBe(10);
      expect(pool.readBox(1)[0]).toBe(20);
      expect(pool.readBox(2)[0]).toBe(30);
    });

    it("handles empty data write without error", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 64 });
      pool.writeBox(0, new Uint8Array(0));
      expect(pool.readBox(0)[0]).toBe(0);
    });
  });

  // --------------------------------------------------------------------------
  // Non-happy path
  // --------------------------------------------------------------------------

  describe("error handling", () => {
    it("throws on invalid poolSize (0, negative, non-finite)", () => {
      expect(() => createMemoryPool({ poolSize: 0, boxSize: 64 })).toThrow(
        "Invalid poolSize",
      );
      expect(() => createMemoryPool({ poolSize: -1, boxSize: 64 })).toThrow(
        "Invalid poolSize",
      );
      expect(() => createMemoryPool({ poolSize: NaN, boxSize: 64 })).toThrow(
        "Invalid poolSize",
      );
      expect(() => createMemoryPool({ poolSize: Infinity, boxSize: 64 })).toThrow(
        "Invalid poolSize",
      );
    });

    it("readBox throws RangeError on out-of-bounds index", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: 64 });
      expect(() => pool.readBox(-1)).toThrow(RangeError);
      expect(() => pool.readBox(2)).toThrow(RangeError);
      expect(() => pool.readBox(999)).toThrow(RangeError);
    });

    it("writeBox throws RangeError on out-of-bounds index", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: 64 });
      expect(() => pool.writeBox(-1, new Uint8Array([1]))).toThrow(RangeError);
      expect(() => pool.writeBox(2, new Uint8Array([1]))).toThrow(RangeError);
      expect(() => pool.writeBox(999, new Uint8Array([1]))).toThrow(RangeError);
    });
  });

  // --------------------------------------------------------------------------
  // Edge cases
  // --------------------------------------------------------------------------

  describe("edge cases", () => {
    it("works with poolSize 1", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: 32 });
      expect(pool.poolSize).toBe(1);
      pool.writeBox(0, new Uint8Array([42]));
      expect(pool.readBox(0)[0]).toBe(42);
    });

    it("works with boxSize 1", () => {
      const pool = createMemoryPool({ poolSize: 2, boxSize: 1 });
      expect(pool.boxSize).toBe(1);
      expect(pool.sab.byteLength).toBe(2);
      pool.writeBox(0, new Uint8Array([42]));
      expect(pool.readBox(0)[0]).toBe(42);
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

    it("handles maximum reasonable buffer size", () => {
      const pool = createMemoryPool({ poolSize: 1, boxSize: "1MB" });
      expect(pool.boxSize).toBe(1024 * 1024);
      expect(pool.sab.byteLength).toBe(1024 * 1024);
      const view = pool.readBox(0);
      expect(view.length).toBe(1024 * 1024);
    });

    it("concurrent-style access: interleaved reads and writes are consistent", () => {
      const pool = createMemoryPool({ poolSize: 4, boxSize: 64 });

      // Write to all boxes
      for (let i = 0; i < 4; i++) {
        pool.writeBox(i, new Uint8Array([i + 1]));
      }

      // Read all boxes — each should have its own value
      for (let i = 0; i < 4; i++) {
        expect(pool.readBox(i)[0]).toBe(i + 1);
      }

      // Overwrite box 2, verify others unchanged
      pool.writeBox(2, new Uint8Array([99]));
      expect(pool.readBox(0)[0]).toBe(1);
      expect(pool.readBox(1)[0]).toBe(2);
      expect(pool.readBox(2)[0]).toBe(99);
      expect(pool.readBox(3)[0]).toBe(4);
    });
  });
});
