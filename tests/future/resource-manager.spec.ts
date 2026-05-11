import { describe, expect, it } from "vitest";
import { z } from "zod";
import { createResourceManager } from "../../src/future/resource-manager";

describe("resourceManager", () => {
  // ── Constraint tests ──

  it("rejects unknown resource", async () => {
    const rm = createResourceManager({});
    await expect(rm.execute("nonexistent", "foo", {})).rejects.toThrow(
      "Resource not found: nonexistent",
    );
  });

  it("rejects unknown method", async () => {
    const rm = createResourceManager({
      db: {
        query: {
          input: z.object({ sql: z.string() }),
          output: z.array(z.unknown()),
          handler: (async ({ sql }: { sql: string }) => [{ id: 1 }]) as any,
        },
      },
    });
    await expect(rm.execute("db", "delete", {})).rejects.toThrow(
      "Method not found: db.delete",
    );
  });

  it("rejects calling release as method", async () => {
    const rm = createResourceManager({
      db: {
        query: {
          input: z.object({ sql: z.string() }),
          output: z.array(z.unknown()),
          handler: async () => [],
        },
        release: async () => {},
      },
    });
    await expect(rm.execute("db", "release", {})).rejects.toThrow(
      "Cannot call release as method",
    );
  });

  // ── Happy path ──

  it("executes resource method with valid input", async () => {
    const rm = createResourceManager({
      db: {
        query: {
          input: z.object({ sql: z.string() }),
          output: z.array(z.unknown()),
          handler: (async ({ sql }: { sql: string }) => [{ result: sql }]) as any,
        },
      },
    });
    const result = await rm.execute("db", "query", { sql: "SELECT 1" });
    expect(result).toEqual([{ result: "SELECT 1" }]);
  });

  it("validates output and returns result", async () => {
    const rm = createResourceManager({
      cache: {
        get: {
          input: z.object({ key: z.string() }),
          output: z.object({ value: z.string() }),
          handler: (async ({ key }: { key: string }) => ({ value: `val-${key}` })) as any,
        },
      },
    });
    const result = await rm.execute("cache", "get", { key: "user:1" });
    expect(result).toEqual({ value: "val-user:1" });
  });

  it("calls release hooks on releaseAll", async () => {
    let dbReleased = false;
    let cacheReleased = false;
    const rm = createResourceManager({
      db: {
        query: {
          input: z.object({ sql: z.string() }),
          output: z.unknown(),
          handler: async () => null,
        },
        release: async () => {
          dbReleased = true;
        },
      },
      cache: {
        get: {
          input: z.object({ key: z.string() }),
          output: z.unknown(),
          handler: async () => null,
        },
        release: async () => {
          cacheReleased = true;
        },
      },
    });
    await rm.releaseAll();
    expect(dbReleased).toBe(true);
    expect(cacheReleased).toBe(true);
  });

  // ── Non-happy path ──

  it("throws on invalid input (Zod validation)", async () => {
    const rm = createResourceManager({
      db: {
        query: {
          input: z.object({ sql: z.string() }),
          output: z.unknown(),
          handler: async () => null,
        },
      },
    });
    await expect(rm.execute("db", "query", { sql: 123 })).rejects.toThrow();
  });

  it("throws on invalid output (Zod validation)", async () => {
    const rm = createResourceManager({
      db: {
        query: {
          input: z.object({ sql: z.string() }),
          output: z.object({ id: z.number() }),
          handler: async () => ({ id: "not-a-number" }),
        },
      },
    });
    await expect(rm.execute("db", "query", { sql: "SELECT 1" })).rejects.toThrow();
  });

  it("continues releaseAll even if one release fails", async () => {
    let secondReleased = false;
    const rm = createResourceManager({
      db: {
        query: {
          input: z.object({ sql: z.string() }),
          output: z.unknown(),
          handler: async () => null,
        },
        release: async () => {
          throw new Error("db release failed");
        },
      },
      cache: {
        get: {
          input: z.object({ key: z.string() }),
          output: z.unknown(),
          handler: async () => null,
        },
        release: async () => {
          secondReleased = true;
        },
      },
    });
    await expect(rm.releaseAll()).resolves.toBeUndefined();
    expect(secondReleased).toBe(true);
  });

  // ── Edge cases ──

  it("works with empty resources config", async () => {
    const rm = createResourceManager({});
    await expect(rm.execute("anything", "foo", {})).rejects.toThrow(
      "Resource not found: anything",
    );
    await expect(rm.releaseAll()).resolves.toBeUndefined();
  });

  it("works with undefined resources config", async () => {
    const rm = createResourceManager(undefined);
    await expect(rm.execute("x", "y", {})).rejects.toThrow(
      "Resource not found: x",
    );
    await expect(rm.releaseAll()).resolves.toBeUndefined();
  });

  it("handles handler that throws", async () => {
    const rm = createResourceManager({
      db: {
        query: {
          input: z.object({ sql: z.string() }),
          output: z.unknown(),
          handler: async () => {
            throw new Error("handler crash");
          },
        },
      },
    });
    await expect(rm.execute("db", "query", { sql: "SELECT 1" })).rejects.toThrow(
      "handler crash",
    );
  });

  it("handles resource without release function", async () => {
    const rm = createResourceManager({
      db: {
        query: {
          input: z.object({ sql: z.string() }),
          output: z.unknown(),
          handler: async () => null,
        },
      },
    });
    await expect(rm.releaseAll()).resolves.toBeUndefined();
  });
});
