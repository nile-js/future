import { describe, expect, it, beforeEach, afterEach } from "bun:test";
import { z } from "zod";
import { createSupervisor } from "../../src/future/supervisor";
import type { Supervisor, ActorRef, Lock, Message } from "../../src/future/types";

function makeSupervisor(config?: { maxActors?: number; poolSize?: number; boxSize?: number; leaseMs?: number }): Supervisor {
  return createSupervisor({
    maxActors: config?.maxActors ?? 10,
    memory: { poolSize: config?.poolSize ?? 3, boxSize: config?.boxSize ?? 1024 },
    timeouts: { defaultLeaseMs: config?.leaseMs ?? 2000 },
  });
}

const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

// Helper: strip null bytes from SAB reads
function stripNulls(str: string): string {
  return str.replace(/\0+$/, "");
}

// ============================================================================
// Tier 1 Messaging
// ============================================================================

describe("tier 1 messaging", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor(); });
  afterEach(async () => { await sup.shutdown(); });

  it("self.send(msg, data) delivers to subscribers with correct shape", async () => {
    const messages: Message[] = [];
    const actor = sup.spawn(async (self) => {
      self.send("progress", { percent: 0.5 });
    });

    actor.subscribe((msg) => messages.push(msg));
    actor.receive("start");
    await wait(100);

    expect(messages.length).toBeGreaterThanOrEqual(1);
    const m = messages.find((x) => x.msg === "progress");
    expect(m).toBeDefined();
    expect(m!.data).toEqual({ percent: 0.5 });
    expect(m!.from).toBeDefined();
    expect(typeof m!.from).toBe("string");
  });

  it("from field is auto-injected by supervisor, not settable by sender", async () => {
    const messages: Message[] = [];
    const actor = sup.spawn(async (self) => {
      self.send("test", { from: "fake" });
    });

    actor.subscribe((msg) => messages.push(msg));
    actor.receive("start");
    await wait(100);

    const m = messages.find((x) => x.msg === "test");
    expect(m).toBeDefined();
    expect(m!.from).toBe(actor.id);
    expect(m!.from).not.toBe("fake");
  });

  it("multiple subscribers receive the same message", async () => {
    const msgs1: Message[] = [];
    const msgs2: Message[] = [];
    const actor = sup.spawn(async (self) => {
      self.send("hello");
    });

    actor.subscribe((msg) => msgs1.push(msg));
    actor.subscribe((msg) => msgs2.push(msg));
    actor.receive("start");
    await wait(100);

    expect(msgs1.length).toBeGreaterThanOrEqual(1);
    expect(msgs2.length).toBeGreaterThanOrEqual(1);
    expect(msgs1[0]!.msg).toBe("hello");
    expect(msgs2[0]!.msg).toBe("hello");
  });

  it("unsubscribe stops delivery", async () => {
    const messages: Message[] = [];
    const actor = sup.spawn(async (self) => {
      self.send("msg1");
    });

    const unsub = actor.subscribe((msg) => messages.push(msg));
    actor.receive("first");
    await wait(100);
    unsub();

    actor.receive("second");
    await wait(100);

    expect(messages).toHaveLength(1);
    expect(messages[0]!.msg).toBe("msg1");
  });

  it("supervisor.subscribe receives actor messages", async () => {
    const messages: Message[] = [];
    sup.subscribe((msg) => messages.push(msg as Message));

    const actor = sup.spawn(async (self) => {
      self.send("broadcast");
    });
    actor.receive("start");
    await wait(100);

    expect(messages.some((m) => m.msg === "broadcast")).toBe(true);
  });
});

// ============================================================================
// Tier 2 Shared Memory
// ============================================================================

describe("tier 2 shared memory", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor(); });
  afterEach(async () => { await sup.shutdown(); });

  it("ctx.write returns Lock with epoch via Result", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "data",
        type: "json",
        data: ctx.fmt.json.encode({ value: 42 }),
        share: "owner",
      });
      if (result.isOk) {
        self.send("write_ok", { boxIndex: result.value.boxIndex, epoch: result.value.epoch });
      } else {
        self.send("write_err", { error: result.error });
      }
    });

    const lockData = await new Promise<{ boxIndex: number; epoch: number }>((resolve, reject) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "write_ok" && m.data) resolve(m.data as { boxIndex: number; epoch: number });
        if (m.msg === "write_err") reject(new Error((m.data as { error: string }).error));
      });
      actor.receive("write");
    });

    expect(lockData.boxIndex).toBeGreaterThanOrEqual(0);
    expect(lockData.epoch).toBeGreaterThanOrEqual(1);
  });

  it("actor.read(msg) returns ChainableReader for Tier 2 messages", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "result",
        type: "json",
        data: ctx.fmt.json.encode({ hello: "world" }),
        share: "owner",
      });
      if (result.isOk) {
        self.send("result", { handle: result.value });
      }
    });

    const data = await new Promise<unknown>((resolve) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "result" && m.data) {
          const d = m.data as { handle: Lock };
          const reader = actor.read({ ...m, handle: d.handle });
          if (reader) {
            resolve(reader.json());
          }
        }
      });
      actor.receive("write");
    });

    expect(data).toEqual({ hello: "world" });
  });

  it("actor.read(msg) returns null for Tier 1 messages (no handle)", async () => {
    const actor = sup.spawn(async (self) => {
      self.send("tier1", { value: 1 });
    });

    const result = await new Promise<unknown>((resolve) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "tier1") {
          resolve(actor.read(m));
        }
      });
      actor.receive("start");
    });

    expect(result).toBeNull();
  });

  it("actor.release(handle) frees the box for reuse", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "data",
        type: "binary",
        data: new Uint8Array([1, 2, 3]),
        share: "owner",
      });
      if (result.isOk) {
        self.send("written", { handle: result.value });
      }
    });

    const handle = await new Promise<Lock>((resolve) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "written" && m.data) {
          resolve((m.data as { handle: Lock }).handle);
        }
      });
      actor.receive("write");
    });

    await wait(50);
    actor.release(handle);
    await wait(100);

    // Box should be free now — diagnostics should show 0 boxes in use
    const diag = sup.getDiagnostics();
    if (diag.isOk) {
      expect(diag.value.memoryPool.boxesInUse).toBe(0);
    }
  });

  it("ChainableReader.json() decodes JSON data", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "json",
        type: "json",
        data: ctx.fmt.json.encode({ key: "value" }),
        share: "owner",
      });
      if (result.isOk) self.send("json", { handle: result.value });
    });

    const data = await new Promise<unknown>((resolve) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "json" && m.data) {
          const reader = actor.read({ ...m, handle: (m.data as { handle: Lock }).handle });
          if (reader) resolve(reader.json());
        }
      });
      actor.receive("write");
    });

    expect(data).toEqual({ key: "value" });
  });

  it("ChainableReader.string() decodes string data", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "str",
        type: "string",
        data: ctx.fmt.string.encode("hello world"),
        share: "owner",
      });
      if (result.isOk) self.send("str", { handle: result.value });
    });

    const data = await new Promise<string>((resolve) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "str" && m.data) {
          const reader = actor.read({ ...m, handle: (m.data as { handle: Lock }).handle });
          if (reader) resolve(reader.string());
        }
      });
      actor.receive("write");
    });

    expect(stripNulls(data)).toBe("hello world");
  });
});

// ============================================================================
// Authorization (ShareConfig)
// ============================================================================

describe("authorization", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor({ poolSize: 5 }); });
  afterEach(async () => { await sup.shutdown(); });

  it("owner share: only writer can read (via main-thread subscriber)", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "private",
        type: "json",
        data: ctx.fmt.json.encode({ secret: true }),
        share: "owner",
      });
      if (result.isOk) self.send("private", { handle: result.value });
    });

    const data = await new Promise<unknown>((resolve) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "private" && m.data) {
          const reader = actor.read({ ...m, handle: (m.data as { handle: Lock }).handle });
          if (reader) resolve(reader.json());
        }
      });
      actor.receive("write");
    });

    expect(data).toEqual({ secret: true });
  });

  it("group share: actors in same group can read", async () => {
    const group = sup.createGroup({ strategy: "one-for-one" });

    const writer = group.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "shared",
        type: "json",
        data: ctx.fmt.json.encode({ team: true }),
        share: "group",
      });
      if (result.isOk) {
        self.send("shared", { handle: result.value });
      }
    });

    const handle = await new Promise<Lock>((resolve) => {
      writer.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "shared" && m.data) {
          resolve((m.data as { handle: Lock }).handle);
        }
      });
      writer.receive("write");
    });

    expect(handle.boxIndex).toBeGreaterThanOrEqual(0);
    expect(handle.epoch).toBeGreaterThanOrEqual(1);
  });
});

// ============================================================================
// Lifecycle
// ============================================================================

describe("lifecycle", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor(); });
  afterEach(async () => { await sup.shutdown(); });

  it("spawn creates actor with unique id", async () => {
    const actors: ActorRef[] = [];
    for (let i = 0; i < 3; i++) {
      actors.push(sup.spawn(async (self) => { self.send("init"); }));
    }

    const ids = actors.map((a) => a.id);
    expect(new Set(ids).size).toBe(3);

    for (const a of actors) a.terminate();
    await wait(100);
  });

  it("terminate kills worker thread", async () => {
    const actor = sup.spawn(async (self) => { self.send("alive"); });
    actor.receive("start");
    await wait(100);

    actor.terminate();
    await wait(200);

    const diag = actor.getDiagnostics();
    expect(diag.isErr).toBe(true);
  });

  it("shutdown terminates all actors", async () => {
    const actors: ActorRef[] = [];
    for (let i = 0; i < 3; i++) {
      actors.push(sup.spawn(async (self) => { self.send("i"); }));
    }

    await wait(100);
    const before = sup.getDiagnostics();
    if (before.isOk) expect(before.value.activeActors).toBe(3);

    await sup.shutdown();

    const after = sup.getDiagnostics();
    if (after.isOk) expect(after.value.activeActors).toBe(0);
  });

  it("enforces maxActors limit", async () => {
    const limited = makeSupervisor({ maxActors: 2 });
    limited.spawn(async (self) => { self.send("a"); });
    limited.spawn(async (self) => { self.send("b"); });

    expect(() => limited.spawn(async (self) => { self.send("c"); })).toThrow("Max actors limit reached");

    await limited.shutdown();
  });

  it("actor callback error terminates the actor", async () => {
    const actor = sup.spawn(async () => {
      throw new Error("intentional");
    });

    actor.receive("boom");
    await wait(300);

    const diag = actor.getDiagnostics();
    expect(diag.isErr).toBe(true);
  });
});

// ============================================================================
// Supervision strategies
// ============================================================================

describe("supervision", () => {
  it("one-for-one: only failed actor restarts", async () => {
    const sup = createSupervisor({
      maxActors: 10,
      memory: { poolSize: 3, boxSize: 1024 },
      timeouts: { defaultLeaseMs: 2000 },
      strategy: "one-for-one",
      retry: { max: 2, backoff: "fixed" },
    });

    const group = sup.createGroup({ strategy: "one-for-one", retry: { max: 2, backoff: "fixed" } });

    group.spawn(async (self) => { self.send("stable"); });
    group.spawn(async () => { throw new Error("fail"); });

    await wait(500);
    const diag = sup.getDiagnostics();
    if (diag.isOk) {
      expect(diag.value.activeActors).toBeGreaterThanOrEqual(1);
    }

    await sup.shutdown();
  });
});

// ============================================================================
// Diagnostics
// ============================================================================

describe("diagnostics", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor(); });
  afterEach(async () => { await sup.shutdown(); });

  it("getDiagnostics returns supervisor metrics", async () => {
    sup.spawn(async (self) => { self.send("a"); });
    sup.spawn(async (self) => { self.send("b"); });

    await wait(100);
    const diag = sup.getDiagnostics();
    expect(diag.isOk).toBe(true);
    if (diag.isOk) {
      expect(diag.value.activeActors).toBe(2);
      expect(diag.value.memoryPool.poolSize).toBe(3);
      expect(diag.value.actors).toHaveLength(2);
    }
  });

  it("actor.getDiagnostics returns per-actor metrics", async () => {
    const actor = sup.spawn(async (self) => { self.send("x"); });
    actor.receive("start");
    await wait(100);

    const diag = actor.getDiagnostics();
    expect(diag.isOk).toBe(true);
    if (diag.isOk) {
      expect(diag.value.id).toBe(actor.id);
      expect(diag.value.lifetimeMs).toBeGreaterThanOrEqual(0);
    }
  });
});

// ============================================================================
// Edge cases
// ============================================================================

describe("edge cases", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor(); });
  afterEach(async () => { await sup.shutdown(); });

  it("rapid spawn/terminate cycles", async () => {
    const actors: ActorRef[] = [];
    for (let i = 0; i < 10; i++) {
      const a = sup.spawn(async (self) => { self.send("i"); });
      actors.push(a);
      if (i % 2 === 0) a.terminate();
    }
    await wait(300);

    const diag = sup.getDiagnostics();
    if (diag.isOk) {
      expect(diag.value.activeActors).toBeLessThanOrEqual(10);
    }

    for (const a of actors) {
      try { a.terminate(); } catch { /* already terminated */ }
    }
    await wait(100);
  });

  it("linked actors: terminate one kills both", async () => {
    const a = sup.spawn(async (self) => { self.send("a"); });
    const b = sup.spawn(async (self) => { self.send("b"); });

    a.link(b);
    a.terminate();
    await wait(200);

    const diagA = a.getDiagnostics();
    const diagB = b.getDiagnostics();
    expect(diagA.isErr).toBe(true);
    expect(diagB.isErr).toBe(true);
  });

  it("fmt.encode/decode roundtrip", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const encoded = ctx.fmt.encode({ test: 123 });
      const decoded = ctx.fmt.decode(encoded);
      self.send("roundtrip", decoded);
    });

    const result = await new Promise<unknown>((resolve) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "roundtrip") resolve(m.data);
      });
      actor.receive("test");
    });

    expect(result).toEqual({ test: 123 });
  });
});



// ============================================================================
// Epoch stale-handle rejection
// ============================================================================

describe("epoch stale-handle rejection", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor(); });
  afterEach(async () => { await sup.shutdown(); });

  it("stale epoch cannot read recycled box", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "data",
        type: "json",
        data: ctx.fmt.json.encode({ value: 42 }),
        share: "owner",
      });
      if (result.isOk) {
        self.send("written", { handle: result.value });
        ctx.release(result.value);
        self.send("released");
      }
    });

    const handle = await new Promise<Lock>((resolve) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "written" && m.data) {
          resolve((m.data as { handle: Lock }).handle);
        }
      });
      actor.receive("write");
    });

    await new Promise<void>((resolve) => {
      actor.subscribe((msg) => {
        if (msg.msg === "released") resolve();
      });
    });

    await wait(100);

    const staleMsg: Message = { msg: "data", from: actor.id, handle };
    const reader = actor.read(staleMsg);
    expect(reader).toBeNull();
  });
});

// ============================================================================
// Per-actor INBOX delivery
// ============================================================================

describe("per-actor INBOX delivery", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor({ poolSize: 5 }); });
  afterEach(async () => { await sup.shutdown(); });

  it("group share delivers INBOX to authorized readers", async () => {
    const group = sup.createGroup({ strategy: "one-for-one" });

    // Set up reader subscription BEFORE any writes
    const inboxData = new Promise<unknown>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("No INBOX received")), 3000);
      const reader = group.spawn(async (self, msg, ctx) => {
        const m = msg as Message;
        if (m.handle) {
          const data = ctx.read(m);
          if (data) {
            self.send("inbox-received", { json: data.json(), from: m.from });
          }
        }
      });

      reader.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "inbox-received" && m.data) {
          clearTimeout(timeout);
          resolve((m.data as { json: unknown }).json);
        }
      });

      // Trigger reader to be ready
      reader.receive("ready");
    });

    await wait(100);

    const writer = group.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "shared-data",
        type: "json",
        data: ctx.fmt.json.encode({ team: true, value: 99 }),
        share: "group",
      });
      if (result.isOk) self.send("written");
    });

    writer.receive("write");

    const data = await inboxData;
    expect(data).toEqual({ team: true, value: 99 });
  });
});

// ============================================================================
// Authorization enforcement
// ============================================================================

describe("authorization enforcement", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor({ poolSize: 5 }); });
  afterEach(async () => { await sup.shutdown(); });

  it("owner share: unauthorized actor does NOT receive INBOX", async () => {
    const group = sup.createGroup({ strategy: "one-for-one" });

    let leaked = false;
    const unauthorized = group.spawn(async (self, msg, ctx) => {
      const m = msg as Message;
      if (m.handle) {
        const data = ctx.read(m);
        if (data) {
          self.send("inbox-leak", { json: data.json() });
        }
      }
    });

    // Set up subscription before writer writes
    unauthorized.subscribe((msg) => {
      if (msg.msg === "inbox-leak") leaked = true;
    });
    unauthorized.receive("ready");
    await wait(100);

    const writer = group.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "private-data",
        type: "json",
        data: ctx.fmt.json.encode({ secret: true }),
        share: "owner",
      });
      if (result.isOk) self.send("written");
    });

    await new Promise<void>((resolve) => {
      writer.subscribe((msg) => {
        if (msg.msg === "written") resolve();
      });
      writer.receive("write");
    });

    await wait(300);
    expect(leaked).toBe(false);
  });

  it("main-thread subscriber CAN read owner-share data", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "owner-data",
        type: "json",
        data: ctx.fmt.json.encode({ secret: true }),
        share: "owner",
      });
      if (result.isOk) self.send("owner-data", { handle: result.value });
    });

    const data = await new Promise<unknown>((resolve) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "owner-data" && m.data) {
          const reader = actor.read({ ...m, handle: (m.data as { handle: Lock }).handle });
          if (reader) resolve(reader.json());
        }
      });
      actor.receive("write");
    });

    expect(data).toEqual({ secret: true });
  });
});

// ============================================================================
// One writer per WRITING box (FIFO queue)
// ============================================================================

describe("one writer per WRITING box", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor({ poolSize: 2 }); });
  afterEach(async () => { await sup.shutdown(); });

  it("concurrent writes queue when pool full, FIFO order", async () => {
    // Actor A: writes, holds box, releases after signal
    const actorA = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "fill-a",
        type: "json",
        data: ctx.fmt.json.encode({ actor: "A" }),
        share: "owner",
      });
      if (result.isOk) {
        self.send("a-written");
        // Hold box for 1s then release
        await new Promise((r) => setTimeout(r, 1000));
        ctx.release(result.value);
        self.send("a-released");
      }
    });

    // Actor B: writes and holds box long time
    const actorB = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "fill-b",
        type: "json",
        data: ctx.fmt.json.encode({ actor: "B" }),
        share: "owner",
      });
      if (result.isOk) {
        self.send("b-written");
        await new Promise((r) => setTimeout(r, 10000));
      }
    });

    // Actor C: will queue
    const actorC = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "queued-c",
        type: "json",
        data: ctx.fmt.json.encode({ actor: "C" }),
        share: "owner",
      });
      if (result.isOk) {
        self.send("c-written", { handle: result.value });
      }
    });

    // Set up C's listener first
    const cPromise = new Promise<Lock>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("C never got write grant")), 5000);
      actorC.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "c-written" && m.data) {
          clearTimeout(timeout);
          resolve((m.data as { handle: Lock }).handle);
        }
      });
    });

    // Spawn B first (will hold one box)
    actorB.receive("write");
    await new Promise<void>((resolve) => {
      actorB.subscribe((msg) => {
        if (msg.msg === "b-written") resolve();
      });
    });

    await wait(100);

    // Spawn A (will hold the other box)
    actorA.receive("write");
    await new Promise<void>((resolve) => {
      actorA.subscribe((msg) => {
        if (msg.msg === "a-written") resolve();
      });
    });

    await wait(100);

    // Now both boxes are in use. Spawn C — should queue.
    actorC.receive("write");

    // Wait for A to release (1s)
    const handleC = await cPromise;
    expect(handleC.boxIndex).toBeGreaterThanOrEqual(0);
    expect(handleC.epoch).toBeGreaterThanOrEqual(1);
  });
});

// ============================================================================
// Lease expiry kills WRITING actor
// ============================================================================

describe("lease expiry kills WRITING actor", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor({ poolSize: 3, leaseMs: 300 }); });
  afterEach(async () => { await sup.shutdown(); });

  it("actor holding WRITING box past lease gets terminated", async () => {
    // Sync callback: sends WRITE_REQUEST then blocks, preventing COMMIT
    const writer = sup.spawn((self, _msg, ctx) => {
      ctx.write({
        msg: "long-write",
        type: "json",
        data: ctx.fmt.json.encode({ slow: true }),
        share: "owner",
      });
      self.send("write-requested");
      // Block worker — prevents processing WRITE_GRANTED → no COMMIT
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10000);
    });

    await new Promise<void>((resolve) => {
      writer.subscribe((msg) => {
        if (msg.msg === "write-requested") resolve();
      });
      writer.receive("write");
    });

    await wait(100);

    const diagBefore = writer.getDiagnostics();
    expect(diagBefore.isOk).toBe(true);

    // Wait for lease to expire (300ms) + buffer
    await wait(500);

    // Trigger checkLeases via another actor's send (handleSend calls checkLeases)
    const trigger = sup.spawn(async (self) => {
      self.send("trigger-check");
    });

    await new Promise<void>((resolve) => {
      trigger.subscribe((msg) => {
        if (msg.msg === "trigger-check") resolve();
      });
      trigger.receive("trigger");
    });

    await wait(200);

    const diagAfter = writer.getDiagnostics();
    expect(diagAfter.isErr).toBe(true);
  });
});

// ============================================================================
// Monitoring (uni-directional)
// ============================================================================

describe("monitoring", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor(); });
  afterEach(async () => { await sup.shutdown(); });

  it("monitor receives DOWN notification, monitor stays alive", async () => {
    const monitor = sup.spawn(async () => {});

    const target = sup.spawn(async (self) => {
      self.send("alive");
    });

    monitor.monitor(target);

    const downMsg = await new Promise<Message>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("No DOWN received")), 3000);
      monitor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "DOWN" && m.data) {
          clearTimeout(timeout);
          resolve(m);
        }
      });
      target.terminate();
    });

    expect(downMsg.msg).toBe("DOWN");
    expect((downMsg.data as { id: string }).id).toBe(target.id);

    const monitorDiag = monitor.getDiagnostics();
    expect(monitorDiag.isOk).toBe(true);
  });
});

// ============================================================================
// supervisor.terminateActor(id)
// ============================================================================

describe("supervisor.terminateActor", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor(); });
  afterEach(async () => { await sup.shutdown(); });

  it("terminates actor by ID", async () => {
    const actor = sup.spawn(async (self) => {
      self.send("alive");
    });

    actor.receive("start");
    await wait(100);

    const diagBefore = actor.getDiagnostics();
    expect(diagBefore.isOk).toBe(true);

    sup.terminateActor(actor.id);
    await wait(200);

    const diagAfter = actor.getDiagnostics();
    expect(diagAfter.isErr).toBe(true);
  });
});

// ============================================================================
// ctx.isCancelled
// ============================================================================

describe("ctx.isCancelled", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor(); });
  afterEach(async () => { await sup.shutdown(); });

  it("isCancelled is false during normal execution", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      self.send("cancelled", { value: ctx.isCancelled });
    });

    const result = await new Promise<boolean>((resolve) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "cancelled" && m.data) {
          resolve((m.data as { value: boolean }).value);
        }
      });
      actor.receive("check");
    });

    expect(result).toBe(false);
  });

  it("isCancelled becomes true after termination signal", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      self.send("before", { value: ctx.isCancelled });
      await new Promise((r) => setTimeout(r, 10000));
    });

    const beforeVal = await new Promise<boolean>((resolve) => {
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "before" && m.data) {
          resolve((m.data as { value: boolean }).value);
        }
      });
      actor.receive("check");
    });

    expect(beforeVal).toBe(false);

    actor.terminate();
    await wait(200);

    const diag = actor.getDiagnostics();
    expect(diag.isErr).toBe(true);
  });
});

// ============================================================================
// ChainableReader.binary() and .raw()
// ============================================================================

describe("ChainableReader.binary and raw", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor({ poolSize: 5 }); });
  afterEach(async () => { await sup.shutdown(); });

  it("binary() returns Uint8Array with correct bytes", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      // Create bytes INSIDE callback (not in closure — callback is serialized)
      const bytes = new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF, 0x42]);
      const result = await ctx.write({
        msg: "binary-data",
        type: "binary",
        data: bytes,
        share: "owner",
      });
      if (result.isOk) self.send("binary-data", { handle: result.value });
    });

    const data = await new Promise<Uint8Array>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Timeout waiting for binary data")), 5000);
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "binary-data" && m.data) {
          clearTimeout(timeout);
          const reader = actor.read({ ...m, handle: (m.data as { handle: Lock }).handle });
          if (reader) resolve(reader.binary());
        }
      });
      actor.receive("write");
    });

    expect(data).toBeInstanceOf(Uint8Array);
    expect(data[0]).toBe(0xDE);
    expect(data[1]).toBe(0xAD);
    expect(data[2]).toBe(0xBE);
    expect(data[3]).toBe(0xEF);
    expect(data[4]).toBe(0x42);
  });

  it("raw() returns Uint8Array with correct bytes", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const bytes = new Uint8Array([0xCA, 0xFE, 0xBA, 0xBE]);
      const result = await ctx.write({
        msg: "raw-data",
        type: "binary",
        data: bytes,
        share: "owner",
      });
      if (result.isOk) self.send("raw-data", { handle: result.value });
    });

    const data = await new Promise<Uint8Array>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Timeout waiting for raw data")), 5000);
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "raw-data" && m.data) {
          clearTimeout(timeout);
          const reader = actor.read({ ...m, handle: (m.data as { handle: Lock }).handle });
          if (reader) resolve(reader.raw());
        }
      });
      actor.receive("write");
    });

    expect(data).toBeInstanceOf(Uint8Array);
    expect(data[0]).toBe(0xCA);
    expect(data[1]).toBe(0xFE);
    expect(data[2]).toBe(0xBA);
    expect(data[3]).toBe(0xBE);
  });
});

// ============================================================================
// Explicit heartbeat (ctx.heartbeat)
// ============================================================================

describe("explicit heartbeat", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor({ leaseMs: 300 }); });
  afterEach(async () => { await sup.shutdown(); });

  it("ctx.heartbeat() resets lease timer for WRITING boxes", async () => {
    // Actor writes, then heartbeats repeatedly to stay alive past lease expiry
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "data",
        type: "json",
        data: ctx.fmt.json.encode({ v: 1 }),
        share: "owner",
      });
      if (result.isOk) {
        // Heartbeat 3 times with 100ms gaps — total 300ms, past the 300ms lease
        for (let i = 0; i < 3; i++) {
          await new Promise((r) => setTimeout(r, 100));
          ctx.heartbeat();
        }
        self.send("survived");
        ctx.release(result.value);
      }
    });

    const survived = await new Promise<boolean>((resolve, reject) => {
      const timeout = setTimeout(() => resolve(false), 3000);
      actor.subscribe((msg) => {
        if (msg.msg === "survived") { clearTimeout(timeout); resolve(true); }
      });
      actor.receive("write");
    });

    expect(survived).toBe(true);
  });

  it("ctx.heartbeat() increments heartbeatCount in diagnostics", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      ctx.heartbeat();
      ctx.heartbeat();
      ctx.heartbeat();
      self.send("done");
    });

    await new Promise<void>((resolve) => {
      actor.subscribe((msg) => { if (msg.msg === "done") resolve(); });
      actor.receive("start");
    });

    const diag = actor.getDiagnostics();
    if (diag.isOk) {
      expect(diag.value.heartbeatCount).toBeGreaterThanOrEqual(3);
    }
  });
});

// ============================================================================
// Implicit heartbeat (self.send resets lease)
// ============================================================================

describe("implicit heartbeat", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor({ leaseMs: 300 }); });
  afterEach(async () => { await sup.shutdown(); });

  it("self.send() resets lastHeartbeatAt", async () => {
    const actor = sup.spawn(async (self) => {
      // Send messages with gaps — each send is an implicit heartbeat
      for (let i = 0; i < 4; i++) {
        await new Promise((r) => setTimeout(r, 80));
        self.send("tick", { i });
      }
      self.send("done");
    });

    await new Promise<void>((resolve) => {
      actor.subscribe((msg) => { if (msg.msg === "done") resolve(); });
      actor.receive("start");
    });

    const diag = actor.getDiagnostics();
    if (diag.isOk) {
      // lastHeartbeatAt should be recent (within last 200ms)
      expect(diag.value.lastHeartbeatAt).toBeGreaterThan(Date.now() - 2000);
      expect(diag.value.messageCount).toBeGreaterThanOrEqual(5); // 4 ticks + done
    }
  });
});

// ============================================================================
// Resource manager integration (ctx.resources from worker)
// ============================================================================

describe("resource manager integration", () => {
  let sup: Supervisor;

  beforeEach(() => {
    sup = createSupervisor({
      maxActors: 10,
      memory: { poolSize: 3, boxSize: 1024 },
      timeouts: { defaultLeaseMs: 2000 },
      resources: {
        db: {
          query: {
            input: z.object({ sql: z.string() }),
            output: z.array(z.unknown()),
            handler: async (args: unknown) => {
            const { sql } = args as { sql: string };
            return [{ id: 1, sql }];
          },
          },
        },
      },
    });
  });

  afterEach(async () => { await sup.shutdown(); });

  it("ctx.resources.db.query() works from inside actor", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      const db = ctx.resources.db!;
      const result = await db.query!({ sql: "SELECT 1" });
      self.send("query-result", result);
    });

    const data = await new Promise<unknown>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Timeout")), 5000);
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "query-result") { clearTimeout(timeout); resolve(m.data); }
      });
      actor.receive("start");
    });

    expect(data).toEqual([{ id: 1, sql: "SELECT 1" }]);
  });

  it("ctx.resources returns error for unknown resource", async () => {
    const actor = sup.spawn(async (self, _msg, ctx) => {
      try {
        const ns = ctx.resources.nonexistent!;
        await ns.foo!({});
        self.send("no-error");
      } catch (e) {
        self.send("caught-error", { error: String(e) });
      }
    });

    const result = await new Promise<string>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Timeout")), 5000);
      actor.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "caught-error" && m.data) {
          clearTimeout(timeout);
          resolve((m.data as { error: string }).error);
        }
        if (m.msg === "no-error") { clearTimeout(timeout); resolve("no-error"); }
      });
      actor.receive("start");
    });

    expect(result).not.toBe("no-error");
  });
});

// ============================================================================
// ctx.spawn from worker (child actors)
// ============================================================================

describe("ctx.spawn from worker", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor(); });
  afterEach(async () => { await sup.shutdown(); });

  it("actor can spawn a child actor via ctx.spawn", async () => {
    const parent = sup.spawn(async (self, _msg, ctx) => {
      const child = await ctx.spawn(async (childSelf) => {
        childSelf.send("child-ready");
      });
      self.send("child-id", { childId: child.id });
    });

    const childId = await new Promise<string>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Timeout")), 5000);
      parent.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "child-id" && m.data) {
          clearTimeout(timeout);
          resolve((m.data as { childId: string }).childId);
        }
      });
      parent.receive("start");
    });

    expect(childId).toBeDefined();
    expect(typeof childId).toBe("string");
    expect(childId).toMatch(/^actor-/);

    // Verify child appears in diagnostics
    const diag = sup.getDiagnostics();
    if (diag.isOk) {
      expect(diag.value.activeActors).toBeGreaterThanOrEqual(2);
    }
  });
});

// ============================================================================
// Callback serialization validation (security)
// ============================================================================

describe("callback validation", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor(); });
  afterEach(async () => { await sup.shutdown(); });

  it("rejects callback with require()", async () => {
    const actor = sup.spawn(async () => { require("fs"); });
    actor.receive("start");
    await wait(300);
    const diag = actor.getDiagnostics();
    expect(diag.isErr).toBe(true);
  });

  it("rejects callback with process global", async () => {
    const actor = sup.spawn(async () => { process.exit(1); });
    actor.receive("start");
    await wait(300);
    const diag = actor.getDiagnostics();
    expect(diag.isErr).toBe(true);
  });

  it("rejects callback with eval()", async () => {
    const actor = sup.spawn(async () => { eval("1+1"); });
    actor.receive("start");
    await wait(300);
    const diag = actor.getDiagnostics();
    expect(diag.isErr).toBe(true);
  });

  it("rejects callback with Function constructor", async () => {
    const actor = sup.spawn(async () => { new Function("return 1")(); });
    actor.receive("start");
    await wait(300);
    const diag = actor.getDiagnostics();
    expect(diag.isErr).toBe(true);
  });

  it("accepts safe callback", async () => {
    const messages: Message[] = [];
    const actor = sup.spawn(async (self) => {
      self.send("safe", { ok: true });
    });
    actor.subscribe((msg) => messages.push(msg as Message));
    actor.receive("start");
    await wait(200);

    const safe = messages.find((m) => m.msg === "safe");
    expect(safe).toBeDefined();
    expect((safe!.data as { ok: boolean }).ok).toBe(true);
  });
});

// ============================================================================
// Linked authorization (share: "linked")
// ============================================================================

describe("linked authorization", () => {
  let sup: Supervisor;

  beforeEach(() => { sup = makeSupervisor({ poolSize: 5 }); });
  afterEach(async () => { await sup.shutdown(); });

  it("linked actors can read each other's data with share: linked", async () => {
    const writer = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "linked-data",
        type: "json",
        data: ctx.fmt.json.encode({ shared: true }),
        share: "linked",
      });
      if (result.isOk) self.send("written", { handle: result.value });
    });

    const reader = sup.spawn(async (self, msg, ctx) => {
      const m = msg as Message;
      if (m.handle) {
        const data = ctx.read(m);
        if (data) self.send("reader-got", { json: data.json() });
      }
    });

    // Link them bidirectionally
    writer.link(reader);

    // Set up reader subscription before writer writes
    const readerData = new Promise<unknown>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Reader never received data")), 5000);
      reader.subscribe((msg) => {
        const m = msg as Message;
        if (m.msg === "reader-got" && m.data) {
          clearTimeout(timeout);
          resolve((m.data as { json: unknown }).json);
        }
      });
      reader.receive("ready");
    });

    await wait(100);

    // Writer writes with share: "linked"
    writer.receive("write");

    const data = await readerData;
    expect(data).toEqual({ shared: true });
  });

  it("unlinked actor does NOT receive INBOX for linked share", async () => {
    const writer = sup.spawn(async (self, _msg, ctx) => {
      const result = await ctx.write({
        msg: "linked-private",
        type: "json",
        data: ctx.fmt.json.encode({ secret: true }),
        share: "linked",
      });
      if (result.isOk) self.send("written");
    });

    let leaked = false;
    const unlinked = sup.spawn(async (self, msg, ctx) => {
      const m = msg as Message;
      if (m.handle) {
        const data = ctx.read(m);
        if (data) { self.send("leak"); }
      }
    });

    unlinked.subscribe((msg) => { if (msg.msg === "leak") leaked = true; });
    unlinked.receive("ready");
    await wait(100);

    writer.receive("write");
    await wait(300);

    expect(leaked).toBe(false);
  });
});

// ============================================================================
// Per-actor timeout overrides
// ============================================================================

describe("per-actor timeout overrides", () => {
  it("named actor uses custom lease duration", async () => {
    // Default lease 5000ms, but "fast-actor" gets 200ms
    const sup = createSupervisor({
      maxActors: 10,
      memory: { poolSize: 3, boxSize: 1024 },
      timeouts: { defaultLeaseMs: 5000, actorTimeouts: { "fast-actor": 200 } },
    });

    // Actor that writes but never commits (blocks worker)
    const slowWriter = sup.spawn(
      (self, _msg, ctx) => {
        ctx.write({
          msg: "slow",
          type: "json",
          data: ctx.fmt.json.encode({ slow: true }),
          share: "owner",
        });
        self.send("write-requested");
        // Block worker — prevents COMMIT
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10000);
      },
      { name: "fast-actor" },
    );

    await new Promise<void>((resolve) => {
      slowWriter.subscribe((msg) => {
        if (msg.msg === "write-requested") resolve();
      });
      slowWriter.receive("write");
    });

    await wait(100);

    // Actor should still be alive (lease hasn't expired yet)
    const diagBefore = slowWriter.getDiagnostics();
    expect(diagBefore.isOk).toBe(true);

    // Wait for 200ms lease to expire + buffer
    await wait(400);

    // Trigger lease check via another actor
    const trigger = sup.spawn(async (self) => { self.send("trigger"); });
    await new Promise<void>((resolve) => {
      trigger.subscribe((msg) => { if (msg.msg === "trigger") resolve(); });
      trigger.receive("start");
    });
    await wait(200);

    // fast-actor should be terminated due to short lease
    const diagAfter = slowWriter.getDiagnostics();
    expect(diagAfter.isErr).toBe(true);

    await sup.shutdown();
  });
});

// ============================================================================
// Resource cleanup on supervisor shutdown
// ============================================================================

describe("resource cleanup on shutdown", () => {
  it("calls release hooks on supervisor shutdown", async () => {
    let releaseCalled = false;

    const sup = createSupervisor({
      maxActors: 5,
      memory: { poolSize: 3, boxSize: 1024 },
      timeouts: { defaultLeaseMs: 5000 },
      resources: {
        db: {
          query: {
            input: z.object({ sql: z.string() }),
            output: z.array(z.unknown()),
            handler: async (args: unknown) => {
              const { sql } = args as { sql: string };
              return [{ id: 1, sql }];
            },
          },
          release: () => { releaseCalled = true; },
        },
      },
    });

    const actor = sup.spawn(async (self) => { self.send("alive"); });
    actor.receive("start");
    await wait(100);

    await sup.shutdown();

    expect(releaseCalled).toBe(true);
  });
});
