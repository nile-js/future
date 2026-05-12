# Shared Memory

**Category:** Guide

## Intent

Explain how two-tier communication works in future, with focus on Tier 2 shared memory: write flow, read flow, authorization, lease system, buffer serialization, and locking.

## Responsibilities

- Two-tier communication model (Tier 1 vs Tier 2)
- Write flow: requesting a box, writing data, committing
- Read flow: receiving a handle, reading data, releasing
- Authorization (ShareConfig) for access control
- Lease system for automatic cleanup
- Buffer allocation and serialization via ctx.fmt
- Lock and epoch for use-after-free prevention

## Non-Goals

- Does not cover worker model or state machine (see [Architecture](https://github.com/nile-js/future/blob/main/docs/architecture.md))
- Does not cover supervision or linking (see [Supervision](https://github.com/nile-js/future/blob/main/docs/supervision.md))

## Two-Tier Communication

**Tier 1 (Control):** Uses postMessage for lightweight signals, status updates, heartbeats, resource intents, pubsub, child spawn, and inbox delivery. Messages carry a string key and optional JSON data.

**Tier 2 (Data):** Uses SharedArrayBuffer for zero-copy large data transfer. Workers write to assigned boxes. Data is immutable after commit. No CAS operations, no atomic contention.

## Write Flow

1. Worker calls `ctx.write({ msg, type, data, share })`. This sends a `WRITE_REQUEST` to the main thread.
2. Main thread finds a FREE box (or queues the request in a FIFO write queue if none available). It sets `BoxEntry.state = WRITING`, assigns the writer, and increments the epoch.
3. Main thread sends `WRITE_GRANTED { lock: { boxIndex, epoch } }` back to the worker.
4. Worker copies data into the SAB segment at `lock.boxIndex`, stripping trailing null bytes.
5. Worker sends `COMMIT { lock }`. Main thread marks `BoxEntry.state = READY`, computes `expiresAt`, and routes the box handle to authorized inboxes.
6. Main thread delivers `INBOX { handle, from, msg, type }` to authorized readers.

## Read Flow

1. Reader receives an `INBOX` message with `handle`, `from`, `msg`, and `type`.
2. Reader calls `ctx.read(msg)` which returns a `ChainableReader` or `null` if no handle is present.
3. Data is read directly from the SAB. Zero-copy. No async round-trip. No state transition.
4. Reader calls `ctx.release(handle)`. This sends a `RELEASE { lock }` message to the main thread.
5. Main thread decrements `refCount`. If 0, the box returns to FREE and the write queue is served if pending.

```typescript
const reader = ctx.read(msg);
if (reader) {
  const json = reader.json();
  const str = reader.string();
  const bin = reader.binary();
  const cbor = reader.cbor();
  const raw = reader.raw(); // Raw Uint8Array, main thread only
  ctx.release(msg.handle);
}
```

On the main thread, `actor.read(msg)` works the same way:

```typescript
const reader = actor.read(msg);
if (reader) {
  const value = reader.json();
  actor.release(msg.handle);
}
```

If `msg.handle` is absent (Tier 1 message), the read call returns `null`.

## Authorization (ShareConfig)

Access to shared memory boxes is controlled by `ShareConfig`:

```typescript
type ShareConfig = "owner" | "group" | "linked" | readonly ActorId[];
```

- **"owner" (default):** Only the writing actor can read. Main-thread subscribers can always read.
- **"group":** All actors in the same supervision group can read.
- **"linked":** Actors linked to the writer can read (bi-directional and uni-directional links).
- **ActorId[]:** Explicit allowlist of actor IDs.

```typescript
const result = await ctx.write({
  msg: "private",
  type: "json",
  data: ctx.fmt.encode(sensitiveData),
  share: "owner",
});

const result = await ctx.write({
  msg: "shared",
  type: "binary",
  data: teamData,
  share: "group",
});

const result = await ctx.write({
  msg: "targeted",
  type: "cbor",
  data: targetedData,
  share: [actorA.id, actorB.id],
});
```

The supervisor enforces authorization at the inbox routing layer. Unauthorized read attempts are silently dropped.

## Lease System

Each committed box has an `expiresAt` timestamp computed at COMMIT time. The lease is reset when a new reader acquires a handle to a READY box.

Cleanup is opportunistic: on any actor-supervisor interaction (self.send, ctx.write, ctx.release, ctx.heartbeat, resource call), the supervisor checks all BoxEntry entries for expired leases. If a lease has expired:

- The box is force-released (all reader refs cleared)
- The box is set to FREE
- If the writer is in WRITING state, the writer is terminated
- The write queue is served if pending

There is no setTimeout overhead. Cleanup is deterministic and interaction-driven.

## Buffer and Serialization (ctx.fmt)

```typescript
// Allocate heap buffers (copied into SAB on ctx.write)
const buf = ctx.fmt.alloc(1024);
const u8 = ctx.fmt.alloc.u8(256);
const i32 = ctx.fmt.alloc.i32(64);
const f64 = ctx.fmt.alloc.f64(32);

// Auto-detect encoding
const encoded = ctx.fmt.encode({ key: "value" });  // object to JSON bytes
const decoded = ctx.fmt.decode(encoded);             // bytes to object

// Create buffer from data
const buf = ctx.fmt.from("hello");                  // string to UTF-8 bytes
const buf = ctx.fmt.from({ name: "kizz" });         // object to JSON bytes

// Explicit codecs
const json = ctx.fmt.json.encode({ a: 1 });
const obj = ctx.fmt.json.decode(json);
const str = ctx.fmt.string.encode("hello");
const text = ctx.fmt.string.decode(str);
```

Note: `ctx.fmt.alloc()` creates heap buffers, not SAB-backed buffers. Data is copied into the SAB on `ctx.write()`. CBOR codec is not implemented by default and will throw on use; it can be configured via the `codecs` option in the supervisor config.

## Lock and Epoch

The `Lock` type is a lightweight reference to a shared memory box:

```typescript
type Lock = { readonly boxIndex: number; readonly epoch: number };
```

The epoch increments each time a box is freed and reallocated. Any read or release attempt with a stale epoch is rejected. This prevents use-after-free bugs when a box is recycled while another actor holds an old handle.
