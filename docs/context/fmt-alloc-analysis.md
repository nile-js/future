# Context: fmt.alloc Feature Analysis

## Summary

This document provides a comprehensive analysis of the `fmt.alloc` feature in @nilejs/future, comparing the current implementation against the specification.

---

## 1. Current fmt.alloc Implementation

### Location: `src/future/worker-bootstrap.ts` (lines 61-103)

```typescript
function createFormatUtils(): FormatUtils {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const encodeString = (str: string): Uint8Array => encoder.encode(str);
  const encodeJson = (obj: unknown): Uint8Array => encoder.encode(JSON.stringify(obj));

  const allocBase = (size: number): Uint8Array => new Uint8Array(size);
  const alloc = Object.assign(allocBase, {
    u8: (length: number) => new Uint8Array(length),
    i8: (length: number) => new Int8Array(length),
    u16: (length: number) => new Uint16Array(length),
    i16: (length: number) => new Int16Array(length),
    u32: (length: number) => new Uint32Array(length),
    i32: (length: number) => new Int32Array(length),
    u64: (length: number) => new BigUint64Array(length),
    i64: (length: number) => new BigInt64Array(length),
    f32: (length: number) => new Float32Array(length),
    f64: (length: number) => new Float64Array(length),
  });

  const coerce = (data: unknown): Uint8Array => {
    if (data instanceof Uint8Array) return data;
    if (typeof data === "string") return encodeString(data);
    return encodeJson(data);
  };

  return {
    alloc,
    from: coerce,
    encode: coerce,
    decode: (buffer) => {
      const str = decoder.decode(buffer);
      const parsed = safeParseJson(str);
      return parsed.ok ? parsed.value : str;
    },
    json: { encode: (obj) => encodeJson(obj), decode: (buf) => JSON.parse(decoder.decode(buf)) },
    string: { encode: (str) => encodeString(str), decode: (buf) => decoder.decode(buf) },
    cbor: {
      encode: () => { throw new Error("CBOR codec not configured"); },
      decode: () => { throw new Error("CBOR codec not configured"); },
    },
  };
}
```

### How It Works

The current implementation:
1. **`alloc(size)`** - Creates a standard `Uint8Array` of the specified size (line 67)
2. **Typed variants** - Creates typed arrays (Int8Array, Uint16Array, etc.) via Object.assign (lines 68-79)
3. **NOT SAB-backed** - All allocations use standard heap-allocated arrays, NOT SharedArrayBuffer views

---

## 2. What the Spec Says

### ADR 005: Context-Based Formatting & Serialization

From spec-final.md (lines 45-55):

> * **Decision:** Provide buffer allocation and serialization utilities via `ctx.fmt` namespace.
> * **Why:** Developers should not think about `Uint8Array`, `TextEncoder`, or manual serialization. Keep low-level details abstracted.
> * **Design:**
>   - `ctx.fmt.alloc(size)` - Buffer allocation (replaces `new Uint8Array(length)`)

### Section 17: Buffer & Serialization Utilities

From spec-final.md (lines 826-920):

**Buffer Allocation (lines 846-863):**
```typescript
// Allocate a buffer (replaces new Uint8Array(length))
const buffer = ctx.fmt.alloc(size);
const buffer = ctx.fmt.alloc(1024);  // 1KB buffer

// Typed allocations
const u8 = ctx.fmt.alloc.u8(length);    // Uint8Array
const i32 = ctx.fmt.alloc.i32(length);  // Int32Array
const f64 = ctx.fmt.alloc.f64(length);  // Float64Array
```

**Typed Variants (lines 906-919):**
```typescript
ctx.fmt.alloc.u8(length)    // Uint8Array (alias for base alloc)
ctx.fmt.alloc.i8(length)    // Int8Array
ctx.fmt.alloc.u16(length)   // Uint16Array
ctx.fmt.alloc.i16(length)   // Int16Array
ctx.fmt.alloc.u32(length)   // Uint32Array
ctx.fmt.alloc.i32(length)   // Int32Array
ctx.fmt.alloc.u64(length)   // BigUint64Array
ctx.fmt.alloc.i64(length)   // BigInt64Array
ctx.fmt.alloc.f32(length)   // Float32Array
ctx.fmt.alloc.f64(length)   // Float64Array
```

### Key Spec Requirements

1. **Primary Purpose:** Replace `new Uint8Array(length)` - provide simpler DX
2. **SAB-Backed?:** The spec does NOT explicitly require SAB-backed allocation
3. **Use Case:** Preparing data for `ctx.write()` which then copies into SAB boxes
4. **Zero-Copy Goal:** The spec mentions "zero-abstract buffer" (line 1360) for developer experience

---

## 3. FmtType Definition

### Location: `src/future/types.ts` (line 6)

```typescript
/** Serialization format for shared-memory messages — determines encode/decode path */
export type FmtType = "json" | "string" | "binary" | "cbor";
```

### FormatUtils Type Definition

From `src/future/types.ts` (lines 44-59):

```typescript
/** FormatUtils — encoding/decoding on ctx.fmt. Typed alloc sub-methods allocate SAB-backed views. */
export type FormatUtils = {
  readonly from: (data: string | object | Uint8Array) => Uint8Array;
  readonly encode: (data: unknown) => Uint8Array;
  readonly decode: (buffer: Uint8Array) => unknown;
  readonly json: { readonly encode: (obj: unknown) => Uint8Array; readonly decode: (buf: Uint8Array) => unknown };
  readonly string: { readonly encode: (str: string) => Uint8Array; readonly decode: (buf: Uint8Array) => string };
  readonly cbor: { readonly encode: (data: unknown) => Uint8Array; readonly decode: (buf: Uint8Array) => unknown };
  readonly alloc: ((size: number) => Uint8Array) & {
    readonly u8: (length: number) => Uint8Array; readonly i8: (length: number) => Int8Array;
    readonly u16: (length: number) => Uint16Array; readonly i16: (length: number) => Int16Array;
    readonly u16: (length: number) => Uint16Array; readonly i16: (length: number) => Int16Array;
    readonly u32: (length: number) => Uint32Array; readonly i32: (length: number) => Int32Array;
    readonly u64: (length: number) => BigUint64Array; readonly i64: (length: number) => BigInt64Array;
    readonly f32: (length: number) => Float32Array; readonly f64: (length: number) => Float64Array;
  };
};
```

**Important:** The JSDoc comment says "Typed alloc sub-methods allocate SAB-backed views" but this is NOT currently implemented!

---

## 4. Existing Tests

### Location: `tests/future/supervisor.spec.ts`

**Current fmt tests found:**
- Line 535-551: `"fmt.encode/decode roundtrip"` - Tests encode/decode but NOT alloc
- Lines 129, 157, 239, 264, 300, 327, 571, 649, 697, 719, 756, 773, 787, 853: All use `ctx.fmt.json.encode()` or `ctx.fmt.string.encode()`

**No tests found for:**
- `ctx.fmt.alloc()` - NOT TESTED
- `ctx.fmt.alloc.u8()` - NOT TESTED
- `ctx.fmt.alloc.i32()` - NOT TESTED
- Any typed alloc variants - NOT TESTED

### Test Files Searched:
- `/home/kizz/future/tests/future/supervisor.spec.ts`
- `/home/kizz/future/tests/future/memory-pool.spec.ts`
- `/home/kizz/future/tests/future/diagnostics.spec.ts`
- `/home/kizz/future/tests/future/restart.spec.ts`
- `/home/kizz/future/tests/future/group-manager.spec.ts`
- `/home/kizz/future/tests/future/resource-manager.spec.ts`

---

## 5. Gap Analysis

### Current State vs Spec

| Aspect | Current Implementation | Spec Requirement | Gap |
|--------|----------------------|------------------|-----|
| **Basic alloc** | ✅ `new Uint8Array(size)` | ✅ "replaces new Uint8Array" | ✅ Aligned |
| **Typed variants** | ✅ All 10 variants work | ✅ All 10 variants specified | ✅ Aligned |
| **SAB-backed** | ❌ Not implemented | ⚠️ **UNCLEAR** - Type JSDoc says yes, spec examples imply no | ⚠️ **AMBIGUOUS** |
| **Tests** | ❌ No alloc tests exist | Should test all variants | ❌ **MISSING** |
| **Return type** | Regular TypedArrays | TypedArrays (not explicitly SAB-backed) | ✅ Aligned |

### Critical Ambiguity: SAB-Backed or Not?

**Evidence FOR SAB-backed (from types.ts JSDoc):**
- Line 44: `"Typed alloc sub-methods allocate SAB-backed views."`

**Evidence AGAINST SAB-backed (from spec):**
- Spec section 17 shows `ctx.fmt.alloc()` being used to create buffers that are then passed to `ctx.write()`
- The `ctx.write()` implementation copies data INTO the SAB box (see worker-bootstrap.ts lines 158-160)
- If alloc were SAB-backed, the copy in write() would be redundant

**Architecture Analysis:**
1. The SAB is managed as a pool of fixed-size boxes (ADR 002, 003)
2. `ctx.write()` acquires a box, copies data, and commits
3. If `fmt.alloc()` returned SAB-backed buffers, they'd need to be from the same pool
4. But the pool boxes are managed by the supervisor, not allocated on-demand

**Conclusion:** The JSDoc in types.ts appears to be **INCORRECT or OUTDATED**. The spec intent is that `fmt.alloc()` provides a convenient way to allocate working buffers (like `new Uint8Array`), NOT that they be SAB-backed. The SAB is used only for the fixed pool boxes accessed via `ctx.write()`.

### Recommended Fixes

1. **Fix types.ts JSDoc** (line 44):
   - Change: `"Typed alloc sub-methods allocate SAB-backed views."`
   - To: `"Typed alloc sub-methods allocate working buffers for data preparation."`

2. **Add comprehensive tests** for `ctx.fmt.alloc` and all typed variants

3. **Clarify in spec** that alloc is NOT SAB-backed (if that's the intent)

---

## 6. Usage Patterns in Codebase

### How fmt.alloc Should Be Used (per spec scenarios):

**Scenario 1: Real-Time Financial Processor** (line 948):
```typescript
const results = ctx.fmt.alloc(transactions.length);
for (let i = 0; i < transactions.length; i++) {
  results[i] = isValid ? 1 : 0;
}
```

**Scenario 4: Fault-Tolerant Pipeline** (line 1089):
```typescript
data: ctx.fmt.encode(rawData),  // encode uses alloc internally
```

### How fmt is Actually Used in Tests:

All tests use `ctx.fmt.json.encode()` or `ctx.fmt.string.encode()` - no direct `alloc` usage.

---

## 7. Action Items

1. **Documentation Fix**: Update types.ts JSDoc to remove "SAB-backed" claim
2. **Test Coverage**: Add tests for all alloc variants
3. **Spec Clarification**: Add note in spec that alloc creates regular buffers, not SAB-backed
4. **Consider**: If SAB-backed allocation IS desired, it would require significant architecture changes to allow dynamic SAB allocation (currently uses fixed pool)

---

*Generated: Context analysis for fmt.alloc feature*
*Files analyzed: worker-bootstrap.ts, types.ts, spec-final.md, supervisor.spec.ts*
