// ============================================================================
// Box Size Parsing
// ============================================================================

const SIZE_UNITS: Readonly<Record<string, number>> = {
  b: 1,
  byte: 1,
  bytes: 1,
  kb: 1024,
  kib: 1024,
  mb: 1024 * 1024,
  mib: 1024 * 1024,
  gb: 1024 * 1024 * 1024,
  gib: 1024 * 1024 * 1024,
};

/**
 * Parse human-readable box size string or pass through numeric bytes.
 * Supports: "64", "1KB", "2MB", "1GB" (case-insensitive).
 * Numeric values are treated as byte counts directly.
 *
 * @param size - Size string (e.g. "1KB") or numeric byte count
 * @returns Byte count as number
 * @throws On invalid format or non-positive result
 */
export function parseBoxSize(size: string | number): number {
  if (typeof size === "number") {
    if (!Number.isFinite(size) || size <= 0) {
      throw new Error(`Invalid box size: ${size}. Must be a positive finite number.`);
    }
    return Math.floor(size);
  }

  const match = size.trim().toLowerCase().match(/^(\d+(?:\.\d+)?)\s*([a-z]+)$/);
  if (!match) {
    const asNum = Number(size);
    if (Number.isFinite(asNum) && asNum > 0) return Math.floor(asNum);
    throw new Error(`Invalid box size string: "${size}". Expected format like "1KB" or "64".`);
  }

  const valueStr = match[1];
  const unit = match[2];
  const multiplier = SIZE_UNITS[unit!];
  if (multiplier === undefined) {
    throw new Error(
      `Unknown size unit: "${unit}". Supported: ${Object.keys(SIZE_UNITS).join(", ")}.`,
    );
  }

  const bytes = Math.floor(parseFloat(valueStr!) * multiplier);
  if (bytes <= 0) {
    throw new Error(`Box size resolves to ${bytes} bytes. Must be positive.`);
  }

  return bytes;
}

// ============================================================================
// Memory Pool Type
// ============================================================================

/**
 * Raw SharedArrayBuffer memory pool for inter-thread actor communication.
 *
 * Per ADR 002: The SAB contains ONLY raw data boxes — no state board, no lease
 * tracker, no Atomics. All state tracking (FREE/WRITING/READY, ref counts,
 * lease timestamps) lives in supervisor-side `BoxEntry[]` plain objects.
 * This eliminates CAS contention entirely and keeps the SAB a pure data bus,
 * making state transitions deterministic and observable from the supervisor.
 */
export type MemoryPool = {
  /** The underlying SharedArrayBuffer — raw data boxes only */
  readonly sab: SharedArrayBuffer;
  /** Number of boxes in the pool */
  readonly poolSize: number;
  /** Byte size of each box */
  readonly boxSize: number;
  /**
   * Read a box — returns a zero-copy Uint8Array VIEW into the SAB.
   * Caller must ensure the box is in a readable state (supervisor tracks this).
   *
   * @param boxIndex - Zero-based box index
   * @returns Uint8Array subarray view (no copy)
   * @throws RangeError if boxIndex is out of bounds
   */
  readonly readBox: (boxIndex: number) => Uint8Array;
  /**
   * Write data into a box — copies source bytes into the SAB region.
   * Data larger than boxSize is truncated. Data smaller leaves trailing bytes.
   *
   * @param boxIndex - Zero-based box index
   * @param data - Source data to copy into the box
   * @throws RangeError if boxIndex is out of bounds
   */
  readonly writeBox: (boxIndex: number, data: Uint8Array) => void;
};

// ============================================================================
// Memory Pool Factory
// ============================================================================

/**
 * Create a SharedArrayBuffer-backed memory pool for inter-thread actor communication.
 *
 * The SAB layout is a flat array of data boxes — no state board, no lease tracker,
 * no Atomics. State transitions are managed entirely by the supervisor via
 * `BoxEntry[]` plain objects, eliminating CAS contention and making the SAB
 * a pure, deterministic data bus (ADR 002).
 *
 * @param params.poolSize - Number of memory boxes in the pool
 * @param params.boxSize - Size per box in bytes (string like "1KB" or number)
 * @returns Memory pool handle with SAB, dimensions, and read/write accessors
 */
export function createMemoryPool({
  poolSize,
  boxSize,
}: {
  readonly poolSize: number;
  readonly boxSize: string | number;
}): MemoryPool {
  if (!Number.isFinite(poolSize) || poolSize <= 0) {
    throw new Error(`Invalid poolSize: ${poolSize}. Must be a positive integer.`);
  }

  const resolvedBoxSize = parseBoxSize(boxSize);
  const totalBytes = poolSize * resolvedBoxSize;
  const sab = new SharedArrayBuffer(totalBytes);
  const dataRegion = new Uint8Array(sab);

  /**
   * Read data from a box — returns a VIEW into the SAB (zero-copy).
   * The supervisor is responsible for ensuring the box is in a readable state
   * before calling this; the memory pool has no state awareness.
   */
  function readBox(boxIndex: number): Uint8Array {
    if (boxIndex < 0 || boxIndex >= poolSize) {
      throw new RangeError(`Box index ${boxIndex} out of range [0, ${poolSize}).`);
    }
    const offset = boxIndex * resolvedBoxSize;
    return dataRegion.subarray(offset, offset + resolvedBoxSize);
  }

  /**
   * Write data into a box — copies from source into the SAB region.
   * Data larger than boxSize is truncated. Data smaller leaves trailing bytes
   * (the SAB is zero-initialized on creation).
   */
  function writeBox(boxIndex: number, data: Uint8Array): void {
    if (boxIndex < 0 || boxIndex >= poolSize) {
      throw new RangeError(`Box index ${boxIndex} out of range [0, ${poolSize}).`);
    }
    const offset = boxIndex * resolvedBoxSize;
    const writeLen = Math.min(data.length, resolvedBoxSize);
    dataRegion.set(data.subarray(0, writeLen), offset);
  }

  return Object.freeze({
    sab,
    poolSize,
    boxSize: resolvedBoxSize,
    readBox,
    writeBox,
  });
}