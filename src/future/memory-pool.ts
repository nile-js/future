import type { Lock } from "./types";
import { BOX_CLEAN, BOX_LOCKED, BOX_READY, BOX_READING } from "./types";

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
// SAB Layout Constants
// ============================================================================

/** Byte offset for state board region within SAB */
const STATE_BOARD_OFFSET = 0;
/** Byte size of each Int32 state slot */
const STATE_SLOT_BYTES = 4;
/** Byte size of each BigInt64 lease slot */
const LEASE_SLOT_BYTES = 8;

/**
 * Calculate byte offset for lease tracker region.
 * Aligned to 8-byte boundary after state board.
 */
function leaseTrackerOffset(poolSize: number): number {
  const stateBytes = poolSize * STATE_SLOT_BYTES;
  // Align to 8-byte boundary for BigInt64Array
  return Math.ceil((STATE_BOARD_OFFSET + stateBytes) / 8) * 8;
}

/**
 * Calculate byte offset for data region.
 * Aligned to 8-byte boundary after lease tracker.
 */
function dataRegionOffset(poolSize: number): number {
  const leaseOff = leaseTrackerOffset(poolSize);
  const leaseBytes = poolSize * LEASE_SLOT_BYTES;
  // Align to 8-byte boundary for typed array views
  return Math.ceil((leaseOff + leaseBytes) / 8) * 8;
}

/** Total SAB byte length */
function totalSABBytes(poolSize: number, boxSize: number): number {
  return dataRegionOffset(poolSize) + poolSize * boxSize;
}

// ============================================================================
// Memory Pool Factory
// ============================================================================

/**
 * Create a SharedArrayBuffer-backed memory pool for inter-thread actor communication.
 *
 * Layout per supervisor:
 * - State Board: Int32Array[poolSize] — 0=CLEAN, 1=LOCKED, 2=READY, 3=READING
 * - Lease Tracker: BigInt64Array[poolSize] — expiresAt ms, 0n = no lease
 * - Data Boxes: Uint8Array[poolSize * boxSize]
 *
 * All regions are aligned for typed array compatibility.
 * State transitions use Atomics for thread-safe CAS operations.
 *
 * @param params.poolSize - Number of memory boxes in the pool
 * @param params.boxSize - Size per box in bytes (string like "1KB" or number)
 * @returns Memory pool handle with typed views and atomic operations
 */
export function createMemoryPool({
  poolSize,
  boxSize,
}: {
  readonly poolSize: number;
  readonly boxSize: string | number;
}): {
  readonly sab: SharedArrayBuffer;
  readonly poolSize: number;
  readonly boxSize: number;
  readonly stateBoard: Int32Array;
  readonly leaseTracker: BigInt64Array;
  readonly dataRegion: Uint8Array;
  readonly tryAcquireBox: () => Lock | null;
  readonly getBoxOffset: (boxIndex: number) => number;
  readonly markReady: (boxIndex: number) => void;
  readonly markReading: (boxIndex: number) => void;
  readonly markClean: (boxIndex: number) => void;
  readonly setLease: (boxIndex: number, expiresAt: number) => void;
  readonly clearLease: (boxIndex: number) => void;
  readonly isLeaseExpired: (boxIndex: number) => boolean;
  readonly readBox: (boxIndex: number) => Uint8Array;
  readonly writeBox: (boxIndex: number, data: Uint8Array) => void;
} {
  if (!Number.isFinite(poolSize) || poolSize <= 0) {
    throw new Error(`Invalid poolSize: ${poolSize}. Must be a positive integer.`);
  }

  const resolvedBoxSize = parseBoxSize(boxSize);
  const totalBytes = totalSABBytes(poolSize, resolvedBoxSize);
  const sab = new SharedArrayBuffer(totalBytes);

  const stateBoard = new Int32Array(sab, STATE_BOARD_OFFSET, poolSize);
  const leaseOff = leaseTrackerOffset(poolSize);
  const leaseTracker = new BigInt64Array(sab, leaseOff, poolSize);
  const dataOff = dataRegionOffset(poolSize);
  const dataRegion = new Uint8Array(sab, dataOff, poolSize * resolvedBoxSize);

  // Initialize all states to CLEAN (0) — SharedArrayBuffer zero-initialized but explicit
  for (let i = 0; i < poolSize; i++) {
    Atomics.store(stateBoard, i, BOX_CLEAN);
  }

  /**
   * Try to acquire an exclusive lock on a free box.
   * Uses compareExchange for thread-safe acquisition: CLEAN → LOCKED.
   * Scans all boxes, returns first available.
   *
   * @returns Lock on success, null if no boxes available
   */
  function tryAcquireBox(): Lock | null {
    for (let i = 0; i < poolSize; i++) {
      const prev = Atomics.compareExchange(stateBoard, i, BOX_CLEAN, BOX_LOCKED);
      if (prev === BOX_CLEAN) {
        const byteOffset = dataOff + i * resolvedBoxSize;
        return Object.freeze({
          boxIndex: i,
          byteOffset,
          length: resolvedBoxSize,
        });
      }
    }
    return null;
  }

  /**
   * Get byte offset of a box within the SAB data region.
   *
   * @param boxIndex - Zero-based box index
   * @returns Byte offset from SAB start
   */
  function getBoxOffset(boxIndex: number): number {
    if (boxIndex < 0 || boxIndex >= poolSize) {
      throw new RangeError(`Box index ${boxIndex} out of range [0, ${poolSize}).`);
    }
    return dataOff + boxIndex * resolvedBoxSize;
  }

  /**
   * Mark a box as READY — data written, subscribers can be notified.
   *
   * @param boxIndex - Box to mark ready
   */
  function markReady(boxIndex: number): void {
    if (boxIndex < 0 || boxIndex >= poolSize) {
      throw new RangeError(`Box index ${boxIndex} out of range [0, ${poolSize}).`);
    }
    Atomics.store(stateBoard, boxIndex, BOX_READY);
  }

  /**
   * Mark a box as READING — a worker is reading data from this box.
   * Multiple readers can hold a box in READING state simultaneously.
   *
   * @param boxIndex - Box to mark as reading
   */
  function markReading(boxIndex: number): void {
    if (boxIndex < 0 || boxIndex >= poolSize) {
      throw new RangeError(`Box index ${boxIndex} out of range [0, ${poolSize}).`);
    }
    Atomics.store(stateBoard, boxIndex, BOX_READING);
  }

  /**
   * Mark a box as CLEAN — released back to pool. Also clears lease.
   *
   * @param boxIndex - Box to release
   */
  function markClean(boxIndex: number): void {
    if (boxIndex < 0 || boxIndex >= poolSize) {
      throw new RangeError(`Box index ${boxIndex} out of range [0, ${poolSize}).`);
    }
    Atomics.store(stateBoard, boxIndex, BOX_CLEAN);
    clearLease(boxIndex);
  }

  /**
   * Set lease expiry timestamp on a box.
   *
   * @param boxIndex - Box to lease
   * @param expiresAt - Unix ms timestamp when lease expires
   */
  function setLease(boxIndex: number, expiresAt: number): void {
    if (boxIndex < 0 || boxIndex >= poolSize) {
      throw new RangeError(`Box index ${boxIndex} out of range [0, ${poolSize}).`);
    }
    Atomics.store(leaseTracker, boxIndex, BigInt(expiresAt));
  }

  /**
   * Clear lease on a box — sets to 0n (no lease).
   *
   * @param boxIndex - Box to clear lease on
   */
  function clearLease(boxIndex: number): void {
    if (boxIndex < 0 || boxIndex >= poolSize) {
      throw new RangeError(`Box index ${boxIndex} out of range [0, ${poolSize}).`);
    }
    Atomics.store(leaseTracker, boxIndex, 0n);
  }

  /**
   * Check if a box's lease has expired.
   * A lease with value 0n (no lease) is considered expired.
   *
   * @param boxIndex - Box to check
   * @returns true if lease expired or no lease set
   */
  function isLeaseExpired(boxIndex: number): boolean {
    if (boxIndex < 0 || boxIndex >= poolSize) {
      throw new RangeError(`Box index ${boxIndex} out of range [0, ${poolSize}).`);
    }
    const expiresAt = Atomics.load(leaseTracker, boxIndex);
    if (expiresAt === 0n) return true;
    return Date.now() >= Number(expiresAt);
  }

  /**
   * Read data from a box — returns a VIEW into the SAB (zero-copy).
   * Caller must ensure box state is READY or LOCKED before reading.
   *
   * @param boxIndex - Box to read from
   * @returns Uint8Array view into the data region (no copy)
   */
  function readBox(boxIndex: number): Uint8Array {
    if (boxIndex < 0 || boxIndex >= poolSize) {
      throw new RangeError(`Box index ${boxIndex} out of range [0, ${poolSize}).`);
    }
    const offset = boxIndex * resolvedBoxSize;
    return dataRegion.subarray(offset, offset + resolvedBoxSize);
  }

  /**
   * Write data into a box — copies from source into the data region.
   * Data larger than boxSize is truncated. Data smaller leaves trailing bytes.
   *
   * @param boxIndex - Box to write into
   * @param data - Source data to copy
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
    stateBoard,
    leaseTracker,
    dataRegion,
    tryAcquireBox,
    getBoxOffset,
    markReady,
    markReading,
    markClean,
    setLease,
    clearLease,
    isLeaseExpired,
    readBox,
    writeBox,
  });
}