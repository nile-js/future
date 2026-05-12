import { Ok, Err, safeTry } from "slang-ts";
import type { Result } from "slang-ts";
import type { ActorRef } from "./types";

// ============================================================================
// Backoff Calculation
// ============================================================================

/**
 * Calculate delay in milliseconds for the next retry attempt.
 *
 * @param options.attempt — zero-indexed attempt number (0 = first retry after initial failure)
 * @param options.backoff — backoff strategy: exponential, linear, or fixed
 * @param options.baseDelayMs — base delay in ms, defaults to 100
 * @returns delay in milliseconds before the next attempt
 *
 * @example
 * calculateBackoff({ attempt: 0, backoff: "exponential" }) // 100
 * calculateBackoff({ attempt: 1, backoff: "exponential" }) // 200
 * calculateBackoff({ attempt: 2, backoff: "linear" })      // 200
 * calculateBackoff({ attempt: 3, backoff: "fixed" })       // 100
 */
export function calculateBackoff(options: {
  readonly attempt: number;
  readonly backoff: "exponential" | "linear" | "fixed";
  readonly baseDelayMs?: number;
}): number {
  const { attempt, backoff, baseDelayMs = 100 } = options;
  const base = Math.max(1, baseDelayMs);

  const strategy: Record<typeof backoff, (a: number) => number> = {
    exponential: (a) => base * Math.pow(2, a),
    linear: (a) => base * (a + 1),
    fixed: () => base,
  };

  return strategy[backoff](attempt);
}

// ============================================================================
// Retry Spawn
// ============================================================================

/**
 * Retry a spawn operation with configurable backoff.
 *
 * Attempts `spawnFn()` up to `policy.max + 1` times (initial attempt + max retries).
 * On failure, waits `calculateBackoff(...)` ms before the next try.
 * Returns `Ok(actorRef)` on success or `Err` with the last error message on exhaustion.
 *
 * @param options.spawnFn — function that creates an actor, may throw or return invalid ref
 * @param options.policy — retry configuration: max retries, backoff strategy, optional base delay
 * @returns `Result<ActorRef, string>` — Ok with the ref, or Err with failure details
 */
export async function retrySpawn(options: {
  readonly spawnFn: () => ActorRef;
  readonly policy: { readonly max: number; readonly backoff: "exponential" | "linear" | "fixed"; readonly delayMs?: number };
}): Promise<Result<ActorRef, string>> {
  const { spawnFn, policy } = options;
  const totalAttempts = policy.max + 1;
  const baseDelay = policy.delayMs ?? 100;
  let lastError = "";

  for (let attempt = 0; attempt < totalAttempts; attempt++) {
    const result = await safeTry(() => spawnFn());

    if (result.isOk) {
      const ref = result.value;
      if (ref && typeof ref === "object" && ref.id) {
        return Ok(ref);
      }
      lastError = "Spawn returned invalid ActorRef";
    } else {
      lastError = (result as { type: "Err"; error: string }).error;
    }

    // Don't sleep after the last failed attempt
    if (attempt < totalAttempts - 1) {
      const delay = calculateBackoff({ attempt, backoff: policy.backoff, baseDelayMs: baseDelay });
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  return Err(`Restart failed after ${totalAttempts} attempts: ${lastError}`);
}