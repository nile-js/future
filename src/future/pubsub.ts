import type { MainToWorkerMessage, WorkerToMainMessage } from "./types";
import { safeTry } from "slang-ts";
import type { Result } from "slang-ts";

// ============================================================================
// PubSub — Tier 1 message bus
// ============================================================================

/**
 * Create a lightweight pub/sub message bus.
 * Subscribers are stored in a Set — add returns unsubscribe function.
 * Publish iterates all subscribers synchronously.
 *
 * @returns PubSub handle with subscribe, publish, subscriberCount, clear
 */
export function createPubSub(): {
  readonly subscribe: (fn: (msg: unknown) => void) => () => void;
  readonly publish: (msg: unknown) => void;
  readonly subscriberCount: () => number;
  readonly clear: () => void;
} {
  const subscribers = new Set<(msg: unknown) => void>();

  /**
   * Subscribe to all published messages.
   * Returns unsubscribe function for cleanup.
   *
   * @param fn - Handler called for each published message
   * @returns Unsubscribe function — removes handler, idempotent
   */
  function subscribe(fn: (msg: unknown) => void): () => void {
    subscribers.add(fn);
    return () => {
      subscribers.delete(fn);
    };
  }

  /**
   * Publish a message to all subscribers.
   * Iterates current snapshot — subscribers added/removed during
   * publish won't affect this cycle.
   *
   * @param msg - Message payload delivered to each subscriber
   */
  function publish(msg: unknown): void {
    for (const fn of subscribers) {
      fn(msg);
    }
  }

  /** Current subscriber count */
  function subscriberCount(): number {
    return subscribers.size;
  }

  /** Remove all subscribers */
  function clear(): void {
    subscribers.clear();
  }

  return Object.freeze({ subscribe, publish, subscriberCount, clear });
}

// ============================================================================
// MessageChannel — typed worker thread bridge
// ============================================================================

/** Default timeout for request/response pattern (ms) */
const DEFAULT_REQUEST_TIMEOUT_MS = 5000;

/**
 * Create a typed message channel over a Worker thread.
 * Provides send, request/response, and listener management.
 *
 * @param worker - Node.js Worker thread to communicate with
 * @returns MessageChannel handle with send, request, onMessage, dispose
 */
export function createMessageChannel(worker: import("node:worker_threads").Worker): {
  readonly send: (msg: MainToWorkerMessage) => void;
  readonly request: <T extends WorkerToMainMessage>(
    msg: MainToWorkerMessage,
    options?: {
      readonly timeoutMs?: number;
      readonly match?: (response: WorkerToMainMessage) => response is T;
    },
  ) => Promise<T>;
  readonly onMessage: (fn: (msg: WorkerToMainMessage) => void) => () => void;
  readonly dispose: () => void;
} {
  type PendingRequest = {
    readonly resolve: (value: unknown) => void;
    readonly reject: (error: string) => void;
    readonly timer: ReturnType<typeof setTimeout>;
    readonly match: (response: WorkerToMainMessage) => boolean;
  };

  const pending = new Map<string, PendingRequest>();

  /** Generic message listeners */
  const listeners = new Set<(msg: WorkerToMainMessage) => void>();

  /**
   * Route incoming worker messages to pending requests and listeners.
   * Checks pending map first for request/response matching.
   */
  function handleMessage(raw: unknown): void {
    const msg = raw as WorkerToMainMessage;

    // Check if any pending request matches this response
    if (typeof msg === "object" && msg !== null && "id" in msg) {
      const id = (msg as { id: unknown }).id;
      if (typeof id === "string" && pending.has(id)) {
        const req = pending.get(id)!;
        clearTimeout(req.timer);
        pending.delete(id);
        if (req.match(msg)) {
          req.resolve(msg);
        }
        // If match fails, still remove — request expired or mismatched
        return;
      }
    }

    // Deliver to all generic listeners
    for (const fn of listeners) {
      fn(msg);
    }
  }

  // Wire up worker message handler
  worker.on("message", handleMessage);

  /**
   * Send a message to the worker thread (fire-and-forget).
   *
   * @param msg - Typed message payload
   */
  function send(msg: MainToWorkerMessage): void {
    worker.postMessage(msg);
  }

  /**
   * Send a message and wait for a matching response.
   * Uses `match` fn to identify the response, or falls back to `id` field matching.
   * Times out after `timeoutMs` (default 5000ms).
   *
   * @param msg - Message to send
   * @param options.timeoutMs - Max wait time in ms (default 5000)
   * @param options.match - Type guard to identify the correct response
   * @returns Promise resolving to the matched response
   */
  async function request<T extends WorkerToMainMessage>(
    msg: MainToWorkerMessage,
    options?: {
      readonly timeoutMs?: number;
      readonly match?: (response: WorkerToMainMessage) => response is T;
    },
  ): Promise<T> {
    const timeoutMs = options?.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    const matchFn = options?.match;

    // Extract request id for correlation
    const requestId =
      typeof msg === "object" && msg !== null && "id" in msg
        ? (msg as { id: unknown }).id
        : undefined;

    if (requestId === undefined) {
      throw new Error("Request message must have an `id` field for response correlation.");
    }

    const id = requestId as string;

    // Default match: same id field
    const match = matchFn ?? ((m: WorkerToMainMessage): m is T => {
      return typeof m === "object" && m !== null && "id" in m && (m as { id: unknown }).id === id;
    });

    const result = await safeTry<T>(() => {
      return new Promise<T>((resolve, reject) => {
        const timer = setTimeout(() => {
          pending.delete(id);
          reject(new Error(`Request timed out after ${timeoutMs}ms (id: ${id}).`));
        }, timeoutMs);

        pending.set(id, {
          resolve: resolve as (value: unknown) => void,
          reject: (error: string) => reject(new Error(error)),
          timer,
          match: match as (response: WorkerToMainMessage) => boolean,
        });

        worker.postMessage(msg);
      });
    });

    if (result.isErr) {
      throw new Error(result.error);
    }

    return result.value;
  }

  /**
   * Register a listener for all worker messages.
   * Returns unsubscribe function.
   *
   * @param fn - Handler for incoming worker messages
   * @returns Unsubscribe function
   */
  function onMessage(fn: (msg: WorkerToMainMessage) => void): () => void {
    listeners.add(fn);
    return () => {
      listeners.delete(fn);
    };
  }

  /**
   * Remove all listeners and clear pending requests.
   * Removes the worker message handler. Call on shutdown.
   */
  function dispose(): void {
    // Reject all pending requests
    for (const [id, req] of pending) {
      clearTimeout(req.timer);
      req.reject("MessageChannel disposed.");
    }
    pending.clear();
    listeners.clear();
    worker.off("message", handleMessage);
  }

  return Object.freeze({ send, request, onMessage, dispose });
}