/**
 * @nilejs/future — High-performance actor and promise primitives for Bun and Node.js.
 *
 * Exports the future actor system (createSupervisor) and re-exports
 * slang functional utilities (Result, Option, match, safeTry, etc.).
 */

// Future actor system
export { createSupervisor } from "./src/future";
export type * from "./src/future";

// Slang functional utilities
export * from "./src";
