/**
 * Barrel export for @nilejs/future public API.
 */

// Types
export type {
  ActorCallback,
  ActorConfig,
  ActorContext,
  ActorDiagnostics,
  ActorGroup,
  ActorGroupConfig,
  ActorRef,
  ActorSelf,
  BoxState,
  DiagnosticsConfig,
  FormatUtils,
  Lock,
  MemoryConfig,
  ResourceConfig,
  ResourcesConfig,
  RetryConfig,
  Supervisor,
  SupervisorConfig,
  SupervisorDiagnostics,
  SupervisionStrategy,
  TimeoutConfig,
} from "./types";

// Constants
export { BOX_CLEAN, BOX_LOCKED, BOX_READY, BOX_READING } from "./types";

// Public API
export { createSupervisor } from "./supervisor";
