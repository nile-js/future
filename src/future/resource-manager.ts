import type { ResourcesConfig } from "./types";
import { safeTry } from "../safe-try";

/**
 * Main-thread resource registry. Validates input/output with Zod.
 * Executes handlers. Calls release hooks on cleanup.
 *
 * @param resources - Resource configuration map, or undefined for empty
 * @returns Execute and releaseAll functions
 */
export function createResourceManager(resources: ResourcesConfig | undefined): {
  readonly execute: (resource: string, method: string, args: unknown) => Promise<unknown>;
  readonly releaseAll: () => Promise<void>;
} {
  const config = resources ?? {};

  const execute = async (resource: string, method: string, args: unknown): Promise<unknown> => {
    const resourceConfig = config[resource];
    if (!resourceConfig) throw new Error(`Resource not found: ${resource}`);

    const entry = resourceConfig[method];
    if (!entry) throw new Error(`Method not found: ${resource}.${method}`);
    if (typeof entry === "function") throw new Error("Cannot call release as method");

    const inputResult = entry.input.safeParse(args);
    if (!inputResult.success) throw new Error(inputResult.error.message);

    const handlerResult = await safeTry(() => entry.handler(inputResult.data));
    if (handlerResult.isErr) throw new Error(handlerResult.error);

    const outputResult = entry.output.safeParse(handlerResult.value);
    if (!outputResult.success) throw new Error(outputResult.error.message);

    return outputResult.data;
  };

  const releaseAll = async (): Promise<void> => {
    for (const [, resourceConfig] of Object.entries(config)) {
      const release = resourceConfig.release;
      if (typeof release === "function") {
        const releaseResult = await safeTry(() => release());
        if (releaseResult.isErr) {
          // Continue releasing other resources — error swallowed per design
        }
      }
    }
  };

  return { execute, releaseAll } as const;
}
