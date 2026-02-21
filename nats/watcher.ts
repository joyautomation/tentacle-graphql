import type { PlcVariableKV } from "@tentacle/nats-schema";
import { subscribeToVariableUpdates } from "./client.ts";

export interface VariableFilter {
  variableIds?: string[];
  datatypes?: string[];
  origins?: string[];
  qualities?: string[];
}

export class WatcherManager {
  /**
   * Start watching variable updates from all modules and return an async iterable
   * that yields filtered PlcVariableKV objects
   */
  async watch(
    filter?: VariableFilter,
  ): Promise<AsyncIterable<PlcVariableKV> & { close: () => Promise<void> }> {
    const variableUpdates = await subscribeToVariableUpdates();

    let isActive = true;

    return {
      [Symbol.asyncIterator]: async function* () {
        try {
          for await (const variable of variableUpdates) {
            if (!isActive) break;

            // Apply filters
            if (filter) {
              if (
                filter.variableIds &&
                !filter.variableIds.includes(variable.variableId)
              ) {
                continue;
              }
              if (
                filter.datatypes &&
                !filter.datatypes.includes(variable.datatype)
              ) {
                continue;
              }
              if (
                filter.origins &&
                !filter.origins.includes(variable.origin)
              ) {
                continue;
              }
              if (
                filter.qualities &&
                !filter.qualities.includes(variable.quality)
              ) {
                continue;
              }
            }

            yield variable;
          }
        } catch (error) {
          console.warn("Variable watcher error:", error);
        }
      },

      close: async () => {
        isActive = false;
      },
    };
  }
}
