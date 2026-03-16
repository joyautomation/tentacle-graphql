import type { PlcVariableKV } from "@tentacle/nats-schema";
import { subscribeToVariableUpdates } from "./client.ts";

export interface VariableFilter {
  moduleId?: string;
  variableIds?: string[];
  datatypes?: string[];
  origins?: string[];
  qualities?: string[];
}

function passesFilter(variable: PlcVariableKV, filter?: VariableFilter): boolean {
  if (!filter) return true;
  if (filter.variableIds && !filter.variableIds.includes(variable.variableId)) return false;
  if (filter.datatypes && !filter.datatypes.includes(variable.datatype)) return false;
  if (filter.origins && !filter.origins.includes(variable.origin)) return false;
  if (filter.qualities && !filter.qualities.includes(variable.quality)) return false;
  return true;
}

export class WatcherManager {
  /**
   * Start watching variable updates and return an async iterable
   * that yields filtered PlcVariableKV objects one at a time.
   * When moduleId is provided, only subscribes to that module's data stream.
   */
  async watch(
    filter?: VariableFilter,
  ): Promise<AsyncIterable<PlcVariableKV> & { close: () => Promise<void> }> {
    const variableUpdates = await subscribeToVariableUpdates(filter?.moduleId);

    let isActive = true;

    return {
      [Symbol.asyncIterator]: async function* () {
        try {
          for await (const variable of variableUpdates) {
            if (!isActive) break;
            if (!passesFilter(variable, filter)) continue;
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

  /**
   * Start watching variable updates and return an async iterable
   * that yields batches of changed variables at a fixed interval.
   * Accumulates updates in a Map (latest value per variableId) and
   * flushes every `intervalMs` milliseconds.
   */
  async watchBatched(
    filter?: VariableFilter,
    intervalMs = 2500,
  ): Promise<AsyncIterable<PlcVariableKV[]> & { close: () => Promise<void> }> {
    const variableUpdates = await subscribeToVariableUpdates(filter?.moduleId);

    let isActive = true;
    const pending = new Map<string, PlcVariableKV>();

    // Background task: consume NATS messages into the pending map
    const consumer = (async () => {
      try {
        for await (const variable of variableUpdates) {
          if (!isActive) break;
          if (!passesFilter(variable, filter)) continue;
          pending.set(variable.variableId, variable);
        }
      } catch (error) {
        if (isActive) console.warn("Variable batch consumer error:", error);
      }
    })();

    return {
      [Symbol.asyncIterator]: async function* () {
        try {
          while (isActive) {
            await new Promise((resolve) => setTimeout(resolve, intervalMs));
            if (!isActive) break;
            if (pending.size > 0) {
              const batch = [...pending.values()];
              pending.clear();
              yield batch;
            }
          }
        } catch (error) {
          console.warn("Variable batch watcher error:", error);
        }
      },

      close: async () => {
        isActive = false;
        await consumer.catch(() => {});
      },
    };
  }
}
