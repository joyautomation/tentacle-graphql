import { builder } from "./builder.ts";
import { WatcherManager } from "../nats/watcher.ts";
import type { PlcVariableKV } from "@tentacle/nats-schema";

const watcherManager = new WatcherManager();

builder.subscriptionType({
  fields: (t) => ({
    // Subscribe to all variable updates in a project
    variableUpdates: t.field({
      type: "Variable",
      args: {
        projectId: t.arg.string({ required: true }),
      },
      subscribe: async function* (_root: unknown, args: { projectId: string }) {
        const watcher = await watcherManager.watch(args.projectId);

        try {
          for await (const variable of watcher) {
            yield variable;
          }
        } finally {
          await watcher.close();
        }
      },
      resolve: (variable: PlcVariableKV) => variable,
    }),

    // Subscribe to updates for a specific variable
    variableChanged: t.field({
      type: "Variable",
      args: {
        projectId: t.arg.string({ required: true }),
        variableId: t.arg.string({ required: true }),
      },
      subscribe: async function* (_root: unknown, args: { projectId: string; variableId: string }) {
        const watcher = await watcherManager.watch(args.projectId, {
          variableIds: [args.variableId],
        });

        try {
          for await (const variable of watcher) {
            yield variable;
          }
        } finally {
          await watcher.close();
        }
      },
      resolve: (variable: PlcVariableKV) => variable,
    }),
  }),
});
