import { builder } from "./builder.ts";
import { WatcherManager } from "../nats/watcher.ts";
import { subscribeToBrowseProgress } from "../nats/client.ts";
import type { PlcVariableKV, BrowseProgressMessage } from "@tentacle/nats-schema";
import { BrowseProgressRef } from "./types.ts";

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

    // Subscribe to browse progress updates
    // Use after calling browseTags mutation with async: true
    browseProgress: t.field({
      type: BrowseProgressRef,
      args: {
        browseId: t.arg.string({ required: true }),
      },
      subscribe: async function* (_root: unknown, args: { browseId: string }) {
        const progressStream = await subscribeToBrowseProgress(args.browseId);

        for await (const progress of progressStream) {
          yield progress;
        }
      },
      resolve: (progress: BrowseProgressMessage) => progress,
    }),
  }),
});
