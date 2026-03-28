import { builder } from "./builder.ts";
import { WatcherManager } from "../nats/watcher.ts";
import { subscribeToBrowseProgress, subscribeToGatewayBrowseProgress, type GatewayBrowseProgress } from "../nats/client.ts";
import { subscribeToServiceLogs } from "../modules/logs.ts";
import { subscribeToNatsTraffic, type NatsTrafficEntry } from "../modules/nats-traffic.ts";
import { subscribeToNetworkState } from "../modules/network.ts";
import { subscribeToNftablesConfig } from "../modules/nftables.ts";
import type { PlcVariableKV, BrowseProgressMessage, ServiceLogEntry, NetworkStateMessage, NftablesConfig } from "@tentacle/nats-schema";
import { BrowseProgressRef, LogEntryRef, NatsTrafficEntryRef, NetworkStateRef, NftablesConfigRef, GatewayBrowseProgressRef } from "./types.ts";

const watcherManager = new WatcherManager();

builder.subscriptionType({
  fields: (t) => ({
    // Subscribe to variable updates, optionally filtered by moduleId
    variableUpdates: t.field({
      type: "Variable",
      args: {
        moduleId: t.arg.string({ required: false }),
      },
      subscribe: async function* (_root: unknown, args: { moduleId?: string | null }) {
        const watcher = await watcherManager.watch({
          moduleId: args.moduleId ?? undefined,
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

    // Subscribe to batched variable updates — emits all changed variables every 2.5s
    variableBatchUpdates: t.field({
      type: ["Variable"],
      args: {
        moduleId: t.arg.string({ required: false }),
      },
      subscribe: async function* (_root: unknown, args: { moduleId?: string | null }) {
        const watcher = await watcherManager.watchBatched({
          moduleId: args.moduleId ?? undefined,
        });

        try {
          for await (const batch of watcher) {
            yield batch;
          }
        } finally {
          await watcher.close();
        }
      },
      resolve: (batch: PlcVariableKV[]) => batch,
    }),

    // Subscribe to updates for a specific variable
    variableChanged: t.field({
      type: "Variable",
      args: {
        variableId: t.arg.string({ required: true }),
      },
      subscribe: async function* (_root: unknown, args: { variableId: string }) {
        const watcher = await watcherManager.watch({
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

    // Subscribe to gateway device browse progress
    gatewayBrowseProgress: t.field({
      type: GatewayBrowseProgressRef,
      args: {
        browseId: t.arg.string({ required: true }),
        protocol: t.arg.string({ required: true }),
      },
      subscribe: async function* (_root: unknown, args: { browseId: string; protocol: string }) {
        const progressStream = await subscribeToGatewayBrowseProgress(args.protocol, args.browseId);
        for await (const progress of progressStream) {
          yield progress;
        }
      },
      resolve: (progress: GatewayBrowseProgress) => progress,
    }),

    // Subscribe to real-time service log entries
    serviceLogs: t.field({
      type: LogEntryRef,
      args: {
        serviceType: t.arg.string({ required: true }),
      },
      subscribe: async function* (_root: unknown, args: { serviceType: string }) {
        for await (const entry of subscribeToServiceLogs(args.serviceType)) {
          yield entry;
        }
      },
      resolve: (entry: ServiceLogEntry) => entry,
    }),

    // Subscribe to real-time NATS traffic
    natsTraffic: t.field({
      type: NatsTrafficEntryRef,
      args: {
        filter: t.arg.string({ required: false }),
      },
      subscribe: async function* (_root: unknown, args: { filter?: string | null }) {
        for await (const entry of subscribeToNatsTraffic(args.filter ?? undefined)) {
          yield entry;
        }
      },
      resolve: (entry: NatsTrafficEntry) => entry,
    }),

    // Subscribe to real-time network interface state updates
    networkState: t.field({
      type: NetworkStateRef,
      subscribe: async function* () {
        for await (const state of subscribeToNetworkState()) {
          yield state;
        }
      },
      resolve: (state: NetworkStateMessage) => state,
    }),

    // Subscribe to real-time nftables config updates
    nftablesConfig: t.field({
      type: NftablesConfigRef,
      subscribe: async function* () {
        for await (const config of subscribeToNftablesConfig()) {
          yield config;
        }
      },
      resolve: (config: NftablesConfig) => config,
    }),
  }),
});
