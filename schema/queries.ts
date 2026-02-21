import { builder } from "./builder.ts";
import {
  listVariables,
  getVariable,
  getAllHeartbeats,
  getNatsConnection,
} from "../nats/client.ts";
import { getRecentLogs } from "../modules/logs.ts";
import { LogEntryRef, NatsTrafficEntryRef, NetworkStateRef, NetworkInterfaceConfigRef, NftablesConfigRef } from "./types.ts";
import { getMode } from "../types/config.ts";
import { getRecentTraffic } from "../modules/nats-traffic.ts";
import { requestNetworkState, requestNetworkConfig } from "../modules/network.ts";
import { requestNftablesConfig } from "../modules/nftables.ts";

builder.queryType({
  fields: (t) => ({
    // Current deployment mode (dev, systemd, docker, kubernetes)
    mode: t.field({
      type: "String",
      description: "Current deployment mode of tentacle-graphql",
      resolve: () => getMode(),
    }),

    // List variables (optionally filter by moduleId)
    variables: t.field({
      type: ["Variable"],
      args: {
        moduleId: t.arg.string({ required: false }),
      },
      resolve: async (_root: unknown, args: { moduleId?: string | null }) => {
        return await listVariables(args.moduleId ?? undefined);
      },
    }),

    // Get a specific variable
    variable: t.field({
      type: "Variable",
      nullable: true,
      args: {
        variableId: t.arg.string({ required: true }),
      },
      resolve: async (_root: unknown, args: { variableId: string }) => {
        return await getVariable(args.variableId);
      },
    }),

    // Get recent log entries for a service
    serviceLogs: t.field({
      type: [LogEntryRef],
      args: {
        serviceType: t.arg.string({ required: true }),
        limit: t.arg.int({ required: false }),
      },
      description: "Get recent log entries for a service type from the ring buffer",
      resolve: (_root: unknown, args: { serviceType: string; limit?: number | null }) => {
        return getRecentLogs(args.serviceType, args.limit ?? undefined);
      },
    }),

    // Get recent NATS traffic entries
    natsTraffic: t.field({
      type: [NatsTrafficEntryRef],
      args: {
        limit: t.arg.int({ required: false }),
      },
      description: "Get recent NATS traffic entries from the ring buffer",
      resolve: (_root: unknown, args: { limit?: number | null }) => {
        return getRecentTraffic(args.limit ?? undefined);
      },
    }),

    // List all active services
    services: t.field({
      type: ["Service"],
      description: "List active tentacle services.",
      resolve: async () => {
        return await getAllHeartbeats();
      },
    }),

    // Get fresh network interface state from the kernel
    networkInterfaces: t.field({
      type: NetworkStateRef,
      description: "Request fresh network interface state from tentacle-network (live kernel data)",
      resolve: async () => {
        try {
          const nc = getNatsConnection();
          return await requestNetworkState(nc);
        } catch {
          return { moduleId: "network", timestamp: Date.now(), interfaces: [] };
        }
      },
    }),

    // Get current netplan configuration
    networkConfig: t.field({
      type: [NetworkInterfaceConfigRef],
      description: "Get current network configuration from tentacle-network's netplan file",
      resolve: async () => {
        try {
          const nc = getNatsConnection();
          return await requestNetworkConfig(nc);
        } catch {
          return [];
        }
      },
    }),

    // Get current nftables NAT configuration
    nftablesConfig: t.field({
      type: NftablesConfigRef,
      description: "Get current nftables NAT configuration",
      resolve: async () => {
        try {
          const nc = getNatsConnection();
          return await requestNftablesConfig(nc);
        } catch {
          return { natRules: [] };
        }
      },
    }),
  }),
});
