import { builder } from "./builder.ts";
import {
  listVariables,
  getVariable,
  getAllHeartbeats,
  getAllServiceEnabled,
  getNatsConnection,
  getAllDesiredServices,
  getAllServiceStatuses,
} from "../nats/client.ts";
import { getRecentLogs } from "../modules/logs.ts";
import { ConfigEntryRef, LogEntryRef, NatsTrafficEntryRef, NetworkStateRef, NetworkInterfaceConfigRef, NftablesConfigRef, MqttMetricsResponseRef, VariableHistoryRef, VariableHistoryInputRef, UsageStatsRef, StoreForwardStatusRef, GatewayConfigRef, GatewayBrowseResultRef, GatewayBrowseStateRef, DesiredServiceRef, ServiceStatusRef, ModuleRegistryInfoRef, ModuleVersionInfoRef } from "./types.ts";
import { getMode } from "../types/config.ts";
import { getRecentTraffic } from "../modules/nats-traffic.ts";
import { requestNetworkState, requestNetworkConfig } from "../modules/network.ts";
import { requestNftablesConfig } from "../modules/nftables.ts";
import { requestMqttMetrics } from "../modules/mqtt.ts";
import { getHistory, getUsage, getHistoryPool } from "../modules/history.ts";
import { requestStoreForwardStatus } from "../modules/store-forward.ts";
import { getServiceConfig, getAllConfig } from "../modules/service-config.ts";
import { getGatewayConfig, listGatewayConfigs, getCachedBrowseResult, getBrowseState, getAllBrowseStates } from "../modules/gateway.ts";
import { requestModuleRegistry, requestInternetCheck, requestModuleVersions } from "../modules/orchestrator.ts";

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
        const [heartbeats, enabledMap] = await Promise.all([
          getAllHeartbeats(),
          getAllServiceEnabled(),
        ]);
        // Merge enabled state into heartbeats for batch resolution
        return heartbeats.map((hb) => ({
          ...hb,
          _enabled: enabledMap.has(hb.moduleId) ? enabledMap.get(hb.moduleId) : true,
        }));
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

    // Get current MQTT metrics and templates
    mqttMetrics: t.field({
      type: MqttMetricsResponseRef,
      description: "Get current MQTT Sparkplug B metrics and template definitions from tentacle-mqtt",
      resolve: async () => {
        try {
          const nc = getNatsConnection();
          return await requestMqttMetrics(nc);
        } catch (err) {
          console.error("mqttMetrics error:", err);
          return { metrics: [], templates: [], deviceId: "", timestamp: Date.now() };
        }
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // History Queries
    // ═══════════════════════════════════════════════════════════════════════

    // Query historical variable data with optional downsampling
    history: t.field({
      type: [VariableHistoryRef],
      description: "Query historical variable data with time-bucketed aggregation",
      args: {
        start: t.arg({ type: "DateTime", required: true }),
        end: t.arg({ type: "DateTime", required: true }),
        variables: t.arg({ type: [VariableHistoryInputRef], required: true }),
        interval: t.arg.string({ required: false, description: "Time bucket interval (e.g., '1m', '1h'). Auto-calculated if omitted." }),
        samples: t.arg.int({ required: false, description: "Target number of data points (default 100). Used to auto-calculate interval." }),
        raw: t.arg.boolean({ required: false, description: "If true, return raw points without aggregation" }),
      },
      resolve: async (_root, args) => {
        if (!getHistoryPool()) {
          return [];
        }
        try {
          return await getHistory({
            variables: args.variables.map((v) => ({ moduleId: v.moduleId, variableId: v.variableId })),
            start: new Date(args.start),
            end: new Date(args.end),
            interval: args.interval,
            samples: args.samples,
            raw: args.raw,
          });
        } catch (err) {
          console.error("history query error:", err);
          return [];
        }
      },
    }),

    // History usage statistics
    historyUsage: t.field({
      type: UsageStatsRef,
      nullable: true,
      description: "Get history storage usage statistics",
      resolve: async () => {
        if (!getHistoryPool()) return null;
        try {
          return await getUsage();
        } catch (err) {
          console.error("historyUsage error:", err);
          return null;
        }
      },
    }),

    // Whether history is available
    historyEnabled: t.field({
      type: "Boolean",
      description: "Whether the history database is configured and available",
      resolve: () => getHistoryPool() !== null,
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Store & Forward
    // ═══════════════════════════════════════════════════════════════════════

    storeForwardStatus: t.field({
      type: StoreForwardStatusRef,
      nullable: true,
      description: "Get MQTT Store & Forward buffer status, including primary host state and drain progress",
      resolve: async () => {
        try {
          const nc = getNatsConnection();
          return await requestStoreForwardStatus(nc);
        } catch {
          return null;
        }
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Service Configuration
    // ═══════════════════════════════════════════════════════════════════════

    serviceConfig: t.field({
      type: [ConfigEntryRef],
      description: "Get all config entries for a service module from NATS KV",
      args: {
        moduleId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        return await getServiceConfig(args.moduleId);
      },
    }),

    allConfig: t.field({
      type: [ConfigEntryRef],
      description: "Get all config entries across all service modules",
      resolve: async () => {
        return await getAllConfig();
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Gateway Configuration
    // ═══════════════════════════════════════════════════════════════════════

    gatewayConfig: t.field({
      type: GatewayConfigRef,
      description: "Get the full gateway configuration (devices + variables) for a gateway instance",
      args: {
        gatewayId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        return await getGatewayConfig(args.gatewayId);
      },
    }),

    gatewayConfigs: t.field({
      type: [GatewayConfigRef],
      description: "List all gateway configurations",
      resolve: async () => {
        return await listGatewayConfigs();
      },
    }),

    gatewayBrowseCache: t.field({
      type: GatewayBrowseResultRef,
      nullable: true,
      description: "Get cached browse results for a device (returns null if no cache exists)",
      args: {
        deviceId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        return await getCachedBrowseResult(args.deviceId);
      },
    }),

    gatewayBrowseStates: t.field({
      type: [GatewayBrowseStateRef],
      description: "Get all active/recent browse states across all devices",
      resolve: async () => {
        return await getAllBrowseStates();
      },
    }),

    gatewayBrowseState: t.field({
      type: GatewayBrowseStateRef,
      nullable: true,
      description: "Get the browse state for a specific device",
      args: {
        deviceId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        return await getBrowseState(args.deviceId);
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Orchestrator
    // ═══════════════════════════════════════════════════════════════════════

    desiredServices: t.field({
      type: [DesiredServiceRef],
      description: "List all desired service states from the orchestrator KV",
      resolve: async () => {
        return await getAllDesiredServices();
      },
    }),

    serviceStatuses: t.field({
      type: [ServiceStatusRef],
      description: "List all service statuses reported by the orchestrator",
      resolve: async () => {
        return await getAllServiceStatuses();
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Module Management
    // ═══════════════════════════════════════════════════════════════════════

    availableModules: t.field({
      type: [ModuleRegistryInfoRef],
      description: "List all modules in the orchestrator registry (installed and available)",
      resolve: async () => {
        try {
          const nc = getNatsConnection();
          return await requestModuleRegistry(nc);
        } catch {
          return [];
        }
      },
    }),

    internetConnectivity: t.field({
      type: "Boolean",
      description: "Check if the server has internet connectivity (can reach GitHub for downloads)",
      resolve: async () => {
        try {
          const nc = getNatsConnection();
          return await requestInternetCheck(nc);
        } catch {
          return false;
        }
      },
    }),

    moduleVersions: t.field({
      type: ModuleVersionInfoRef,
      description: "Get version information for a specific module (installed, active, latest)",
      args: {
        moduleId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        try {
          const nc = getNatsConnection();
          return await requestModuleVersions(nc, args.moduleId);
        } catch {
          return {
            moduleId: args.moduleId,
            installedVersions: [],
            latestVersion: null,
            activeVersion: null,
          };
        }
      },
    }),
  }),
});
