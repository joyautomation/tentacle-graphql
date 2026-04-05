import { builder } from "./builder.ts";
import type { PlcVariableKV, DeadBandConfig, ServiceHeartbeat, BrowseProgressMessage } from "@tentacle/nats-schema";
import { getServiceEnabled, getAllHeartbeats } from "../nats/client.ts";

// Extended heartbeat with enabled state for GraphQL resolution
type ServiceWithEnabled = ServiceHeartbeat & { _enabled?: boolean };

// Service object type (represents a tentacle service instance)
const ServiceRef = builder.objectRef<ServiceWithEnabled>("Service");
builder.objectType(ServiceRef, {
  fields: (t) => ({
    serviceType: t.exposeString("serviceType"),
    moduleId: t.exposeString("moduleId"),
    lastSeen: t.field({
      type: "DateTime",
      resolve: (s) => new Date(s.lastSeen),
    }),
    startedAt: t.field({
      type: "DateTime",
      resolve: (s) => new Date(s.startedAt),
    }),
    version: t.exposeString("version", { nullable: true }),
    uptime: t.field({
      type: "Int",
      description: "Uptime in seconds",
      resolve: (s) => Math.floor((Date.now() - s.startedAt) / 1000),
    }),
    metadata: t.field({
      type: "JSON",
      nullable: true,
      resolve: (s) => s.metadata || null,
    }),
    enabled: t.field({
      type: "Boolean",
      description: "Whether the service is enabled and actively performing work",
      resolve: async (s) => {
        // Use pre-fetched value if available (from batch query)
        if (s._enabled !== undefined) return s._enabled;
        return await getServiceEnabled(s.moduleId);
      },
    }),
  }),
});

// DeadBandConfig object type
const DeadBandConfigType = builder.objectRef<DeadBandConfig>(
  "DeadBandConfig",
);
builder.objectType(DeadBandConfigType, {
  fields: (t) => ({
    value: t.exposeFloat("value"),
    minTime: t.exposeInt("minTime", { nullable: true }),
    maxTime: t.exposeInt("maxTime", { nullable: true }),
  }),
});

// Variable object type (based on PlcVariableKV)
builder.objectType("Variable", {
  fields: (t) => ({
    moduleId: t.exposeString("moduleId"),
    deviceId: t.exposeString("deviceId", { nullable: true }),
    variableId: t.exposeString("variableId"),
    value: t.field({
      type: "JSON",
      nullable: true,
      resolve: (v) => v.value,
    }),
    datatype: t.exposeString("datatype"),
    cipType: t.field({
      type: "String",
      nullable: true,
      description: "CIP type for EtherNet/IP tags (e.g. DINT, REAL, STRING)",
      resolve: (v) => (v as Record<string, unknown>).cipType as string | null ?? null,
    }),
    udtType: t.field({
      type: "String",
      nullable: true,
      description: "UDT template type name (e.g. Analog_Input, LevelControl_Pump)",
      resolve: (v) => (v as Record<string, unknown>).udtType as string | null ?? null,
    }),
    lastUpdated: t.field({
      type: "DateTime",
      resolve: (v) => new Date(v.lastUpdated),
    }),
    origin: t.exposeString("origin"),
    quality: t.exposeString("quality"),
    source: t.exposeString("source", { nullable: true }),
    deadband: t.field({
      type: DeadBandConfigType,
      nullable: true,
      resolve: (v) => v.deadband || null,
    }),
    disableRBE: t.exposeBoolean("disableRBE", { nullable: true }),
  }),
});

// VariableFilter input type
builder.inputType("VariableFilter", {
  fields: (t) => ({
    variableIds: t.stringList({ required: false }),
    datatypes: t.stringList({ required: false }),
    sources: t.stringList({ required: false }),
    qualities: t.stringList({ required: false }),
  }),
});

// VariableUpdate input type
builder.inputType("VariableUpdate", {
  fields: (t) => ({
    variableId: t.string({ required: true }),
    value: t.field({ type: "JSON", required: true }),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// Browse Progress Types
// ═══════════════════════════════════════════════════════════════════════════

// BrowseProgress object type - for subscription updates during browse
export const BrowseProgressRef = builder.objectRef<BrowseProgressMessage>("BrowseProgress");
builder.objectType(BrowseProgressRef, {
  fields: (t) => ({
    browseId: t.exposeString("browseId"),
    moduleId: t.exposeString("moduleId"),
    deviceId: t.exposeString("deviceId"),
    phase: t.exposeString("phase"),
    totalTags: t.exposeInt("totalTags"),
    completedTags: t.exposeInt("completedTags"),
    errorCount: t.exposeInt("errorCount"),
    message: t.exposeString("message", { nullable: true }),
    timestamp: t.field({
      type: "DateTime",
      resolve: (p) => new Date(p.timestamp),
    }),
    percentComplete: t.field({
      type: "Float",
      description: "Percentage of browse operation complete (0-100)",
      resolve: (p) => p.totalTags > 0 ? Math.round((p.completedTags / p.totalTags) * 100) : 0,
    }),
  }),
});

// BrowseResult type - returned from browseTags mutation
export interface BrowseResultShape {
  browseId: string;
  variables?: PlcVariableKV[];
}

export const BrowseResultRef = builder.objectRef<BrowseResultShape>("BrowseResult");
builder.objectType(BrowseResultRef, {
  fields: (t) => ({
    browseId: t.exposeString("browseId"),
    variables: t.field({
      type: ["Variable"],
      nullable: true,
      resolve: (r) => r.variables || null,
    }),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// Service Log Types
// ═══════════════════════════════════════════════════════════════════════════

import type { ServiceLogEntry } from "@tentacle/nats-schema";

const LogEntryRef = builder.objectRef<ServiceLogEntry>("LogEntry");
builder.objectType(LogEntryRef, {
  fields: (t) => ({
    timestamp: t.field({
      type: "DateTime",
      resolve: (e) => new Date(e.timestamp),
    }),
    level: t.exposeString("level"),
    message: t.exposeString("message"),
    serviceType: t.exposeString("serviceType"),
    moduleId: t.exposeString("moduleId"),
    logger: t.exposeString("logger", { nullable: true }),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// NATS Traffic Types
// ═══════════════════════════════════════════════════════════════════════════

import type { NatsTrafficEntry } from "../modules/nats-traffic.ts";

const NatsTrafficEntryRef = builder.objectRef<NatsTrafficEntry>("NatsTrafficEntry");
builder.objectType(NatsTrafficEntryRef, {
  fields: (t) => ({
    timestamp: t.field({
      type: "DateTime",
      resolve: (e) => new Date(e.timestamp),
    }),
    subject: t.exposeString("subject"),
    size: t.exposeInt("size"),
    payload: t.exposeString("payload"),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// Network Types
// ═══════════════════════════════════════════════════════════════════════════

import type {
  NetworkInterfaceStats,
  NetworkAddress,
  NetworkInterface,
  NetworkStateMessage,
  NetworkInterfaceConfig,
  NetworkCommandResponse,
} from "@tentacle/nats-schema";

const NetworkInterfaceStatsRef = builder.objectRef<NetworkInterfaceStats>("NetworkInterfaceStats");
builder.objectType(NetworkInterfaceStatsRef, {
  fields: (t) => ({
    rxBytes: t.exposeFloat("rxBytes"),
    txBytes: t.exposeFloat("txBytes"),
    rxPackets: t.exposeFloat("rxPackets"),
    txPackets: t.exposeFloat("txPackets"),
    rxErrors: t.exposeFloat("rxErrors"),
    txErrors: t.exposeFloat("txErrors"),
    rxDropped: t.exposeFloat("rxDropped"),
    txDropped: t.exposeFloat("txDropped"),
  }),
});

const NetworkAddressRef = builder.objectRef<NetworkAddress>("NetworkAddress");
builder.objectType(NetworkAddressRef, {
  fields: (t) => ({
    family: t.exposeString("family"),
    address: t.exposeString("address"),
    prefixlen: t.exposeInt("prefixlen"),
    scope: t.exposeString("scope"),
    broadcast: t.exposeString("broadcast", { nullable: true }),
  }),
});

const NetworkInterfaceRef = builder.objectRef<NetworkInterface>("NetworkInterface");
builder.objectType(NetworkInterfaceRef, {
  fields: (t) => ({
    name: t.exposeString("name"),
    operstate: t.exposeString("operstate"),
    carrier: t.exposeBoolean("carrier"),
    speed: t.exposeInt("speed", { nullable: true }),
    duplex: t.exposeString("duplex", { nullable: true }),
    mac: t.exposeString("mac"),
    mtu: t.exposeInt("mtu"),
    type: t.exposeInt("type"),
    flags: t.exposeStringList("flags"),
    addresses: t.field({
      type: [NetworkAddressRef],
      resolve: (iface) => iface.addresses,
    }),
    statistics: t.field({
      type: NetworkInterfaceStatsRef,
      resolve: (iface) => iface.statistics,
    }),
  }),
});

const NetworkStateRef = builder.objectRef<NetworkStateMessage>("NetworkState");
builder.objectType(NetworkStateRef, {
  fields: (t) => ({
    moduleId: t.exposeString("moduleId"),
    timestamp: t.field({
      type: "DateTime",
      resolve: (s) => new Date(s.timestamp),
    }),
    interfaces: t.field({
      type: [NetworkInterfaceRef],
      resolve: (s) => s.interfaces,
    }),
  }),
});

const NetworkInterfaceConfigRef = builder.objectRef<NetworkInterfaceConfig>("NetworkInterfaceConfig");
builder.objectType(NetworkInterfaceConfigRef, {
  fields: (t) => ({
    interfaceName: t.exposeString("interfaceName"),
    dhcp4: t.exposeBoolean("dhcp4", { nullable: true }),
    addresses: t.exposeStringList("addresses", { nullable: true }),
    gateway4: t.exposeString("gateway4", { nullable: true }),
    nameservers: t.exposeStringList("nameservers", { nullable: true }),
    mtu: t.exposeInt("mtu", { nullable: true }),
  }),
});

const NetworkCommandResultRef = builder.objectRef<NetworkCommandResponse>("NetworkCommandResult");
builder.objectType(NetworkCommandResultRef, {
  fields: (t) => ({
    requestId: t.exposeString("requestId"),
    success: t.exposeBoolean("success"),
    error: t.exposeString("error", { nullable: true }),
    timestamp: t.field({
      type: "DateTime",
      resolve: (r) => new Date(r.timestamp),
    }),
  }),
});

// Input type for applying network config
const NetworkInterfaceConfigInputRef = builder.inputType("NetworkInterfaceConfigInput", {
  fields: (t) => ({
    interfaceName: t.string({ required: true }),
    dhcp4: t.boolean({ required: false }),
    addresses: t.stringList({ required: false }),
    gateway4: t.string({ required: false }),
    nameservers: t.stringList({ required: false }),
    mtu: t.int({ required: false }),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// Nftables Types
// ═══════════════════════════════════════════════════════════════════════════

import type {
  NatRule,
  NftablesConfig,
  NftablesCommandResponse,
} from "@tentacle/nats-schema";

const NatRuleRef = builder.objectRef<NatRule>("NatRule");
builder.objectType(NatRuleRef, {
  fields: (t) => ({
    id: t.exposeString("id"),
    enabled: t.exposeBoolean("enabled"),
    protocol: t.exposeString("protocol"),
    connectingDevices: t.exposeString("connectingDevices"),
    incomingInterface: t.exposeString("incomingInterface"),
    outgoingInterface: t.exposeString("outgoingInterface"),
    natAddr: t.exposeString("natAddr"),
    originalPort: t.exposeString("originalPort"),
    translatedPort: t.exposeString("translatedPort"),
    deviceAddr: t.exposeString("deviceAddr"),
    deviceName: t.exposeString("deviceName"),
    doubleNat: t.exposeBoolean("doubleNat"),
    doubleNatAddr: t.exposeString("doubleNatAddr"),
    comment: t.exposeString("comment"),
  }),
});

const NftablesConfigRef = builder.objectRef<NftablesConfig>("NftablesConfig");
builder.objectType(NftablesConfigRef, {
  fields: (t) => ({
    natRules: t.field({
      type: [NatRuleRef],
      resolve: (c) => c.natRules,
    }),
  }),
});

const NftablesCommandResultRef = builder.objectRef<NftablesCommandResponse>("NftablesCommandResult");
builder.objectType(NftablesCommandResultRef, {
  fields: (t) => ({
    requestId: t.exposeString("requestId"),
    success: t.exposeBoolean("success"),
    error: t.exposeString("error", { nullable: true }),
    timestamp: t.field({
      type: "DateTime",
      resolve: (r) => new Date(r.timestamp),
    }),
  }),
});

const NatRuleInputRef = builder.inputType("NatRuleInput", {
  fields: (t) => ({
    id: t.string({ required: true }),
    enabled: t.boolean({ required: true }),
    protocol: t.string({ required: true }),
    connectingDevices: t.string({ required: true }),
    incomingInterface: t.string({ required: true }),
    outgoingInterface: t.string({ required: true }),
    natAddr: t.string({ required: true }),
    originalPort: t.string({ required: true }),
    translatedPort: t.string({ required: true }),
    deviceAddr: t.string({ required: true }),
    deviceName: t.string({ required: true }),
    doubleNat: t.boolean({ required: true }),
    doubleNatAddr: t.string({ required: true }),
    comment: t.string({ required: true }),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// MQTT Metrics Types
// ═══════════════════════════════════════════════════════════════════════════

import type { MqttMetricInfo, MqttTemplateInfo, MqttMetricsResponse, UdtMemberDefinition } from "@tentacle/nats-schema";

const MqttMetricInfoRef = builder.objectRef<MqttMetricInfo & { timestamp?: number | null }>("MqttMetricInfo");
builder.objectType(MqttMetricInfoRef, {
  fields: (t) => ({
    name: t.exposeString("name"),
    sparkplugType: t.exposeString("sparkplugType"),
    value: t.field({
      type: "JSON",
      nullable: true,
      resolve: (m) => m.value,
    }),
    moduleId: t.exposeString("moduleId"),
    datatype: t.exposeString("datatype"),
    templateRef: t.exposeString("templateRef", { nullable: true }),
    timestamp: t.field({
      type: "Float",
      nullable: true,
      resolve: (m) => (m as MqttMetricInfo & { timestamp?: number | null }).timestamp ?? null,
    }),
  }),
});

const MqttTemplateMemberRef = builder.objectRef<UdtMemberDefinition>("MqttTemplateMember");
builder.objectType(MqttTemplateMemberRef, {
  fields: (t) => ({
    name: t.exposeString("name"),
    datatype: t.exposeString("datatype"),
    description: t.exposeString("description", { nullable: true }),
    templateRef: t.exposeString("templateRef", { nullable: true }),
    isArray: t.exposeBoolean("isArray", { nullable: true }),
  }),
});

const MqttTemplateInfoRef = builder.objectRef<MqttTemplateInfo>("MqttTemplateInfo");
builder.objectType(MqttTemplateInfoRef, {
  fields: (t) => ({
    name: t.exposeString("name"),
    version: t.exposeString("version", { nullable: true }),
    members: t.field({
      type: [MqttTemplateMemberRef],
      resolve: (tmpl) => tmpl.members,
    }),
  }),
});

const MqttMetricsResponseRef = builder.objectRef<MqttMetricsResponse>("MqttMetricsResponse");
builder.objectType(MqttMetricsResponseRef, {
  fields: (t) => ({
    metrics: t.field({
      type: [MqttMetricInfoRef],
      resolve: (r) => r.metrics,
    }),
    templates: t.field({
      type: [MqttTemplateInfoRef],
      resolve: (r) => r.templates,
    }),
    deviceId: t.exposeString("deviceId"),
    timestamp: t.field({
      type: "DateTime",
      resolve: (r) => new Date(r.timestamp),
    }),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// History Types
// ═══════════════════════════════════════════════════════════════════════════

import type { HistoryPoint, VariableHistory } from "../modules/history.ts";

const HistoryPointRef = builder.objectRef<HistoryPoint>("HistoryPoint");
builder.objectType(HistoryPointRef, {
  fields: (t) => ({
    value: t.exposeString("value", { nullable: true }),
    timestamp: t.field({
      type: "DateTime",
      resolve: (p) => new Date(p.timestamp),
    }),
  }),
});

const VariableHistoryRef = builder.objectRef<VariableHistory>("VariableHistory");
builder.objectType(VariableHistoryRef, {
  fields: (t) => ({
    moduleId: t.exposeString("moduleId"),
    variableId: t.exposeString("variableId"),
    history: t.field({
      type: [HistoryPointRef],
      resolve: (h) => h.history,
    }),
  }),
});

const VariableHistoryInputRef = builder.inputType("VariableHistoryInput", {
  fields: (t) => ({
    moduleId: t.string({ required: true }),
    variableId: t.string({ required: true }),
  }),
});

type UsageMonth = { year: number; month: number; count: number };
type UsageStats = { totalCount: number; byMonth: UsageMonth[] };

const UsageMonthRef = builder.objectRef<UsageMonth>("UsageMonth");
builder.objectType(UsageMonthRef, {
  fields: (t) => ({
    year: t.exposeInt("year"),
    month: t.exposeInt("month"),
    count: t.exposeInt("count"),
  }),
});

const UsageStatsRef = builder.objectRef<UsageStats>("UsageStats");
builder.objectType(UsageStatsRef, {
  fields: (t) => ({
    totalCount: t.exposeInt("totalCount"),
    byMonth: t.field({
      type: [UsageMonthRef],
      resolve: (u) => u.byMonth,
    }),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// Store & Forward Types
// ═══════════════════════════════════════════════════════════════════════════

import type { StoreForwardStatus } from "../modules/store-forward.ts";

type TimelinePoint = { timestamp: number; state: string };

const TimelinePointRef = builder.objectRef<TimelinePoint>("TimelinePoint");
builder.objectType(TimelinePointRef, {
  fields: (t) => ({
    timestamp: t.field({ type: "DateTime", resolve: (p) => new Date(p.timestamp) }),
    state: t.exposeString("state"),
  }),
});

const StoreForwardStatusRef = builder.objectRef<StoreForwardStatus>("StoreForwardStatus");
builder.objectType(StoreForwardStatusRef, {
  fields: (t) => ({
    primaryHostId: t.exposeString("primaryHostId", { nullable: true }),
    primaryHostOnline: t.exposeBoolean("primaryHostOnline"),
    bufferedRecords: t.exposeInt("bufferedRecords"),
    bufferSizeBytes: t.exposeInt("bufferSizeBytes"),
    bufferCapacityRecords: t.exposeInt("bufferCapacityRecords"),
    bufferCapacityBytes: t.exposeInt("bufferCapacityBytes"),
    bufferUsedPercentRecords: t.exposeFloat("bufferUsedPercentRecords"),
    bufferUsedPercentBytes: t.exposeFloat("bufferUsedPercentBytes"),
    draining: t.exposeBoolean("draining"),
    drainProgress: t.exposeFloat("drainProgress"),
    drainRecordsRemaining: t.exposeInt("drainRecordsRemaining"),
    drainTotalRecords: t.exposeInt("drainTotalRecords"),
    drainEtaSeconds: t.exposeFloat("drainEtaSeconds"),
    drainStartedAt: t.field({
      type: "DateTime",
      nullable: true,
      resolve: (s) => s.drainStartedAt ? new Date(s.drainStartedAt) : null,
    }),
    totalBuffered: t.exposeInt("totalBuffered"),
    totalDrained: t.exposeInt("totalDrained"),
    totalEvicted: t.exposeInt("totalEvicted"),
    publishRate: t.exposeFloat("publishRate"),
    timeline: t.field({
      type: [TimelinePointRef],
      resolve: (s) => s.timeline,
    }),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// Service Config Types
// ═══════════════════════════════════════════════════════════════════════════

import type { ConfigEntry } from "../modules/service-config.ts";

const ConfigEntryRef = builder.objectRef<ConfigEntry>("ConfigEntry");
builder.objectType(ConfigEntryRef, {
  fields: (t) => ({
    key: t.exposeString("key"),
    envVar: t.exposeString("envVar"),
    value: t.exposeString("value"),
    moduleId: t.exposeString("moduleId"),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// Gateway Config Types
// ═══════════════════════════════════════════════════════════════════════════

import type {
  GatewayConfigKV,
  GatewayDeviceConfig,
  GatewayVariableConfig,
} from "@tentacle/nats-schema";

// Object types for gateway device configs (union via protocol field)
const GatewayDeviceRef = builder.objectRef<GatewayDeviceConfig & { _deviceId?: string }>("GatewayDevice");
builder.objectType(GatewayDeviceRef, {
  fields: (t) => ({
    deviceId: t.field({
      type: "String",
      resolve: (d) => d._deviceId ?? "",
    }),
    protocol: t.exposeString("protocol"),
    config: t.field({
      type: "JSON",
      description: "Protocol-specific connection config",
      resolve: (d) => {
        const { _deviceId: _, protocol: __, scanRate: _sr, deadband: _db, disableRBE: _rbe, templateNameOverrides: _tno, ...rest } = d as Record<string, unknown>;
        return rest;
      },
    }),
    scanRate: t.field({
      type: "Int",
      nullable: true,
      description: "Polling interval in ms",
      resolve: (d) => (d as any).scanRate ?? null,
    }),
    deadband: t.field({
      type: DeadBandConfigType,
      nullable: true,
      description: "Default deadband (RBE) config for all variables on this device",
      resolve: (d) => (d as any).deadband ?? null,
    }),
    disableRBE: t.field({
      type: "Boolean",
      nullable: true,
      description: "Disable RBE for all variables on this device",
      resolve: (d) => (d as any).disableRBE ?? null,
    }),
    templateNameOverrides: t.field({
      type: "JSON",
      nullable: true,
      description: "Map of original browse template name → overridden unique name",
      resolve: (d) => (d as any).templateNameOverrides ?? null,
    }),
  }),
});

const GatewayVariableRef = builder.objectRef<GatewayVariableConfig>("GatewayVariable");
builder.objectType(GatewayVariableRef, {
  fields: (t) => ({
    id: t.exposeString("id"),
    description: t.exposeString("description", { nullable: true }),
    datatype: t.exposeString("datatype"),
    default: t.field({
      type: "JSON",
      resolve: (v) => v.default,
    }),
    deviceId: t.exposeString("deviceId"),
    tag: t.exposeString("tag"),
    bidirectional: t.exposeBoolean("bidirectional", { nullable: true }),
    deadband: t.field({
      type: DeadBandConfigType,
      nullable: true,
      resolve: (v) => v.deadband || null,
    }),
    disableRBE: t.exposeBoolean("disableRBE", { nullable: true }),
  }),
});

// UDT output types for GatewayConfig
type GatewayUdtTemplateMemberShape = { name: string; datatype: string; templateRef?: string; defaultDeadband?: DeadBandConfig | null };
type GatewayUdtTemplateShape = { name: string; version?: string; members: GatewayUdtTemplateMemberShape[] };
type GatewayUdtVariableShape = { id: string; deviceId: string; tag: string; templateName: string; memberTags: Record<string, string>; memberCipTypes?: Record<string, string>; memberDeadbands?: Record<string, DeadBandConfig> | null };

const GatewayUdtTemplateMemberOutputRef = builder.objectRef<GatewayUdtTemplateMemberShape>("GatewayUdtTemplateMemberOutput");
builder.objectType(GatewayUdtTemplateMemberOutputRef, {
  fields: (t) => ({
    name: t.exposeString("name"),
    datatype: t.exposeString("datatype"),
    templateRef: t.exposeString("templateRef", { nullable: true }),
    defaultDeadband: t.field({
      type: DeadBandConfigType,
      nullable: true,
      resolve: (m) => m.defaultDeadband ?? null,
    }),
  }),
});

const GatewayUdtTemplateOutputRef = builder.objectRef<GatewayUdtTemplateShape>("GatewayUdtTemplateOutput");
builder.objectType(GatewayUdtTemplateOutputRef, {
  fields: (t) => ({
    name: t.exposeString("name"),
    version: t.exposeString("version", { nullable: true }),
    members: t.field({
      type: [GatewayUdtTemplateMemberOutputRef],
      resolve: (tmpl) => tmpl.members,
    }),
  }),
});

const GatewayUdtVariableOutputRef = builder.objectRef<GatewayUdtVariableShape>("GatewayUdtVariableOutput");
builder.objectType(GatewayUdtVariableOutputRef, {
  fields: (t) => ({
    id: t.exposeString("id"),
    deviceId: t.exposeString("deviceId"),
    tag: t.exposeString("tag"),
    templateName: t.exposeString("templateName"),
    memberTags: t.field({ type: "JSON", resolve: (v) => v.memberTags }),
    memberCipTypes: t.field({ type: "JSON", nullable: true, resolve: (v) => v.memberCipTypes ?? null }),
    memberDeadbands: t.field({ type: "JSON", nullable: true, resolve: (v) => v.memberDeadbands ?? null }),
  }),
});

const GatewayConfigRef = builder.objectRef<GatewayConfigKV>("GatewayConfig");
builder.objectType(GatewayConfigRef, {
  fields: (t) => ({
    gatewayId: t.exposeString("gatewayId"),
    devices: t.field({
      type: [GatewayDeviceRef],
      resolve: (c) =>
        Object.entries(c.devices).map(([id, dev]) => ({
          ...dev,
          _deviceId: id,
        })),
    }),
    variables: t.field({
      type: [GatewayVariableRef],
      resolve: (c) => Object.values(c.variables),
    }),
    udtTemplates: t.field({
      type: [GatewayUdtTemplateOutputRef],
      resolve: (c) => Object.values((c as any).udtTemplates ?? {}),
    }),
    udtVariables: t.field({
      type: [GatewayUdtVariableOutputRef],
      resolve: (c) => Object.values((c as any).udtVariables ?? {}),
    }),
    updatedAt: t.field({
      type: "DateTime",
      resolve: (c) => new Date(c.updatedAt),
    }),
    availableProtocols: t.field({
      type: ["String"],
      description: "Protocol types that have an active service module connected",
      resolve: async () => {
        const protocolServiceTypes = new Set(["ethernetip", "opcua", "snmp", "modbus"]);
        const heartbeats = await getAllHeartbeats();
        return heartbeats
          .map((hb) => hb.serviceType)
          .filter((st) => protocolServiceTypes.has(st));
      },
    }),
  }),
});

// Input types for gateway mutations
const GatewayDeviceInputRef = builder.inputType("GatewayDeviceInput", {
  fields: (t) => ({
    deviceId: t.string({ required: true }),
    protocol: t.string({ required: true }),
    host: t.string({ required: false }),
    port: t.int({ required: false }),
    endpointUrl: t.string({ required: false }),
    version: t.string({ required: false }),
    community: t.string({ required: false }),
    unitId: t.int({ required: false }),
    scanRate: t.int({ required: false }),
    deadband: t.field({ type: DeadBandConfigInputRef, required: false }),
    disableRBE: t.boolean({ required: false }),
  }),
});

const DeadBandConfigInputRef = builder.inputType("DeadBandConfigInput", {
  fields: (t) => ({
    value: t.float({ required: true }),
    minTime: t.int({ required: false }),
    maxTime: t.int({ required: false }),
  }),
});

const GatewayVariableInputRef = builder.inputType("GatewayVariableInput", {
  fields: (t) => ({
    id: t.string({ required: true }),
    description: t.string({ required: false }),
    datatype: t.string({ required: true }),
    default: t.field({ type: "JSON", required: false }),
    deviceId: t.string({ required: true }),
    tag: t.string({ required: true }),
    cipType: t.string({ required: false }),
    bidirectional: t.boolean({ required: false }),
    deadband: t.field({ type: DeadBandConfigInputRef, required: false }),
    disableRBE: t.boolean({ required: false }),
    functionCode: t.int({ required: false }),
    modbusDatatype: t.string({ required: false }),
    byteOrder: t.string({ required: false }),
    address: t.int({ required: false }),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// Gateway Browse Types
// ═══════════════════════════════════════════════════════════════════════════

import type { GatewayBrowseItem, GatewayBrowseResult, GatewayBrowseUdt, GatewayBrowseUdtMember } from "../nats/client.ts";

const GatewayBrowseItemRef = builder.objectRef<GatewayBrowseItem>("GatewayBrowseItem");
builder.objectType(GatewayBrowseItemRef, {
  fields: (t) => ({
    tag: t.exposeString("tag"),
    name: t.exposeString("name"),
    datatype: t.exposeString("datatype"),
    value: t.field({
      type: "JSON",
      nullable: true,
      resolve: (item) => item.value,
    }),
    protocolType: t.exposeString("protocolType"),
  }),
});

const GatewayBrowseUdtMemberRef = builder.objectRef<GatewayBrowseUdtMember>("GatewayBrowseUdtMember");
builder.objectType(GatewayBrowseUdtMemberRef, {
  fields: (t) => ({
    name: t.exposeString("name"),
    datatype: t.exposeString("datatype"),
    cipType: t.exposeString("cipType"),
    udtType: t.exposeString("udtType"),
    isArray: t.exposeBoolean("isArray"),
  }),
});

const GatewayBrowseUdtRef = builder.objectRef<GatewayBrowseUdt>("GatewayBrowseUdt");
builder.objectType(GatewayBrowseUdtRef, {
  fields: (t) => ({
    name: t.exposeString("name"),
    members: t.field({
      type: [GatewayBrowseUdtMemberRef],
      resolve: (udt) => udt.members,
    }),
  }),
});

const GatewayBrowseResultRef = builder.objectRef<GatewayBrowseResult & { cachedAt?: number }>("GatewayBrowseResult");
builder.objectType(GatewayBrowseResultRef, {
  fields: (t) => ({
    deviceId: t.exposeString("deviceId"),
    protocol: t.exposeString("protocol"),
    items: t.field({
      type: [GatewayBrowseItemRef],
      resolve: (r) => r.items,
    }),
    udts: t.field({
      type: [GatewayBrowseUdtRef],
      resolve: (r) => r.udts,
    }),
    structTags: t.field({
      type: "JSON",
      resolve: (r) => r.structTags,
    }),
    cachedAt: t.field({
      type: "DateTime",
      nullable: true,
      description: "When this result was cached (null if fresh browse)",
      resolve: (r) => r.cachedAt ? new Date(r.cachedAt) : null,
    }),
  }),
});

const GatewayBrowseInputRef = builder.inputType("GatewayBrowseInput", {
  fields: (t) => ({
    deviceId: t.string({ required: true }),
    protocol: t.string({ required: true }),
    host: t.string({ required: false }),
    port: t.int({ required: false }),
    endpointUrl: t.string({ required: false }),
    version: t.string({ required: false }),
    community: t.string({ required: false }),
    rootOid: t.string({ required: false }),
    browseId: t.string({ required: false, description: "Client-generated browse ID for progress tracking" }),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// Gateway Import Input Types (type-centric import)
// ═══════════════════════════════════════════════════════════════════════════

const GatewayUdtTemplateMemberInputRef = builder.inputType("GatewayUdtTemplateMemberInput", {
  fields: (t) => ({
    name: t.string({ required: true }),
    datatype: t.string({ required: true }),
    templateRef: t.string({ required: false }),
    defaultDeadband: t.field({ type: DeadBandConfigInputRef, required: false }),
  }),
});

const GatewayUdtTemplateInputRef = builder.inputType("GatewayUdtTemplateInput", {
  fields: (t) => ({
    name: t.string({ required: true }),
    version: t.string({ required: false }),
    members: t.field({ type: [GatewayUdtTemplateMemberInputRef], required: true }),
  }),
});

const GatewayUdtVariableInputRef = builder.inputType("GatewayUdtVariableInput", {
  fields: (t) => ({
    id: t.string({ required: true }),
    deviceId: t.string({ required: true }),
    tag: t.string({ required: true }),
    templateName: t.string({ required: true }),
    memberTags: t.field({ type: "JSON", required: true }),
    memberCipTypes: t.field({ type: "JSON", required: false }),
    memberDeadbands: t.field({ type: "JSON", required: false }),
  }),
});

// Gateway browse progress — normalized from protocol-specific formats
type GatewayBrowseProgressShape = {
  browseId: string;
  deviceId: string;
  phase: string;
  discoveredCount: number;
  totalCount: number;
  message: string;
  timestamp: number;
};

const GatewayBrowseProgressRef = builder.objectRef<GatewayBrowseProgressShape>("GatewayBrowseProgress");
builder.objectType(GatewayBrowseProgressRef, {
  fields: (t) => ({
    browseId: t.exposeString("browseId"),
    deviceId: t.exposeString("deviceId"),
    phase: t.exposeString("phase"),
    discoveredCount: t.exposeInt("discoveredCount"),
    totalCount: t.exposeInt("totalCount"),
    message: t.exposeString("message"),
    timestamp: t.field({
      type: "DateTime",
      resolve: (p) => new Date(p.timestamp),
    }),
  }),
});

// Gateway browse state — singleton persistent state per device
import type { GatewayBrowseState } from "../modules/gateway.ts";

const GatewayBrowseStateRef = builder.objectRef<GatewayBrowseState>("GatewayBrowseState");
builder.objectType(GatewayBrowseStateRef, {
  fields: (t) => ({
    deviceId: t.exposeString("deviceId"),
    browseId: t.exposeString("browseId"),
    protocol: t.exposeString("protocol"),
    status: t.exposeString("status"),
    phase: t.exposeString("phase"),
    discoveredCount: t.exposeInt("discoveredCount"),
    totalCount: t.exposeInt("totalCount"),
    message: t.exposeString("message"),
    startedAt: t.field({
      type: "DateTime",
      resolve: (s) => new Date(s.startedAt),
    }),
    updatedAt: t.field({
      type: "DateTime",
      resolve: (s) => new Date(s.updatedAt),
    }),
    error: t.exposeString("error", { nullable: true }),
  }),
});

// Result from startGatewayBrowse mutation (non-blocking)
type StartBrowseResultShape = { browseId: string; deviceId: string };

const StartBrowseResultRef = builder.objectRef<StartBrowseResultShape>("StartBrowseResult");
builder.objectType(StartBrowseResultRef, {
  fields: (t) => ({
    browseId: t.exposeString("browseId"),
    deviceId: t.exposeString("deviceId"),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// Orchestrator Types
// ═══════════════════════════════════════════════════════════════════════════

import type { DesiredServiceKV, ServiceStatusKV, ModuleRegistryInfo, ModuleVersionInfo } from "@tentacle/nats-schema";

// ═══════════════════════════════════════════════════════════════════════════
// Module Registry & Version Types
// ═══════════════════════════════════════════════════════════════════════════

const ModuleRegistryInfoRef = builder.objectRef<ModuleRegistryInfo>("ModuleRegistryInfo");
builder.objectType(ModuleRegistryInfoRef, {
  fields: (t) => ({
    moduleId: t.exposeString("moduleId"),
    repo: t.exposeString("repo"),
    description: t.exposeString("description"),
    category: t.exposeString("category"),
    runtime: t.exposeString("runtime"),
  }),
});

const ModuleVersionInfoRef = builder.objectRef<ModuleVersionInfo>("ModuleVersionInfo");
builder.objectType(ModuleVersionInfoRef, {
  fields: (t) => ({
    moduleId: t.exposeString("moduleId"),
    installedVersions: t.exposeStringList("installedVersions"),
    latestVersion: t.exposeString("latestVersion", { nullable: true }),
    activeVersion: t.exposeString("activeVersion", { nullable: true }),
  }),
});

const DesiredServiceRef = builder.objectRef<DesiredServiceKV>("DesiredService");
builder.objectType(DesiredServiceRef, {
  fields: (t) => ({
    moduleId: t.exposeString("moduleId"),
    version: t.exposeString("version"),
    running: t.exposeBoolean("running"),
    updatedAt: t.field({
      type: "DateTime",
      resolve: (d) => new Date(d.updatedAt),
    }),
  }),
});

const ServiceStatusRef = builder.objectRef<ServiceStatusKV>("ServiceStatus");
builder.objectType(ServiceStatusRef, {
  fields: (t) => ({
    moduleId: t.exposeString("moduleId"),
    installedVersions: t.exposeStringList("installedVersions"),
    activeVersion: t.exposeString("activeVersion", { nullable: true }),
    systemdState: t.exposeString("systemdState"),
    reconcileState: t.exposeString("reconcileState"),
    lastError: t.exposeString("lastError", { nullable: true }),
    runtime: t.exposeString("runtime"),
    category: t.exposeString("category"),
    repo: t.exposeString("repo"),
    updatedAt: t.field({
      type: "DateTime",
      resolve: (s) => new Date(s.updatedAt),
    }),
  }),
});

export {
  ModuleRegistryInfoRef,
  ModuleVersionInfoRef,
  DesiredServiceRef,
  ServiceStatusRef,
  ConfigEntryRef,
  LogEntryRef,
  NatsTrafficEntryRef,
  NetworkStateRef,
  NetworkInterfaceConfigRef,
  NetworkCommandResultRef,
  NetworkInterfaceConfigInputRef,
  NftablesConfigRef,
  NftablesCommandResultRef,
  NatRuleInputRef,
  MqttMetricsResponseRef,
  VariableHistoryRef,
  VariableHistoryInputRef,
  UsageStatsRef,
  StoreForwardStatusRef,
  GatewayConfigRef,
  GatewayDeviceInputRef,
  GatewayVariableInputRef,
  GatewayBrowseResultRef,
  GatewayBrowseInputRef,
  GatewayBrowseProgressRef,
  GatewayBrowseStateRef,
  StartBrowseResultRef,
  GatewayUdtTemplateInputRef,
  GatewayUdtVariableInputRef,
};
