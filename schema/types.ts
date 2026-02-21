import { builder } from "./builder.ts";
import type { PlcVariableKV, DeadBandConfig, ServiceHeartbeat, BrowseProgressMessage } from "@tentacle/nats-schema";

// Service object type (represents a tentacle service instance)
const ServiceRef = builder.objectRef<ServiceHeartbeat>("Service");
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
  }),
});

// DeadBandConfig object type
const DeadBandConfigType = builder.objectRef<DeadBandConfig>(
  "DeadBandConfig",
);
builder.objectType(DeadBandConfigType, {
  fields: (t) => ({
    value: t.exposeFloat("value"),
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

export {
  LogEntryRef,
  NatsTrafficEntryRef,
  NetworkStateRef,
  NetworkInterfaceConfigRef,
  NetworkCommandResultRef,
  NetworkInterfaceConfigInputRef,
  NftablesConfigRef,
  NftablesCommandResultRef,
  NatRuleInputRef,
};
