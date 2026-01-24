import { builder } from "./builder.ts";
import type { PlcVariableKV, DeadBandConfig } from "@tentacle/nats-schema";

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
    projectId: t.exposeString("projectId"),
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
    source: t.exposeString("source"),
    quality: t.exposeString("quality"),
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

// Device object type for EtherNet/IP PLCs
builder.objectType("Device", {
  fields: (t) => ({
    id: t.exposeString("id"),
    projectId: t.exposeString("projectId"),
    host: t.exposeString("host"),
    port: t.exposeInt("port"),
    type: t.exposeString("type"),
    slot: t.exposeInt("slot", { nullable: true }),
    scanRate: t.exposeInt("scanRate"),
    enabled: t.exposeBoolean("enabled"),
  }),
});

// Tag object type
builder.objectType("Tag", {
  fields: (t) => ({
    id: t.exposeString("id"),
    deviceId: t.exposeString("deviceId"),
    address: t.exposeString("address"),
    datatype: t.exposeString("datatype", { nullable: true }),
    writable: t.exposeBoolean("writable"),
    deadbandValue: t.exposeFloat("deadbandValue", { nullable: true }),
    deadbandMaxTime: t.exposeInt("deadbandMaxTime", { nullable: true }),
    disableRBE: t.exposeBoolean("disableRBE", { nullable: true }),
  }),
});

// Input type shapes for TypeScript
export interface DeviceInputShape {
  host: string;
  port?: number;
  type: string;
  slot?: number;
  scanRate?: number;
  enabled?: boolean;
}

export interface TagInputShape {
  address: string;
  datatype?: string;
  writable?: boolean;
  deadbandValue?: number;
  deadbandMaxTime?: number;
  disableRBE?: boolean;
}

// DeviceInput for creating/updating devices
export const DeviceInputRef = builder.inputRef<DeviceInputShape>("DeviceInput");
builder.inputType(DeviceInputRef, {
  fields: (t) => ({
    host: t.string({ required: true }),
    port: t.int({ required: false, defaultValue: 44818 }),
    type: t.string({ required: true }), // "rockwell" or "generic-cip"
    slot: t.int({ required: false }),
    scanRate: t.int({ required: false, defaultValue: 1000 }),
    enabled: t.boolean({ required: false, defaultValue: true }),
  }),
});

// TagInput for creating/updating tags
export const TagInputRef = builder.inputRef<TagInputShape>("TagInput");
builder.inputType(TagInputRef, {
  fields: (t) => ({
    address: t.string({ required: true }),
    datatype: t.string({ required: false }),
    writable: t.boolean({ required: false, defaultValue: false }),
    deadbandValue: t.float({ required: false }),
    deadbandMaxTime: t.int({ required: false }),
    disableRBE: t.boolean({ required: false }),
  }),
});

// ═══════════════════════════════════════════════════════════════════════════
// MQTT Configuration Types
// ═══════════════════════════════════════════════════════════════════════════

import type {
  MqttVariableConfig,
  MqttDefaults,
  MqttProjectConfig,
} from "../nats/mqtt.ts";

// Type shapes for MQTT config
type MqttVariableEntry = MqttVariableConfig & { variableId: string };

// Object refs (must be created before objectType)
const MqttVariableConfigRef = builder.objectRef<MqttVariableConfig>("MqttVariableConfig");
const MqttDefaultsRef = builder.objectRef<MqttDefaults>("MqttDefaults");
const MqttVariableEntryRef = builder.objectRef<MqttVariableEntry>("MqttVariableEntry");
const MqttProjectConfigRef = builder.objectRef<MqttProjectConfig>("MqttProjectConfig");

// MqttVariableConfig object type
builder.objectType(MqttVariableConfigRef, {
  fields: (t) => ({
    enabled: t.exposeBoolean("enabled"),
    deadband: t.field({
      type: DeadBandConfigType,
      nullable: true,
      resolve: (v) => v.deadband || null,
    }),
  }),
});

// MqttDefaults object type
builder.objectType(MqttDefaultsRef, {
  fields: (t) => ({
    deadband: t.field({
      type: DeadBandConfigType,
      resolve: (v) => v.deadband,
    }),
  }),
});

// MqttVariableEntry for returning variable config with its ID
builder.objectType(MqttVariableEntryRef, {
  fields: (t) => ({
    variableId: t.exposeString("variableId"),
    enabled: t.exposeBoolean("enabled"),
    deadband: t.field({
      type: DeadBandConfigType,
      nullable: true,
      resolve: (v) => v.deadband || null,
    }),
  }),
});

// MqttProjectConfig object type
builder.objectType(MqttProjectConfigRef, {
  fields: (t) => ({
    defaults: t.field({
      type: MqttDefaultsRef,
      resolve: (v) => v.defaults,
    }),
    variables: t.field({
      type: [MqttVariableEntryRef],
      resolve: (v) =>
        Object.entries(v.variables).map(([variableId, config]) => ({
          variableId,
          ...config,
        })),
    }),
    enabledCount: t.field({
      type: "Int",
      resolve: (v) =>
        Object.values(v.variables).filter((c) => c.enabled).length,
    }),
  }),
});

// Export refs for use in mutations/queries
export { MqttVariableConfigRef, MqttDefaultsRef, MqttProjectConfigRef };

// Input types for MQTT config
export interface DeadBandInputShape {
  value: number;
  maxTime?: number | null;
}

export const DeadBandInputRef = builder.inputRef<DeadBandInputShape>("DeadBandInput");
builder.inputType(DeadBandInputRef, {
  fields: (t) => ({
    value: t.float({ required: true }),
    maxTime: t.int({ required: false }),
  }),
});

export interface MqttVariableConfigInputShape {
  enabled: boolean;
  deadband?: DeadBandInputShape | null;
}

export const MqttVariableConfigInputRef = builder.inputRef<MqttVariableConfigInputShape>(
  "MqttVariableConfigInput",
);
builder.inputType(MqttVariableConfigInputRef, {
  fields: (t) => ({
    enabled: t.boolean({ required: true }),
    deadband: t.field({ type: DeadBandInputRef, required: false }),
  }),
});
