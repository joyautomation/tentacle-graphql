import { builder } from "./builder.ts";
import { publishCommand, getVariable, browseTags, subscribeTags, unsubscribeTags } from "../nats/client.ts";
import {
  upsertDevice,
  deleteDevice,
  upsertTag,
  deleteTag,
  setDeviceTags,
} from "../nats/config.ts";
import {
  setMqttDefaults,
  setMqttVariable,
  enableMqttVariables,
  disableMqttVariables,
} from "../nats/mqtt.ts";
import {
  DeviceInputRef,
  TagInputRef,
  DeadBandInputRef,
  MqttVariableConfigInputRef,
  MqttVariableConfigRef,
  MqttDefaultsRef,
  type DeviceInputShape,
  type TagInputShape,
} from "./types.ts";

builder.mutationType({
  fields: (t) => ({
    // Update a single variable
    updateVariable: t.field({
      type: "Variable",
      args: {
        projectId: t.arg.string({ required: true }),
        variableId: t.arg.string({ required: true }),
        value: t.arg.string({ required: true }), // Accept as JSON string
      },
      resolve: async (_root, args) => {
        // Parse the JSON value
        let parsedValue: unknown;
        try {
          parsedValue = JSON.parse(args.value);
        } catch {
          parsedValue = args.value; // Use as-is if not valid JSON
        }

        // Publish command to NATS
        await publishCommand(args.projectId, args.variableId, parsedValue);

        // Wait briefly for update to propagate
        await new Promise((resolve) => setTimeout(resolve, 100));

        // Fetch and return updated variable
        const updated = await getVariable(args.projectId, args.variableId);

        if (!updated) {
          throw new Error(
            `Variable ${args.variableId} not found after update. Does the variable exist?`,
          );
        }

        return updated;
      },
    }),

    // Create or update a device
    upsertDevice: t.field({
      type: "Device",
      args: {
        projectId: t.arg.string({ required: true }),
        deviceId: t.arg.string({ required: true }),
        input: t.arg({ type: DeviceInputRef, required: true }),
      },
      resolve: async (_root, args) => {
        return await upsertDevice(args.projectId, args.deviceId, args.input);
      },
    }),

    // Delete a device
    deleteDevice: t.field({
      type: "Boolean",
      args: {
        projectId: t.arg.string({ required: true }),
        deviceId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        return await deleteDevice(args.projectId, args.deviceId);
      },
    }),

    // Create or update a tag
    upsertTag: t.field({
      type: "Tag",
      args: {
        projectId: t.arg.string({ required: true }),
        deviceId: t.arg.string({ required: true }),
        tagId: t.arg.string({ required: true }),
        input: t.arg({ type: TagInputRef, required: true }),
      },
      resolve: async (_root, args) => {
        return await upsertTag(args.projectId, args.deviceId, args.tagId, args.input);
      },
    }),

    // Delete a tag
    deleteTag: t.field({
      type: "Boolean",
      args: {
        projectId: t.arg.string({ required: true }),
        deviceId: t.arg.string({ required: true }),
        tagId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        return await deleteTag(args.projectId, args.deviceId, args.tagId);
      },
    }),

    // Bulk set tags for a device (replaces all existing tags)
    setDeviceTags: t.field({
      type: ["String"],
      args: {
        projectId: t.arg.string({ required: true }),
        deviceId: t.arg.string({ required: true }),
        tags: t.arg.stringList({ required: true }),
      },
      resolve: async (_root, args) => {
        return await setDeviceTags(args.projectId, args.deviceId, args.tags);
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // MQTT Configuration Mutations
    // ═══════════════════════════════════════════════════════════════════════

    // Set MQTT defaults (global deadband settings)
    setMqttDefaults: t.field({
      type: MqttDefaultsRef,
      args: {
        projectId: t.arg.string({ required: true }),
        deadband: t.arg({ type: DeadBandInputRef, required: true }),
      },
      resolve: async (_root, args) => {
        const deadband = {
          value: args.deadband.value,
          maxTime: args.deadband.maxTime ?? undefined,
        };
        return await setMqttDefaults(args.projectId, deadband);
      },
    }),

    // Set config for a single MQTT variable (granular edit)
    setMqttVariable: t.field({
      type: MqttVariableConfigRef,
      args: {
        projectId: t.arg.string({ required: true }),
        variableId: t.arg.string({ required: true }),
        config: t.arg({ type: MqttVariableConfigInputRef, required: true }),
      },
      resolve: async (_root, args) => {
        const config = {
          enabled: args.config.enabled,
          deadband: args.config.deadband
            ? {
                value: args.config.deadband.value,
                maxTime: args.config.deadband.maxTime ?? undefined,
              }
            : undefined,
        };
        return await setMqttVariable(args.projectId, args.variableId, config);
      },
    }),

    // Bulk enable variables for MQTT
    enableMqttVariables: t.field({
      type: ["String"],
      args: {
        projectId: t.arg.string({ required: true }),
        variableIds: t.arg.stringList({ required: true }),
        config: t.arg({ type: MqttVariableConfigInputRef, required: false }),
      },
      resolve: async (_root, args) => {
        const config = args.config
          ? {
              enabled: args.config.enabled,
              deadband: args.config.deadband
                ? {
                    value: args.config.deadband.value,
                    maxTime: args.config.deadband.maxTime ?? undefined,
                  }
                : undefined,
            }
          : undefined;
        return await enableMqttVariables(args.projectId, args.variableIds, config);
      },
    }),

    // Bulk disable variables for MQTT
    disableMqttVariables: t.field({
      type: ["String"],
      args: {
        projectId: t.arg.string({ required: true }),
        variableIds: t.arg.stringList({ required: true }),
      },
      resolve: async (_root, args) => {
        return await disableMqttVariables(args.projectId, args.variableIds);
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Scanner Control Mutations
    // ═══════════════════════════════════════════════════════════════════════

    // Trigger a browse operation to discover available PLC tags
    browseTags: t.field({
      type: ["Variable"],
      args: {
        projectId: t.arg.string({ required: true }),
        plcId: t.arg.string({ required: false }), // Optional: browse specific PLC
      },
      resolve: async (_root, args) => {
        const variables = await browseTags(args.projectId, args.plcId ?? undefined);
        return variables;
      },
    }),

    // Subscribe tags to be polled by the scanner
    subscribeToTags: t.field({
      type: "Boolean",
      args: {
        projectId: t.arg.string({ required: true }),
        tags: t.arg.stringList({ required: true }),
        subscriberId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        const result = await subscribeTags(args.projectId, args.tags, args.subscriberId);
        return result.success;
      },
    }),

    // Unsubscribe tags from polling
    unsubscribeFromTags: t.field({
      type: "Boolean",
      args: {
        projectId: t.arg.string({ required: true }),
        tags: t.arg.stringList({ required: true }),
        subscriberId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        const result = await unsubscribeTags(args.projectId, args.tags, args.subscriberId);
        return result.success;
      },
    }),
  }),
});
