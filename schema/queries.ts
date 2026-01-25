import { builder } from "./builder.ts";
import {
  getProjectsWithInfo,
  listVariables,
  getVariable,
} from "../nats/client.ts";
import {
  listDevices,
  getDevice,
  listTags,
} from "../nats/config.ts";
import { getMqttConfig } from "../nats/mqtt.ts";
import { MqttProjectConfigRef } from "./types.ts";

builder.queryType({
  fields: (t) => ({
    // List all available projects with activity info
    projects: t.field({
      type: ["Project"],
      resolve: async () => {
        return await getProjectsWithInfo();
      },
    }),

    // List variables for a project
    variables: t.field({
      type: ["Variable"],
      args: {
        projectId: t.arg.string({ required: true }),
      },
      resolve: async (_root: unknown, args: { projectId: string }) => {
        const variables = await listVariables(args.projectId);
        return variables;
      },
    }),

    // Get a specific variable
    variable: t.field({
      type: "Variable",
      nullable: true,
      args: {
        projectId: t.arg.string({ required: true }),
        variableId: t.arg.string({ required: true }),
      },
      resolve: async (_root: unknown, args: { projectId: string; variableId: string }) => {
        return await getVariable(args.projectId, args.variableId);
      },
    }),

    // List all devices for a project
    devices: t.field({
      type: ["Device"],
      args: {
        projectId: t.arg.string({ required: true }),
      },
      resolve: async (_root: unknown, args: { projectId: string }) => {
        return await listDevices(args.projectId);
      },
    }),

    // Get a specific device
    device: t.field({
      type: "Device",
      nullable: true,
      args: {
        projectId: t.arg.string({ required: true }),
        deviceId: t.arg.string({ required: true }),
      },
      resolve: async (_root: unknown, args: { projectId: string; deviceId: string }) => {
        return await getDevice(args.projectId, args.deviceId);
      },
    }),

    // List tags for a device
    tags: t.field({
      type: ["Tag"],
      args: {
        projectId: t.arg.string({ required: true }),
        deviceId: t.arg.string({ required: true }),
      },
      resolve: async (_root: unknown, args: { projectId: string; deviceId: string }) => {
        return await listTags(args.projectId, args.deviceId);
      },
    }),

    // Get MQTT configuration for a project
    mqttConfig: t.field({
      type: MqttProjectConfigRef,
      args: {
        projectId: t.arg.string({ required: true }),
      },
      resolve: async (_root: unknown, args: { projectId: string }) => {
        return await getMqttConfig(args.projectId);
      },
    }),
  }),
});
