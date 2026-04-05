import { builder } from "./builder.ts";
import { publishCommand, getVariable, browseTags, subscribeTags, unsubscribeTags, getNatsConnection, setServiceEnabled, getHeartbeat, browseGatewayDevice, startGatewayBrowse, setDesiredService, deleteDesiredService } from "../nats/client.ts";
import { updateConfigValue } from "../modules/service-config.ts";
import {
  ConfigEntryRef,
  BrowseResultRef,
  NetworkCommandResultRef,
  NetworkInterfaceConfigInputRef,
  NftablesCommandResultRef,
  NatRuleInputRef,
  GatewayConfigRef,
  GatewayDeviceInputRef,
  GatewayVariableInputRef,
  GatewayBrowseResultRef,
  GatewayBrowseInputRef,
  StartBrowseResultRef,
  GatewayUdtTemplateInputRef,
  GatewayUdtVariableInputRef,
  DesiredServiceRef,
} from "./types.ts";
import { applyNetworkConfig } from "../modules/network.ts";
import { applyNftablesConfig } from "../modules/nftables.ts";
import {
  setGatewayDevice,
  deleteGatewayDevice,
  setGatewayVariable,
  setGatewayVariables,
  deleteGatewayVariable,
  deleteGatewayVariables,
  deleteGatewayUdtVariable,
  deleteGatewayUdtVariables,
  importGatewayBrowse,
  syncGatewayDeviceVariables,
  updateGatewayUdtConfig,
  cacheBrowseResult,
  setTemplateNameOverrides,
} from "../modules/gateway.ts";
import type { NetworkInterfaceConfig, NatRule, GatewayDeviceConfig, GatewayVariableConfig, DeadBandConfig } from "@tentacle/nats-schema";

builder.mutationType({
  fields: (t) => ({
    // Update a single variable
    updateVariable: t.field({
      type: "Variable",
      args: {
        moduleId: t.arg.string({ required: true }),
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

        // Publish command to the module's command topic
        await publishCommand(args.moduleId, args.variableId, parsedValue);

        // Wait briefly for update to propagate
        await new Promise((resolve) => setTimeout(resolve, 100));

        // Fetch and return updated variable
        const updated = await getVariable(args.variableId);

        if (!updated) {
          throw new Error(
            `Variable ${args.variableId} not found after update. Does the variable exist?`,
          );
        }

        return updated;
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Service Enable/Disable
    // ═══════════════════════════════════════════════════════════════════════

    setServiceEnabled: t.field({
      type: "Service",
      args: {
        moduleId: t.arg.string({ required: true }),
        enabled: t.arg.boolean({ required: true }),
      },
      resolve: async (_root, args) => {
        const state = await setServiceEnabled(args.moduleId, args.enabled);

        // Wait briefly for the service to react to the KV change
        await new Promise((resolve) => setTimeout(resolve, 500));

        // Return the service heartbeat with the new enabled state
        const heartbeat = await getHeartbeat(args.moduleId);
        if (!heartbeat) {
          // Service might not be running but we still set the state
          return {
            serviceType: "graphql" as const,
            moduleId: args.moduleId,
            lastSeen: Date.now(),
            startedAt: Date.now(),
            _enabled: state.enabled,
          };
        }
        return { ...heartbeat, _enabled: state.enabled };
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Scanner Control Mutations
    // ═══════════════════════════════════════════════════════════════════════

    // Trigger a browse operation to discover available PLC tags
    browseTags: t.field({
      type: BrowseResultRef,
      args: {
        plcId: t.arg.string({ required: false }),
        async: t.arg.boolean({ required: false, defaultValue: false }),
      },
      resolve: async (_root, args) => {
        return await browseTags(args.plcId ?? undefined, args.async ?? false);
      },
    }),

    // Subscribe tags to be polled by the scanner
    subscribeToTags: t.field({
      type: "Boolean",
      args: {
        tags: t.arg.stringList({ required: true }),
        subscriberId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        const result = await subscribeTags(args.tags, args.subscriberId);
        return result.success;
      },
    }),

    // Unsubscribe tags from polling
    unsubscribeFromTags: t.field({
      type: "Boolean",
      args: {
        tags: t.arg.stringList({ required: true }),
        subscriberId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        const result = await unsubscribeTags(args.tags, args.subscriberId);
        return result.success;
      },
    }),

    // Apply network configuration via netplan
    applyNetworkConfig: t.field({
      type: NetworkCommandResultRef,
      args: {
        interfaces: t.arg({ type: [NetworkInterfaceConfigInputRef], required: true }),
      },
      resolve: async (_root, args) => {
        const nc = getNatsConnection();
        const configs: NetworkInterfaceConfig[] = args.interfaces.map((i) => ({
          interfaceName: i.interfaceName,
          dhcp4: i.dhcp4 ?? undefined,
          addresses: i.addresses ?? undefined,
          gateway4: i.gateway4 ?? undefined,
          nameservers: i.nameservers ?? undefined,
          mtu: i.mtu ?? undefined,
        }));
        return await applyNetworkConfig(nc, configs);
      },
    }),

    // Apply nftables NAT configuration
    applyNftablesConfig: t.field({
      type: NftablesCommandResultRef,
      args: {
        natRules: t.arg({ type: [NatRuleInputRef], required: true }),
      },
      resolve: async (_root, args) => {
        const nc = getNatsConnection();
        const natRules: NatRule[] = args.natRules.map((r) => ({
          id: r.id,
          enabled: r.enabled,
          protocol: r.protocol,
          connectingDevices: r.connectingDevices,
          incomingInterface: r.incomingInterface,
          outgoingInterface: r.outgoingInterface,
          natAddr: r.natAddr,
          originalPort: r.originalPort,
          translatedPort: r.translatedPort,
          deviceAddr: r.deviceAddr,
          deviceName: r.deviceName,
          doubleNat: r.doubleNat,
          doubleNatAddr: r.doubleNatAddr,
          comment: r.comment,
        }));
        return await applyNftablesConfig(nc, { natRules });
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Service Configuration
    // ═══════════════════════════════════════════════════════════════════════

    updateServiceConfig: t.field({
      type: ConfigEntryRef,
      description: "Update a service config value in NATS KV. The service will pick up the change via its KV watcher.",
      args: {
        moduleId: t.arg.string({ required: true }),
        envVar: t.arg.string({ required: true }),
        value: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        return await updateConfigValue(args.moduleId, args.envVar, args.value);
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Gateway Configuration
    // ═══════════════════════════════════════════════════════════════════════

    setGatewayDevice: t.field({
      type: GatewayConfigRef,
      description: "Add or update a device in the gateway config. Gateway rebuilds automatically.",
      args: {
        gatewayId: t.arg.string({ required: true }),
        device: t.arg({ type: GatewayDeviceInputRef, required: true }),
      },
      resolve: async (_root, args) => {
        const d = args.device;
        let deviceConfig: GatewayDeviceConfig;

        switch (d.protocol) {
          case "ethernetip":
            deviceConfig = {
              protocol: "ethernetip",
              host: d.host ?? "localhost",
              port: d.port ?? undefined,
            };
            break;
          case "opcua":
            deviceConfig = {
              protocol: "opcua",
              endpointUrl: d.endpointUrl ?? "opc.tcp://localhost:4840",
            };
            break;
          case "snmp":
            deviceConfig = {
              protocol: "snmp",
              host: d.host ?? "localhost",
              port: d.port ?? undefined,
              version: (d.version as "1" | "2c" | "3") ?? "2c",
              community: d.community ?? undefined,
            };
            break;
          case "modbus":
            deviceConfig = {
              protocol: "modbus",
              host: d.host ?? "localhost",
              port: d.port ?? undefined,
              unitId: d.unitId ?? undefined,
            };
            break;
          default:
            throw new Error(`Unknown protocol: ${d.protocol}`);
        }

        // Apply shared device-level settings
        if (d.scanRate != null) (deviceConfig as any).scanRate = d.scanRate;
        if (d.deadband) (deviceConfig as any).deadband = { value: d.deadband.value, minTime: d.deadband.minTime ?? undefined, maxTime: d.deadband.maxTime ?? undefined } as DeadBandConfig;
        if (d.disableRBE != null) (deviceConfig as any).disableRBE = d.disableRBE;

        return await setGatewayDevice(args.gatewayId, d.deviceId, deviceConfig);
      },
    }),

    deleteGatewayDevice: t.field({
      type: GatewayConfigRef,
      description: "Remove a device and its associated variables from the gateway config.",
      args: {
        gatewayId: t.arg.string({ required: true }),
        deviceId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        return await deleteGatewayDevice(args.gatewayId, args.deviceId);
      },
    }),

    setTemplateNameOverrides: t.field({
      type: GatewayConfigRef,
      description: "Set template name overrides for a device (browse original name → unique name).",
      args: {
        gatewayId: t.arg.string({ required: true }),
        deviceId: t.arg.string({ required: true }),
        overrides: t.arg({ type: "JSON", required: true }),
      },
      resolve: async (_root, args) => {
        return await setTemplateNameOverrides(args.gatewayId, args.deviceId, args.overrides as Record<string, string>);
      },
    }),

    setGatewayVariable: t.field({
      type: GatewayConfigRef,
      description: "Add or update a variable in the gateway config.",
      args: {
        gatewayId: t.arg.string({ required: true }),
        variable: t.arg({ type: GatewayVariableInputRef, required: true }),
      },
      resolve: async (_root, args) => {
        const v = args.variable;
        const varConfig: GatewayVariableConfig = {
          id: v.id,
          description: v.description ?? undefined,
          datatype: v.datatype as "number" | "boolean" | "string",
          default: (v.default ?? (v.datatype === "boolean" ? false : v.datatype === "string" ? "" : 0)) as number | boolean | string,
          deviceId: v.deviceId,
          tag: v.tag,
          cipType: v.cipType ?? undefined,
          bidirectional: v.bidirectional ?? undefined,
          deadband: v.deadband ? { value: v.deadband.value, minTime: v.deadband.minTime ?? undefined, maxTime: v.deadband.maxTime ?? undefined } as DeadBandConfig : undefined,
          disableRBE: v.disableRBE ?? undefined,
          functionCode: v.functionCode ?? undefined,
          modbusDatatype: v.modbusDatatype ?? undefined,
          byteOrder: v.byteOrder ?? undefined,
          address: v.address ?? undefined,
        };
        return await setGatewayVariable(args.gatewayId, varConfig);
      },
    }),

    setGatewayVariables: t.field({
      type: GatewayConfigRef,
      description: "Bulk add/update variables in the gateway config. For mass import from tag browser.",
      args: {
        gatewayId: t.arg.string({ required: true }),
        variables: t.arg({ type: [GatewayVariableInputRef], required: true }),
      },
      resolve: async (_root, args) => {
        const varConfigs: GatewayVariableConfig[] = args.variables.map((v) => ({
          id: v.id,
          description: v.description ?? undefined,
          datatype: v.datatype as "number" | "boolean" | "string",
          default: (v.default ?? (v.datatype === "boolean" ? false : v.datatype === "string" ? "" : 0)) as number | boolean | string,
          deviceId: v.deviceId,
          tag: v.tag,
          cipType: v.cipType ?? undefined,
          bidirectional: v.bidirectional ?? undefined,
          deadband: v.deadband ? { value: v.deadband.value, minTime: v.deadband.minTime ?? undefined, maxTime: v.deadband.maxTime ?? undefined } as DeadBandConfig : undefined,
          disableRBE: v.disableRBE ?? undefined,
          functionCode: v.functionCode ?? undefined,
          modbusDatatype: v.modbusDatatype ?? undefined,
          byteOrder: v.byteOrder ?? undefined,
          address: v.address ?? undefined,
        }));
        return await setGatewayVariables(args.gatewayId, varConfigs);
      },
    }),

    deleteGatewayVariable: t.field({
      type: GatewayConfigRef,
      description: "Remove a variable from the gateway config.",
      args: {
        gatewayId: t.arg.string({ required: true }),
        variableId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        return await deleteGatewayVariable(args.gatewayId, args.variableId);
      },
    }),

    deleteGatewayVariables: t.field({
      type: GatewayConfigRef,
      description: "Bulk remove variables from the gateway config.",
      args: {
        gatewayId: t.arg.string({ required: true }),
        variableIds: t.arg.stringList({ required: true }),
      },
      resolve: async (_root, args) => {
        return await deleteGatewayVariables(args.gatewayId, args.variableIds);
      },
    }),

    deleteGatewayUdtVariable: t.field({
      type: GatewayConfigRef,
      description: "Remove a UDT variable (and orphaned templates) from the gateway config.",
      args: {
        gatewayId: t.arg.string({ required: true }),
        udtVariableId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        return await deleteGatewayUdtVariable(args.gatewayId, args.udtVariableId);
      },
    }),

    deleteGatewayUdtVariables: t.field({
      type: GatewayConfigRef,
      description: "Bulk remove UDT variables (and orphaned templates) from the gateway config.",
      args: {
        gatewayId: t.arg.string({ required: true }),
        udtVariableIds: t.arg.stringList({ required: true }),
      },
      resolve: async (_root, args) => {
        return await deleteGatewayUdtVariables(args.gatewayId, args.udtVariableIds);
      },
    }),

    syncGatewayDeviceVariables: t.field({
      type: GatewayConfigRef,
      description: "Atomically replace all variables for a device. Checked = published, unchecked = removed.",
      args: {
        gatewayId: t.arg.string({ required: true }),
        deviceId: t.arg.string({ required: true }),
        atomicVariables: t.arg({ type: [GatewayVariableInputRef], required: false }),
        udtTemplates: t.arg({ type: [GatewayUdtTemplateInputRef], required: false }),
        udtVariables: t.arg({ type: [GatewayUdtVariableInputRef], required: false }),
      },
      resolve: async (_root, args) => {
        const atomicVars = (args.atomicVariables ?? []).map((v) => ({
          id: v.id,
          description: v.description ?? undefined,
          datatype: v.datatype as "number" | "boolean" | "string",
          default: (v.default ?? (v.datatype === "boolean" ? false : v.datatype === "string" ? "" : 0)) as number | boolean | string,
          deviceId: v.deviceId,
          tag: v.tag,
          cipType: v.cipType ?? undefined,
          bidirectional: v.bidirectional ?? undefined,
          deadband: v.deadband ? { value: v.deadband.value, minTime: v.deadband.minTime ?? undefined, maxTime: v.deadband.maxTime ?? undefined } as DeadBandConfig : undefined,
          disableRBE: v.disableRBE ?? undefined,
          functionCode: v.functionCode ?? undefined,
          modbusDatatype: v.modbusDatatype ?? undefined,
          byteOrder: v.byteOrder ?? undefined,
          address: v.address ?? undefined,
        }));

        const templates = (args.udtTemplates ?? []).map((t) => ({
          name: t.name,
          version: t.version ?? undefined,
          members: t.members.map((m) => ({
            name: m.name,
            datatype: m.datatype,
            templateRef: m.templateRef ?? undefined,
            defaultDeadband: m.defaultDeadband ? { value: m.defaultDeadband.value, minTime: m.defaultDeadband.minTime ?? undefined, maxTime: m.defaultDeadband.maxTime ?? undefined } as DeadBandConfig : undefined,
          })),
        }));

        const udtVars = (args.udtVariables ?? []).map((uv) => ({
          id: uv.id,
          deviceId: uv.deviceId,
          tag: uv.tag,
          templateName: uv.templateName,
          memberTags: uv.memberTags as Record<string, string>,
          memberCipTypes: uv.memberCipTypes as Record<string, string> | undefined,
          memberDeadbands: uv.memberDeadbands as Record<string, DeadBandConfig> | undefined,
        }));

        return await syncGatewayDeviceVariables(args.gatewayId, args.deviceId, atomicVars, templates, udtVars);
      },
    }),

    importGatewayBrowse: t.field({
      type: GatewayConfigRef,
      description: "Import browsed tags: atomic variables, UDT templates, and UDT variable instances.",
      args: {
        gatewayId: t.arg.string({ required: true }),
        atomicVariables: t.arg({ type: [GatewayVariableInputRef], required: false }),
        udtTemplates: t.arg({ type: [GatewayUdtTemplateInputRef], required: false }),
        udtVariables: t.arg({ type: [GatewayUdtVariableInputRef], required: false }),
      },
      resolve: async (_root, args) => {
        const atomicVars = (args.atomicVariables ?? []).map((v) => ({
          id: v.id,
          description: v.description ?? undefined,
          datatype: v.datatype as "number" | "boolean" | "string",
          default: (v.default ?? (v.datatype === "boolean" ? false : v.datatype === "string" ? "" : 0)) as number | boolean | string,
          deviceId: v.deviceId,
          tag: v.tag,
          cipType: v.cipType ?? undefined,
          bidirectional: v.bidirectional ?? undefined,
          deadband: v.deadband ? { value: v.deadband.value, minTime: v.deadband.minTime ?? undefined, maxTime: v.deadband.maxTime ?? undefined } as DeadBandConfig : undefined,
          disableRBE: v.disableRBE ?? undefined,
          functionCode: v.functionCode ?? undefined,
          modbusDatatype: v.modbusDatatype ?? undefined,
          byteOrder: v.byteOrder ?? undefined,
          address: v.address ?? undefined,
        }));

        const templates = (args.udtTemplates ?? []).map((t) => ({
          name: t.name,
          version: t.version ?? undefined,
          members: t.members.map((m) => ({
            name: m.name,
            datatype: m.datatype,
            templateRef: m.templateRef ?? undefined,
            defaultDeadband: m.defaultDeadband ? { value: m.defaultDeadband.value, minTime: m.defaultDeadband.minTime ?? undefined, maxTime: m.defaultDeadband.maxTime ?? undefined } as DeadBandConfig : undefined,
          })),
        }));

        const udtVars = (args.udtVariables ?? []).map((uv) => ({
          id: uv.id,
          deviceId: uv.deviceId,
          tag: uv.tag,
          templateName: uv.templateName,
          memberTags: uv.memberTags as Record<string, string>,
          memberCipTypes: uv.memberCipTypes as Record<string, string> | undefined,
          memberDeadbands: uv.memberDeadbands as Record<string, DeadBandConfig> | undefined,
        }));

        return await importGatewayBrowse(args.gatewayId, atomicVars, templates, udtVars);
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // UDT Tag Config — update template defaults + instance overrides
    // ═══════════════════════════════════════════════════════════════════════

    updateGatewayUdtConfig: t.field({
      type: GatewayConfigRef,
      description: "Update UDT template member defaults and/or instance-level member deadband overrides.",
      args: {
        gatewayId: t.arg.string({ required: true }),
        templateName: t.arg.string({ required: true }),
        memberUpdates: t.arg({ type: "JSON", required: false }),
        instanceUpdates: t.arg({ type: "JSON", required: false }),
      },
      resolve: async (_root, args) => {
        return await updateGatewayUdtConfig(
          args.gatewayId,
          args.templateName,
          args.memberUpdates as { name: string; defaultDeadband?: DeadBandConfig }[] | undefined,
          args.instanceUpdates as { id: string; memberDeadbands: Record<string, DeadBandConfig> }[] | undefined,
        );
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Gateway Device Browse
    // ═══════════════════════════════════════════════════════════════════════

    browseGatewayDevice: t.field({
      type: GatewayBrowseResultRef,
      description: "Browse a gateway device to discover available tags/OIDs using ad-hoc connection params.",
      args: {
        input: t.arg({ type: GatewayBrowseInputRef, required: true }),
      },
      resolve: async (_root, args) => {
        const i = args.input;
        const result = await browseGatewayDevice({
          deviceId: i.deviceId,
          protocol: i.protocol,
          host: i.host ?? undefined,
          port: i.port ?? undefined,
          endpointUrl: i.endpointUrl ?? undefined,
          version: i.version ?? undefined,
          community: i.community ?? undefined,
          rootOid: i.rootOid ?? undefined,
          browseId: i.browseId ?? undefined,
        });
        // Cache successful browse results
        if (result.items.length > 0) {
          cacheBrowseResult(i.deviceId, { ...result, cachedAt: Date.now() }).catch(() => {});
        }
        return result;
      },
    }),

    startGatewayBrowse: t.field({
      type: StartBrowseResultRef,
      description: "Start a non-blocking browse of a gateway device. Returns immediately with browseId. Use gatewayBrowseStates query or gatewayBrowseProgress subscription to track progress.",
      args: {
        input: t.arg({ type: GatewayBrowseInputRef, required: true }),
      },
      resolve: async (_root, args) => {
        const i = args.input;
        return await startGatewayBrowse({
          deviceId: i.deviceId,
          protocol: i.protocol,
          host: i.host ?? undefined,
          port: i.port ?? undefined,
          endpointUrl: i.endpointUrl ?? undefined,
          version: i.version ?? undefined,
          community: i.community ?? undefined,
          rootOid: i.rootOid ?? undefined,
        });
      },
    }),

    // ═══════════════════════════════════════════════════════════════════════
    // Orchestrator
    // ═══════════════════════════════════════════════════════════════════════

    setDesiredService: t.field({
      type: DesiredServiceRef,
      description: "Set the desired state for a service module (version + running). The orchestrator will reconcile.",
      args: {
        moduleId: t.arg.string({ required: true }),
        version: t.arg.string({ required: true }),
        running: t.arg.boolean({ required: true }),
      },
      resolve: async (_root, args) => {
        return await setDesiredService(args.moduleId, args.version, args.running);
      },
    }),

    deleteDesiredService: t.field({
      type: "Boolean",
      description: "Remove a module from the orchestrator's desired state. The orchestrator will stop managing it.",
      args: {
        moduleId: t.arg.string({ required: true }),
      },
      resolve: async (_root, args) => {
        return await deleteDesiredService(args.moduleId);
      },
    }),
  }),
});
