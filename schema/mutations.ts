import { builder } from "./builder.ts";
import { publishCommand, getVariable, browseTags, subscribeTags, unsubscribeTags, getNatsConnection } from "../nats/client.ts";
import {
  BrowseResultRef,
  NetworkCommandResultRef,
  NetworkInterfaceConfigInputRef,
  NftablesCommandResultRef,
  NatRuleInputRef,
} from "./types.ts";
import { applyNetworkConfig } from "../modules/network.ts";
import { applyNftablesConfig } from "../modules/nftables.ts";
import type { NetworkInterfaceConfig, NatRule } from "@tentacle/nats-schema";

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
  }),
});
