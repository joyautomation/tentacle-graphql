/**
 * Gateway Configuration Module
 *
 * Manages the gateway_config NATS KV bucket — CRUD operations for
 * gateway devices and variables. Changes are picked up by tentacle-gateway
 * via its KV watcher.
 */

import type { NatsConnection } from "@nats-io/transport-deno";
import { jetstream } from "@nats-io/jetstream";
import { Kvm, type KV } from "@nats-io/kv";
import { createLogger, LogLevel } from "@joyautomation/coral";
import type {
  GatewayConfigKV,
  GatewayDeviceConfig,
  GatewayVariableConfig,
} from "@tentacle/nats-schema";

const log = createLogger("graphql-gateway", LogLevel.info);

const BUCKET = "gateway_config";
let gatewayKv: KV | null = null;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export async function initGatewayKv(nc: NatsConnection): Promise<void> {
  try {
    const js = jetstream(nc);
    const kvm = new Kvm(js);
    gatewayKv = await kvm.create(BUCKET, {
      history: 5,
      ttl: 0,
    });
    log.info("Gateway config KV bucket ready");
  } catch (err) {
    log.warn(`Failed to initialize gateway KV: ${err}`);
  }
}

function getKv(): KV {
  if (!gatewayKv) {
    throw new Error("Gateway KV not initialized");
  }
  return gatewayKv;
}

/** Get the full config for a gateway instance, or create an empty one */
async function getOrCreateConfig(gatewayId: string): Promise<GatewayConfigKV> {
  const kv = getKv();
  try {
    const entry = await kv.get(gatewayId);
    if (entry?.value) {
      return JSON.parse(decoder.decode(entry.value)) as GatewayConfigKV;
    }
  } catch {
    // Key doesn't exist
  }
  return {
    gatewayId,
    devices: {},
    variables: {},
    updatedAt: Date.now(),
  };
}

/** Save config back to KV */
async function saveConfig(config: GatewayConfigKV): Promise<void> {
  const kv = getKv();
  config.updatedAt = Date.now();
  await kv.put(config.gatewayId, encoder.encode(JSON.stringify(config)));
}

// ═══════════════════════════════════════════════════════════════════════════
// Query operations
// ═══════════════════════════════════════════════════════════════════════════

export async function getGatewayConfig(
  gatewayId: string,
): Promise<GatewayConfigKV> {
  return await getOrCreateConfig(gatewayId);
}

export async function listGatewayConfigs(): Promise<GatewayConfigKV[]> {
  const kv = getKv();
  const configs: GatewayConfigKV[] = [];
  try {
    const keys = await kv.keys();
    for await (const key of keys) {
      try {
        const entry = await kv.get(key);
        if (entry?.value) {
          configs.push(
            JSON.parse(decoder.decode(entry.value)) as GatewayConfigKV,
          );
        }
      } catch { /* skip */ }
    }
  } catch { /* bucket may not exist */ }
  return configs;
}

// ═══════════════════════════════════════════════════════════════════════════
// Device operations
// ═══════════════════════════════════════════════════════════════════════════

export async function setGatewayDevice(
  gatewayId: string,
  deviceId: string,
  device: GatewayDeviceConfig,
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);
  config.devices[deviceId] = device;
  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: set device "${deviceId}" (${device.protocol})`);
  return config;
}

export async function deleteGatewayDevice(
  gatewayId: string,
  deviceId: string,
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);
  delete config.devices[deviceId];
  // Also remove any variables that reference this device
  for (const [varId, varConfig] of Object.entries(config.variables)) {
    if (varConfig.deviceId === deviceId) {
      delete config.variables[varId];
    }
  }
  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: deleted device "${deviceId}" and orphaned variables`);
  return config;
}

// ═══════════════════════════════════════════════════════════════════════════
// Variable operations
// ═══════════════════════════════════════════════════════════════════════════

export async function setGatewayVariable(
  gatewayId: string,
  variable: GatewayVariableConfig,
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);
  if (!config.devices[variable.deviceId]) {
    throw new Error(
      `Device "${variable.deviceId}" not found in gateway "${gatewayId}". Add the device first.`,
    );
  }
  config.variables[variable.id] = variable;
  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: set variable "${variable.id}"`);
  return config;
}

export async function setGatewayVariables(
  gatewayId: string,
  variables: GatewayVariableConfig[],
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);
  for (const variable of variables) {
    if (!config.devices[variable.deviceId]) {
      throw new Error(
        `Device "${variable.deviceId}" not found in gateway "${gatewayId}". Add the device first.`,
      );
    }
    config.variables[variable.id] = variable;
  }
  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: set ${variables.length} variable(s)`);
  return config;
}

export async function deleteGatewayVariable(
  gatewayId: string,
  variableId: string,
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);
  delete config.variables[variableId];
  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: deleted variable "${variableId}"`);
  return config;
}

export async function deleteGatewayVariables(
  gatewayId: string,
  variableIds: string[],
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);
  for (const id of variableIds) {
    delete config.variables[id];
  }
  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: deleted ${variableIds.length} variable(s)`);
  return config;
}
