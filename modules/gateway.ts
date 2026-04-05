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
  DeadBandConfig,
} from "@tentacle/nats-schema";

const log = createLogger("graphql-gateway", LogLevel.info);

const BUCKET = "gateway_config";
const BROWSE_CACHE_BUCKET = "gateway_browse_cache";
const BROWSE_STATE_BUCKET = "gateway_browse_state";
let gatewayKv: KV | null = null;
let browseCacheKv: KV | null = null;
let browseStateKv: KV | null = null;

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
    browseCacheKv = await kvm.create(BROWSE_CACHE_BUCKET, {
      history: 1,
      ttl: 24 * 3600 * 1000, // 24 hour TTL
    });
    browseStateKv = await kvm.create(BROWSE_STATE_BUCKET, {
      history: 1,
      ttl: 10 * 60 * 1000, // 10 minute TTL — auto-expire stale browse states
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
  // Preserve templateNameOverrides from existing device when not provided
  const existing = config.devices[deviceId];
  if (existing?.templateNameOverrides && !device.templateNameOverrides) {
    device.templateNameOverrides = existing.templateNameOverrides;
  }
  config.devices[deviceId] = device;
  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: set device "${deviceId}" (${device.protocol})`);
  return config;
}

export async function setTemplateNameOverrides(
  gatewayId: string,
  deviceId: string,
  overrides: Record<string, string>,
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);
  if (!config.devices[deviceId]) {
    throw new Error(
      `Device "${deviceId}" not found in gateway "${gatewayId}".`,
    );
  }
  if (Object.keys(overrides).length === 0) {
    delete (config.devices[deviceId] as any).templateNameOverrides;
  } else {
    (config.devices[deviceId] as any).templateNameOverrides = overrides;
  }
  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: set template name overrides for device "${deviceId}"`);
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
  // Remove UDT variables that reference this device and orphaned templates
  if (config.udtVariables) {
    for (const [id, uv] of Object.entries(config.udtVariables)) {
      if (uv.deviceId === deviceId) {
        delete config.udtVariables[id];
      }
    }
    // Clean up templates no longer referenced by any UDT variable
    if (config.udtTemplates) {
      const usedTemplates = new Set<string>();
      for (const uv of Object.values(config.udtVariables)) {
        usedTemplates.add(uv.templateName);
      }
      for (const tmplName of Object.keys(config.udtTemplates)) {
        if (!usedTemplates.has(tmplName)) {
          delete config.udtTemplates[tmplName];
        }
      }
    }
  }
  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: deleted device "${deviceId}" and orphaned variables/templates`);
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

export async function deleteGatewayUdtVariable(
  gatewayId: string,
  udtVariableId: string,
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);
  if (config.udtVariables) {
    delete config.udtVariables[udtVariableId];
  }
  // Clean up orphaned templates (templates not referenced by any remaining UDT variable)
  if (config.udtTemplates && config.udtVariables) {
    const referencedTemplates = new Set<string>();
    for (const uv of Object.values(config.udtVariables)) {
      referencedTemplates.add(uv.templateName);
    }
    for (const tmplName of Object.keys(config.udtTemplates)) {
      if (!referencedTemplates.has(tmplName)) {
        delete config.udtTemplates[tmplName];
      }
    }
  }
  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: deleted UDT variable "${udtVariableId}"`);
  return config;
}

export async function deleteGatewayUdtVariables(
  gatewayId: string,
  udtVariableIds: string[],
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);
  if (config.udtVariables) {
    for (const id of udtVariableIds) {
      delete config.udtVariables[id];
    }
  }
  // Clean up orphaned templates
  if (config.udtTemplates && config.udtVariables) {
    const referencedTemplates = new Set<string>();
    for (const uv of Object.values(config.udtVariables)) {
      referencedTemplates.add(uv.templateName);
    }
    for (const tmplName of Object.keys(config.udtTemplates)) {
      if (!referencedTemplates.has(tmplName)) {
        delete config.udtTemplates[tmplName];
      }
    }
  }
  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: deleted ${udtVariableIds.length} UDT variable(s)`);
  return config;
}

// ═══════════════════════════════════════════════════════════════════════════
// Type-centric import (atomic variables + UDT templates + UDT variables)
// ═══════════════════════════════════════════════════════════════════════════

export type ImportUdtTemplate = {
  name: string;
  version?: string;
  members: Array<{ name: string; datatype: string; templateRef?: string }>;
};

export type ImportUdtVariable = {
  id: string;
  deviceId: string;
  tag: string;
  templateName: string;
  memberTags: Record<string, string>;
  memberCipTypes?: Record<string, string>;
};

export async function importGatewayBrowse(
  gatewayId: string,
  atomicVariables: GatewayVariableConfig[],
  udtTemplates: ImportUdtTemplate[],
  udtVariables: ImportUdtVariable[],
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);

  // Validate device references
  for (const v of atomicVariables) {
    if (!config.devices[v.deviceId]) {
      throw new Error(`Device "${v.deviceId}" not found in gateway "${gatewayId}". Add the device first.`);
    }
  }
  for (const uv of udtVariables) {
    if (!config.devices[uv.deviceId]) {
      throw new Error(`Device "${uv.deviceId}" not found in gateway "${gatewayId}". Add the device first.`);
    }
  }

  // Write atomic variables
  for (const v of atomicVariables) {
    config.variables[v.id] = v;
  }

  // Write UDT templates and variables
  if (!config.udtTemplates) config.udtTemplates = {};
  if (!config.udtVariables) config.udtVariables = {};

  for (const tmpl of udtTemplates) {
    config.udtTemplates[tmpl.name] = {
      name: tmpl.name,
      version: tmpl.version ?? "1.0",
      members: tmpl.members,
    };
  }

  for (const uv of udtVariables) {
    config.udtVariables[uv.id] = {
      id: uv.id,
      deviceId: uv.deviceId,
      tag: uv.tag,
      templateName: uv.templateName,
      memberTags: uv.memberTags,
      memberCipTypes: uv.memberCipTypes,
    };
  }

  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: imported ${atomicVariables.length} atomic var(s), ${udtTemplates.length} UDT template(s), ${udtVariables.length} UDT variable(s)`);
  return config;
}

// ═══════════════════════════════════════════════════════════════════════════
// Sync: atomically replace all variables for a device
// ═══════════════════════════════════════════════════════════════════════════

export async function syncGatewayDeviceVariables(
  gatewayId: string,
  deviceId: string,
  atomicVariables: GatewayVariableConfig[],
  udtTemplates: ImportUdtTemplate[],
  udtVariables: ImportUdtVariable[],
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);
  if (!config.devices[deviceId]) {
    throw new Error(`Device "${deviceId}" not found in gateway "${gatewayId}".`);
  }

  // Remove all existing atomic variables for this device
  for (const [varId, v] of Object.entries(config.variables)) {
    if (v.deviceId === deviceId) {
      delete config.variables[varId];
    }
  }

  // Remove all existing UDT variables for this device
  if (!config.udtTemplates) config.udtTemplates = {};
  if (!config.udtVariables) config.udtVariables = {};

  for (const [varId, v] of Object.entries(config.udtVariables)) {
    if (v.deviceId === deviceId) {
      delete config.udtVariables[varId];
    }
  }

  // Add new atomic variables
  for (const v of atomicVariables) {
    config.variables[v.id] = v;
  }

  // Add new UDT templates
  for (const tmpl of udtTemplates) {
    config.udtTemplates[tmpl.name] = {
      name: tmpl.name,
      version: tmpl.version ?? "1.0",
      members: tmpl.members,
    };
  }

  // Add new UDT variables
  for (const uv of udtVariables) {
    config.udtVariables[uv.id] = {
      id: uv.id,
      deviceId: uv.deviceId,
      tag: uv.tag,
      templateName: uv.templateName,
      memberTags: uv.memberTags,
      memberCipTypes: uv.memberCipTypes,
    };
  }

  // Clean up orphaned templates
  const referencedTemplates = new Set<string>();
  for (const uv of Object.values(config.udtVariables!)) {
    referencedTemplates.add(uv.templateName);
  }
  for (const tmplName of Object.keys(config.udtTemplates!)) {
    if (!referencedTemplates.has(tmplName)) {
      delete config.udtTemplates![tmplName];
    }
  }

  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: synced device ${deviceId} → ${atomicVariables.length} atomic, ${udtTemplates.length} templates, ${udtVariables.length} UDT vars`);
  return config;
}

// ═══════════════════════════════════════════════════════════════════════════
// UDT Tag Config — update template defaults + instance overrides
// ═══════════════════════════════════════════════════════════════════════════

export type UdtTemplateMemberUpdate = {
  name: string;
  defaultDeadband?: DeadBandConfig;
};

export type UdtInstanceOverrideUpdate = {
  id: string;
  memberDeadbands: Record<string, DeadBandConfig>;
};

/**
 * Update template member defaults (defaultDeadband) and/or instance-level
 * member deadband overrides in a single atomic write.
 */
export async function updateGatewayUdtConfig(
  gatewayId: string,
  templateName: string,
  memberUpdates?: UdtTemplateMemberUpdate[],
  instanceUpdates?: UdtInstanceOverrideUpdate[],
): Promise<GatewayConfigKV> {
  const config = await getOrCreateConfig(gatewayId);

  if (!config.udtTemplates?.[templateName]) {
    throw new Error(`UDT template "${templateName}" not found in gateway "${gatewayId}".`);
  }

  // Update template member defaults
  if (memberUpdates) {
    const tmpl = config.udtTemplates[templateName];
    for (const upd of memberUpdates) {
      const member = tmpl.members.find(m => m.name === upd.name);
      if (member) {
        if (upd.defaultDeadband) {
          member.defaultDeadband = upd.defaultDeadband;
        } else {
          delete member.defaultDeadband;
        }
      }
    }
  }

  // Update instance-level member deadband overrides
  if (instanceUpdates && config.udtVariables) {
    for (const upd of instanceUpdates) {
      const inst = config.udtVariables[upd.id];
      if (inst && inst.templateName === templateName) {
        if (Object.keys(upd.memberDeadbands).length > 0) {
          inst.memberDeadbands = upd.memberDeadbands;
        } else {
          delete inst.memberDeadbands;
        }
      }
    }
  }

  await saveConfig(config);
  log.info(`Gateway ${gatewayId}: updated UDT config for template "${templateName}"`);
  return config;
}

// ═══════════════════════════════════════════════════════════════════════════
// Browse result caching
// ═══════════════════════════════════════════════════════════════════════════

export type CachedBrowseResult = {
  deviceId: string;
  protocol: string;
  items: Array<{ tag: string; name: string; datatype: string; value: unknown; protocolType: string }>;
  udts: Array<{ name: string; members: Array<{ name: string; datatype: string; cipType: string; udtType: string; isArray: boolean }> }>;
  structTags: Record<string, string>;
  cachedAt: number;
};

export async function cacheBrowseResult(
  deviceId: string,
  result: CachedBrowseResult,
): Promise<void> {
  if (!browseCacheKv) return;
  try {
    await browseCacheKv.put(deviceId, encoder.encode(JSON.stringify(result)));
    log.info(`Cached browse result for device ${deviceId} (${result.items.length} items, ${result.udts.length} UDTs)`);
  } catch (err) {
    log.warn(`Failed to cache browse result for ${deviceId}: ${err}`);
  }
}

export async function getCachedBrowseResult(
  deviceId: string,
): Promise<CachedBrowseResult | null> {
  if (!browseCacheKv) return null;
  try {
    const entry = await browseCacheKv.get(deviceId);
    if (entry?.value) {
      return JSON.parse(decoder.decode(entry.value)) as CachedBrowseResult;
    }
  } catch {
    // Cache miss or expired
  }
  return null;
}

// ═══════════════════════════════════════════════════════════════════════════
// Browse state — singleton persistent state per device
// ═══════════════════════════════════════════════════════════════════════════

export type GatewayBrowseState = {
  deviceId: string;
  browseId: string;
  protocol: string;
  status: "browsing" | "completed" | "failed";
  phase: string;
  discoveredCount: number;
  totalCount: number;
  message: string;
  startedAt: number;
  updatedAt: number;
  error?: string;
};

function getBrowseStateKv(): KV {
  if (!browseStateKv) {
    throw new Error("Browse state KV not initialized");
  }
  return browseStateKv;
}

export async function getBrowseState(
  deviceId: string,
): Promise<GatewayBrowseState | null> {
  if (!browseStateKv) return null;
  try {
    const entry = await browseStateKv.get(deviceId);
    if (entry?.value) {
      return JSON.parse(decoder.decode(entry.value)) as GatewayBrowseState;
    }
  } catch {
    // Key doesn't exist or expired
  }
  return null;
}

export async function getAllBrowseStates(): Promise<GatewayBrowseState[]> {
  if (!browseStateKv) return [];
  const states: GatewayBrowseState[] = [];
  try {
    const keys = await browseStateKv.keys();
    for await (const key of keys) {
      try {
        const entry = await browseStateKv.get(key);
        if (entry?.value) {
          states.push(
            JSON.parse(decoder.decode(entry.value)) as GatewayBrowseState,
          );
        }
      } catch { /* skip */ }
    }
  } catch { /* bucket may not exist */ }
  return states;
}

export async function setBrowseState(
  state: GatewayBrowseState,
): Promise<void> {
  const kv = getBrowseStateKv();
  await kv.put(state.deviceId, encoder.encode(JSON.stringify(state)));
}

export async function deleteBrowseState(
  deviceId: string,
): Promise<void> {
  const kv = getBrowseStateKv();
  try {
    await kv.delete(deviceId);
  } catch {
    // Key may not exist
  }
}
