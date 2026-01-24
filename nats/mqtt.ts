/**
 * MQTT configuration KV operations
 *
 * Bucket: mqtt-config-{projectId}
 * Key structure:
 *   mqtt.defaults   - Default deadband config for all variables
 *   mqtt.variables  - JSON object with all per-variable configs
 *
 * Matches the format used by tentacle-ethernetip's mqttConfig.ts
 */

import { getJetStreamClient, getNatsConnection } from "./client.ts";
import { StorageType, DiscardPolicy } from "@nats-io/jetstream";
import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("graphql:mqtt", LogLevel.info);
const MQTT_BUCKET_PREFIX = "mqtt-config-";

// ═══════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════

export interface DeadBandConfig {
  value: number;
  maxTime?: number;
}

export interface MqttVariableConfig {
  enabled: boolean;
  deadband?: DeadBandConfig;
}

export interface MqttDefaults {
  deadband: DeadBandConfig;
}

export interface MqttProjectConfig {
  defaults: MqttDefaults;
  variables: Record<string, MqttVariableConfig>;
}

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

function getMqttBucketInfo(projectId: string) {
  const bucketName = `${MQTT_BUCKET_PREFIX}${projectId}`;
  return {
    bucketName,
    streamName: `KV_${bucketName}`,
    subjectPrefix: `$KV.${bucketName}`,
  };
}

async function ensureMqttBucket(projectId: string): Promise<void> {
  const js = getJetStreamClient();
  const jsm = await js.jetstreamManager();
  const { bucketName, streamName, subjectPrefix } = getMqttBucketInfo(projectId);

  try {
    await jsm.streams.info(streamName);
  } catch {
    await jsm.streams.add({
      name: streamName,
      subjects: [`${subjectPrefix}.>`],
      storage: StorageType.File,
      discard: DiscardPolicy.New,
      max_msgs_per_subject: 1,
      max_age: 0,
      allow_rollup_hdrs: true,
    });
    log.info(`Created MQTT config bucket: ${bucketName}`);
  }
}

async function mqttKvGet(projectId: string, key: string): Promise<Uint8Array | null> {
  const js = getJetStreamClient();
  const jsm = await js.jetstreamManager();
  const { streamName, subjectPrefix } = getMqttBucketInfo(projectId);

  try {
    const msg = await jsm.streams.getMessage(streamName, {
      last_by_subj: `${subjectPrefix}.${key}`,
    });
    return msg?.data ?? null;
  } catch {
    return null;
  }
}

function mqttKvPut(projectId: string, key: string, value: string): void {
  const nc = getNatsConnection();
  const { subjectPrefix } = getMqttBucketInfo(projectId);
  nc.publish(`${subjectPrefix}.${key}`, new TextEncoder().encode(value));
}

// ═══════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Get current MQTT config for a project
 */
export async function getMqttConfig(projectId: string): Promise<MqttProjectConfig> {
  // Default config
  const config: MqttProjectConfig = {
    defaults: {
      deadband: { value: 0, maxTime: 60000 },
    },
    variables: {},
  };

  // Load defaults
  const defaultsData = await mqttKvGet(projectId, "mqtt.defaults");
  if (defaultsData && defaultsData.length > 0) {
    try {
      const parsed = JSON.parse(new TextDecoder().decode(defaultsData));
      config.defaults = parsed;
    } catch (e) {
      log.warn(`Failed to parse MQTT defaults: ${e}`);
    }
  }

  // Load variables
  const variablesData = await mqttKvGet(projectId, "mqtt.variables");
  if (variablesData && variablesData.length > 0) {
    try {
      const parsed = JSON.parse(new TextDecoder().decode(variablesData));
      config.variables = parsed;
    } catch (e) {
      log.warn(`Failed to parse MQTT variables: ${e}`);
    }
  }

  return config;
}

/**
 * Set MQTT defaults for a project
 */
export async function setMqttDefaults(
  projectId: string,
  deadband: DeadBandConfig,
): Promise<MqttDefaults> {
  await ensureMqttBucket(projectId);

  const defaults: MqttDefaults = { deadband };
  mqttKvPut(projectId, "mqtt.defaults", JSON.stringify(defaults));

  log.info(`Set MQTT defaults for ${projectId}: deadband=${deadband.value}`);
  return defaults;
}

/**
 * Set config for a single MQTT variable (granular edit)
 */
export async function setMqttVariable(
  projectId: string,
  variableId: string,
  varConfig: MqttVariableConfig,
): Promise<MqttVariableConfig> {
  await ensureMqttBucket(projectId);

  // Load existing variables
  const config = await getMqttConfig(projectId);
  config.variables[variableId] = varConfig;

  // Save back
  mqttKvPut(projectId, "mqtt.variables", JSON.stringify(config.variables));

  log.info(`Set MQTT config for ${projectId}/${variableId}: enabled=${varConfig.enabled}`);
  return varConfig;
}

/**
 * Bulk enable variables with optional shared config
 */
export async function enableMqttVariables(
  projectId: string,
  variableIds: string[],
  partialConfig?: Partial<MqttVariableConfig>,
): Promise<string[]> {
  await ensureMqttBucket(projectId);

  // Load existing variables
  const config = await getMqttConfig(projectId);

  // Enable each variable
  for (const variableId of variableIds) {
    const existing = config.variables[variableId];
    config.variables[variableId] = {
      enabled: true,
      deadband: partialConfig?.deadband ?? existing?.deadband,
    };
  }

  // Save back
  mqttKvPut(projectId, "mqtt.variables", JSON.stringify(config.variables));

  log.info(`Enabled MQTT for ${variableIds.length} variables in ${projectId}`);
  return variableIds;
}

/**
 * Bulk disable variables
 */
export async function disableMqttVariables(
  projectId: string,
  variableIds: string[],
): Promise<string[]> {
  await ensureMqttBucket(projectId);

  // Load existing variables
  const config = await getMqttConfig(projectId);

  // Disable each variable (keep config, just set enabled=false)
  for (const variableId of variableIds) {
    const existing = config.variables[variableId];
    if (existing) {
      existing.enabled = false;
    } else {
      config.variables[variableId] = { enabled: false };
    }
  }

  // Save back
  mqttKvPut(projectId, "mqtt.variables", JSON.stringify(config.variables));

  log.info(`Disabled MQTT for ${variableIds.length} variables in ${projectId}`);
  return variableIds;
}

/**
 * Get list of enabled variable IDs
 */
export function getEnabledVariables(config: MqttProjectConfig): string[] {
  return Object.entries(config.variables)
    .filter(([_, v]) => v.enabled)
    .map(([id, _]) => id);
}
