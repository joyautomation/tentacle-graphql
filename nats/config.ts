/**
 * NATS KV operations for field-config bucket
 *
 * Manages device and tag configuration for tentacle-ethernetip
 * Bucket: field-config-{projectId}
 *
 * Key structure:
 *   plc.{plcId}          - Device config (host, port, scanRate, enabled)
 *   plc.{plcId}.tags     - Array of tag names/addresses
 *   plc.{plcId}.tagconfig.{tagId} - Detailed tag config (optional)
 */

import { getJetStreamClient, getNatsConnection, deleteVariablesByPattern } from "./client.ts";
import { StorageType, DiscardPolicy } from "@nats-io/jetstream";
import { createLogger, LogLevel } from "@joyautomation/coral";
import type { DeviceConfig, TagConfig } from "../schema/builder.ts";

const log = createLogger("graphql-config", LogLevel.info);

const CONFIG_BUCKET_PREFIX = "field-config-";

// ═══════════════════════════════════════════════════════════════════════════
// Helper Functions
// ═══════════════════════════════════════════════════════════════════════════

function getBucketInfo(projectId: string) {
  const bucketName = `${CONFIG_BUCKET_PREFIX}${projectId}`;
  return {
    bucketName,
    streamName: `KV_${bucketName}`,
    subjectPrefix: `$KV.${bucketName}`,
  };
}

/**
 * Ensure the config bucket exists for a project
 */
async function ensureBucket(projectId: string): Promise<void> {
  const js = getJetStreamClient();
  const jsm = await js.jetstreamManager();
  const { bucketName, streamName, subjectPrefix } = getBucketInfo(projectId);

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
    log.info(`Created config bucket: ${bucketName}`);
  }
}

/**
 * Get a value from KV
 */
async function kvGet(projectId: string, key: string): Promise<Uint8Array | null> {
  const js = getJetStreamClient();
  const jsm = await js.jetstreamManager();
  const { streamName, subjectPrefix } = getBucketInfo(projectId);

  try {
    const msg = await jsm.streams.getMessage(streamName, {
      last_by_subj: `${subjectPrefix}.${key}`,
    });
    return msg?.data ?? null;
  } catch {
    return null;
  }
}

/**
 * Put a value to KV
 */
function kvPut(projectId: string, key: string, value: string): void {
  const nc = getNatsConnection();
  const { subjectPrefix } = getBucketInfo(projectId);
  nc.publish(`${subjectPrefix}.${key}`, new TextEncoder().encode(value));
}

/**
 * Delete a value from KV
 */
function kvDelete(projectId: string, key: string): void {
  const nc = getNatsConnection();
  const { subjectPrefix } = getBucketInfo(projectId);
  // NATS KV delete: publish empty message
  nc.publish(`${subjectPrefix}.${key}`, new Uint8Array(0));
}

// ═══════════════════════════════════════════════════════════════════════════
// Device Operations
// ═══════════════════════════════════════════════════════════════════════════

/**
 * List all devices for a project
 */
export async function listDevices(projectId: string): Promise<DeviceConfig[]> {
  const js = getJetStreamClient();
  const jsm = await js.jetstreamManager();
  const { streamName, subjectPrefix } = getBucketInfo(projectId);

  try {
    const streamInfo = await jsm.streams.info(streamName, {
      subjects_filter: `${subjectPrefix}.plc.>`,
    });

    const subjects = streamInfo.state.subjects;
    if (!subjects) {
      return [];
    }

    // Get unique PLC IDs from subjects like $KV.bucket.plc.{id}
    const plcIds = new Set<string>();
    for (const subject of Object.keys(subjects)) {
      const parts = subject.replace(`${subjectPrefix}.`, "").split(".");
      // Match plc.{id} but not plc.{id}.tags or plc.{id}.tagconfig.{tagId}
      if (parts[0] === "plc" && parts[1] && parts.length === 2) {
        plcIds.add(parts[1]);
      }
    }

    // Fetch each device config
    const devices: DeviceConfig[] = [];
    for (const plcId of plcIds) {
      const device = await getDevice(projectId, plcId);
      if (device) {
        devices.push(device);
      }
    }

    return devices;
  } catch {
    return [];
  }
}

/**
 * Get a single device by ID
 */
export async function getDevice(
  projectId: string,
  deviceId: string,
): Promise<DeviceConfig | null> {
  const configData = await kvGet(projectId, `plc.${deviceId}`);
  if (!configData || configData.length === 0) {
    return null;
  }

  try {
    const data = JSON.parse(new TextDecoder().decode(configData));
    return {
      id: deviceId,
      projectId,
      host: data.host || "localhost",
      port: data.port || 44818,
      type: data.type || "rockwell",
      slot: data.slot,
      scanRate: data.scanRate || 1000,
      enabled: data.enabled ?? false,
    };
  } catch (err) {
    log.warn(`Failed to parse device config for ${deviceId}: ${err}`);
    return null;
  }
}

/**
 * Create or update a device
 */
export async function upsertDevice(
  projectId: string,
  deviceId: string,
  input: {
    host: string;
    port?: number | null;
    type?: string | null;
    slot?: number | null;
    scanRate?: number | null;
    enabled?: boolean | null;
  },
): Promise<DeviceConfig> {
  await ensureBucket(projectId);

  const configData = {
    host: input.host,
    port: input.port ?? 44818,
    type: input.type ?? "rockwell",
    slot: input.slot ?? undefined,
    scanRate: input.scanRate ?? 1000,
    enabled: input.enabled ?? true,
  };

  kvPut(projectId, `plc.${deviceId}`, JSON.stringify(configData));

  // Initialize empty tags array if device is new
  const existingTags = await kvGet(projectId, `plc.${deviceId}.tags`);
  if (!existingTags || existingTags.length === 0) {
    kvPut(projectId, `plc.${deviceId}.tags`, JSON.stringify([]));
  }

  log.info(`Upserted device: ${projectId}/${deviceId}`);

  return {
    id: deviceId,
    projectId,
    ...configData,
  } as DeviceConfig;
}

/**
 * Delete a device and all its tags
 */
export async function deleteDevice(
  projectId: string,
  deviceId: string,
): Promise<boolean> {
  // Delete device config
  kvDelete(projectId, `plc.${deviceId}`);
  // Delete tags list
  kvDelete(projectId, `plc.${deviceId}.tags`);

  // Delete any detailed tag configs
  const tags = await listTags(projectId, deviceId);
  for (const tag of tags) {
    kvDelete(projectId, `plc.${deviceId}.tagconfig.${tag.id}`);
  }

  // Delete variables matching the device ID pattern
  // Variables are named like "RTU60_..." for device "rtu60"
  await deleteVariablesByPattern(projectId, deviceId);

  log.info(`Deleted device: ${projectId}/${deviceId}`);
  return true;
}

// ═══════════════════════════════════════════════════════════════════════════
// Tag Operations
// ═══════════════════════════════════════════════════════════════════════════

/**
 * List all tags for a device
 */
export async function listTags(
  projectId: string,
  deviceId: string,
): Promise<TagConfig[]> {
  // Get the simple tags array
  const tagsData = await kvGet(projectId, `plc.${deviceId}.tags`);
  if (!tagsData || tagsData.length === 0) {
    return [];
  }

  try {
    const tagNames = JSON.parse(new TextDecoder().decode(tagsData));
    if (!Array.isArray(tagNames)) {
      return [];
    }

    // Build tag configs, merging detailed config if available
    const tags: TagConfig[] = [];
    for (const tagName of tagNames) {
      const tagId = tagName.replace(/\./g, "_"); // Normalize ID
      const detailedConfig = await getTagConfig(projectId, deviceId, tagId);

      tags.push({
        id: tagId,
        deviceId,
        address: tagName,
        datatype: detailedConfig?.datatype,
        writable: detailedConfig?.writable ?? false,
        deadbandValue: detailedConfig?.deadbandValue,
        deadbandMaxTime: detailedConfig?.deadbandMaxTime,
        disableRBE: detailedConfig?.disableRBE,
      });
    }

    return tags;
  } catch (err) {
    log.warn(`Failed to parse tags for ${deviceId}: ${err}`);
    return [];
  }
}

/**
 * Get detailed tag configuration (optional extended config)
 */
async function getTagConfig(
  projectId: string,
  deviceId: string,
  tagId: string,
): Promise<Partial<TagConfig> | null> {
  const data = await kvGet(projectId, `plc.${deviceId}.tagconfig.${tagId}`);
  if (!data || data.length === 0) {
    return null;
  }

  try {
    return JSON.parse(new TextDecoder().decode(data));
  } catch {
    return null;
  }
}

/**
 * Add or update a tag
 */
export async function upsertTag(
  projectId: string,
  deviceId: string,
  tagId: string,
  input: {
    address: string;
    datatype?: string | null;
    writable?: boolean | null;
    deadbandValue?: number | null;
    deadbandMaxTime?: number | null;
    disableRBE?: boolean | null;
  },
): Promise<TagConfig> {
  await ensureBucket(projectId);

  // Get current tags array
  const tagsData = await kvGet(projectId, `plc.${deviceId}.tags`);
  let tagNames: string[] = [];
  if (tagsData && tagsData.length > 0) {
    try {
      tagNames = JSON.parse(new TextDecoder().decode(tagsData));
    } catch {
      tagNames = [];
    }
  }

  // Add tag address if not already in list
  if (!tagNames.includes(input.address)) {
    tagNames.push(input.address);
    kvPut(projectId, `plc.${deviceId}.tags`, JSON.stringify(tagNames));
  }

  // Store detailed tag config
  const tagConfig = {
    address: input.address,
    datatype: input.datatype ?? undefined,
    writable: input.writable ?? false,
    deadbandValue: input.deadbandValue ?? undefined,
    deadbandMaxTime: input.deadbandMaxTime ?? undefined,
    disableRBE: input.disableRBE ?? undefined,
  };
  kvPut(projectId, `plc.${deviceId}.tagconfig.${tagId}`, JSON.stringify(tagConfig));

  log.info(`Upserted tag: ${projectId}/${deviceId}/${tagId}`);

  return {
    id: tagId,
    deviceId,
    ...tagConfig,
  };
}

/**
 * Delete a tag
 */
export async function deleteTag(
  projectId: string,
  deviceId: string,
  tagId: string,
): Promise<boolean> {
  // Get current tags array
  const tagsData = await kvGet(projectId, `plc.${deviceId}.tags`);
  if (tagsData && tagsData.length > 0) {
    try {
      const tagNames: string[] = JSON.parse(new TextDecoder().decode(tagsData));

      // Get the tag config to find the address
      const tagConfig = await getTagConfig(projectId, deviceId, tagId);

      // Remove the tag from the array
      const filtered = tagConfig
        ? tagNames.filter((name) => name !== tagConfig.address)
        : tagNames.filter((name) => name.replace(/\./g, "_") !== tagId);

      kvPut(projectId, `plc.${deviceId}.tags`, JSON.stringify(filtered));
    } catch {
      // Ignore parse errors
    }
  }

  // Delete detailed config
  kvDelete(projectId, `plc.${deviceId}.tagconfig.${tagId}`);

  log.info(`Deleted tag: ${projectId}/${deviceId}/${tagId}`);
  return true;
}

/**
 * Bulk set all tags for a device (replaces existing)
 */
export async function setDeviceTags(
  projectId: string,
  deviceId: string,
  tagAddresses: string[],
): Promise<string[]> {
  await ensureBucket(projectId);

  kvPut(projectId, `plc.${deviceId}.tags`, JSON.stringify(tagAddresses));

  log.info(`Set ${tagAddresses.length} tags for ${projectId}/${deviceId}`);
  return tagAddresses;
}
