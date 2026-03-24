/**
 * Service Config Module
 * Reads/writes service configuration via NATS KV tentacle_config bucket.
 */

import type { NatsConnection } from "@nats-io/transport-deno";
import { jetstream } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("service-config", LogLevel.info);

const CONFIG_BUCKET = "tentacle_config";

export type ConfigEntry = {
  key: string;
  envVar: string;
  value: string;
  moduleId: string;
};

let configKv: Awaited<ReturnType<Kvm["create"]>> | null = null;

export async function initConfigKv(nc: NatsConnection): Promise<void> {
  try {
    const js = jetstream(nc);
    const kvm = new Kvm(js);
    configKv = await kvm.create(CONFIG_BUCKET, {
      history: 5,
      ttl: 0,
    });
    log.info("Config KV bucket ready");
  } catch (err) {
    log.warn(`Failed to initialize config KV: ${err}`);
  }
}

/**
 * Get all config entries for a service module
 */
export async function getServiceConfig(moduleId: string): Promise<ConfigEntry[]> {
  if (!configKv) return [];

  const entries: ConfigEntry[] = [];
  const decoder = new TextDecoder();
  const prefix = `${moduleId}.`;

  try {
    const keys = await configKv.keys();
    for await (const key of keys) {
      if (!key.startsWith(prefix)) continue;
      try {
        const entry = await configKv.get(key);
        if (entry?.value) {
          const envVar = key.slice(prefix.length);
          entries.push({
            key,
            envVar,
            value: decoder.decode(entry.value),
            moduleId,
          });
        }
      } catch { /* skip */ }
    }
  } catch { /* bucket may not exist yet */ }

  return entries;
}

/**
 * Get all config entries across all modules
 */
export async function getAllConfig(): Promise<ConfigEntry[]> {
  if (!configKv) return [];

  const entries: ConfigEntry[] = [];
  const decoder = new TextDecoder();

  try {
    const keys = await configKv.keys();
    for await (const key of keys) {
      try {
        const entry = await configKv.get(key);
        if (entry?.value) {
          const dotIdx = key.indexOf(".");
          const moduleId = dotIdx > 0 ? key.slice(0, dotIdx) : "unknown";
          const envVar = dotIdx > 0 ? key.slice(dotIdx + 1) : key;
          entries.push({
            key,
            envVar,
            value: decoder.decode(entry.value),
            moduleId,
          });
        }
      } catch { /* skip */ }
    }
  } catch { /* bucket may not exist yet */ }

  return entries;
}

/**
 * Update a config value
 */
export async function updateConfigValue(
  moduleId: string,
  envVar: string,
  value: string,
): Promise<ConfigEntry> {
  if (!configKv) {
    throw new Error("Config KV not initialized");
  }

  const key = `${moduleId}.${envVar}`;
  const encoder = new TextEncoder();
  await configKv.put(key, encoder.encode(value));

  log.info(`Config updated: ${key} = ${value}`);

  return { key, envVar, value, moduleId };
}
