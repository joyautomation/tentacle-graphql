import { connect, type NatsConnection } from "@nats-io/transport-deno";
import { jetstream, type JetStreamClient } from "@nats-io/jetstream";
import { Kvm, type KV } from "@nats-io/kv";
import { createLogger, LogLevel } from "@joyautomation/coral";
import type { PlcVariableKV, ServiceHeartbeat, ServiceEnabledKV, BrowseProgressMessage } from "@tentacle/nats-schema";
import { NATS_TOPICS, NATS_SUBSCRIPTIONS, substituteTopic, isBrowseProgressMessage } from "@tentacle/nats-schema";
import type { NatsConfig } from "../types/config.ts";

const log = createLogger("graphql-nats", LogLevel.info);

let connection: NatsConnection | null = null;
let jsClient: JetStreamClient | null = null;
let heartbeatsKv: KV | null = null;
let serviceEnabledKv: KV | null = null;

export async function connectToNats(config: NatsConfig): Promise<void> {
  if (connection) {
    return; // Already connected
  }

  connection = await connect({
    servers: config.servers,
    user: config.user,
    pass: config.pass,
    token: config.token,
  });

  jsClient = jetstream(connection);

  // Initialize KV buckets (or create if they don't exist)
  const kvm = new Kvm(jsClient);
  heartbeatsKv = await kvm.create("service_heartbeats", {
    history: 1,
    ttl: 60 * 1000, // 1 minute TTL
  });

  serviceEnabledKv = await kvm.create("service_enabled", {
    history: 1,
    ttl: 0, // No expiration — persists until explicitly changed
  });

  log.info(`Connected to NATS at ${config.servers}`);
}

export function getNatsConnection(): NatsConnection {
  if (!connection) {
    throw new Error("NATS not connected. Call connectToNats() first.");
  }
  return connection;
}

export function getJetStreamClient(): JetStreamClient {
  if (!jsClient) {
    throw new Error("JetStream not initialized. Call connectToNats() first.");
  }
  return jsClient;
}

/**
 * Get all heartbeats across all modules
 */
export async function getAllHeartbeats(): Promise<ServiceHeartbeat[]> {
  if (!heartbeatsKv) {
    return [];
  }

  const heartbeats: ServiceHeartbeat[] = [];
  const decoder = new TextDecoder();

  try {
    const keys = await heartbeatsKv.keys();
    for await (const key of keys) {
      try {
        const entry = await heartbeatsKv.get(key);
        if (entry?.value) {
          const heartbeat = JSON.parse(decoder.decode(entry.value)) as ServiceHeartbeat;
          heartbeats.push(heartbeat);
        }
      } catch {
        // Entry may have expired or failed to parse
      }
    }
  } catch {
    // KV bucket may not exist yet
  }

  return heartbeats;
}

/**
 * Get a single module's heartbeat from KV
 */
export async function getHeartbeat(moduleId: string): Promise<ServiceHeartbeat | null> {
  if (!heartbeatsKv) return null;

  const decoder = new TextDecoder();
  try {
    const entry = await heartbeatsKv.get(moduleId);
    if (entry?.value) {
      return JSON.parse(decoder.decode(entry.value)) as ServiceHeartbeat;
    }
  } catch {
    // Entry may have expired or doesn't exist
  }
  return null;
}

/**
 * Get the enabled state for a specific module.
 * Returns true if no explicit state is set (enabled by default).
 */
export async function getServiceEnabled(moduleId: string): Promise<boolean> {
  if (!serviceEnabledKv) return true;

  const decoder = new TextDecoder();
  try {
    const entry = await serviceEnabledKv.get(moduleId);
    if (entry?.value) {
      const state = JSON.parse(decoder.decode(entry.value)) as ServiceEnabledKV;
      return state.enabled;
    }
  } catch {
    // Key doesn't exist = enabled by default
  }
  return true;
}

/**
 * Get enabled state for all modules that have an explicit entry.
 * Returns a map of moduleId → enabled.
 */
export async function getAllServiceEnabled(): Promise<Map<string, boolean>> {
  const result = new Map<string, boolean>();
  if (!serviceEnabledKv) return result;

  const decoder = new TextDecoder();
  try {
    const keys = await serviceEnabledKv.keys();
    for await (const key of keys) {
      try {
        const entry = await serviceEnabledKv.get(key);
        if (entry?.value) {
          const state = JSON.parse(decoder.decode(entry.value)) as ServiceEnabledKV;
          result.set(state.moduleId, state.enabled);
        }
      } catch {
        // Entry may have been deleted
      }
    }
  } catch {
    // KV bucket may not exist yet
  }
  return result;
}

/**
 * Set the enabled state for a service module.
 */
export async function setServiceEnabled(moduleId: string, enabled: boolean): Promise<ServiceEnabledKV> {
  if (!serviceEnabledKv) {
    throw new Error("Service enabled KV not initialized. NATS not connected.");
  }

  const state: ServiceEnabledKV = {
    moduleId,
    enabled,
    updatedAt: Date.now(),
  };

  const encoder = new TextEncoder();
  await serviceEnabledKv.put(moduleId, encoder.encode(JSON.stringify(state)));
  log.info(`Set service ${moduleId} enabled=${enabled}`);

  return state;
}

/**
 * Publish a shutdown command to a module via NATS
 */
export function publishShutdown(moduleId: string): void {
  const nc = getNatsConnection();
  const subject = `${moduleId}.shutdown`;
  nc.publish(subject, new Uint8Array(0));
  log.info(`Published shutdown command to ${subject}`);
}

/**
 * Normalize KV data to full PlcVariableKV schema
 */
function normalizeVariable(
  data: Record<string, unknown>,
  variableId: string,
): PlcVariableKV {
  const result: PlcVariableKV & { cipType?: string; udtType?: string } = {
    moduleId: (data.moduleId as string) || "unknown",
    deviceId: data.deviceId as string | undefined,
    variableId: (data.variableId as string) || variableId,
    value: "value" in data && data.value !== null ? (data.value as string | number | boolean | Record<string, unknown>) : 0,
    datatype: (data.datatype as PlcVariableKV["datatype"]) || "number",
    lastUpdated: (data.lastUpdated as number) || Date.now(),
    origin: (data.origin as PlcVariableKV["origin"]) || "plc",
    quality: (data.quality as PlcVariableKV["quality"]) || "good",
    source: data.source as string | undefined,
    deadband: data.deadband as PlcVariableKV["deadband"],
    disableRBE: data.disableRBE as boolean | undefined,
  };
  if (data.cipType) result.cipType = data.cipType as string;
  // UDT type name — from EIP scanner's structType field, or from tentacle-plc's udtTemplate
  if (data.structType) {
    result.udtType = data.structType as string;
  } else if (data.udtTemplate && typeof data.udtTemplate === "object") {
    result.udtType = (data.udtTemplate as { name?: string }).name;
  }
  return result;
}

/**
 * Get a specific variable using NATS request/reply
 */
export async function getVariable(
  variableId: string,
): Promise<PlcVariableKV | null> {
  try {
    const variables = await listVariables();
    return variables.find((v) => v.variableId === variableId) || null;
  } catch {
    return null;
  }
}

/**
 * List all variables using NATS request/reply
 * Sends a request to tentacle-ethernetip which responds with current poll list
 */
export async function listVariables(moduleId?: string): Promise<PlcVariableKV[]> {
  const nc = getNatsConnection();

  if (moduleId) {
    // Query a specific module
    return await requestModuleVariables(nc, moduleId);
  }

  // No moduleId specified — query all PLC-type services for their variables
  const heartbeats = await getAllHeartbeats();
  const plcModules = heartbeats.filter(h => h.serviceType === "plc");

  if (plcModules.length === 0) {
    // Fallback: try ethernetip directly
    return await requestModuleVariables(nc, "ethernetip");
  }

  // Query all PLC modules in parallel and merge results
  const results = await Promise.allSettled(
    plcModules.map(h => requestModuleVariables(nc, h.moduleId))
  );

  const allVars: PlcVariableKV[] = [];
  for (const result of results) {
    if (result.status === "fulfilled") {
      allVars.push(...result.value);
    }
  }
  return allVars;
}

async function requestModuleVariables(nc: NatsConnection, moduleId: string): Promise<PlcVariableKV[]> {
  try {
    const subject = substituteTopic(NATS_TOPICS.module.variables, { moduleId });
    const response = await nc.request(subject, new Uint8Array(0), { timeout: 5000 });

    if (response.data && response.data.length > 0) {
      const variables = JSON.parse(new TextDecoder().decode(response.data));
      return variables.map((v: Record<string, unknown>) =>
        normalizeVariable(v, v.variableId as string)
      );
    }
    return [];
  } catch (error) {
    log.debug(`No response for variables from ${moduleId}:`, error);
    return [];
  }
}

/**
 * Subscribe to variable updates using pub/sub.
 * When moduleId is provided, subscribes to {moduleId}.data.> for that module only.
 * Otherwise subscribes to *.data.> for all modules.
 */
export async function subscribeToVariableUpdates(moduleId?: string): Promise<AsyncIterable<PlcVariableKV>> {
  const nc = getNatsConnection();
  const subject = moduleId
    ? NATS_SUBSCRIPTIONS.allModuleData(moduleId)
    : NATS_SUBSCRIPTIONS.allData();

  const sub = nc.subscribe(subject);

  return {
    [Symbol.asyncIterator]: async function* () {
      try {
        for await (const msg of sub) {
          try {
            const data = msg.data;
            if (data && data.length > 0) {
              const rawData = JSON.parse(new TextDecoder().decode(data));
              // Extract moduleId from subject: {moduleId}.data.{variableId}
              const subjectParts = msg.subject.split(".");
              const sourceModuleId = subjectParts[0];
              const variableId = rawData.variableId as string;
              if (!rawData.moduleId) {
                rawData.moduleId = sourceModuleId;
              }
              const variable = normalizeVariable(rawData, variableId);
              yield variable;
            }
          } catch (error) {
            log.warn("Error parsing variable from subscription:", error);
          }
        }
      } catch (error) {
        log.warn("Error in variable subscription:", error);
      } finally {
        sub.unsubscribe();
      }
    },
  };
}

/**
 * Publish a command to update a variable on a specific module
 */
export async function publishCommand(
  moduleId: string,
  variableId: string,
  value: unknown,
): Promise<void> {
  const nc = getNatsConnection();
  const subject = substituteTopic(NATS_TOPICS.module.command, { moduleId, variableId });
  const payload = JSON.stringify(value);

  nc.publish(subject, payload);
  log.debug(`Published command to ${subject}:`, value);
}

/**
 * Browse result type for async mode
 */
export type BrowseResult = {
  browseId: string;
  variables?: PlcVariableKV[];
};

/**
 * Trigger a browse operation on PLC(s) to discover available tags
 */
export async function browseTags(
  plcId?: string,
  async?: boolean,
): Promise<BrowseResult> {
  try {
    const nc = getNatsConnection();
    const subject = NATS_TOPICS.ethernetip.browse;

    const payload: Record<string, unknown> = {};
    if (plcId) payload.plcId = plcId;
    if (async) payload.async = true;

    const requestData = Object.keys(payload).length > 0
      ? new TextEncoder().encode(JSON.stringify(payload))
      : new Uint8Array(0);

    log.info(`Requesting browse${plcId ? ` (PLC: ${plcId})` : ""}${async ? " (async)" : ""}`);

    const timeout = async ? 5000 : 120000;

    const response = await nc.request(subject, requestData, { timeout });

    if (response.data && response.data.length > 0) {
      const data = JSON.parse(new TextDecoder().decode(response.data));

      if (async && data.browseId) {
        log.info(`Async browse started with ID: ${data.browseId}`);
        return { browseId: data.browseId };
      }

      if (Array.isArray(data)) {
        log.info(`Browse complete: ${data.length} tags discovered`);
        const variables = data.map((v: Record<string, unknown>) =>
          normalizeVariable(v, v.variableId as string)
        );
        return { browseId: crypto.randomUUID(), variables };
      }
    }

    return { browseId: crypto.randomUUID(), variables: [] };
  } catch (error) {
    log.error(`Browse request failed:`, error);
    throw new Error(`Browse failed: ${error}`);
  }
}

/**
 * Subscribe tags to be polled by the scanner
 */
export async function subscribeTags(
  tags: string[],
  subscriberId: string,
): Promise<{ success: boolean; count: number }> {
  try {
    const nc = getNatsConnection();
    const subject = NATS_TOPICS.ethernetip.subscribe;
    const payload = JSON.stringify({ tags, subscriberId });

    const response = await nc.request(
      subject,
      new TextEncoder().encode(payload),
      { timeout: 5000 },
    );

    if (response.data && response.data.length > 0) {
      return JSON.parse(new TextDecoder().decode(response.data));
    }

    return { success: false, count: 0 };
  } catch (error) {
    log.error(`Subscribe request failed:`, error);
    throw new Error(`Subscribe failed: ${error}`);
  }
}

/**
 * Unsubscribe tags from polling
 */
export async function unsubscribeTags(
  tags: string[],
  subscriberId: string,
): Promise<{ success: boolean; count: number }> {
  try {
    const nc = getNatsConnection();
    const subject = NATS_TOPICS.ethernetip.unsubscribe;
    const payload = JSON.stringify({ tags, subscriberId });

    const response = await nc.request(
      subject,
      new TextEncoder().encode(payload),
      { timeout: 5000 },
    );

    if (response.data && response.data.length > 0) {
      return JSON.parse(new TextDecoder().decode(response.data));
    }

    return { success: false, count: 0 };
  } catch (error) {
    log.error(`Unsubscribe request failed:`, error);
    throw new Error(`Unsubscribe failed: ${error}`);
  }
}

/**
 * Subscribe to browse progress updates for a specific browse operation
 */
export async function subscribeToBrowseProgress(
  browseId: string,
): Promise<AsyncIterable<BrowseProgressMessage>> {
  const nc = getNatsConnection();
  const subject = substituteTopic(NATS_TOPICS.ethernetip.browseProgress, { browseId });

  log.info(`Subscribing to browse progress on ${subject}`);
  const sub = nc.subscribe(subject);

  return {
    [Symbol.asyncIterator]: async function* () {
      try {
        for await (const msg of sub) {
          try {
            const data = msg.data;
            if (data && data.length > 0) {
              const rawData = JSON.parse(new TextDecoder().decode(data));
              if (isBrowseProgressMessage(rawData)) {
                yield rawData;
                if (rawData.phase === "completed" || rawData.phase === "failed") {
                  break;
                }
              }
            }
          } catch (error) {
            log.warn("Error parsing browse progress:", error);
          }
        }
      } catch (error) {
        log.warn("Error in browse progress subscription:", error);
      } finally {
        sub.unsubscribe();
        log.info(`Unsubscribed from browse progress ${browseId}`);
      }
    },
  };
}

/**
 * Disconnect from NATS
 */
export async function disconnectNats(): Promise<void> {
  if (connection) {
    await connection.close();
    connection = null;
    jsClient = null;
    log.info("Disconnected from NATS");
  }
}
