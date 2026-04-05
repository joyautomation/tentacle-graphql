import { connect, type NatsConnection } from "@nats-io/transport-deno";
import { jetstream, type JetStreamClient } from "@nats-io/jetstream";
import { Kvm, type KV } from "@nats-io/kv";
import { createLogger, LogLevel } from "@joyautomation/coral";
import type { PlcVariableKV, ServiceHeartbeat, ServiceEnabledKV, BrowseProgressMessage, DesiredServiceKV, ServiceStatusKV } from "@tentacle/nats-schema";
import { NATS_TOPICS, NATS_SUBSCRIPTIONS, substituteTopic, isBrowseProgressMessage } from "@tentacle/nats-schema";
import type { NatsConfig } from "../types/config.ts";
import { getBrowseState, setBrowseState, cacheBrowseResult } from "../modules/gateway.ts";

const log = createLogger("graphql-nats", LogLevel.info);

let connection: NatsConnection | null = null;
let jsClient: JetStreamClient | null = null;
let heartbeatsKv: KV | null = null;
let serviceEnabledKv: KV | null = null;
let desiredServicesKv: KV | null = null;
let serviceStatusKv: KV | null = null;

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

  desiredServicesKv = await kvm.create("desired_services", {
    history: 1,
    ttl: 0,
  });

  serviceStatusKv = await kvm.create("service_status", {
    history: 1,
    ttl: 120 * 1000, // 2 minutes
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

// ═══════════════════════════════════════════════════════════════════════════
// Gateway Device Browse (ad-hoc connections)
// ═══════════════════════════════════════════════════════════════════════════

export type GatewayBrowseItem = {
  tag: string;
  name: string;
  datatype: string;
  value: unknown;
  protocolType: string;
};

export type GatewayBrowseUdtMember = {
  name: string;
  datatype: string;
  cipType: string;
  udtType: string;
  isArray: boolean;
};

export type GatewayBrowseUdt = {
  name: string;
  members: GatewayBrowseUdtMember[];
};

export type GatewayBrowseResult = {
  deviceId: string;
  protocol: string;
  items: GatewayBrowseItem[];
  udts: GatewayBrowseUdt[];
  structTags: Record<string, string>;
};

/**
 * Browse a device via its scanner using ad-hoc connection params.
 * Routes to the correct NATS topic based on protocol.
 */
export async function browseGatewayDevice(input: {
  deviceId: string;
  protocol: string;
  host?: string;
  port?: number;
  endpointUrl?: string;
  version?: string;
  community?: string;
  rootOid?: string;
  browseId?: string;
}): Promise<GatewayBrowseResult> {
  const nc = getNatsConnection();
  const decoder = new TextDecoder();

  switch (input.protocol) {
    case "snmp": {
      // Map gateway version format ("1","2c","3") to scanner format ("v1","v2c","v3")
      const version = input.version?.startsWith("v") ? input.version : `v${input.version ?? "2c"}`;
      const payload = {
        deviceId: input.deviceId,
        host: input.host ?? "localhost",
        port: input.port ?? 161,
        version,
        community: input.community ?? "public",
        rootOid: input.rootOid ?? ".1.3.6.1",
        ...(input.browseId ? { browseId: input.browseId } : {}),
      };

      log.info(`Gateway browse SNMP device ${input.deviceId} at ${payload.host}:${payload.port}`);
      const response = await nc.request(
        NATS_TOPICS.snmp.browse,
        new TextEncoder().encode(JSON.stringify(payload)),
        { timeout: 120000 },
      );

      if (response.data && response.data.length > 0) {
        const result = JSON.parse(decoder.decode(response.data));
        // SNMP browse returns { deviceId, rootOid, oids: OidInfo[] }
        const oids = result.oids ?? [];
        const items: GatewayBrowseItem[] = oids.map((oid: { oid: string; name?: string; key?: string; value: unknown; snmpType: string; datatype: string }) => ({
          tag: oid.oid,
          name: oid.name || oid.key || oid.oid,
          datatype: oid.datatype,
          value: oid.value,
          protocolType: oid.snmpType,
        }));
        return { deviceId: input.deviceId, protocol: "snmp", items, udts: [], structTags: {} };
      }
      return { deviceId: input.deviceId, protocol: "snmp", items: [], udts: [], structTags: {} };
    }

    case "ethernetip": {
      log.info(`Gateway browse EtherNet/IP device ${input.deviceId} at ${input.host}`);
      const payload: Record<string, unknown> = {
        deviceId: input.deviceId,
        host: input.host ?? "",
        ...(input.port ? { port: input.port } : {}),
        ...(input.browseId ? { browseId: input.browseId } : {}),
      };
      // Scanner responds immediately with { browseId } and runs browse async.
      // We need to subscribe to the result topic before sending the request
      // to avoid a race condition.
      const browseId = input.browseId ?? crypto.randomUUID();
      if (!payload.browseId) payload.browseId = browseId;

      const resultSubject = `ethernetip.browse.result.${browseId}`;
      const resultSub = nc.subscribe(resultSubject, { max: 1 });

      // Send the browse request
      await nc.request(
        NATS_TOPICS.ethernetip.browse,
        new TextEncoder().encode(JSON.stringify(payload)),
        { timeout: 10000 },
      );

      // Wait for the async result with a timeout (browse can take a few minutes for large PLCs)
      const BROWSE_TIMEOUT_MS = 300000; // 5 minutes
      try {
        const result = await Promise.race([
          (async () => {
            for await (const msg of resultSub) {
              if (msg.data && msg.data.length > 0) {
                return JSON.parse(decoder.decode(msg.data));
              }
            }
            return null;
          })(),
          new Promise<null>((_, reject) =>
            setTimeout(() => reject(new Error("Browse timed out")), BROWSE_TIMEOUT_MS)
          ),
        ]);

        if (result) {
          const variables = result.variables;
          const items: GatewayBrowseItem[] = Array.isArray(variables)
            ? variables.map((v: Record<string, unknown>) => ({
                tag: String(v.variableId ?? v.tag ?? ""),
                name: String(v.variableId ?? v.tag ?? ""),
                datatype: String(v.datatype ?? "string"),
                value: v.value,
                protocolType: String(v.cipType ?? ""),
              }))
            : [];

          // Extract UDT templates from browse result
          const rawUdts = result.udts as Record<string, { name: string; members: Array<{ name: string; datatype: string; udtType?: string; isArray?: boolean }> }> | undefined;
          const udts: GatewayBrowseUdt[] = rawUdts
            ? Object.values(rawUdts).map((udt) => ({
                name: udt.name,
                members: (udt.members ?? []).map((m) => ({
                  name: m.name,
                  datatype: m.datatype,
                  cipType: m.cipType ?? "",
                  udtType: m.udtType ?? "",
                  isArray: m.isArray ?? false,
                })),
              }))
            : [];

          // Extract struct tag → UDT type mapping
          const structTags = (result.structTags as Record<string, string>) ?? {};

          return { deviceId: input.deviceId, protocol: "ethernetip", items, udts, structTags };
        }
      } catch (err) {
        log.error(`Browse failed for device ${input.deviceId}: ${err}`);
      } finally {
        resultSub.unsubscribe();
      }
      return { deviceId: input.deviceId, protocol: "ethernetip", items: [], udts: [], structTags: {} };
    }

    default:
      throw new Error(`Browse not yet supported for protocol: ${input.protocol}`);
  }
}

/**
 * Start a non-blocking browse of a gateway device.
 * Writes initial state to KV, spawns a background task that runs the browse
 * and updates KV state as progress comes in.
 */
export async function startGatewayBrowse(input: {
  deviceId: string;
  protocol: string;
  host?: string;
  port?: number;
  endpointUrl?: string;
  version?: string;
  community?: string;
  rootOid?: string;
}): Promise<{ browseId: string; deviceId: string }> {
  // Check for existing active browse on this device
  const existing = await getBrowseState(input.deviceId);
  if (existing && existing.status === "browsing") {
    throw new Error(
      `Browse already in progress for device ${input.deviceId} (browseId: ${existing.browseId})`
    );
  }

  const browseId = crypto.randomUUID();
  const now = Date.now();

  // Write initial browse state to KV
  await setBrowseState({
    deviceId: input.deviceId,
    browseId,
    protocol: input.protocol,
    status: "browsing",
    phase: "connecting",
    discoveredCount: 0,
    totalCount: 0,
    message: "Starting browse...",
    startedAt: now,
    updatedAt: now,
  });

  // Spawn background task (fire-and-forget)
  (async () => {
    const nc = getNatsConnection();
    const progressSubject = `${input.protocol}.browse.progress.${browseId}`;
    const progressSub = nc.subscribe(progressSubject);

    // Background: listen for progress and update KV state
    const progressLoop = (async () => {
      try {
        for await (const msg of progressSub) {
          try {
            if (msg.data && msg.data.length > 0) {
              const raw = JSON.parse(new TextDecoder().decode(msg.data)) as Record<string, unknown>;
              const phase = String(raw.phase ?? "walking");
              const discoveredCount = Number(raw.completedOids ?? raw.completedTags ?? 0);
              const totalCount = Number(raw.totalOids ?? raw.totalTags ?? 0);
              const message = String(raw.message ?? "");

              await setBrowseState({
                deviceId: input.deviceId,
                browseId,
                protocol: input.protocol,
                status: "browsing",
                phase,
                discoveredCount,
                totalCount,
                message,
                startedAt: now,
                updatedAt: Date.now(),
              });

              if (phase === "completed" || phase === "failed") {
                break;
              }
            }
          } catch {
            // Skip unparseable messages
          }
        }
      } catch {
        // Subscription ended
      }
    })();

    try {
      // Run the blocking browse
      const result = await browseGatewayDevice({
        ...input,
        browseId,
      });

      // Cache successful results
      if (result.items.length > 0 || result.udts.length > 0) {
        await cacheBrowseResult(input.deviceId, { ...result, cachedAt: Date.now() });
      }

      // Mark as completed
      await setBrowseState({
        deviceId: input.deviceId,
        browseId,
        protocol: input.protocol,
        status: "completed",
        phase: "completed",
        discoveredCount: result.items.length,
        totalCount: result.items.length,
        message: `Discovered ${result.items.length} tags, ${result.udts.length} UDT types`,
        startedAt: now,
        updatedAt: Date.now(),
      });
    } catch (err) {
      // Mark as failed
      const errorMessage = err instanceof Error ? err.message : String(err);
      log.error(`Background browse failed for ${input.deviceId}: ${errorMessage}`);
      await setBrowseState({
        deviceId: input.deviceId,
        browseId,
        protocol: input.protocol,
        status: "failed",
        phase: "failed",
        discoveredCount: 0,
        totalCount: 0,
        message: errorMessage,
        startedAt: now,
        updatedAt: Date.now(),
        error: errorMessage,
      });
    } finally {
      progressSub.unsubscribe();
      // Wait for the progress loop to finish cleanly
      await progressLoop;
    }
  })();

  return { browseId, deviceId: input.deviceId };
}

export type GatewayBrowseProgress = {
  browseId: string;
  deviceId: string;
  phase: string;
  discoveredCount: number;
  totalCount: number;
  message: string;
  timestamp: number;
};

/**
 * Subscribe to gateway browse progress updates.
 * Normalizes protocol-specific progress formats into a common shape.
 */
export async function subscribeToGatewayBrowseProgress(
  protocol: string,
  browseId: string,
): Promise<AsyncIterable<GatewayBrowseProgress>> {
  const nc = getNatsConnection();
  const subject = `${protocol}.browse.progress.${browseId}`;

  log.info(`Subscribing to gateway browse progress on ${subject}`);
  const sub = nc.subscribe(subject);

  return {
    [Symbol.asyncIterator]: async function* () {
      try {
        for await (const msg of sub) {
          try {
            if (msg.data && msg.data.length > 0) {
              const raw = JSON.parse(new TextDecoder().decode(msg.data)) as Record<string, unknown>;
              const progress: GatewayBrowseProgress = {
                browseId: String(raw.browseId ?? browseId),
                deviceId: String(raw.deviceId ?? ""),
                phase: String(raw.phase ?? "walking"),
                // Normalize: SNMP uses completedOids, EIP uses completedTags
                discoveredCount: Number(raw.completedOids ?? raw.completedTags ?? 0),
                totalCount: Number(raw.totalOids ?? raw.totalTags ?? 0),
                message: String(raw.message ?? ""),
                timestamp: Number(raw.timestamp ?? Date.now()),
              };
              yield progress;
              if (progress.phase === "completed" || progress.phase === "failed") {
                break;
              }
            }
          } catch {
            // Skip unparseable messages
          }
        }
      } finally {
        sub.unsubscribe();
        log.info(`Unsubscribed from gateway browse progress ${browseId}`);
      }
    },
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// Desired Services (orchestrator)
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Get all desired service entries
 */
export async function getAllDesiredServices(): Promise<DesiredServiceKV[]> {
  if (!desiredServicesKv) return [];
  const results: DesiredServiceKV[] = [];
  const decoder = new TextDecoder();
  try {
    const keys = await desiredServicesKv.keys();
    for await (const key of keys) {
      try {
        const entry = await desiredServicesKv.get(key);
        if (entry?.value) {
          results.push(JSON.parse(decoder.decode(entry.value)) as DesiredServiceKV);
        }
      } catch { /* expired or invalid */ }
    }
  } catch { /* bucket may not exist */ }
  return results;
}

/**
 * Set the desired state for a service module
 */
export async function setDesiredService(moduleId: string, version: string, running: boolean): Promise<DesiredServiceKV> {
  if (!desiredServicesKv) {
    throw new Error("Desired services KV not initialized. NATS not connected.");
  }
  const state: DesiredServiceKV = {
    moduleId,
    version,
    running,
    updatedAt: Date.now(),
  };
  const encoder = new TextEncoder();
  await desiredServicesKv.put(moduleId, encoder.encode(JSON.stringify(state)));
  log.info(`Set desired service ${moduleId}: version=${version}, running=${running}`);
  return state;
}

/**
 * Delete a desired service entry
 */
export async function deleteDesiredService(moduleId: string): Promise<boolean> {
  if (!desiredServicesKv) return false;
  try {
    await desiredServicesKv.delete(moduleId);
    log.info(`Deleted desired service: ${moduleId}`);
    return true;
  } catch {
    return false;
  }
}

/**
 * Get all service status entries reported by the orchestrator
 */
export async function getAllServiceStatuses(): Promise<ServiceStatusKV[]> {
  if (!serviceStatusKv) return [];
  const results: ServiceStatusKV[] = [];
  const decoder = new TextDecoder();
  try {
    const keys = await serviceStatusKv.keys();
    for await (const key of keys) {
      try {
        const entry = await serviceStatusKv.get(key);
        if (entry?.value) {
          results.push(JSON.parse(decoder.decode(entry.value)) as ServiceStatusKV);
        }
      } catch { /* expired or invalid */ }
    }
  } catch { /* bucket may not exist */ }
  return results;
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
