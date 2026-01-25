import { connect, type NatsConnection } from "@nats-io/transport-deno";
import { jetstream, type JetStreamClient } from "@nats-io/jetstream";
import { createLogger, LogLevel } from "@joyautomation/coral";
import type { PlcVariableKV } from "@tentacle/nats-schema";
import type { NatsConfig } from "../types/config.ts";

const log = createLogger("graphql-nats", LogLevel.info);

let connection: NatsConnection | null = null;
let jsClient: JetStreamClient | null = null;

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
 * List all available projects by discovering config KV buckets
 */
export async function listProjects(): Promise<string[]> {
  const js = getJetStreamClient();
  const jsm = await js.jetstreamManager();

  const projects: string[] = [];

  try {
    const streamsList = await jsm.streams.list().next();
    for (const streamInfo of streamsList) {
      const streamName = streamInfo.config.name;
      // Config KV buckets: KV_field-config-{projectId}
      if (streamName?.startsWith("KV_field-config-")) {
        const projectId = streamName.replace("KV_field-config-", "");
        projects.push(projectId);
      }
    }
  } catch {
    // No streams yet or permission denied
  }

  return projects;
}

/**
 * Normalize KV data to full PlcVariableKV schema
 * Always ensures all required fields have values, filling in defaults where needed
 */
function normalizeVariable(
  data: Record<string, unknown>,
  projectId: string,
  variableId: string,
): PlcVariableKV {
  // Always ensure all required fields have values
  // Even "new format" entries might be missing some fields
  return {
    projectId: (data.projectId as string) || projectId,
    variableId: (data.variableId as string) || variableId,
    value: "value" in data && data.value !== null ? (data.value as string | number | boolean | Record<string, unknown>) : 0,
    datatype: (data.datatype as PlcVariableKV["datatype"]) || "unknown",
    lastUpdated: (data.lastUpdated as number) || Date.now(),
    source: (data.source as PlcVariableKV["source"]) || "plc",
    quality: (data.quality as PlcVariableKV["quality"]) || "good",
    deadband: data.deadband as PlcVariableKV["deadband"],
    disableRBE: data.disableRBE as boolean | undefined,
  };
}

/**
 * Get a specific variable using NATS request/reply
 * Requests all variables and filters for the specific one
 */
export async function getVariable(
  projectId: string,
  variableId: string,
): Promise<PlcVariableKV | null> {
  try {
    const variables = await listVariables(projectId);
    return variables.find((v) => v.variableId === variableId) || null;
  } catch {
    return null;
  }
}

/**
 * List all variables in a project using NATS request/reply
 * Sends a request to tentacle-ethernetip which responds with current poll list
 */
export async function listVariables(projectId: string): Promise<PlcVariableKV[]> {
  try {
    const nc = getNatsConnection();
    const subject = `plc.variables.${projectId}`;

    // Request variables from the scanner service with 5 second timeout
    const response = await nc.request(subject, new Uint8Array(0), { timeout: 5000 });

    if (response.data && response.data.length > 0) {
      const variables = JSON.parse(new TextDecoder().decode(response.data));
      // Normalize the response to match PlcVariableKV schema
      return variables.map((v: Record<string, unknown>) =>
        normalizeVariable(v, projectId, v.variableId as string)
      );
    }

    return [];
  } catch (error) {
    // Request timeout or no responders - scanner might not be running
    log.debug(`No response for variables request on project ${projectId}:`, error);
    return [];
  }
}

/**
 * Delete variables matching a pattern - DEPRECATED
 * Variables are now ephemeral (in-memory only in scanner).
 * When a device is removed from config, the scanner automatically
 * clears its cached values - no explicit cleanup needed.
 */
export async function deleteVariablesByPattern(
  _projectId: string,
  _pattern: string,
): Promise<number> {
  // No-op: Variables are ephemeral and auto-cleaned when device is removed from scanner config
  return 0;
}

/**
 * Subscribe to variable updates for a project using pub/sub
 * Subscribes to plc.data.{projectId} for real-time streaming
 * variableId is in the payload (not subject) because it contains dots (UDT paths)
 */
export async function subscribeToVariableUpdates(
  projectId: string,
): Promise<AsyncIterable<PlcVariableKV>> {
  const nc = getNatsConnection();
  const subject = `plc.data.${projectId}`;

  // Subscribe to variable updates for this project
  const sub = nc.subscribe(subject);

  return {
    [Symbol.asyncIterator]: async function* () {
      try {
        for await (const msg of sub) {
          try {
            const data = msg.data;
            if (data && data.length > 0) {
              const rawData = JSON.parse(new TextDecoder().decode(data));
              // variableId comes from payload, not subject
              const variableId = rawData.variableId as string;
              const variable = normalizeVariable(rawData, projectId, variableId);
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
 * Publish a command to update a variable
 */
export async function publishCommand(
  projectId: string,
  variableId: string,
  value: unknown,
): Promise<void> {
  const nc = getNatsConnection();
  const subject = `${projectId}/${variableId}`;
  const payload = JSON.stringify(value);

  nc.publish(subject, payload);
  log.debug(`Published command to ${subject}:`, value);
}

/**
 * Trigger a browse operation on PLC(s) to discover available tags
 * Returns the list of discovered variables (fills cache for future queries)
 */
export async function browseTags(
  projectId: string,
  plcId?: string,
): Promise<PlcVariableKV[]> {
  try {
    const nc = getNatsConnection();
    const subject = `plc.browse.${projectId}`;

    // Build request payload
    const requestData = plcId ? JSON.stringify({ plcId }) : new Uint8Array(0);

    // Browse can take a while (UDT expansion), use longer timeout
    log.info(`Requesting browse for project ${projectId}${plcId ? ` (PLC: ${plcId})` : ""}`);
    const response = await nc.request(
      subject,
      typeof requestData === "string" ? new TextEncoder().encode(requestData) : requestData,
      { timeout: 60000 }, // 60 second timeout for browse
    );

    if (response.data && response.data.length > 0) {
      const variables = JSON.parse(new TextDecoder().decode(response.data));
      log.info(`Browse complete: ${variables.length} tags discovered`);
      return variables.map((v: Record<string, unknown>) =>
        normalizeVariable(v, projectId, v.variableId as string)
      );
    }

    return [];
  } catch (error) {
    log.error(`Browse request failed for project ${projectId}:`, error);
    throw new Error(`Browse failed: ${error}`);
  }
}

/**
 * Subscribe tags to be polled by the scanner
 */
export async function subscribeTags(
  projectId: string,
  tags: string[],
  subscriberId: string,
): Promise<{ success: boolean; count: number }> {
  try {
    const nc = getNatsConnection();
    const subject = `plc.subscribe.${projectId}`;
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
  projectId: string,
  tags: string[],
  subscriberId: string,
): Promise<{ success: boolean; count: number }> {
  try {
    const nc = getNatsConnection();
    const subject = `plc.unsubscribe.${projectId}`;
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
