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
 * List all available projects by discovering KV buckets
 */
export async function listProjects(): Promise<string[]> {
  const js = getJetStreamClient();
  const jsm = await js.jetstreamManager();

  const projects: string[] = [];

  try {
    const streamsList = await jsm.streams.list().next();
    for (const streamInfo of streamsList) {
      const streamName = streamInfo.config.name;
      // tentacle-plc uses $KV_ prefix (non-standard)
      if (streamName?.startsWith("$KV_plc-variables-")) {
        const projectId = streamName.replace("$KV_plc-variables-", "");
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
    value: (data.value ?? data) as string | number | boolean | Record<string, unknown>,
    datatype: (data.datatype as PlcVariableKV["datatype"]) || "unknown",
    lastUpdated: (data.lastUpdated as number) || Date.now(),
    source: (data.source as PlcVariableKV["source"]) || "plc",
    quality: (data.quality as PlcVariableKV["quality"]) || "good",
    deadband: data.deadband as PlcVariableKV["deadband"],
    disableRBE: data.disableRBE as boolean | undefined,
  };
}

/**
 * Get a specific variable from KV
 */
export async function getVariable(
  projectId: string,
  variableId: string,
): Promise<PlcVariableKV | null> {
  try {
    const js = getJetStreamClient();
    const jsm = await js.jetstreamManager();
    const bucketName = `plc-variables-${projectId}`;
    const streamName = `$KV_${bucketName}`;
    const subjectPrefix = `$KV.${bucketName}`;

    const subject = `${subjectPrefix}.${variableId}`;
    const msg = await jsm.streams.getMessage(streamName, {
      last_by_subj: subject,
    });

    if (msg && msg.data && msg.data.length > 0) {
      const rawData = JSON.parse(new TextDecoder().decode(msg.data));
      return normalizeVariable(rawData, projectId, variableId);
    }
    return null;
  } catch {
    return null;
  }
}

/**
 * List all variables in a project using subject filtering
 */
export async function listVariables(projectId: string): Promise<PlcVariableKV[]> {
  try {
    const js = getJetStreamClient();
    const jsm = await js.jetstreamManager();
    const bucketName = `plc-variables-${projectId}`;
    const streamName = `$KV_${bucketName}`;
    const subjectPrefix = `$KV.${bucketName}`;

    // Get stream info with subjects filter to find all keys efficiently
    let streamInfo;
    try {
      streamInfo = await jsm.streams.info(streamName, {
        subjects_filter: `${subjectPrefix}.>`,
      });
    } catch {
      return []; // Project doesn't exist
    }

    // Get unique subjects from stream state
    const subjects = streamInfo.state.subjects;
    if (!subjects || Object.keys(subjects).length === 0) {
      return [];
    }

    // Fetch last message for each subject in parallel
    const fetchPromises = Object.keys(subjects).map(async (subject) => {
      try {
        const msg = await jsm.streams.getMessage(streamName, {
          last_by_subj: subject,
        });
        if (msg && msg.data && msg.data.length > 0) {
          const key = subject.replace(`${subjectPrefix}.`, "");
          const rawData = JSON.parse(new TextDecoder().decode(msg.data));
          return normalizeVariable(rawData, projectId, key);
        }
      } catch {
        // Skip if message not found
      }
      return null;
    });

    const results = await Promise.all(fetchPromises);
    return results.filter((v): v is PlcVariableKV => v !== null);
  } catch (error) {
    log.warn(`Failed to list variables for project ${projectId}:`, error);
    return [];
  }
}

/**
 * Subscribe to KV updates for a project using basic NATS subscription
 */
export async function subscribeToKVUpdates(
  projectId: string,
): Promise<AsyncIterable<PlcVariableKV>> {
  const nc = getNatsConnection();
  const bucketName = `plc-variables-${projectId}`;
  const subjectPrefix = `$KV.${bucketName}`;

  // Subscribe to all KV updates for this bucket
  const sub = nc.subscribe(`${subjectPrefix}.>`);

  return {
    [Symbol.asyncIterator]: async function* () {
      try {
        for await (const msg of sub) {
          try {
            const data = msg.data;
            if (data && data.length > 0) {
              const variableId = msg.subject.replace(`${subjectPrefix}.`, "");
              const rawData = JSON.parse(new TextDecoder().decode(data));
              const variable = normalizeVariable(rawData, projectId, variableId);
              yield variable;
            }
          } catch (error) {
            log.warn("Error parsing variable from KV subscription:", error);
          }
        }
      } catch (error) {
        log.warn("Error in KV subscription:", error);
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
