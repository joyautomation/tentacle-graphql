/**
 * Orchestrator request/reply helpers
 *
 * Sends NATS requests to tentacle-orchestrator for module registry,
 * internet connectivity, and version information.
 */

import type { NatsConnection } from "@nats-io/transport-deno";
import type {
  OrchestratorCommandRequest,
  OrchestratorCommandResponse,
  ModuleRegistryInfo,
  ModuleVersionInfo,
} from "@tentacle/nats-schema";
import { NATS_TOPICS } from "@tentacle/nats-schema";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Request the module registry from the orchestrator.
 * Returns the list of all modules the orchestrator knows about.
 */
export async function requestModuleRegistry(
  nc: NatsConnection,
): Promise<ModuleRegistryInfo[]> {
  const request: OrchestratorCommandRequest = {
    requestId: crypto.randomUUID(),
    action: "get-registry",
    timestamp: Date.now(),
  };
  const msg = await nc.request(
    NATS_TOPICS.orchestrator.command,
    encoder.encode(JSON.stringify(request)),
    { timeout: 3000 },
  );
  const response: OrchestratorCommandResponse = JSON.parse(
    decoder.decode(msg.data),
  );
  if (!response.success) {
    throw new Error(response.error ?? "Failed to get module registry");
  }
  return response.modules ?? [];
}

/**
 * Check if the server has internet connectivity (can reach GitHub).
 */
export async function requestInternetCheck(
  nc: NatsConnection,
): Promise<boolean> {
  const request: OrchestratorCommandRequest = {
    requestId: crypto.randomUUID(),
    action: "check-internet",
    timestamp: Date.now(),
  };
  const msg = await nc.request(
    NATS_TOPICS.orchestrator.command,
    encoder.encode(JSON.stringify(request)),
    { timeout: 10000 },
  );
  const response: OrchestratorCommandResponse = JSON.parse(
    decoder.decode(msg.data),
  );
  if (!response.success) {
    throw new Error(response.error ?? "Failed to check internet");
  }
  return response.online ?? false;
}

/**
 * Request version information for a specific module.
 * Returns installed versions, active version, and latest available from GitHub.
 */
export async function requestModuleVersions(
  nc: NatsConnection,
  moduleId: string,
): Promise<ModuleVersionInfo> {
  const request: OrchestratorCommandRequest = {
    requestId: crypto.randomUUID(),
    action: "get-module-versions",
    moduleId,
    timestamp: Date.now(),
  };
  const msg = await nc.request(
    NATS_TOPICS.orchestrator.command,
    encoder.encode(JSON.stringify(request)),
    { timeout: 15000 },
  );
  const response: OrchestratorCommandResponse = JSON.parse(
    decoder.decode(msg.data),
  );
  if (!response.success) {
    throw new Error(response.error ?? "Failed to get module versions");
  }
  return response.versions ?? {
    moduleId,
    installedVersions: [],
    latestVersion: null,
    activeVersion: null,
  };
}
