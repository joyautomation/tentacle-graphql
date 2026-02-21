/**
 * Network State Collector
 *
 * Subscribes to network.interfaces on NATS for periodic state updates,
 * maintains a ring buffer of snapshots, and provides request/reply helpers
 * for on-demand state and config commands.
 */

import type { NatsConnection, Subscription } from "@nats-io/transport-deno";
import type {
  NetworkStateMessage,
  NetworkInterfaceConfig,
  NetworkCommandRequest,
  NetworkCommandResponse,
} from "@tentacle/nats-schema";
import { NATS_TOPICS } from "@tentacle/nats-schema";
import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("graphql:network", LogLevel.info);

const RING_BUFFER_SIZE = 50;

// Ring buffer of recent network state snapshots
const stateBuffer: NetworkStateMessage[] = [];
let latestState: NetworkStateMessage | null = null;

// Active subscribers for GraphQL subscriptions
type NetworkStateSubscriber = (state: NetworkStateMessage) => void;
const subscribers = new Set<NetworkStateSubscriber>();

let globalSubscription: Subscription | null = null;

function addToBuffer(state: NetworkStateMessage): void {
  stateBuffer.push(state);
  if (stateBuffer.length > RING_BUFFER_SIZE) {
    stateBuffer.shift();
  }
}

function notifySubscribers(state: NetworkStateMessage): void {
  for (const callback of subscribers) {
    try {
      callback(state);
    } catch {
      // Don't let one bad subscriber break others
    }
  }
}

/**
 * Initialize the network state collector. Call once after NATS connects.
 * Subscribes to network.interfaces for periodic state updates.
 */
export function initNetworkCollector(nc: NatsConnection): void {
  if (globalSubscription) {
    return;
  }

  const sub = nc.subscribe(NATS_TOPICS.network.interfaces);
  globalSubscription = sub;

  log.info(`Network collector started (subscribing to ${NATS_TOPICS.network.interfaces})`);

  (async () => {
    const decoder = new TextDecoder();
    for await (const msg of sub) {
      try {
        const state: NetworkStateMessage = JSON.parse(decoder.decode(msg.data));
        latestState = state;
        addToBuffer(state);
        notifySubscribers(state);
      } catch {
        // Ignore malformed messages
      }
    }
  })();
}

/**
 * Get the latest cached network state (from the periodic subscription).
 */
export function getLatestNetworkState(): NetworkStateMessage | null {
  return latestState;
}

/**
 * Request fresh network state via NATS request/reply.
 * Returns live kernel state, not cached data.
 */
export async function requestNetworkState(
  nc: NatsConnection,
): Promise<NetworkStateMessage> {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  const msg = await nc.request(
    NATS_TOPICS.network.state,
    encoder.encode(JSON.stringify({ timestamp: Date.now() })),
    { timeout: 2000 },
  );

  return JSON.parse(decoder.decode(msg.data));
}

/**
 * Request current netplan configuration via NATS request/reply.
 */
export async function requestNetworkConfig(
  nc: NatsConnection,
): Promise<NetworkInterfaceConfig[]> {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  const request: NetworkCommandRequest = {
    requestId: crypto.randomUUID(),
    action: "get-config",
    timestamp: Date.now(),
  };

  const msg = await nc.request(
    NATS_TOPICS.network.command,
    encoder.encode(JSON.stringify(request)),
    { timeout: 2000 },
  );

  const response: NetworkCommandResponse = JSON.parse(decoder.decode(msg.data));
  if (!response.success) {
    throw new Error(response.error ?? "Failed to get network config");
  }
  return response.config ?? [];
}

/**
 * Apply network configuration via NATS request/reply.
 * Sends config to tentacle-network which writes 60-tentacle.yaml and runs netplan apply.
 */
export async function applyNetworkConfig(
  nc: NatsConnection,
  interfaces: NetworkInterfaceConfig[],
): Promise<NetworkCommandResponse> {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  const request: NetworkCommandRequest = {
    requestId: crypto.randomUUID(),
    action: "apply-config",
    interfaces,
    timestamp: Date.now(),
  };

  const msg = await nc.request(
    NATS_TOPICS.network.command,
    encoder.encode(JSON.stringify(request)),
    { timeout: 15000 }, // Longer timeout for netplan apply
  );

  return JSON.parse(decoder.decode(msg.data));
}

/**
 * Subscribe to real-time network state updates.
 * Returns an async iterable suitable for GraphQL subscriptions.
 */
export async function* subscribeToNetworkState(): AsyncGenerator<NetworkStateMessage> {
  const queue: NetworkStateMessage[] = [];
  let resolve: (() => void) | null = null;

  const callback: NetworkStateSubscriber = (state) => {
    queue.push(state);
    if (resolve) {
      resolve();
      resolve = null;
    }
  };

  subscribers.add(callback);

  try {
    while (true) {
      if (queue.length > 0) {
        yield queue.shift()!;
      } else {
        await new Promise<void>((r) => {
          resolve = r;
        });
      }
    }
  } finally {
    subscribers.delete(callback);
  }
}

/**
 * Stop the network collector.
 */
export function stopNetworkCollector(): void {
  if (globalSubscription) {
    globalSubscription.unsubscribe();
    globalSubscription = null;
  }
  stateBuffer.length = 0;
  latestState = null;
  subscribers.clear();
}
