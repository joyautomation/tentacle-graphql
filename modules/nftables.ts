/**
 * Nftables Config Collector
 *
 * Subscribes to nftables.rules on NATS for periodic config broadcasts,
 * maintains a ring buffer, and provides request/reply helpers for
 * on-demand state and config commands.
 */

import type { NatsConnection, Subscription } from "@nats-io/transport-deno";
import type {
  NftablesConfig,
  NftablesCommandRequest,
  NftablesCommandResponse,
} from "@tentacle/nats-schema";
import { NATS_TOPICS } from "@tentacle/nats-schema";
import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("graphql:nftables", LogLevel.info);

const RING_BUFFER_SIZE = 50;

// Ring buffer of recent config broadcasts
interface NftablesConfigBroadcast {
  moduleId: string;
  timestamp: number;
  config: NftablesConfig;
}

const configBuffer: NftablesConfigBroadcast[] = [];
let latestConfig: NftablesConfig | null = null;

// Active subscribers for GraphQL subscriptions
type NftablesConfigSubscriber = (config: NftablesConfig) => void;
const subscribers = new Set<NftablesConfigSubscriber>();

let globalSubscription: Subscription | null = null;

function addToBuffer(entry: NftablesConfigBroadcast): void {
  configBuffer.push(entry);
  if (configBuffer.length > RING_BUFFER_SIZE) {
    configBuffer.shift();
  }
}

function notifySubscribers(config: NftablesConfig): void {
  for (const callback of subscribers) {
    try {
      callback(config);
    } catch {
      // Don't let one bad subscriber break others
    }
  }
}

/**
 * Initialize the nftables config collector. Call once after NATS connects.
 * Subscribes to nftables.rules for periodic config broadcasts.
 */
export function initNftablesCollector(nc: NatsConnection): void {
  if (globalSubscription) {
    return;
  }

  const sub = nc.subscribe(NATS_TOPICS.nftables.rules);
  globalSubscription = sub;

  log.info(`Nftables collector started (subscribing to ${NATS_TOPICS.nftables.rules})`);

  (async () => {
    const decoder = new TextDecoder();
    for await (const msg of sub) {
      try {
        const broadcast: NftablesConfigBroadcast = JSON.parse(decoder.decode(msg.data));
        latestConfig = broadcast.config;
        addToBuffer(broadcast);
        notifySubscribers(broadcast.config);
      } catch {
        // Ignore malformed messages
      }
    }
  })();
}

/**
 * Get the latest cached nftables config (from the periodic subscription).
 */
export function getLatestNftablesConfig(): NftablesConfig | null {
  return latestConfig;
}

/**
 * Request live nftables ruleset via NATS request/reply.
 * Returns raw JSON string from `nft -j list ruleset`.
 */
export async function requestNftablesState(
  nc: NatsConnection,
): Promise<string> {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  const msg = await nc.request(
    NATS_TOPICS.nftables.state,
    encoder.encode(JSON.stringify({ timestamp: Date.now() })),
    { timeout: 2000 },
  );

  const response = JSON.parse(decoder.decode(msg.data));
  if (response.error) {
    throw new Error(response.error);
  }
  return response.rawRuleset;
}

/**
 * Request current nftables configuration via NATS request/reply.
 */
export async function requestNftablesConfig(
  nc: NatsConnection,
): Promise<NftablesConfig> {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  const request: NftablesCommandRequest = {
    requestId: crypto.randomUUID(),
    action: "get-config",
    timestamp: Date.now(),
  };

  const msg = await nc.request(
    NATS_TOPICS.nftables.command,
    encoder.encode(JSON.stringify(request)),
    { timeout: 2000 },
  );

  const response: NftablesCommandResponse = JSON.parse(decoder.decode(msg.data));
  if (!response.success) {
    throw new Error(response.error ?? "Failed to get nftables config");
  }
  return response.config ?? { natRules: [] };
}

/**
 * Apply nftables configuration via NATS request/reply.
 */
export async function applyNftablesConfig(
  nc: NatsConnection,
  config: NftablesConfig,
): Promise<NftablesCommandResponse> {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  const request: NftablesCommandRequest = {
    requestId: crypto.randomUUID(),
    action: "apply-config",
    natRules: config.natRules,
    timestamp: Date.now(),
  };

  const msg = await nc.request(
    NATS_TOPICS.nftables.command,
    encoder.encode(JSON.stringify(request)),
    { timeout: 15000 }, // Longer timeout for nft apply
  );

  return JSON.parse(decoder.decode(msg.data));
}

/**
 * Subscribe to real-time nftables config updates.
 * Returns an async iterable suitable for GraphQL subscriptions.
 */
export async function* subscribeToNftablesConfig(): AsyncGenerator<NftablesConfig> {
  const queue: NftablesConfig[] = [];
  let resolve: (() => void) | null = null;

  const callback: NftablesConfigSubscriber = (config) => {
    queue.push(config);
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
 * Stop the nftables collector.
 */
export function stopNftablesCollector(): void {
  if (globalSubscription) {
    globalSubscription.unsubscribe();
    globalSubscription = null;
  }
  configBuffer.length = 0;
  latestConfig = null;
  subscribers.clear();
}
