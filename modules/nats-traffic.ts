/**
 * NATS Traffic Collector
 *
 * Subscribes to all NATS subjects (>), maintains a ring buffer,
 * and provides async iterables for GraphQL subscriptions.
 * Filters out noisy internal subjects (JetStream, KV, logs).
 */

import type { NatsConnection, Subscription } from "@nats-io/transport-deno";
import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("graphql:nats-traffic", LogLevel.info);

const RING_BUFFER_SIZE = 200;
const MAX_DISPLAY_LINES = 500;
const MAX_PAYLOAD_PREVIEW = 500;

// ═══════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════

export interface NatsTrafficEntry {
  timestamp: number;
  subject: string;
  size: number;
  payload: string;
}

// ═══════════════════════════════════════════════════════════════════════════
// State
// ═══════════════════════════════════════════════════════════════════════════

const trafficBuffer: NatsTrafficEntry[] = [];

type TrafficSubscriber = (entry: NatsTrafficEntry) => void;
const subscribers = new Set<TrafficSubscriber>();

let globalSubscription: Subscription | null = null;

// Subjects to ignore (noisy internal traffic)
const IGNORED_PREFIXES = [
  "$KV.",
  "$JS.",
  "_INBOX.",
];

function shouldIgnore(subject: string): boolean {
  return IGNORED_PREFIXES.some((prefix) => subject.startsWith(prefix));
}

/**
 * Glob-style filter: * and > both mean "any characters", everything else is literal.
 * Plain text (no wildcards) does a substring match.
 */
function matchesFilter(subject: string, filter: string): boolean {
  if (!filter.includes("*") && !filter.includes(">")) {
    return subject.toLowerCase().includes(filter.toLowerCase());
  }
  // Convert glob to regex: escape regex chars, then replace * and > with .*
  const escaped = filter.replace(/[.+?^${}()|[\]\\]/g, "\\$&");
  const regexStr = escaped.replace(/[*>]/g, ".*");
  return new RegExp(regexStr, "i").test(subject);
}

// ═══════════════════════════════════════════════════════════════════════════
// Core
// ═══════════════════════════════════════════════════════════════════════════

function addToBuffer(entry: NatsTrafficEntry): void {
  trafficBuffer.push(entry);
  if (trafficBuffer.length > RING_BUFFER_SIZE) {
    trafficBuffer.shift();
  }
}

function notifySubscribers(entry: NatsTrafficEntry): void {
  for (const callback of subscribers) {
    try {
      callback(entry);
    } catch {
      // Don't let one bad subscriber break others
    }
  }
}

/**
 * Initialize the traffic collector. Call once after NATS connects.
 */
export function initNatsTrafficCollector(nc: NatsConnection): void {
  if (globalSubscription) {
    return;
  }

  const sub = nc.subscribe(">");
  globalSubscription = sub;

  log.info("NATS traffic collector started (subscribing to >)");

  const decoder = new TextDecoder();

  (async () => {
    for await (const msg of sub) {
      try {
        if (shouldIgnore(msg.subject)) continue;

        const rawPayload = msg.data;
        const size = rawPayload?.length ?? 0;

        let payload = "";
        if (rawPayload && rawPayload.length > 0) {
          try {
            const decoded = decoder.decode(rawPayload);
            payload = decoded.length > MAX_PAYLOAD_PREVIEW
              ? decoded.slice(0, MAX_PAYLOAD_PREVIEW) + "..."
              : decoded;
          } catch {
            payload = `<binary ${size} bytes>`;
          }
        }

        const entry: NatsTrafficEntry = {
          timestamp: Date.now(),
          subject: msg.subject,
          size,
          payload,
        };

        addToBuffer(entry);
        notifySubscribers(entry);
      } catch {
        // Ignore malformed messages
      }
    }
  })();
}

/**
 * Get recent traffic entries from the ring buffer.
 */
export function getRecentTraffic(limit?: number): NatsTrafficEntry[] {
  const maxLines = Math.min(limit ?? MAX_DISPLAY_LINES, MAX_DISPLAY_LINES);
  return trafficBuffer.slice(-maxLines);
}

/**
 * Subscribe to real-time NATS traffic.
 * Optional filter is a NATS subject pattern (e.g., "ethernetip.*", "*.data.>").
 */
export async function* subscribeToNatsTraffic(
  filter?: string,
): AsyncGenerator<NatsTrafficEntry> {
  const queue: NatsTrafficEntry[] = [];
  let resolve: (() => void) | null = null;

  const callback: TrafficSubscriber = (entry) => {
    // Apply client-side filter if provided
    if (filter && !matchesFilter(entry.subject, filter)) {
      return;
    }
    queue.push(entry);
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
 * Stop the traffic collector.
 */
export function stopNatsTrafficCollector(): void {
  if (globalSubscription) {
    globalSubscription.unsubscribe();
    globalSubscription = null;
  }
  trafficBuffer.length = 0;
  subscribers.clear();
}
