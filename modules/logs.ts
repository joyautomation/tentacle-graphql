/**
 * Service Log Collector
 *
 * Subscribes to service.logs.> on NATS, maintains a ring buffer per service type,
 * and provides async iterables for GraphQL subscriptions.
 */

import type { NatsConnection, Subscription } from "@nats-io/transport-deno";
import type { ServiceLogEntry } from "@tentacle/nats-schema";
import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("graphql:logs", LogLevel.info);

const RING_BUFFER_SIZE = 200;
const MAX_DISPLAY_LINES = 500;

// Ring buffer: serviceType -> recent log entries
const logBuffers = new Map<string, ServiceLogEntry[]>();

// Active subscribers waiting for new log entries
type LogSubscriber = (entry: ServiceLogEntry) => void;
const subscribers = new Map<string, Set<LogSubscriber>>();

let globalSubscription: Subscription | null = null;

function addToBuffer(serviceType: string, entry: ServiceLogEntry): void {
  let buffer = logBuffers.get(serviceType);
  if (!buffer) {
    buffer = [];
    logBuffers.set(serviceType, buffer);
  }
  buffer.push(entry);
  if (buffer.length > RING_BUFFER_SIZE) {
    buffer.shift();
  }
}

function notifySubscribers(serviceType: string, entry: ServiceLogEntry): void {
  const subs = subscribers.get(serviceType);
  if (subs) {
    for (const callback of subs) {
      try {
        callback(entry);
      } catch {
        // Don't let one bad subscriber break others
      }
    }
  }
}

/**
 * Initialize the log collector. Call once after NATS connects.
 * Subscribes to service.logs.> and populates ring buffers.
 */
export async function initLogCollector(nc: NatsConnection): Promise<void> {
  if (globalSubscription) {
    return; // Already initialized
  }

  const sub = nc.subscribe("service.logs.>");
  globalSubscription = sub;

  log.info("Log collector started (subscribing to service.logs.>)");

  // Process messages in the background
  (async () => {
    const decoder = new TextDecoder();
    for await (const msg of sub) {
      try {
        const entry: ServiceLogEntry = JSON.parse(decoder.decode(msg.data));
        addToBuffer(entry.serviceType, entry);
        notifySubscribers(entry.serviceType, entry);
      } catch {
        // Ignore malformed log entries
      }
    }
  })();
}

/**
 * Get recent log entries for a service type from the ring buffer.
 */
export function getRecentLogs(
  serviceType: string,
  limit?: number,
): ServiceLogEntry[] {
  const buffer = logBuffers.get(serviceType) ?? [];
  const maxLines = Math.min(limit ?? MAX_DISPLAY_LINES, MAX_DISPLAY_LINES);
  return buffer.slice(-maxLines);
}

/**
 * Subscribe to real-time log entries for a service type.
 * Returns an async iterable suitable for GraphQL subscriptions.
 */
export async function* subscribeToServiceLogs(
  serviceType: string,
): AsyncGenerator<ServiceLogEntry> {
  // Queue to bridge callback-based subscriber to async generator
  const queue: ServiceLogEntry[] = [];
  let resolve: (() => void) | null = null;

  const callback: LogSubscriber = (entry) => {
    queue.push(entry);
    if (resolve) {
      resolve();
      resolve = null;
    }
  };

  // Register subscriber
  let subs = subscribers.get(serviceType);
  if (!subs) {
    subs = new Set();
    subscribers.set(serviceType, subs);
  }
  subs.add(callback);

  try {
    while (true) {
      if (queue.length > 0) {
        yield queue.shift()!;
      } else {
        // Wait for next entry
        await new Promise<void>((r) => {
          resolve = r;
        });
      }
    }
  } finally {
    // Cleanup on unsubscribe
    subs.delete(callback);
    if (subs.size === 0) {
      subscribers.delete(serviceType);
    }
  }
}

/**
 * Stop the log collector.
 */
export function stopLogCollector(): void {
  if (globalSubscription) {
    globalSubscription.unsubscribe();
    globalSubscription = null;
  }
  logBuffers.clear();
  subscribers.clear();
}
