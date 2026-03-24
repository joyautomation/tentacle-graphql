/**
 * Store & Forward status module
 * Queries tentacle-mqtt for store-forward buffer state via NATS request/reply.
 */

import type { NatsConnection } from "@nats-io/transport-deno";
import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("store-forward", LogLevel.info);

export type StoreForwardStatus = {
  primaryHostId: string | null;
  primaryHostOnline: boolean;
  bufferedRecords: number;
  bufferSizeBytes: number;
  bufferCapacityRecords: number;
  bufferCapacityBytes: number;
  bufferUsedPercentRecords: number;
  bufferUsedPercentBytes: number;
  draining: boolean;
  drainProgress: number;
  drainRecordsRemaining: number;
  drainTotalRecords: number;
  drainEtaSeconds: number;
  drainStartedAt: number | null;
  totalBuffered: number;
  totalDrained: number;
  totalEvicted: number;
  publishRate: number;
  timeline: Array<{ timestamp: number; state: string }>;
};

export async function requestStoreForwardStatus(
  nc: NatsConnection,
): Promise<StoreForwardStatus | null> {
  try {
    const response = await nc.request(
      "mqtt.store-forward",
      new Uint8Array(0),
      { timeout: 3000 },
    );

    const data = JSON.parse(new TextDecoder().decode(response.data));
    if (data.error) {
      log.warn(`Store-forward status error: ${data.error}`);
      return null;
    }
    return data as StoreForwardStatus;
  } catch {
    // Store-forward not available (mqtt not running or feature disabled)
    return null;
  }
}
