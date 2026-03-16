/**
 * MQTT Metrics Collector
 *
 * Provides request/reply helper for fetching current MQTT metrics
 * and template definitions from tentacle-mqtt via NATS.
 */

import type { NatsConnection } from "@nats-io/transport-deno";
import type { MqttMetricsResponse } from "@tentacle/nats-schema";
import { NATS_TOPICS } from "@tentacle/nats-schema";

/**
 * Request current MQTT metrics via NATS request/reply.
 */
export async function requestMqttMetrics(
  nc: NatsConnection,
): Promise<MqttMetricsResponse> {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  const msg = await nc.request(
    NATS_TOPICS.mqtt.metrics,
    encoder.encode(JSON.stringify({ timestamp: Date.now() })),
    { timeout: 3000 },
  );

  return JSON.parse(decoder.decode(msg.data));
}
