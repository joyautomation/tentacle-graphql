import { createLogger, LogLevel, type Log } from "@joyautomation/coral";
import { connectToNats, disconnectNats, getNatsConnection } from "./nats/client.ts";
import { createGraphQLServer, startServer } from "./server.ts";
import { loadConfig } from "./types/config.ts";
import { initLogCollector } from "./modules/logs.ts";
import { initNatsTrafficCollector } from "./modules/nats-traffic.ts";
import { initNetworkCollector } from "./modules/network.ts";
import { initNftablesCollector } from "./modules/nftables.ts";
import type { ServiceLogEntry } from "@tentacle/nats-schema";

let log: Log = createLogger("graphql-main", LogLevel.info);

async function main() {
  log.info("Starting tentacle-graphql...");

  // Load configuration
  const config = loadConfig();

  // Connect to NATS
  log.info(`Connecting to NATS at ${config.nats.servers}...`);
  try {
    await connectToNats(config.nats);
    log.info("Connected to NATS");
  } catch (error) {
    log.error("Failed to connect to NATS:", error);
    Deno.exit(1);
  }

  // Enable NATS log streaming for graphql's own logs
  const nc = getNatsConnection();
  const moduleId = "graphql";
  {
    const subject = `service.logs.graphql.${moduleId}`;
    const encoder = new TextEncoder();
    const coralLog = log;
    const publish = (level: string, msg: string, ...args: unknown[]) => {
      try {
        const message = args.length > 0
          ? `${msg} ${args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" ")}`
          : msg;
        const entry: ServiceLogEntry = {
          timestamp: Date.now(),
          level: level as ServiceLogEntry["level"],
          message,
          serviceType: "graphql",
          moduleId,
          logger: "graphql-main",
        };
        nc.publish(subject, encoder.encode(JSON.stringify(entry)));
      } catch { /* never break the service for logging */ }
    };
    log = {
      info: (m: string, ...a: unknown[]) => { coralLog.info(m, ...a); publish("info", m, ...a); },
      warn: (m: string, ...a: unknown[]) => { coralLog.warn(m, ...a); publish("warn", m, ...a); },
      error: (m: string, ...a: unknown[]) => { coralLog.error(m, ...a); publish("error", m, ...a); },
      debug: (m: string, ...a: unknown[]) => { coralLog.debug(m, ...a); publish("debug", m, ...a); },
    } as Log;
  }

  // Initialize log collector (subscribes to service.logs.> for ring buffer + subscriptions)
  await initLogCollector(nc);

  // Initialize NATS traffic collector (subscribes to > for traffic monitor)
  initNatsTrafficCollector(nc);

  // Initialize network state collector (subscribes to network.interfaces)
  initNetworkCollector(nc);

  // Initialize nftables config collector (subscribes to nftables.rules)
  initNftablesCollector(nc);

  // Create and start GraphQL server
  log.info(`Creating GraphQL server on ${config.server.hostname}:${config.server.port}...`);
  const yoga = createGraphQLServer(config.server);
  const server = await startServer(yoga, config.server);

  // Handle graceful shutdown
  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) {
      log.warn("Force exiting...");
      Deno.exit(1);
    }
    shuttingDown = true;
    log.info(`Received ${signal}, shutting down gracefully...`);

    // Disconnect NATS (closes subscriptions)
    try {
      await disconnectNats();
    } catch (error) {
      log.error("Error disconnecting NATS:", error);
    }

    // Give server a short timeout to shutdown, then force exit
    // SSE connections can keep server alive indefinitely
    const shutdownTimeout = setTimeout(() => {
      log.warn("Shutdown timeout, forcing exit...");
      Deno.exit(0);
    }, 2000);

    try {
      await server.shutdown();
      clearTimeout(shutdownTimeout);
      log.info("Shutdown complete");
      Deno.exit(0);
    } catch (error) {
      clearTimeout(shutdownTimeout);
      log.error("Error during shutdown:", error);
      Deno.exit(1);
    }
  };

  Deno.addSignalListener("SIGINT", () => shutdown("SIGINT"));
  Deno.addSignalListener("SIGTERM", () => shutdown("SIGTERM"));
}

// Run main
if (import.meta.main) {
  main().catch((error) => {
    log.error("Fatal error:", error);
    Deno.exit(1);
  });
}
