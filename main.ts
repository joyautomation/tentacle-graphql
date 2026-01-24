import { createLogger, LogLevel } from "@joyautomation/coral";
import { connectToNats, disconnectNats } from "./nats/client.ts";
import { createGraphQLServer, startServer } from "./server.ts";
import { loadConfig } from "./types/config.ts";

const log = createLogger("graphql-main", LogLevel.info);

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

    // Disconnect NATS first (closes subscriptions)
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
