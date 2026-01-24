import { createYoga } from "graphql-yoga";
import { createLogger, LogLevel } from "@joyautomation/coral";
import { schema } from "./schema/schema.ts";
import type { ServerConfig } from "./types/config.ts";

const log = createLogger("graphql-server", LogLevel.info);

export function createGraphQLServer(config: ServerConfig) {
  const yoga = createYoga({
    schema,
    graphiql: config.playgroundEnabled,
    cors: {
      origin: config.corsOrigin || "*",
      credentials: true,
    },
    plugins: [
      // Add logging plugin
      {
        onRequest: ({ request }) => {
          log.debug(
            `${request.method} ${new URL(request.url).pathname}`,
          );
        },
      },
    ],
  });

  return yoga;
}

export async function startServer(
  yoga: ReturnType<typeof createYoga>,
  config: ServerConfig,
) {
  const server = Deno.serve(
    {
      port: config.port,
      hostname: config.hostname,
      onListen: ({ hostname, port }) => {
        log.info(`GraphQL server ready at http://${hostname}:${port}/graphql`);
        if (config.playgroundEnabled) {
          log.info(`GraphQL Playground at http://${hostname}:${port}/graphql`);
        }
      },
    },
    yoga,
  );

  return server;
}
