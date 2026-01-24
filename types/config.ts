export interface NatsConfig {
  servers: string;
  user?: string;
  pass?: string;
  token?: string;
}

export interface ServerConfig {
  port: number;
  hostname: string;
  playgroundEnabled: boolean;
  introspectionEnabled: boolean;
  corsOrigin?: string;
}

export interface Config {
  nats: NatsConfig;
  server: ServerConfig;
}

export function loadConfig(): Config {
  return {
    nats: {
      servers: Deno.env.get("NATS_SERVERS") || "nats://localhost:4222",
      user: Deno.env.get("NATS_USER"),
      pass: Deno.env.get("NATS_PASS"),
      token: Deno.env.get("NATS_TOKEN"),
    },
    server: {
      port: parseInt(Deno.env.get("GRAPHQL_PORT") || "4000"),
      hostname: Deno.env.get("GRAPHQL_HOSTNAME") || "0.0.0.0",
      playgroundEnabled: Deno.env.get("GRAPHQL_PLAYGROUND") !== "false",
      introspectionEnabled: Deno.env.get("GRAPHQL_INTROSPECTION") !== "false",
      corsOrigin: Deno.env.get("GRAPHQL_CORS_ORIGIN"),
    },
  };
}
