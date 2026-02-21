import SchemaBuilder from "@pothos/core";
import type {
  PlcVariableKV,
  ServiceHeartbeat,
  ServiceLogEntry,
  NetworkInterfaceStats,
  NetworkAddress,
  NetworkInterface,
  NetworkStateMessage,
  NetworkInterfaceConfig,
  NetworkCommandResponse,
  NatRule,
  NftablesConfig,
  NftablesCommandResponse,
} from "@tentacle/nats-schema";
import type { NatsTrafficEntry } from "../modules/nats-traffic.ts";

export const builder = new SchemaBuilder<{
  Objects: {
    Variable: PlcVariableKV;
    Service: ServiceHeartbeat;
    LogEntry: ServiceLogEntry;
    NatsTrafficEntry: NatsTrafficEntry;
    NetworkInterfaceStats: NetworkInterfaceStats;
    NetworkAddress: NetworkAddress;
    NetworkInterface: NetworkInterface;
    NetworkState: NetworkStateMessage;
    NetworkInterfaceConfig: NetworkInterfaceConfig;
    NetworkCommandResult: NetworkCommandResponse;
    NatRule: NatRule;
    NftablesConfig: NftablesConfig;
    NftablesCommandResult: NftablesCommandResponse;
  };
  Scalars: {
    JSON: { Input: unknown; Output: unknown };
    DateTime: { Input: Date; Output: Date };
  };
}>({
  plugins: [],
});

// Register JSON scalar for flexible value types
builder.scalarType("JSON", {
  serialize: (value) => value,
  parseValue: (value) => value,
});

// Register DateTime scalar
builder.scalarType("DateTime", {
  serialize: (date) => {
    if (!(date instanceof Date)) {
      return new Date(date as number).toISOString();
    }
    return date.toISOString();
  },
  parseValue: (value) => {
    if (typeof value === "number") {
      return new Date(value);
    }
    return new Date(value as string);
  },
});
