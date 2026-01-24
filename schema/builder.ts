import SchemaBuilder from "@pothos/core";
import type { PlcVariableKV } from "@tentacle/nats-schema";

// Device configuration type for EtherNet/IP PLCs
export interface DeviceConfig {
  id: string;
  projectId: string;
  host: string;
  port: number;
  type: "rockwell" | "generic-cip";
  slot?: number;
  scanRate: number;
  enabled: boolean;
}

// Tag configuration type
export interface TagConfig {
  id: string;
  deviceId: string;
  address: string;
  datatype?: string;
  writable: boolean;
  deadbandValue?: number;
  deadbandMaxTime?: number;
  disableRBE?: boolean;
}

export const builder = new SchemaBuilder<{
  Objects: {
    Variable: PlcVariableKV;
    Device: DeviceConfig;
    Tag: TagConfig;
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
