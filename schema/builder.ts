import SchemaBuilder from "@pothos/core";
import type { PlcVariableKV } from "@tentacle/nats-schema";

export const builder = new SchemaBuilder<{
  Objects: {
    Variable: PlcVariableKV;
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
