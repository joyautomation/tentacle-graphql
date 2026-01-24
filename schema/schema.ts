// This file assembles the GraphQL schema by importing all type definitions
// in the correct order to avoid circular dependency issues

// First, import the builder (must be fully initialized before other imports)
import { builder } from "./builder.ts";

// Then import all schema definitions (these register types on the builder)
import "./types.ts";
import "./queries.ts";
import "./mutations.ts";
import "./subscriptions.ts";

// Finally, build and export the schema
export const schema = builder.toSchema();
