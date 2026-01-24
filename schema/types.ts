import { builder } from "./builder.ts";
import type { PlcVariableKV, DeadBandConfig } from "@tentacle/nats-schema";

// DeadBandConfig object type
const DeadBandConfigType = builder.objectRef<DeadBandConfig>(
  "DeadBandConfig",
);
builder.objectType(DeadBandConfigType, {
  fields: (t) => ({
    value: t.exposeFloat("value"),
    maxTime: t.exposeInt("maxTime", { nullable: true }),
  }),
});

// Variable object type (based on PlcVariableKV)
builder.objectType("Variable", {
  fields: (t) => ({
    projectId: t.exposeString("projectId"),
    variableId: t.exposeString("variableId"),
    value: t.field({
      type: "JSON",
      resolve: (v) => v.value,
    }),
    datatype: t.exposeString("datatype"),
    lastUpdated: t.field({
      type: "DateTime",
      resolve: (v) => new Date(v.lastUpdated),
    }),
    source: t.exposeString("source"),
    quality: t.exposeString("quality"),
    deadband: t.field({
      type: DeadBandConfigType,
      nullable: true,
      resolve: (v) => v.deadband || null,
    }),
    disableRBE: t.exposeBoolean("disableRBE", { nullable: true }),
  }),
});

// VariableFilter input type
builder.inputType("VariableFilter", {
  fields: (t) => ({
    variableIds: t.stringList({ required: false }),
    datatypes: t.stringList({ required: false }),
    sources: t.stringList({ required: false }),
    qualities: t.stringList({ required: false }),
  }),
});

// VariableUpdate input type
builder.inputType("VariableUpdate", {
  fields: (t) => ({
    variableId: t.string({ required: true }),
    value: t.field({ type: "JSON", required: true }),
  }),
});
