import { builder } from "./builder.ts";
import { publishCommand, getVariable } from "../nats/client.ts";

builder.mutationType({
  fields: (t) => ({
    // Update a single variable
    updateVariable: t.field({
      type: "Variable",
      args: {
        projectId: t.arg.string({ required: true }),
        variableId: t.arg.string({ required: true }),
        value: t.arg.string({ required: true }), // Accept as JSON string
      },
      resolve: async (_root: unknown, args: { projectId: string; variableId: string; value: string }) => {
        // Parse the JSON value
        let parsedValue: unknown;
        try {
          parsedValue = JSON.parse(args.value);
        } catch {
          parsedValue = args.value; // Use as-is if not valid JSON
        }

        // Publish command to NATS
        await publishCommand(args.projectId, args.variableId, parsedValue);

        // Wait briefly for update to propagate
        await new Promise((resolve) => setTimeout(resolve, 100));

        // Fetch and return updated variable
        const updated = await getVariable(args.projectId, args.variableId);

        if (!updated) {
          throw new Error(
            `Variable ${args.variableId} not found after update. Does the variable exist?`,
          );
        }

        return updated;
      },
    }),
  }),
});
