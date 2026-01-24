import { builder } from "./builder.ts";
import {
  listProjects,
  listVariables,
  getVariable,
} from "../nats/client.ts";

builder.queryType({
  fields: (t) => ({
    // List all available projects
    projects: t.stringList({
      resolve: async () => {
        return await listProjects();
      },
    }),

    // List variables for a project
    variables: t.field({
      type: ["Variable"],
      args: {
        projectId: t.arg.string({ required: true }),
      },
      resolve: async (_root: unknown, args: { projectId: string }) => {
        const variables = await listVariables(args.projectId);
        return variables;
      },
    }),

    // Get a specific variable
    variable: t.field({
      type: "Variable",
      nullable: true,
      args: {
        projectId: t.arg.string({ required: true }),
        variableId: t.arg.string({ required: true }),
      },
      resolve: async (_root: unknown, args: { projectId: string; variableId: string }) => {
        return await getVariable(args.projectId, args.variableId);
      },
    }),
  }),
});
