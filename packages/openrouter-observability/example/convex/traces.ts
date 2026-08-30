import {
  OpenRouterObservability,
  type OpenRouterObservabilityComponent,
} from "@anandpant/convex-openrouter-observability";
import { componentsGeneric, queryGeneric } from "convex/server";
import { v } from "convex/values";

const components = componentsGeneric() as unknown as {
  openrouterObservability: OpenRouterObservabilityComponent;
};
const observability = new OpenRouterObservability(components.openrouterObservability);

export const recent = queryGeneric({
  args: { limit: v.optional(v.number()) },
  handler: async (ctx, args) => await observability.listRecent(ctx, args),
});
