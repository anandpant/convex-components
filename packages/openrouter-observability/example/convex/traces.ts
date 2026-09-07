import {
  OpenRouterObservability,
  type OpenRouterObservabilityComponent,
} from "@anandpant/convex-openrouter-observability";
import {
  componentsGeneric,
  internalQueryGeneric,
  queryGeneric,
  type QueryBuilder,
} from "convex/server";
import { v } from "convex/values";
import type schema from "./schema.js";
import type { DataModelFromSchemaDefinition } from "convex/server";

const components = componentsGeneric() as unknown as {
  openrouterObservability: OpenRouterObservabilityComponent;
};
const observability = new OpenRouterObservability(components.openrouterObservability);

type DataModel = DataModelFromSchemaDefinition<typeof schema>;
const query: QueryBuilder<DataModel, "public"> = queryGeneric;
const internalQuery: QueryBuilder<DataModel, "internal"> = internalQueryGeneric;

export const byRequest = query({
  args: { requestRecordId: v.id("traceRequests") },
  handler: async (ctx, args) => {
    const identity = await ctx.auth.getUserIdentity();
    if (!identity) throw new Error("Unauthorized");
    const request = await ctx.db.get("traceRequests", args.requestRecordId);
    if (!request || request.ownerSubject !== identity.subject) throw new Error("Forbidden");
    return await observability.listByRequest(ctx, {
      requestId: request.observabilityRequestId,
    });
  },
});

export const recent = internalQuery({
  args: { limit: v.optional(v.number()) },
  handler: async (ctx, args) => await observability.listRecent(ctx, args),
});
