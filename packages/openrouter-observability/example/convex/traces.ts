import {
  OpenRouterObservability,
  type OpenRouterObservabilityComponent,
} from "@shpitdev/convex-openrouter-observability";
import { componentsGeneric, internalQueryGeneric, type QueryBuilder } from "convex/server";
import { v } from "convex/values";
import type schema from "./schema.js";
import type { DataModelFromSchemaDefinition } from "convex/server";

const components = componentsGeneric() as unknown as {
  openrouterObservability: OpenRouterObservabilityComponent;
};
const observability = new OpenRouterObservability(components.openrouterObservability);

type DataModel = DataModelFromSchemaDefinition<typeof schema>;
const internalQuery: QueryBuilder<DataModel, "internal"> = internalQueryGeneric;
const cursor = v.object({ receivedAt: v.number(), _creationTime: v.number() });

export const requestSpanSummaries = internalQuery({
  args: { requestId: v.string(), limit: v.optional(v.number()), cursor: v.optional(cursor) },
  handler: async (ctx, args) =>
    await observability.pageCorrelationSummaries(ctx, {
      correlation: { kind: "request", requestId: args.requestId },
      limit: args.limit,
      cursor: args.cursor,
    }),
});

export const runSpanSummaries = internalQuery({
  args: { runId: v.string(), limit: v.optional(v.number()), cursor: v.optional(cursor) },
  handler: async (ctx, args) =>
    await observability.pageCorrelationSummaries(ctx, {
      correlation: { kind: "run", runId: args.runId },
      limit: args.limit,
      cursor: args.cursor,
    }),
});

export const storedSpan = internalQuery({
  args: { spanDocumentId: v.string() },
  handler: async (ctx, args) =>
    await observability.getSpan(ctx, {
      spanDocumentId: args.spanDocumentId as Parameters<
        typeof observability.getSpan
      >[1]["spanDocumentId"],
    }),
});
