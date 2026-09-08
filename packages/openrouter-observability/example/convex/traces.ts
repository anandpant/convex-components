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

export const sessionSpanSummaries = internalQuery({
  args: { sessionId: v.string(), limit: v.optional(v.number()), cursor: v.optional(cursor) },
  handler: async (ctx, args) =>
    await observability.pageCorrelationSummaries(ctx, {
      correlation: { kind: "session", sessionId: args.sessionId },
      limit: args.limit,
      cursor: args.cursor,
    }),
});

export const traceSpanSummaries = internalQuery({
  args: { traceId: v.string(), limit: v.optional(v.number()), cursor: v.optional(v.string()) },
  handler: async (ctx, args) =>
    await observability.pageTraceSummaries(ctx, {
      traceId: args.traceId,
      limit: args.limit,
      cursor: args.cursor as Parameters<typeof observability.pageTraceSummaries>[1]["cursor"],
    }),
});

export const fullSpanExport = internalQuery({
  args: { spanDocumentId: v.string() },
  handler: async (ctx, args) =>
    await observability.exportFullSpan(ctx, {
      spanDocumentId: args.spanDocumentId as Parameters<
        typeof observability.exportFullSpan
      >[1]["spanDocumentId"],
    }),
});
