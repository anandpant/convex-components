import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import { parsedSpanFields } from "./spanValues.js";

export default defineSchema({
  migrationState: defineTable({
    name: v.string(),
    startedAt: v.optional(v.number()),
    lastScheduledAt: v.optional(v.number()),
    completedAt: v.optional(v.number()),
  }).index("by_name", ["name"]),
  // Temporary compatibility table for hosts migrating from the Prismantix
  // incubator. New deliveries are never written here; the bounded migration
  // removes legacy rows before this table is dropped in a later release.
  deliveries: defineTable({
    rawBody: v.string(),
    receivedAt: v.number(),
    byteLength: v.number(),
  }).index("by_received_at", ["receivedAt"]),
  spanKeys: defineTable({
    traceId: v.string(),
    spanId: v.string(),
    spanDocumentId: v.id("spans"),
  })
    .index("by_trace_span", ["traceId", "spanId"])
    .index("by_span_document", ["spanDocumentId"]),
  spans: defineTable({
    ...parsedSpanFields,
    receivedAt: v.number(),
  })
    .index("by_trace_span", ["traceId", "spanId"])
    .index("by_session", ["sessionId", "receivedAt"])
    .index("by_user", ["userId", "receivedAt"])
    .index("by_request", ["requestId", "receivedAt"])
    .index("by_entity", ["entityType", "entityId", "receivedAt"])
    .index("by_received", ["receivedAt"]),
});
