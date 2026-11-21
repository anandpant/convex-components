import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
import { storedSpanFields } from "./spanValues.js";

export default defineSchema({
  // Legacy tables, no longer read or written. 0.6.0 removes them once the archive migration
  // has cleared their rows.
  migrationState: defineTable({
    name: v.string(),
    startedAt: v.optional(v.number()),
    lastScheduledAt: v.optional(v.number()),
    completedAt: v.optional(v.number()),
    processedSpans: v.optional(v.number()),
    lastProcessedSpanId: v.optional(v.id("spans")),
  }),
  deliveries: defineTable({
    rawBody: v.string(),
    receivedAt: v.number(),
    byteLength: v.number(),
  }),
  spanKeys: defineTable({
    traceId: v.string(),
    spanId: v.string(),
    spanDocumentId: v.id("spans"),
  }),
  // One row per recorded variant, so a downstream copy can page variants added to old spans.
  spanVariants: defineTable({
    spanDocumentId: v.id("spans"),
    traceId: v.string(),
    spanId: v.string(),
    key: v.string(),
    sha256: v.string(),
    byteLength: v.number(),
    receivedAt: v.number(),
  }).index("by_span_sha", ["spanDocumentId", "sha256"]),
  spans: defineTable(storedSpanFields)
    .index("by_trace_span", ["traceId", "spanId"])
    .index("by_request", ["requestId", "receivedAt"])
    .index("by_run", ["runId", "receivedAt"])
    .index("by_job", ["jobId", "receivedAt"]),
});
