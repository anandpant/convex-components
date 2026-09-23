import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";
export const blobValue = v.object({
  key: v.string(),
  sha256: v.string(),
  byteLength: v.number(),
  contentType: v.string(),
});
export default defineSchema({
  calls: defineTable({
    destinationId: v.string(),
    callId: v.string(),
    instanceId: v.string(),
    pluginBootId: v.string(),
    executionId: v.string(),
    receivedAt: v.number(),
    revision: v.number(),
    projectedThroughSequence: v.number(),
    persistedThroughSequence: v.number(),
    terminalSequence: v.optional(v.number()),
    summaryJson: v.string(),
    checkpointJson: v.string(),
    projectionFailure: v.optional(v.object({ reason: v.string(), at: v.number() })),
    requestId: v.optional(v.string()),
    runId: v.optional(v.string()),
    jobId: v.optional(v.string()),
    rootExecutionId: v.optional(v.string()),
    opencodeSessionId: v.optional(v.string()),
  })
    .index("by_call", ["destinationId", "callId"])
    .index("by_received", ["destinationId", "receivedAt"])
    .index("by_request", ["destinationId", "requestId", "receivedAt"])
    .index("by_run", ["destinationId", "runId", "receivedAt"])
    .index("by_job", ["destinationId", "jobId", "receivedAt"])
    .index("by_root_execution", ["destinationId", "rootExecutionId", "receivedAt"])
    .index("by_opencode_session", ["destinationId", "opencodeSessionId", "receivedAt"]),
  segments: defineTable({
    destinationId: v.string(),
    callId: v.string(),
    sequence: v.number(),
    throughSequence: v.number(),
    identity: v.string(),
    digest: v.string(),
    reference: blobValue,
    receivedAt: v.number(),
  })
    .index("by_identity", ["destinationId", "identity"])
    .index("by_sequence", ["destinationId", "callId", "sequence"]),
  receipts: defineTable({
    destinationId: v.string(),
    identity: v.string(),
    digest: v.string(),
    callId: v.string(),
    sequence: v.number(),
    receivedAt: v.number(),
  }).index("by_identity", ["destinationId", "identity"]),
  sourceStatus: defineTable({
    destinationId: v.string(),
    instanceId: v.string(),
    pluginBootId: v.string(),
    lastObservedAt: v.string(),
    lastReceivedAt: v.number(),
    coverage: v.string(),
  }).index("by_source", ["destinationId", "instanceId"]),
});
