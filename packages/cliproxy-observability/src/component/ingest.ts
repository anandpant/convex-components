import { v } from "convex/values";
import { mutation } from "./_generated/server.js";
import { blobValue } from "./schema.js";
const bounded = (value: string, limit: number) => {
  if (new TextEncoder().encode(value).byteLength > limit) throw new Error("component byte limit");
};
const identity = (value: string) => {
  if (!/^[a-f0-9]{64}$/.test(value)) throw new Error("invalid identity");
};
export const admit = mutation({
  args: {
    destinationId: v.string(),
    instanceId: v.string(),
    pluginBootId: v.string(),
    executionId: v.string(),
    callId: v.string(),
    sequence: v.number(),
    throughSequence: v.number(),
    terminalSequence: v.optional(v.number()),
    identity: v.string(),
    digest: v.string(),
    reference: blobValue,
    summaryJson: v.string(),
    observedAt: v.string(),
    correlation: v.object({
      requestId: v.optional(v.string()),
      runId: v.optional(v.string()),
      jobId: v.optional(v.string()),
      rootExecutionId: v.optional(v.string()),
      opencodeSessionId: v.optional(v.string()),
    }),
  },
  handler: async (ctx, args) => {
    identity(args.identity);
    identity(args.digest);
    identity(args.callId);
    bounded(args.summaryJson, 8 * 1024);
    if (
      args.reference.sha256 !== args.digest ||
      args.reference.byteLength > 2 * 1028 * 1024 ||
      args.reference.byteLength < 1 ||
      !Number.isSafeInteger(args.sequence) ||
      args.sequence < 1 ||
      !Number.isSafeInteger(args.throughSequence) ||
      args.throughSequence < args.sequence ||
      args.throughSequence - args.sequence > 255
    )
      throw new Error("invalid segment");
    bounded(JSON.stringify(args), 28 * 1024);
    const receipt = await ctx.db
      .query("receipts")
      .withIndex("by_identity", (q) =>
        q.eq("destinationId", args.destinationId).eq("identity", args.identity),
      )
      .unique();
    if (receipt) {
      if (receipt.digest !== args.digest) throw new Error("capture_digest_conflict");
      return { identity: args.identity, digest: args.digest, callId: args.callId, duplicate: true };
    }
    const existingSequence = await ctx.db
      .query("segments")
      .withIndex("by_sequence", (q) =>
        q
          .eq("destinationId", args.destinationId)
          .eq("callId", args.callId)
          .eq("sequence", args.sequence),
      )
      .unique();
    if (existingSequence) throw new Error("capture_sequence_conflict");
    const previous = await ctx.db
      .query("segments")
      .withIndex("by_sequence", (q) =>
        q
          .eq("destinationId", args.destinationId)
          .eq("callId", args.callId)
          .lt("sequence", args.sequence),
      )
      .order("desc")
      .first();
    const next = await ctx.db
      .query("segments")
      .withIndex("by_sequence", (q) =>
        q
          .eq("destinationId", args.destinationId)
          .eq("callId", args.callId)
          .gt("sequence", args.sequence),
      )
      .order("asc")
      .first();
    if (
      (previous && previous.throughSequence >= args.sequence) ||
      (next && next.sequence <= args.throughSequence)
    )
      throw new Error("capture_sequence_conflict");
    const now = Date.now();
    let call = await ctx.db
      .query("calls")
      .withIndex("by_call", (q) =>
        q.eq("destinationId", args.destinationId).eq("callId", args.callId),
      )
      .unique();
    if (!call) {
      const document = await ctx.db.insert("calls", {
        destinationId: args.destinationId,
        callId: args.callId,
        instanceId: args.instanceId,
        pluginBootId: args.pluginBootId,
        executionId: args.executionId,
        receivedAt: now,
        revision: 0,
        projectedThroughSequence: 0,
        persistedThroughSequence: 0,
        summaryJson: args.summaryJson,
        checkpointJson: "{}",
        ...args.correlation,
      });
      call = await ctx.db.get("calls", document);
    }
    if (!call) throw new Error("call missing");
    if (
      call.instanceId !== args.instanceId ||
      call.pluginBootId !== args.pluginBootId ||
      call.executionId !== args.executionId ||
      (call.terminalSequence !== undefined && args.throughSequence > call.terminalSequence)
    )
      throw new Error("capture_sequence_conflict");
    if (args.sequence === 1) await ctx.db.patch("calls", call._id, args.correlation);
    await ctx.db.insert("segments", {
      destinationId: args.destinationId,
      callId: args.callId,
      sequence: args.sequence,
      throughSequence: args.throughSequence,
      identity: args.identity,
      digest: args.digest,
      reference: args.reference,
      receivedAt: now,
    });
    await ctx.db.insert("receipts", {
      destinationId: args.destinationId,
      identity: args.identity,
      digest: args.digest,
      callId: args.callId,
      sequence: args.sequence,
      receivedAt: now,
    });
    // Bounded contiguous watermark advance. Projection workers continue it in bounded pages.
    const pending = await ctx.db
      .query("segments")
      .withIndex("by_sequence", (q) =>
        q
          .eq("destinationId", args.destinationId)
          .eq("callId", args.callId)
          .gt("sequence", call.persistedThroughSequence),
      )
      .take(8);
    let through = call.persistedThroughSequence;
    for (const segment of pending) {
      if (segment.sequence !== through + 1) break;
      through = segment.throughSequence;
    }
    const patch: { persistedThroughSequence: number; terminalSequence?: number } = {
      persistedThroughSequence: through,
    };
    if (args.terminalSequence !== undefined) patch.terminalSequence = args.terminalSequence;
    await ctx.db.patch("calls", call._id, patch);
    const status = await ctx.db
      .query("sourceStatus")
      .withIndex("by_source", (q) =>
        q.eq("destinationId", args.destinationId).eq("instanceId", args.instanceId),
      )
      .unique();
    const health = {
      destinationId: args.destinationId,
      instanceId: args.instanceId,
      pluginBootId: args.pluginBootId,
      lastObservedAt: args.observedAt,
      lastReceivedAt: now,
      coverage: "precommit_and_pre_hook_unavailable",
      healthJson: status?.pluginBootId === args.pluginBootId ? status.healthJson : undefined,
    };
    if (status && Date.parse(status.lastObservedAt) <= Date.parse(args.observedAt))
      await ctx.db.patch("sourceStatus", status._id, health);
    else if (!status) await ctx.db.insert("sourceStatus", health);
    return { identity: args.identity, digest: args.digest, callId: args.callId, duplicate: false };
  },
});
export const commitProjection = mutation({
  args: {
    destinationId: v.string(),
    callId: v.string(),
    expectedRevision: v.number(),
    throughSequence: v.number(),
    summaryJson: v.string(),
    checkpointJson: v.string(),
  },
  handler: async (ctx, args) => {
    bounded(args.summaryJson, 8 * 1024);
    bounded(args.checkpointJson, 68 * 1024);
    const call = await ctx.db
      .query("calls")
      .withIndex("by_call", (q) =>
        q.eq("destinationId", args.destinationId).eq("callId", args.callId),
      )
      .unique();
    if (!call) throw new Error("call missing");
    if (call.revision !== args.expectedRevision)
      return { committed: false, revision: call.revision };
    if (
      args.throughSequence < call.projectedThroughSequence ||
      args.throughSequence > call.projectedThroughSequence + 8 * 256
    )
      throw new Error("projection range limit");
    const rows = await ctx.db
      .query("segments")
      .withIndex("by_sequence", (q) =>
        q
          .eq("destinationId", args.destinationId)
          .eq("callId", args.callId)
          .gt("sequence", call.projectedThroughSequence),
      )
      .take(8);
    let through = call.projectedThroughSequence;
    for (const row of rows) {
      if (row.sequence !== through + 1 || row.throughSequence > args.throughSequence) break;
      through = row.throughSequence;
    }
    if (through !== args.throughSequence) throw new Error("projection crosses gap");
    await ctx.db.patch("calls", call._id, {
      revision: call.revision + 1,
      projectedThroughSequence: through,
      persistedThroughSequence: Math.max(call.persistedThroughSequence, through),
      summaryJson: args.summaryJson,
      checkpointJson: args.checkpointJson,
      projectionFailure: undefined,
    });
    return { committed: true, revision: call.revision + 1 };
  },
});

export const recordProjectionFailure = mutation({
  args: { destinationId: v.string(), callId: v.string(), expectedRevision: v.number() },
  handler: async (ctx, args) => {
    const call = await ctx.db
      .query("calls")
      .withIndex("by_call", (q) =>
        q.eq("destinationId", args.destinationId).eq("callId", args.callId),
      )
      .unique();
    if (call && call.revision === args.expectedRevision)
      await ctx.db.patch("calls", call._id, {
        projectionFailure: { reason: "projection_processing_failed", at: Date.now() },
      });
  },
});

export const recordHealth = mutation({
  args: {
    destinationId: v.string(),
    instanceId: v.string(),
    pluginBootId: v.string(),
    startedAt: v.string(),
    observedAt: v.string(),
    observationsTotal: v.number(),
    droppedObservationsTotal: v.number(),
    lostControlObservationsTotal: v.number(),
    scopeConflictsTotal: v.number(),
    expiredScopesTotal: v.number(),
    activeCalls: v.number(),
  },
  handler: async (ctx, args) => {
    bounded(JSON.stringify(args), 4096);
    const at = Date.parse(args.observedAt);
    if (!Number.isFinite(at) || !Number.isFinite(Date.parse(args.startedAt)))
      throw new Error("invalid health time");
    for (const value of [
      args.observationsTotal,
      args.droppedObservationsTotal,
      args.lostControlObservationsTotal,
      args.scopeConflictsTotal,
      args.expiredScopesTotal,
      args.activeCalls,
    ])
      if (!Number.isSafeInteger(value) || value < 0) throw new Error("invalid health counter");
    const healthJson = JSON.stringify({
      ...args,
      counterScope: "plugin_boot",
      precommitCoverage: "unknown_before_local_commit",
    });
    const boot = await ctx.db
      .query("bootHealth")
      .withIndex("by_boot", (q) =>
        q
          .eq("destinationId", args.destinationId)
          .eq("instanceId", args.instanceId)
          .eq("pluginBootId", args.pluginBootId),
      )
      .unique();
    if (!boot)
      await ctx.db.insert("bootHealth", {
        destinationId: args.destinationId,
        instanceId: args.instanceId,
        pluginBootId: args.pluginBootId,
        observedAt: at,
        healthJson,
      });
    else if (at >= boot.observedAt)
      await ctx.db.patch("bootHealth", boot._id, { observedAt: at, healthJson });
    const status = await ctx.db
      .query("sourceStatus")
      .withIndex("by_source", (q) =>
        q.eq("destinationId", args.destinationId).eq("instanceId", args.instanceId),
      )
      .unique();
    const row = {
      destinationId: args.destinationId,
      instanceId: args.instanceId,
      pluginBootId: args.pluginBootId,
      lastObservedAt: args.observedAt,
      lastReceivedAt: Date.now(),
      coverage: "precommit_and_pre_hook_unavailable",
      healthJson,
    };
    if (!status) await ctx.db.insert("sourceStatus", row);
    else if (at >= Date.parse(status.lastObservedAt))
      await ctx.db.patch("sourceStatus", status._id, row);
    return { committed: true };
  },
});
