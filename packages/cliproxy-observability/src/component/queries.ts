import { providerIdentity, type ModelCallV1 } from "../model-call/index.js";
import { normalizeUsage } from "../protocols/usage.js";
import { v } from "convex/values";
import { query, type QueryCtx } from "./_generated/server.js";
async function summaryView(
  ctx: QueryCtx,
  row: {
    summaryJson: string;
    destinationId: string;
    instanceId: string;
    pluginBootId: string;
    terminalSequence?: number;
  },
) {
  const summary = JSON.parse(row.summaryJson) as ModelCallV1;
  // Summaries projected before 0.3.0 lack these. Derive them from the facts they recorded,
  // keeping any stored provider name as observed and applying the current token rule to
  // their final native counts.
  if (summary.providerProvenance === undefined) {
    Object.assign(
      summary,
      providerIdentity({ ...summary, observedProvider: summary.providerName }),
    );
    summary.costProvenance = summary.cost.kind;
    if (
      summary.capture.usage === "complete" &&
      summary.clientProtocol !== undefined &&
      summary.clientProtocol !== "unknown"
    )
      Object.assign(
        summary,
        normalizeUsage(
          summary.clientProtocol,
          new Map(summary.usage.map((measurement) => [measurement.nativeField, measurement.value])),
        ),
      );
  }
  if (summary.state === "in_progress" && row.terminalSequence === undefined) {
    const source = await ctx.db
      .query("sourceStatus")
      .withIndex("by_source", (q) =>
        q.eq("destinationId", row.destinationId).eq("instanceId", row.instanceId),
      )
      .unique();
    if (source?.healthJson && source.pluginBootId !== row.pluginBootId) {
      summary.state = "unknown";
      summary.capture.raw = "partial";
      summary.capture.gaps = [...summary.capture.gaps, "completion_unobserved_prior_boot"].slice(
        -8,
      );
    }
  }
  return summary;
}
const owner = { destinationId: v.string(), callId: v.string() };
export const getCall = query({
  args: owner,
  handler: async (ctx, args) => {
    const call = await ctx.db
      .query("calls")
      .withIndex("by_call", (q) =>
        q.eq("destinationId", args.destinationId).eq("callId", args.callId),
      )
      .unique();
    if (!call) return null;
    return {
      ...call,
      summaryJson: JSON.stringify(await summaryView(ctx, call)),
      checkpointJson: undefined,
    };
  },
});
export const getProcessingState = query({
  args: owner,
  handler: async (ctx, args) =>
    ctx.db
      .query("calls")
      .withIndex("by_call", (q) =>
        q.eq("destinationId", args.destinationId).eq("callId", args.callId),
      )
      .unique(),
});
export const pageEventSegments = query({
  args: { ...owner, afterSequence: v.optional(v.number()), limit: v.optional(v.number()) },
  handler: async (ctx, args) => {
    const limit = Math.max(1, Math.min(8, Math.floor(args.limit ?? 7)));
    const rows = await ctx.db
      .query("segments")
      .withIndex("by_sequence", (q) =>
        q
          .eq("destinationId", args.destinationId)
          .eq("callId", args.callId)
          .gt("sequence", args.afterSequence ?? 0),
      )
      .take(limit + 1);
    return {
      segments: rows.slice(0, limit),
      cursor: rows.length
        ? rows[Math.min(rows.length, limit) - 1]!.throughSequence
        : (args.afterSequence ?? 0),
      done: rows.length <= limit,
    };
  },
});
const cursor = v.object({ receivedAt: v.number(), creationTime: v.number() });
export const pageRecentSummaries = query({
  args: {
    destinationId: v.string(),
    cursor: v.optional(cursor),
    limit: v.optional(v.number()),
    correlation: v.optional(
      v.object({
        kind: v.union(
          v.literal("requestId"),
          v.literal("runId"),
          v.literal("jobId"),
          v.literal("rootExecutionId"),
          v.literal("opencodeSessionId"),
        ),
        value: v.string(),
      }),
    ),
  },
  handler: async (ctx, args) => {
    const limit = Math.max(1, Math.min(3, Math.floor(args.limit ?? 3)));
    const correlation = args.correlation;
    const c = args.cursor;
    async function read(mode: "first" | "tie" | "older", take: number) {
      if (correlation?.kind === "requestId")
        return ctx.db
          .query("calls")
          .withIndex("by_request", (q) => {
            const base = q
              .eq("destinationId", args.destinationId)
              .eq("requestId", correlation.value);
            return !c
              ? base
              : mode === "tie"
                ? base.eq("receivedAt", c.receivedAt).lt("_creationTime", c.creationTime)
                : base.lt("receivedAt", c.receivedAt);
          })
          .order("desc")
          .take(take);
      if (correlation?.kind === "runId")
        return ctx.db
          .query("calls")
          .withIndex("by_run", (q) => {
            const base = q.eq("destinationId", args.destinationId).eq("runId", correlation.value);
            return !c
              ? base
              : mode === "tie"
                ? base.eq("receivedAt", c.receivedAt).lt("_creationTime", c.creationTime)
                : base.lt("receivedAt", c.receivedAt);
          })
          .order("desc")
          .take(take);
      if (correlation?.kind === "jobId")
        return ctx.db
          .query("calls")
          .withIndex("by_job", (q) => {
            const base = q.eq("destinationId", args.destinationId).eq("jobId", correlation.value);
            return !c
              ? base
              : mode === "tie"
                ? base.eq("receivedAt", c.receivedAt).lt("_creationTime", c.creationTime)
                : base.lt("receivedAt", c.receivedAt);
          })
          .order("desc")
          .take(take);
      if (correlation?.kind === "rootExecutionId")
        return ctx.db
          .query("calls")
          .withIndex("by_root_execution", (q) => {
            const base = q
              .eq("destinationId", args.destinationId)
              .eq("rootExecutionId", correlation.value);
            return !c
              ? base
              : mode === "tie"
                ? base.eq("receivedAt", c.receivedAt).lt("_creationTime", c.creationTime)
                : base.lt("receivedAt", c.receivedAt);
          })
          .order("desc")
          .take(take);
      if (correlation?.kind === "opencodeSessionId")
        return ctx.db
          .query("calls")
          .withIndex("by_opencode_session", (q) => {
            const base = q
              .eq("destinationId", args.destinationId)
              .eq("opencodeSessionId", correlation.value);
            return !c
              ? base
              : mode === "tie"
                ? base.eq("receivedAt", c.receivedAt).lt("_creationTime", c.creationTime)
                : base.lt("receivedAt", c.receivedAt);
          })
          .order("desc")
          .take(take);
      return ctx.db
        .query("calls")
        .withIndex("by_received", (q) => {
          const base = q.eq("destinationId", args.destinationId);
          return !c
            ? base
            : mode === "tie"
              ? base.eq("receivedAt", c.receivedAt).lt("_creationTime", c.creationTime)
              : base.lt("receivedAt", c.receivedAt);
        })
        .order("desc")
        .take(take);
    }
    const rows = c ? await read("tie", limit + 1) : await read("first", limit + 1);
    if (c && rows.length < limit + 1) rows.push(...(await read("older", limit + 1 - rows.length)));
    const calls: ModelCallV1[] = [];
    let bytes = 256;
    let next = args.cursor;
    let consumed = 0;
    for (const row of rows.slice(0, limit)) {
      const view = await summaryView(ctx, row);
      const summary = {
        ...view,
        sourceDocumentId: row._id,
        receivedAt: row.receivedAt,
        capture: {
          ...view.capture,
          projectedThroughSequence: row.projectedThroughSequence,
          persistedThroughSequence: row.persistedThroughSequence,
        },
      };
      const length = new TextEncoder().encode(JSON.stringify(summary)).byteLength;
      if (bytes + length > 31 * 1024) break;
      calls.push(summary);
      bytes += length;
      consumed++;
      next = { receivedAt: row.receivedAt, creationTime: row._creationTime };
    }
    return { calls, cursor: next ?? null, done: consumed === rows.length };
  },
});
export const getCaptureCoverage = query({
  args: { destinationId: v.string() },
  handler: async (ctx, args) => {
    const sources = await ctx.db
      .query("sourceStatus")
      .withIndex("by_source", (q) => q.eq("destinationId", args.destinationId))
      .take(17);
    return {
      retention: "indefinite" as const,
      preHook: "unavailable" as const,
      previews: "not_enrolled" as const,
      attemptDetail: "unavailable" as const,
      sources: sources.slice(0, 16),
      sourcesComplete: sources.length <= 16,
    };
  },
});

export const getBootHealth = query({
  args: { destinationId: v.string(), instanceId: v.string(), pluginBootId: v.string() },
  handler: async (ctx, args) =>
    ctx.db
      .query("bootHealth")
      .withIndex("by_boot", (q) =>
        q
          .eq("destinationId", args.destinationId)
          .eq("instanceId", args.instanceId)
          .eq("pluginBootId", args.pluginBootId),
      )
      .unique(),
});
