import { v } from "convex/values";
import { mutation } from "./_generated/server.js";
import { slimSpan } from "./spanValues.js";

/**
 * Admit slim span rows whose content the host has already archived. A span seen before, in
 * this delivery or an earlier one, is skipped: its archived object is content-addressed, so a
 * redelivery left at most an identical or unreferenced object behind.
 */
export const admitPrepared = mutation({
  args: { spans: v.array(slimSpan) },
  handler: async (ctx, args) => {
    const seen = new Set<string>();
    let admitted = 0;
    const receivedAt = Date.now();
    for (const span of args.spans) {
      const key = JSON.stringify([span.traceId, span.spanId]);
      if (seen.has(key)) continue;
      seen.add(key);
      const existing = await ctx.db
        .query("spans")
        .withIndex("by_trace_span", (q) => q.eq("traceId", span.traceId).eq("spanId", span.spanId))
        .first();
      if (existing) continue;
      await ctx.db.insert("spans", { ...span, receivedAt });
      admitted += 1;
    }
    return {
      kind: "accepted",
      admitted,
      duplicates: args.spans.length - admitted,
    } as const;
  },
});
