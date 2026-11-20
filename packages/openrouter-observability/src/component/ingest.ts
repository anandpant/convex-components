import { v } from "convex/values";
import { mutation } from "./_generated/server.js";
import { MAX_SPAN_VARIANTS, slimSpan } from "./spanValues.js";

/**
 * Admit slim span rows whose content the host has already archived. A span seen before keeps its
 * row; when a redelivery's content differs, its object is recorded as a variant so no delivered
 * content goes unreferenced.
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
      if (existing) {
        if (existing.payload?.sha256 === span.payload.sha256) continue;
        const known = (existing.variants ?? []).some(
          (variant) => variant.sha256 === span.payload.sha256,
        );
        if (known) continue;
        const variantCount = (existing.variantCount ?? 0) + 1;
        await ctx.db.patch("spans", existing._id, {
          variantCount,
          ...(variantCount <= MAX_SPAN_VARIANTS
            ? { variants: [...(existing.variants ?? []), { ...span.payload, receivedAt }] }
            : {}),
        });
        continue;
      }
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
