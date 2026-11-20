import { v } from "convex/values";
import type { Id } from "./_generated/dataModel.js";
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
    // New rows and newly recorded variants, so the host can forward exactly what it stored.
    const stored: Array<{
      spanDocumentId: Id<"spans">;
      traceId: string;
      spanId: string;
      sha256: string;
      receivedAt: number;
      kind: "span" | "variant";
    }> = [];
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
        stored.push({
          spanDocumentId: existing._id,
          traceId: span.traceId,
          spanId: span.spanId,
          sha256: span.payload.sha256,
          receivedAt,
          kind: "variant",
        });
        continue;
      }
      const spanDocumentId = await ctx.db.insert("spans", { ...span, receivedAt });
      stored.push({
        spanDocumentId,
        traceId: span.traceId,
        spanId: span.spanId,
        sha256: span.payload.sha256,
        receivedAt,
        kind: "span",
      });
      admitted += 1;
    }
    return {
      kind: "accepted",
      admitted,
      duplicates: args.spans.length - admitted,
      stored,
    } as const;
  },
});
