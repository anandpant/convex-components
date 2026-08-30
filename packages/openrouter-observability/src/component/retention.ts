import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import { env, internalMutation } from "./_generated/server.js";

const DEFAULT_RETENTION_DAYS = 30;
const MAX_RETENTION_DAYS = 3650;
// A span can approach Convex's 1 MiB document limit. Keep each cleanup
// transaction below the 16 MiB read and write limits without relying on row
// size estimates after the documents have already been read.
const DELETE_BATCH_SIZE = 8;

function retentionDays() {
  const configured = env.RETENTION_DAYS;
  if (configured === undefined) return DEFAULT_RETENTION_DAYS;
  const days = Number(configured);
  if (!Number.isSafeInteger(days) || days < 1 || days > MAX_RETENTION_DAYS) {
    throw new Error(`RETENTION_DAYS must be an integer from 1 to ${MAX_RETENTION_DAYS}`);
  }
  return days;
}

export const deleteExpired = internalMutation({
  args: { cutoff: v.number() },
  handler: async (ctx, args) => {
    const spans = await ctx.db
      .query("spans")
      .withIndex("by_received", (query) => query.lt("receivedAt", args.cutoff))
      .order("asc")
      .take(DELETE_BATCH_SIZE);
    for (const span of spans) {
      const key = await ctx.db
        .query("spanKeys")
        .withIndex("by_span_document", (query) => query.eq("spanDocumentId", span._id))
        .unique();
      if (key) await ctx.db.delete("spanKeys", key._id);
      await ctx.db.delete("spans", span._id);
    }
    if (spans.length === DELETE_BATCH_SIZE) {
      await ctx.scheduler.runAfter(0, internal.retention.deleteExpired, args);
    }
    return { deletedSpans: spans.length };
  },
});

export const start = internalMutation({
  args: {},
  handler: async (ctx) => {
    const retentionMs = retentionDays() * 24 * 60 * 60 * 1000;
    await ctx.scheduler.runAfter(0, internal.retention.deleteExpired, {
      cutoff: Date.now() - retentionMs,
    });
  },
});
