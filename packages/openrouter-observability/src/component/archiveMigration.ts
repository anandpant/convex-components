import { getConvexSize, v } from "convex/values";
import { mutation, query } from "./_generated/server.js";
import { spanPayloadRef } from "./spanValues.js";

// Temporary: moves rows stored before 0.5.0 to archived objects. A host script pages inline
// rows, writes and confirms each object, then marks the row. 0.6.0 removes this module.

/** A page stops after the row that passes 8 MiB; inline rows are at most ~900 KiB, so a page
 * and a batch of its marks stay under the 16 MiB read and return limits. */
const PAGE_ROWS = 120;
const PAGE_BYTES = 8 * 1024 * 1024;

/** The next rows, in `_id` order after `after`, that still hold their content inline. */
export const pageInline = query({
  args: { after: v.optional(v.id("spans")) },
  handler: async (ctx, args) => {
    const spans = [];
    let bytes = 0;
    let rows = 0;
    let last = null;
    const scan = ctx.db
      .query("spans")
      .withIndex("by_id", (q) => (args.after === undefined ? q : q.gt("_id", args.after)));
    for await (const row of scan) {
      rows += 1;
      last = row._id;
      if (row.payload === undefined) {
        spans.push(row);
        bytes += getConvexSize(row);
      }
      if (rows >= PAGE_ROWS || bytes >= PAGE_BYTES) return { spans, last, done: false };
    }
    return { spans, last, done: true };
  },
});

/**
 * Point inline rows at their confirmed objects and drop the inline content. The caller computes
 * the payload facts from the row it paged, so the patch is the only read of each row; repeating
 * it for an archived row writes the same values again.
 */
export const markArchived = mutation({
  args: {
    items: v.array(
      v.object({
        spanDocumentId: v.id("spans"),
        payload: spanPayloadRef,
        inputUtf8Bytes: v.optional(v.number()),
        outputUtf8Bytes: v.optional(v.number()),
        attributeCount: v.number(),
        resourceAttributeCount: v.number(),
        cacheCreationInputTokens: v.optional(v.number()),
      }),
    ),
  },
  handler: async (ctx, { items }) => {
    for (const { spanDocumentId, ...facts } of items) {
      await ctx.db.patch("spans", spanDocumentId, {
        ...facts,
        input: undefined,
        output: undefined,
        attributes: undefined,
        resourceAttributes: undefined,
        eventsJson: undefined,
        linksJson: undefined,
        statusJson: undefined,
      });
    }
    return { archived: items.length };
  },
});
