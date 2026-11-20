import { getConvexSize, v } from "convex/values";
import { mutation, query } from "./_generated/server.js";
import { storedCacheWriteTokens } from "./parser.js";
import { spanPayloadRef } from "./spanValues.js";

// Temporary: moves rows stored before 0.5.0 to archived objects. A host script pages inline
// rows, writes and confirms each object, then marks the row. 0.6.0 removes this module.

/** A page stops after the row that passes 8 MiB; inline rows are at most ~900 KiB, so a page
 * and a batch of its marks stay under the 16 MiB read and return limits. */
const PAGE_ROWS = 120;
const PAGE_BYTES = 8 * 1024 * 1024;
const encoder = new TextEncoder();

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
 * Point an inline row at its confirmed object and drop the inline content. A row already
 * archived is left as it is, so a repeated or resumed run changes nothing.
 */
export const markArchived = mutation({
  args: { items: v.array(v.object({ spanDocumentId: v.id("spans"), payload: spanPayloadRef })) },
  handler: async (ctx, { items }) => {
    const outcomes = { archived: 0, alreadyArchived: 0, missing: 0 };
    for (const { spanDocumentId, payload } of items) {
      const row = await ctx.db.get("spans", spanDocumentId);
      if (row === null) {
        outcomes.missing += 1;
        continue;
      }
      if (row.payload !== undefined) {
        outcomes.alreadyArchived += 1;
        continue;
      }
      const attributes = row.attributes ?? [];
      await ctx.db.patch("spans", spanDocumentId, {
        payload,
        inputUtf8Bytes: row.input === undefined ? undefined : encoder.encode(row.input).length,
        outputUtf8Bytes: row.output === undefined ? undefined : encoder.encode(row.output).length,
        attributeCount: attributes.length,
        resourceAttributeCount: row.resourceAttributes?.length ?? 0,
        cacheCreationInputTokens: storedCacheWriteTokens(attributes),
        input: undefined,
        output: undefined,
        attributes: undefined,
        resourceAttributes: undefined,
        eventsJson: undefined,
        linksJson: undefined,
        statusJson: undefined,
      });
      outcomes.archived += 1;
    }
    return outcomes;
  },
});
