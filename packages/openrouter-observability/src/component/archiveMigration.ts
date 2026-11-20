import { v } from "convex/values";
import { mutation, query } from "./_generated/server.js";
import { storedCacheWriteTokens } from "./parser.js";
import { spanPayloadRef } from "./spanValues.js";

// Temporary: moves rows stored before 0.5.0 to archived objects. A host script pages inline
// rows, writes and confirms each object, then marks the row. 0.6.0 removes this module.

/** Inline rows are at most ~900 KiB, so 16 per page stays under the read and return limits. */
const PAGE_ROWS = 16;
const encoder = new TextEncoder();

/** The next rows, in `_id` order after `after`, that still hold their content inline. */
export const pageInline = query({
  args: { after: v.optional(v.id("spans")), limit: v.optional(v.number()) },
  handler: async (ctx, args) => {
    const limit = Math.min(args.limit ?? PAGE_ROWS, PAGE_ROWS);
    const rows = await ctx.db
      .query("spans")
      .withIndex("by_id", (q) => (args.after === undefined ? q : q.gt("_id", args.after)))
      .take(limit);
    return {
      spans: rows.filter((row) => row.payload === undefined),
      last: rows.at(-1)?._id ?? null,
      done: rows.length < limit,
    };
  },
});

/**
 * Point an inline row at its confirmed object and drop the inline content. A row already
 * archived is left as it is, so a repeated or resumed run changes nothing.
 */
export const markArchived = mutation({
  args: { spanDocumentId: v.id("spans"), payload: spanPayloadRef },
  handler: async (ctx, { spanDocumentId, payload }) => {
    const row = await ctx.db.get("spans", spanDocumentId);
    if (row === null) return "missing" as const;
    if (row.payload !== undefined) return "already_archived" as const;
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
    return "archived" as const;
  },
});
