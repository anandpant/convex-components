import { v } from "convex/values";
import { query } from "./_generated/server.js";

// Convex documents can approach 1 MiB while a transaction may read at most
// 16 MiB. Eight rows leaves headroom for indexes, metadata, and host wrappers.
const DEFAULT_LIMIT = 8;
const MAXIMUM_LIMIT = 8;
const MAXIMUM_TRACE_PAGE_SIZE = MAXIMUM_LIMIT - 1;

type Cursor = {
  receivedAt: number;
  _creationTime: number;
};

function boundedLimit(limit: number | undefined) {
  const resolved = limit ?? DEFAULT_LIMIT;
  const requirements = [Number.isSafeInteger(resolved), resolved > 0, resolved <= MAXIMUM_LIMIT];
  if (!requirements.every(Boolean)) {
    throw new Error(`limit must be a positive integer no greater than ${MAXIMUM_LIMIT}`);
  }
  return resolved;
}

const listArgs = {
  limit: v.optional(v.number()),
  before: v.optional(
    v.object({
      receivedAt: v.number(),
      _creationTime: v.number(),
    }),
  ),
} as const;

async function readCursorPage<T>(
  limit: number,
  before: Cursor | undefined,
  readFirst: (limit: number) => Promise<Array<T>>,
  readSameTime: (before: Cursor, limit: number) => Promise<Array<T>>,
  readOlder: (before: Cursor, limit: number) => Promise<Array<T>>,
) {
  if (before === undefined) return await readFirst(limit);
  const sameTime = await readSameTime(before, limit);
  const remaining = limit - sameTime.length;
  if (remaining === 0) return sameTime;
  return [...sameTime, ...(await readOlder(before, remaining))];
}

export const getTrace = query({
  args: {
    traceId: v.string(),
    limit: v.optional(v.number()),
    afterSpanId: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit ?? MAXIMUM_TRACE_PAGE_SIZE);
    if (limit > MAXIMUM_TRACE_PAGE_SIZE) {
      throw new Error(
        `trace page limit must be no greater than ${MAXIMUM_TRACE_PAGE_SIZE} to reserve one bounded lookahead read`,
      );
    }
    const rows = await ctx.db
      .query("spans")
      .withIndex("by_trace_span", (index) => {
        const trace = index.eq("traceId", args.traceId);
        return args.afterSpanId === undefined ? trace : trace.gt("spanId", args.afterSpanId);
      })
      .take(limit + 1);
    const page = rows.slice(0, limit);
    const done = rows.length <= limit;
    return {
      page,
      cursor: done ? undefined : page.at(-1)?.spanId,
      done,
    };
  },
});

export const getSpan = query({
  args: { traceId: v.string(), spanId: v.string() },
  handler: async (ctx, args) =>
    await ctx.db
      .query("spans")
      .withIndex("by_trace_span", (index) =>
        index.eq("traceId", args.traceId).eq("spanId", args.spanId),
      )
      .unique(),
});

export const listBySession = query({
  args: { sessionId: v.string(), ...listArgs },
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit);
    return await readCursorPage(
      limit,
      args.before,
      async (take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_session", (index) => index.eq("sessionId", args.sessionId))
          .order("desc")
          .take(take),
      async (before, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_session", (index) =>
            index
              .eq("sessionId", args.sessionId)
              .eq("receivedAt", before.receivedAt)
              .lt("_creationTime", before._creationTime),
          )
          .order("desc")
          .take(take),
      async (before, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_session", (index) =>
            index.eq("sessionId", args.sessionId).lt("receivedAt", before.receivedAt),
          )
          .order("desc")
          .take(take),
    );
  },
});

export const listByUser = query({
  args: { userId: v.string(), ...listArgs },
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit);
    return await readCursorPage(
      limit,
      args.before,
      async (take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_user", (index) => index.eq("userId", args.userId))
          .order("desc")
          .take(take),
      async (before, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_user", (index) =>
            index
              .eq("userId", args.userId)
              .eq("receivedAt", before.receivedAt)
              .lt("_creationTime", before._creationTime),
          )
          .order("desc")
          .take(take),
      async (before, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_user", (index) =>
            index.eq("userId", args.userId).lt("receivedAt", before.receivedAt),
          )
          .order("desc")
          .take(take),
    );
  },
});

export const listByRequest = query({
  args: { requestId: v.string(), ...listArgs },
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit);
    return await readCursorPage(
      limit,
      args.before,
      async (take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_request", (index) => index.eq("requestId", args.requestId))
          .order("desc")
          .take(take),
      async (before, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_request", (index) =>
            index
              .eq("requestId", args.requestId)
              .eq("receivedAt", before.receivedAt)
              .lt("_creationTime", before._creationTime),
          )
          .order("desc")
          .take(take),
      async (before, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_request", (index) =>
            index.eq("requestId", args.requestId).lt("receivedAt", before.receivedAt),
          )
          .order("desc")
          .take(take),
    );
  },
});

export const listByEntity = query({
  args: {
    entityType: v.string(),
    entityId: v.string(),
    ...listArgs,
  },
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit);
    return await readCursorPage(
      limit,
      args.before,
      async (take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_entity", (index) =>
            index.eq("entityType", args.entityType).eq("entityId", args.entityId),
          )
          .order("desc")
          .take(take),
      async (before, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_entity", (index) =>
            index
              .eq("entityType", args.entityType)
              .eq("entityId", args.entityId)
              .eq("receivedAt", before.receivedAt)
              .lt("_creationTime", before._creationTime),
          )
          .order("desc")
          .take(take),
      async (before, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_entity", (index) =>
            index
              .eq("entityType", args.entityType)
              .eq("entityId", args.entityId)
              .lt("receivedAt", before.receivedAt),
          )
          .order("desc")
          .take(take),
    );
  },
});

export const listRecent = query({
  args: listArgs,
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit);
    return await readCursorPage(
      limit,
      args.before,
      async (take) => await ctx.db.query("spans").withIndex("by_received").order("desc").take(take),
      async (before, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_received", (index) =>
            index.eq("receivedAt", before.receivedAt).lt("_creationTime", before._creationTime),
          )
          .order("desc")
          .take(take),
      async (before, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_received", (index) => index.lt("receivedAt", before.receivedAt))
          .order("desc")
          .take(take),
    );
  },
});
