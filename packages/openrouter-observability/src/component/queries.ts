import { v } from "convex/values";
import type { Doc } from "./_generated/dataModel.js";
import { query, type QueryCtx } from "./_generated/server.js";

// A page reads no more than eight full documents. Returned summaries omit all
// stored content and cap display strings, so response size does not follow span size.
const CORRELATION_PAGE_LIMIT = 7;
const TRACE_PAGE_LIMIT = 6;
const SUMMARY_STRING_LIMIT = 128;
const SUMMARY_PAGE_BYTE_LIMIT = 32 * 1024;
const CORRELATION_MIGRATION = "correlation-projections-v1";

type TimeCursor = { receivedAt: number; _creationTime: number };

const timeCursor = v.object({ receivedAt: v.number(), _creationTime: v.number() });
const correlation = v.union(
  v.object({ kind: v.literal("request"), requestId: v.string() }),
  v.object({ kind: v.literal("session"), sessionId: v.string() }),
  v.object({ kind: v.literal("user"), userId: v.string() }),
  v.object({ kind: v.literal("entity"), entityType: v.string(), entityId: v.string() }),
  v.object({ kind: v.literal("run"), runId: v.string() }),
  v.object({ kind: v.literal("job"), jobId: v.string() }),
  v.object({ kind: v.literal("rootExecution"), rootExecutionId: v.string() }),
  v.object({ kind: v.literal("opencodeSession"), opencodeSessionId: v.string() }),
);

function boundedLimit(limit: number | undefined, maximum: number) {
  const resolved = limit ?? maximum;
  if (!Number.isSafeInteger(resolved) || resolved < 1 || resolved > maximum) {
    throw new Error(`limit must be a positive integer no greater than ${maximum}`);
  }
  return resolved;
}

function boundedString(value: string, field: string, truncatedFields: string[]): string;
function boundedString(
  value: string | undefined,
  field: string,
  truncatedFields: string[],
): string | undefined;
function boundedString(value: string | undefined, field: string, truncatedFields: string[]) {
  if (value === undefined || value.length <= SUMMARY_STRING_LIMIT) return value;
  truncatedFields.push(field);
  return value.slice(0, SUMMARY_STRING_LIMIT);
}

function summarize(span: Doc<"spans">) {
  const truncatedFields: string[] = [];
  return {
    spanDocumentId: span._id,
    creationTime: span._creationTime,
    receivedAt: span.receivedAt,
    traceId: boundedString(span.traceId, "traceId", truncatedFields),
    spanId: boundedString(span.spanId, "spanId", truncatedFields),
    parentSpanId: boundedString(span.parentSpanId, "parentSpanId", truncatedFields),
    name: boundedString(span.name, "name", truncatedFields),
    startTimeUnixNano: boundedString(span.startTimeUnixNano, "startTimeUnixNano", truncatedFields),
    endTimeUnixNano: boundedString(span.endTimeUnixNano, "endTimeUnixNano", truncatedFields),
    serviceName: boundedString(span.serviceName, "serviceName", truncatedFields),
    requestId: boundedString(span.requestId, "requestId", truncatedFields),
    sessionId: boundedString(span.sessionId, "sessionId", truncatedFields),
    userId: boundedString(span.userId, "userId", truncatedFields),
    runId: boundedString(span.runId, "runId", truncatedFields),
    jobId: boundedString(span.jobId, "jobId", truncatedFields),
    rootExecutionId: boundedString(span.rootExecutionId, "rootExecutionId", truncatedFields),
    opencodeSessionId: boundedString(span.opencodeSessionId, "opencodeSessionId", truncatedFields),
    entityType: boundedString(span.entityType, "entityType", truncatedFields),
    entityId: boundedString(span.entityId, "entityId", truncatedFields),
    traceName: boundedString(span.traceName, "traceName", truncatedFields),
    spanType: boundedString(span.spanType, "spanType", truncatedFields),
    requestModel: boundedString(span.requestModel, "requestModel", truncatedFields),
    responseModel: boundedString(span.responseModel, "responseModel", truncatedFields),
    providerName: boundedString(span.providerName, "providerName", truncatedFields),
    finishReason: boundedString(span.finishReason, "finishReason", truncatedFields),
    inputTokens: span.inputTokens,
    outputTokens: span.outputTokens,
    totalTokens: span.totalTokens,
    totalCost: span.totalCost,
    inputUtf8Bytes:
      span.input === undefined ? undefined : new TextEncoder().encode(span.input).length,
    outputUtf8Bytes:
      span.output === undefined ? undefined : new TextEncoder().encode(span.output).length,
    attributeCount: span.attributes.length,
    resourceAttributeCount: span.resourceAttributes?.length ?? 0,
    truncatedFields,
  };
}

async function readTimePage<T>(
  limit: number,
  before: TimeCursor | undefined,
  first: (limit: number) => Promise<T[]>,
  sameTime: (before: TimeCursor, limit: number) => Promise<T[]>,
  older: (before: TimeCursor, limit: number) => Promise<T[]>,
) {
  if (before === undefined) return await first(limit);
  const same = await sameTime(before, limit);
  return same.length === limit ? same : [...same, ...(await older(before, limit - same.length))];
}

function fitSummaryPage(rows: Doc<"spans">[], rowLimit: number) {
  const page: ReturnType<typeof summarize>[] = [];
  for (const row of rows.slice(0, rowLimit)) {
    const candidate = [...page, summarize(row)];
    const bytes = new TextEncoder().encode(
      JSON.stringify({ page: candidate, cursor: null, done: false }),
    ).length;
    if (bytes > SUMMARY_PAGE_BYTE_LIMIT - 256 && page.length > 0) break;
    if (bytes > SUMMARY_PAGE_BYTE_LIMIT - 256) {
      throw new Error("one compact span summary exceeds the page byte limit");
    }
    page.push(candidate.at(-1) as ReturnType<typeof summarize>);
  }
  const done = page.length === rows.length && rows.length <= rowLimit;
  const lastDocument = page.length === 0 ? undefined : rows[page.length - 1];
  return { page, done, lastDocument };
}

function pageResult(rows: Doc<"spans">[], limit: number) {
  const fitted = fitSummaryPage(rows, limit);
  const { page, done, lastDocument: last } = fitted;
  return {
    page,
    cursor:
      done || last === undefined
        ? undefined
        : { receivedAt: last.receivedAt, _creationTime: last._creationTime },
    done,
  };
}

async function correlationCoverage(ctx: QueryCtx) {
  const state = await ctx.db
    .query("migrationState")
    .withIndex("by_name", (index) => index.eq("name", CORRELATION_MIGRATION))
    .unique();
  if (state === null && (await ctx.db.query("spans").take(1)).length === 0) {
    return { state: "ready" as const, processedSpans: 0 };
  }
  return state?.completedAt === undefined
    ? {
        state: state === null ? ("not_started" as const) : ("in_progress" as const),
        processedSpans: state?.processedSpans ?? 0,
      }
    : { state: "ready" as const, processedSpans: state.processedSpans ?? 0 };
}

export const getCorrelationProjectionCoverage = query({
  args: {},
  handler: async (ctx) => await correlationCoverage(ctx),
});

export const pageTraceSummaries = query({
  args: {
    traceId: v.string(),
    limit: v.optional(v.number()),
    cursor: v.optional(v.id("spans")),
  },
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit, TRACE_PAGE_LIMIT);
    let afterSpanId: string | undefined;
    if (args.cursor !== undefined) {
      const cursorSpan = await ctx.db.get("spans", args.cursor);
      if (!cursorSpan || cursorSpan.traceId !== args.traceId)
        throw new Error("invalid trace cursor");
      afterSpanId = cursorSpan.spanId;
    }
    const rows = await ctx.db
      .query("spans")
      .withIndex("by_trace_span", (index) => {
        const trace = index.eq("traceId", args.traceId);
        return afterSpanId === undefined ? trace : trace.gt("spanId", afterSpanId);
      })
      .take(limit + 1);
    const fitted = fitSummaryPage(rows, limit);
    const { page, done, lastDocument } = fitted;
    return {
      page,
      cursor: done ? undefined : lastDocument?._id,
      done,
    };
  },
});

export const pageCorrelationSummaries = query({
  args: { correlation, limit: v.optional(v.number()), cursor: v.optional(timeCursor) },
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit, CORRELATION_PAGE_LIMIT);
    const readLimit = limit + 1;
    const before = args.cursor;
    const paginate = async (
      first: (take: number) => Promise<Doc<"spans">[]>,
      same: (cursor: TimeCursor, take: number) => Promise<Doc<"spans">[]>,
      older: (cursor: TimeCursor, take: number) => Promise<Doc<"spans">[]>,
    ) => pageResult(await readTimePage(readLimit, before, first, same, older), limit);

    const value = args.correlation;
    if (["run", "job", "rootExecution", "opencodeSession"].includes(value.kind)) {
      const coverage = await correlationCoverage(ctx);
      if (coverage.state !== "ready") return { status: "not_ready" as const, coverage };
    }

    let result;
    if (value.kind === "request") {
      result = await paginate(
        async (take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_request", (q) => q.eq("requestId", value.requestId))
            .order("desc")
            .take(take),
        async (cursor, take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_request", (q) =>
              q
                .eq("requestId", value.requestId)
                .eq("receivedAt", cursor.receivedAt)
                .lt("_creationTime", cursor._creationTime),
            )
            .order("desc")
            .take(take),
        async (cursor, take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_request", (q) =>
              q.eq("requestId", value.requestId).lt("receivedAt", cursor.receivedAt),
            )
            .order("desc")
            .take(take),
      );
    } else if (value.kind === "session") {
      result = await paginate(
        async (take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_session", (q) => q.eq("sessionId", value.sessionId))
            .order("desc")
            .take(take),
        async (cursor, take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_session", (q) =>
              q
                .eq("sessionId", value.sessionId)
                .eq("receivedAt", cursor.receivedAt)
                .lt("_creationTime", cursor._creationTime),
            )
            .order("desc")
            .take(take),
        async (cursor, take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_session", (q) =>
              q.eq("sessionId", value.sessionId).lt("receivedAt", cursor.receivedAt),
            )
            .order("desc")
            .take(take),
      );
    } else if (value.kind === "user") {
      result = await paginate(
        async (take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_user", (q) => q.eq("userId", value.userId))
            .order("desc")
            .take(take),
        async (cursor, take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_user", (q) =>
              q
                .eq("userId", value.userId)
                .eq("receivedAt", cursor.receivedAt)
                .lt("_creationTime", cursor._creationTime),
            )
            .order("desc")
            .take(take),
        async (cursor, take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_user", (q) =>
              q.eq("userId", value.userId).lt("receivedAt", cursor.receivedAt),
            )
            .order("desc")
            .take(take),
      );
    } else if (value.kind === "entity") {
      result = await paginate(
        async (take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_entity", (q) =>
              q.eq("entityType", value.entityType).eq("entityId", value.entityId),
            )
            .order("desc")
            .take(take),
        async (cursor, take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_entity", (q) =>
              q
                .eq("entityType", value.entityType)
                .eq("entityId", value.entityId)
                .eq("receivedAt", cursor.receivedAt)
                .lt("_creationTime", cursor._creationTime),
            )
            .order("desc")
            .take(take),
        async (cursor, take) =>
          await ctx.db
            .query("spans")
            .withIndex("by_entity", (q) =>
              q
                .eq("entityType", value.entityType)
                .eq("entityId", value.entityId)
                .lt("receivedAt", cursor.receivedAt),
            )
            .order("desc")
            .take(take),
      );
    } else {
      const config =
        value.kind === "run"
          ? { index: "by_run" as const, field: "runId" as const, id: value.runId }
          : value.kind === "job"
            ? { index: "by_job" as const, field: "jobId" as const, id: value.jobId }
            : value.kind === "rootExecution"
              ? {
                  index: "by_root_execution" as const,
                  field: "rootExecutionId" as const,
                  id: value.rootExecutionId,
                }
              : {
                  index: "by_opencode_session" as const,
                  field: "opencodeSessionId" as const,
                  id: value.opencodeSessionId,
                };
      const readProjected = (
        mode: "first" | "same" | "older",
        cursor?: TimeCursor,
        take = readLimit,
      ) => {
        if (config.index === "by_run")
          return ctx.db
            .query("spans")
            .withIndex(config.index, (q) =>
              mode === "first"
                ? q.eq(config.field, config.id)
                : mode === "same"
                  ? q
                      .eq(config.field, config.id)
                      .eq("receivedAt", cursor?.receivedAt ?? 0)
                      .lt("_creationTime", cursor?._creationTime ?? 0)
                  : q.eq(config.field, config.id).lt("receivedAt", cursor?.receivedAt ?? 0),
            )
            .order("desc")
            .take(take);
        if (config.index === "by_job")
          return ctx.db
            .query("spans")
            .withIndex(config.index, (q) =>
              mode === "first"
                ? q.eq(config.field, config.id)
                : mode === "same"
                  ? q
                      .eq(config.field, config.id)
                      .eq("receivedAt", cursor?.receivedAt ?? 0)
                      .lt("_creationTime", cursor?._creationTime ?? 0)
                  : q.eq(config.field, config.id).lt("receivedAt", cursor?.receivedAt ?? 0),
            )
            .order("desc")
            .take(take);
        if (config.index === "by_root_execution")
          return ctx.db
            .query("spans")
            .withIndex(config.index, (q) =>
              mode === "first"
                ? q.eq(config.field, config.id)
                : mode === "same"
                  ? q
                      .eq(config.field, config.id)
                      .eq("receivedAt", cursor?.receivedAt ?? 0)
                      .lt("_creationTime", cursor?._creationTime ?? 0)
                  : q.eq(config.field, config.id).lt("receivedAt", cursor?.receivedAt ?? 0),
            )
            .order("desc")
            .take(take);
        return ctx.db
          .query("spans")
          .withIndex(config.index, (q) =>
            mode === "first"
              ? q.eq(config.field, config.id)
              : mode === "same"
                ? q
                    .eq(config.field, config.id)
                    .eq("receivedAt", cursor?.receivedAt ?? 0)
                    .lt("_creationTime", cursor?._creationTime ?? 0)
                : q.eq(config.field, config.id).lt("receivedAt", cursor?.receivedAt ?? 0),
          )
          .order("desc")
          .take(take);
      };
      result = await paginate(
        async (take) => await readProjected("first", undefined, take),
        async (cursor, take) => await readProjected("same", cursor, take),
        async (cursor, take) => await readProjected("older", cursor, take),
      );
    }
    return { status: "ready" as const, ...result };
  },
});

export const pageRecentSummaries = query({
  args: { limit: v.optional(v.number()), cursor: v.optional(timeCursor) },
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit, CORRELATION_PAGE_LIMIT);
    const rows = await readTimePage(
      limit + 1,
      args.cursor,
      async (take) => await ctx.db.query("spans").withIndex("by_received").order("desc").take(take),
      async (cursor, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_received", (q) =>
            q.eq("receivedAt", cursor.receivedAt).lt("_creationTime", cursor._creationTime),
          )
          .order("desc")
          .take(take),
      async (cursor, take) =>
        await ctx.db
          .query("spans")
          .withIndex("by_received", (q) => q.lt("receivedAt", cursor.receivedAt))
          .order("desc")
          .take(take),
    );
    return pageResult(rows, limit);
  },
});

export const exportFullSpan = query({
  args: { spanDocumentId: v.id("spans") },
  handler: async (ctx, args) => await ctx.db.get("spans", args.spanDocumentId),
});
