import { getConvexSize, v } from "convex/values";
import type { Doc } from "./_generated/dataModel.js";
import { query } from "./_generated/server.js";
import { storedCacheWriteTokens } from "./parser.js";

// A page reads limit + 1 rows. Rows written before 0.5.0 can still hold ~900 KiB inline, so the
// limit stays at 7 until 0.6.0 removes inline content. Summaries cap display strings and the
// whole response, so response size does not follow span size.
const CORRELATION_PAGE_LIMIT = 7;
const SUMMARY_STRING_LIMIT = 128;
const SUMMARY_PAGE_BYTE_LIMIT = 32 * 1024;
const encoder = new TextEncoder();

type TimeCursor = { receivedAt: number; _creationTime: number };

const timeCursor = v.object({ receivedAt: v.number(), _creationTime: v.number() });
const correlation = v.union(
  v.object({ kind: v.literal("request"), requestId: v.string() }),
  v.object({ kind: v.literal("run"), runId: v.string() }),
  v.object({ kind: v.literal("job"), jobId: v.string() }),
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

/** Payload facts from the row; a row written before 0.5.0 still derives them from inline content. */
function payloadFacts(span: Doc<"spans">) {
  if (span.payload !== undefined) {
    return {
      contentBytes: span.payload.byteLength,
      inputUtf8Bytes: span.inputUtf8Bytes,
      outputUtf8Bytes: span.outputUtf8Bytes,
      attributeCount: span.attributeCount ?? 0,
      resourceAttributeCount: span.resourceAttributeCount ?? 0,
      cacheCreationInputTokens: span.cacheCreationInputTokens,
    };
  }
  const attributes = span.attributes ?? [];
  return {
    contentBytes: getConvexSize(span),
    inputUtf8Bytes: span.input === undefined ? undefined : encoder.encode(span.input).length,
    outputUtf8Bytes: span.output === undefined ? undefined : encoder.encode(span.output).length,
    attributeCount: attributes.length,
    resourceAttributeCount: span.resourceAttributes?.length ?? 0,
    cacheCreationInputTokens: storedCacheWriteTokens(attributes),
  };
}

function summarize(span: Doc<"spans">) {
  const truncatedFields: string[] = [];
  const facts = payloadFacts(span);
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
    reasoningTokens: span.reasoningTokens,
    cachedInputTokens: span.cachedInputTokens,
    cacheCreationInputTokens: facts.cacheCreationInputTokens,
    totalCost: span.totalCost,
    isByok: span.isByok,
    byokInferenceUsageCost: span.byokInferenceUsageCost,
    inputUtf8Bytes: facts.inputUtf8Bytes,
    outputUtf8Bytes: facts.outputUtf8Bytes,
    attributeCount: facts.attributeCount,
    resourceAttributeCount: facts.resourceAttributeCount,
    /** Bytes a full read of this span loads: its archived object, or its inline row. */
    contentBytes: facts.contentBytes,
    variantCount: span.variantCount ?? 0,
    truncatedFields,
  };
}

function fitSummaryPage(rows: Doc<"spans">[], rowLimit: number) {
  const page: ReturnType<typeof summarize>[] = [];
  for (const row of rows.slice(0, rowLimit)) {
    const candidate = [...page, summarize(row)];
    const bytes = encoder.encode(
      JSON.stringify({ page: candidate, cursor: null, done: false }),
    ).length;
    if (bytes > SUMMARY_PAGE_BYTE_LIMIT - 256 && page.length > 0) break;
    if (bytes > SUMMARY_PAGE_BYTE_LIMIT - 256) {
      throw new Error("one compact span summary exceeds the page byte limit");
    }
    page.push(candidate.at(-1) as ReturnType<typeof summarize>);
  }
  const done = page.length === rows.length && rows.length <= rowLimit;
  const last = page.length === 0 ? undefined : rows[page.length - 1];
  return {
    page,
    cursor:
      done || last === undefined
        ? undefined
        : { receivedAt: last.receivedAt, _creationTime: last._creationTime },
    done,
  };
}

/** Newest first. A continuation reads the cursor's tied timestamp, then strictly older rows. */
export const pageCorrelationSummaries = query({
  args: { correlation, limit: v.optional(v.number()), cursor: v.optional(timeCursor) },
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit, CORRELATION_PAGE_LIMIT);
    const readLimit = limit + 1;
    const value = args.correlation;
    const read = (
      range: "first" | "same" | "older",
      cursor: TimeCursor | undefined,
      take: number,
    ) => {
      const at = cursor?.receivedAt ?? 0;
      const created = cursor?._creationTime ?? 0;
      const spans = ctx.db.query("spans");
      if (value.kind === "request") {
        return spans
          .withIndex("by_request", (q) => {
            const keyed = q.eq("requestId", value.requestId);
            if (range === "first") return keyed;
            if (range === "same") return keyed.eq("receivedAt", at).lt("_creationTime", created);
            return keyed.lt("receivedAt", at);
          })
          .order("desc")
          .take(take);
      }
      if (value.kind === "run") {
        return spans
          .withIndex("by_run", (q) => {
            const keyed = q.eq("runId", value.runId);
            if (range === "first") return keyed;
            if (range === "same") return keyed.eq("receivedAt", at).lt("_creationTime", created);
            return keyed.lt("receivedAt", at);
          })
          .order("desc")
          .take(take);
      }
      return spans
        .withIndex("by_job", (q) => {
          const keyed = q.eq("jobId", value.jobId);
          if (range === "first") return keyed;
          if (range === "same") return keyed.eq("receivedAt", at).lt("_creationTime", created);
          return keyed.lt("receivedAt", at);
        })
        .order("desc")
        .take(take);
    };
    const before = args.cursor;
    let rows: Doc<"spans">[];
    if (before === undefined) rows = await read("first", undefined, readLimit);
    else {
      rows = await read("same", before, readLimit);
      if (rows.length < readLimit) {
        rows = [...rows, ...(await read("older", before, readLimit - rows.length))];
      }
    }
    return fitSummaryPage(rows, limit);
  },
});

/** The stored row. Its content is in the archived object `payload` names, unless held inline. */
export const getSpan = query({
  args: { spanDocumentId: v.id("spans") },
  handler: async (ctx, args) => await ctx.db.get("spans", args.spanDocumentId),
});

/**
 * Stored rows in creation order after `after`, for a host that reconciles a downstream copy.
 * Rows are slim, so a page of 200 reads about 200 KB; rows still holding inline content are
 * returned without it.
 */
export const pageStoredSince = query({
  args: {
    after: v.optional(v.number()),
    before: v.optional(v.number()),
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit, 200);
    const rows = await ctx.db
      .query("spans")
      .withIndex("by_creation_time", (q) => {
        const lower = args.after === undefined ? q : q.gt("_creationTime", args.after);
        return args.before === undefined ? lower : lower.lt("_creationTime", args.before);
      })
      .take(limit + 1);
    const page = rows.slice(0, limit).map((row) => ({
      spanDocumentId: row._id,
      creationTime: row._creationTime,
      receivedAt: row.receivedAt,
      traceId: row.traceId,
      spanId: row.spanId,
      payload: row.payload,
      variants: row.variants ?? [],
    }));
    return {
      page,
      cursor: page.length === 0 ? args.after : page[page.length - 1]?.creationTime,
      done: rows.length <= limit,
    };
  },
});

/** Recorded variants in creation order after `after`, including those beyond the row's cap. */
export const pageVariantsSince = query({
  args: {
    after: v.optional(v.number()),
    before: v.optional(v.number()),
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const limit = boundedLimit(args.limit, 200);
    const rows = await ctx.db
      .query("spanVariants")
      .withIndex("by_creation_time", (q) => {
        const lower = args.after === undefined ? q : q.gt("_creationTime", args.after);
        return args.before === undefined ? lower : lower.lt("_creationTime", args.before);
      })
      .take(limit + 1);
    const page = rows.slice(0, limit).map((row) => ({
      creationTime: row._creationTime,
      spanDocumentId: row.spanDocumentId,
      traceId: row.traceId,
      spanId: row.spanId,
      payload: { key: row.key, sha256: row.sha256, byteLength: row.byteLength },
      receivedAt: row.receivedAt,
    }));
    return {
      page,
      cursor: page.length === 0 ? args.after : page[page.length - 1]?.creationTime,
      done: rows.length <= limit,
    };
  },
});
