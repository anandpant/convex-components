import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import { internalMutation, type MutationCtx } from "./_generated/server.js";
import {
  InvalidOtlpDeliveryError,
  OtlpBoundExceededError,
  parseOpenRouterOtlpDelivery,
  storedOpenRouterSpanSize,
  type ParsedOpenRouterSpan,
} from "./parser.js";

const MAX_DELIVERY_WRITE_BYTES = 8 * 1024 * 1024;
const SPAN_MIGRATION = "prismantix-spans-v1";
const CORRELATION_MIGRATION = "correlation-projections-v1";
const MIGRATION_RETRY_DELAY_MS = 5 * 60 * 1000;

function isEmptyOtlpEnvelope(value: unknown) {
  const record = value as Record<string, unknown> | null;
  return Array.isArray(record?.resourceSpans) && record.resourceSpans.length === 0;
}

function rejectOtlpError(error: unknown) {
  if (error instanceof OtlpBoundExceededError) {
    return { kind: "rejected", status: 413, message: error.message } as const;
  }
  if (error instanceof InvalidOtlpDeliveryError) {
    return { kind: "rejected", status: 400, message: error.message } as const;
  }
  throw error;
}

function parseJson(rawBody: string) {
  try {
    return { kind: "parsed", delivery: JSON.parse(rawBody) as unknown } as const;
  } catch {
    return { kind: "rejected", status: 400, message: "Invalid JSON" } as const;
  }
}

function parseDelivery(rawBody: string) {
  const parsed = parseJson(rawBody);
  if (parsed.kind === "rejected") return parsed;
  try {
    return {
      kind: "parsed",
      delivery: parsed.delivery,
      spans: parseOpenRouterOtlpDelivery(parsed.delivery),
    } as const;
  } catch (error) {
    return rejectOtlpError(error);
  }
}

async function spanKeyExists(ctx: MutationCtx, span: ParsedOpenRouterSpan) {
  return (
    (await ctx.db
      .query("spanKeys")
      .withIndex("by_trace_span", (query) =>
        query.eq("traceId", span.traceId).eq("spanId", span.spanId),
      )
      .first()) !== null
  );
}

async function spanMigrationIsReady(ctx: MutationCtx) {
  const state = await ctx.db
    .query("migrationState")
    .withIndex("by_name", (query) => query.eq("name", SPAN_MIGRATION))
    .unique();
  if (state?.completedAt !== undefined) return true;
  const now = Date.now();
  if (state !== null) {
    const lastScheduledAt = state.lastScheduledAt ?? state.startedAt ?? 0;
    if (now - lastScheduledAt >= MIGRATION_RETRY_DELAY_MS) {
      await ctx.db.patch("migrationState", state._id, {
        startedAt: state.startedAt ?? now,
        lastScheduledAt: now,
      });
      await ctx.scheduler.runAfter(0, internal.retention.migrateLegacyData, {});
    }
    return false;
  }

  const existingSpans = await ctx.db.query("spans").take(1);
  if (existingSpans.length === 0) {
    await ctx.db.insert("migrationState", {
      name: SPAN_MIGRATION,
      startedAt: now,
      completedAt: now,
    });
    return true;
  }

  await ctx.db.insert("migrationState", {
    name: SPAN_MIGRATION,
    startedAt: now,
    lastScheduledAt: now,
  });
  await ctx.scheduler.runAfter(0, internal.retention.migrateLegacyData, {});
  return false;
}

async function ensureCorrelationMigrationStarted(ctx: MutationCtx) {
  const state = await ctx.db
    .query("migrationState")
    .withIndex("by_name", (query) => query.eq("name", CORRELATION_MIGRATION))
    .unique();
  if (state?.completedAt !== undefined) return;
  const now = Date.now();
  if (state) {
    const lastScheduledAt = state.lastScheduledAt ?? state.startedAt ?? 0;
    if (now - lastScheduledAt < MIGRATION_RETRY_DELAY_MS) return;
    await ctx.db.patch("migrationState", state._id, {
      startedAt: state.startedAt ?? now,
      lastScheduledAt: now,
    });
    await ctx.scheduler.runAfter(0, internal.retention.backfillCorrelationProjections, {});
    return;
  }

  if ((await ctx.db.query("spans").take(1)).length === 0) {
    await ctx.db.insert("migrationState", {
      name: CORRELATION_MIGRATION,
      startedAt: now,
      completedAt: now,
      processedSpans: 0,
    });
    return;
  }

  await ctx.db.insert("migrationState", {
    name: CORRELATION_MIGRATION,
    startedAt: now,
    lastScheduledAt: now,
    processedSpans: 0,
  });
  await ctx.scheduler.runAfter(0, internal.retention.backfillCorrelationProjections, {});
}

function assertDeliveryWriteBound(spans: ParsedOpenRouterSpan[]) {
  const storedBytes = spans.reduce((total, span) => total + storedOpenRouterSpanSize(span), 0);
  if (storedBytes > MAX_DELIVERY_WRITE_BYTES) {
    throw new OtlpBoundExceededError(
      `expanded delivery exceeds the write limit of ${MAX_DELIVERY_WRITE_BYTES} bytes`,
    );
  }
}

export const admit = internalMutation({
  args: {
    rawBody: v.string(),
    isTestConnection: v.boolean(),
  },
  handler: async (ctx, args) => {
    const parsed = parseDelivery(args.rawBody);
    if (parsed.kind === "rejected") return parsed;
    if (args.isTestConnection && isEmptyOtlpEnvelope(parsed.delivery)) {
      return { kind: "test_connection" } as const;
    }
    if (!(await spanMigrationIsReady(ctx))) {
      return { kind: "unavailable" } as const;
    }
    await ensureCorrelationMigrationStarted(ctx);

    const newSpans: ParsedOpenRouterSpan[] = [];
    const deliveryKeys = new Set<string>();
    for (const span of parsed.spans) {
      const key = JSON.stringify([span.traceId, span.spanId]);
      if (deliveryKeys.has(key) || (await spanKeyExists(ctx, span))) continue;
      deliveryKeys.add(key);
      newSpans.push(span);
    }
    try {
      assertDeliveryWriteBound(newSpans);
    } catch (error) {
      return rejectOtlpError(error);
    }

    const receivedAt = Date.now();
    for (const span of newSpans) {
      const spanDocumentId = await ctx.db.insert("spans", { ...span, receivedAt });
      await ctx.db.insert("spanKeys", {
        traceId: span.traceId,
        spanId: span.spanId,
        spanDocumentId,
      });
    }
    return {
      kind: "accepted",
      admitted: newSpans.length,
      duplicates: parsed.spans.length - newSpans.length,
    } as const;
  },
});
