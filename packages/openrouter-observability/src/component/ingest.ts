import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import { mutation, type MutationCtx } from "./_generated/server.js";
import {
  OtlpBoundExceededError,
  storedOpenRouterSpanSize,
  type ParsedOpenRouterSpan,
} from "./parser.js";
import { parsedSpan } from "./spanValues.js";

const MAX_DELIVERY_WRITE_BYTES = 8 * 1024 * 1024;
const SPAN_MIGRATION = "prismantix-spans-v1";
const CORRELATION_MIGRATION = "correlation-projections-v1";
const MIGRATION_RETRY_DELAY_MS = 5 * 60 * 1000;
const MAX_INLINE_CONTENT_BYTES = 64 * 1024;
const encoder = new TextEncoder();
const UNSAFE_JSON_NUMBER_MARKER = "$openrouterObservabilityJsonNumber";

function rejectOtlpError(error: unknown) {
  if (error instanceof OtlpBoundExceededError) {
    return { kind: "rejected", status: 413, message: error.message } as const;
  }
  throw error;
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function recognizedBase64Field(key: string, parent: Record<string, unknown>) {
  if (["b64_json", "image_base64", "audio_base64"].includes(key)) return true;
  if (key !== "data") return false;
  const declaredType = [parent.type, parent.media_type, parent.mime_type]
    .filter((value): value is string => typeof value === "string")
    .join(" ")
    .toLowerCase();
  return declaredType.includes("base64") || /(^|\s)(image|audio)\//.test(declaredType);
}

function assertExternalized(value: unknown, parent?: Record<string, unknown>, key = "") {
  if (typeof value === "string") {
    if (
      /^data:[^,\s]*,/.test(value) ||
      /^data:(?:image|audio|video|application)\//i.test(value) ||
      (parent !== undefined && recognizedBase64Field(key, parent)) ||
      encoder.encode(value).byteLength > MAX_INLINE_CONTENT_BYTES
    ) {
      throw new OtlpBoundExceededError("prepared span contains content that must be externalized");
    }
    return;
  }
  if (Array.isArray(value)) {
    for (const child of value) assertExternalized(child);
    return;
  }
  if (isRecord(value)) {
    if (UNSAFE_JSON_NUMBER_MARKER in value) {
      throw new OtlpBoundExceededError("prepared span contains a number that must be externalized");
    }
    for (const [childKey, child] of Object.entries(value)) {
      assertExternalized(child, value, childKey);
    }
  }
}

function assertPreparedSpans(spans: ParsedOpenRouterSpan[]) {
  for (const span of spans) {
    const values = [
      span.input,
      span.output,
      span.eventsJson,
      span.linksJson,
      span.statusJson,
      ...span.attributes.map(({ valueJson }) => valueJson),
      ...span.resourceAttributes.map(({ valueJson }) => valueJson),
    ];
    for (const raw of values) {
      if (raw === undefined) continue;
      let value: unknown;
      try {
        value = JSON.parse(raw) as unknown;
      } catch {
        value = raw;
      }
      assertExternalized(value);
    }
  }
}

export const admitPrepared = mutation({
  args: { spans: v.array(parsedSpan) },
  handler: async (ctx, args) => {
    if (!(await spanMigrationIsReady(ctx))) return { kind: "unavailable" } as const;
    await ensureCorrelationMigrationStarted(ctx);

    const newSpans: ParsedOpenRouterSpan[] = [];
    const deliveryKeys = new Set<string>();
    for (const value of args.spans) {
      const span: ParsedOpenRouterSpan = {
        ...value,
        resourceAttributes: value.resourceAttributes ?? [],
      };
      const key = JSON.stringify([span.traceId, span.spanId]);
      if (deliveryKeys.has(key) || (await spanKeyExists(ctx, span))) continue;
      deliveryKeys.add(key);
      newSpans.push(span);
    }
    try {
      assertPreparedSpans(newSpans);
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
      duplicates: args.spans.length - newSpans.length,
    } as const;
  },
});
