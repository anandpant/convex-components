import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import type { Id } from "./_generated/dataModel.js";
import { env, internalMutation, type MutationCtx } from "./_generated/server.js";
import { projectStoredCorrelationAttributes } from "./parser.js";

const DEFAULT_RETENTION_DAYS = 30;
const MAX_RETENTION_DAYS = 3650;
// A span can approach Convex's 1 MiB document limit. Keep each cleanup
// transaction below the 16 MiB read and write limits without relying on row
// size estimates after the documents have already been read.
const DELETE_BATCH_SIZE = 8;
const MIGRATION_BATCH_SIZE = 2;
const SPAN_MIGRATION = "prismantix-spans-v1";
const DELIVERY_MIGRATION = "prismantix-deliveries-v1";
const CORRELATION_MIGRATION = "correlation-projections-v1";
const MIGRATION_RETRY_DELAY_MS = 5 * 60 * 1000;

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

async function readMigrationPage(ctx: MutationCtx, after: Id<"spans"> | undefined) {
  return await ctx.db
    .query("spans")
    .withIndex("by_id", (query) => (after === undefined ? query : query.gt("_id", after)))
    .order("asc")
    .take(MIGRATION_BATCH_SIZE + 1);
}

async function migrationState(ctx: MutationCtx, name: string) {
  return await ctx.db
    .query("migrationState")
    .withIndex("by_name", (query) => query.eq("name", name))
    .unique();
}

async function markMigrationComplete(ctx: MutationCtx, name: string) {
  const state = await migrationState(ctx, name);
  if (state?.completedAt !== undefined) return;
  const completedAt = Date.now();
  if (state) {
    await ctx.db.patch("migrationState", state._id, { completedAt });
  } else {
    await ctx.db.insert("migrationState", { name, startedAt: completedAt, completedAt });
  }
}

async function recordMigrationHeartbeat(ctx: MutationCtx, name: string) {
  const state = await migrationState(ctx, name);
  const now = Date.now();
  if (state) {
    await ctx.db.patch("migrationState", state._id, {
      startedAt: state.startedAt ?? now,
      lastScheduledAt: now,
    });
  } else {
    await ctx.db.insert("migrationState", { name, startedAt: now, lastScheduledAt: now });
  }
}

async function ensureMigrationScheduled(
  ctx: MutationCtx,
  name: string,
  schedule: () => Promise<unknown>,
) {
  const state = await migrationState(ctx, name);
  if (state?.completedAt !== undefined) return;
  const now = Date.now();
  const lastScheduledAt = state?.lastScheduledAt ?? state?.startedAt ?? 0;
  if (state && now - lastScheduledAt < MIGRATION_RETRY_DELAY_MS) return;
  await recordMigrationHeartbeat(ctx, name);
  await schedule();
}

export const migrateLegacyData = internalMutation({
  args: { after: v.optional(v.id("spans")) },
  handler: async (ctx, args) => {
    if ((await migrationState(ctx, SPAN_MIGRATION))?.completedAt !== undefined) {
      return { migratedSpans: 0 };
    }
    const spans = await readMigrationPage(ctx, args.after);
    const page = spans.slice(0, MIGRATION_BATCH_SIZE);
    let migratedSpans = 0;
    for (const span of page) {
      if (span.resourceAttributes === undefined) {
        await ctx.db.patch("spans", span._id, { resourceAttributes: [] });
      }
      const key = await ctx.db
        .query("spanKeys")
        .withIndex("by_span_document", (query) => query.eq("spanDocumentId", span._id))
        .unique();
      if (!key) {
        await ctx.db.insert("spanKeys", {
          traceId: span.traceId,
          spanId: span.spanId,
          spanDocumentId: span._id,
        });
      }
      if (span.resourceAttributes === undefined || !key) migratedSpans += 1;
    }

    if (spans.length > MIGRATION_BATCH_SIZE) {
      const last = page.at(-1);
      if (!last) throw new Error("Migration page must contain a span before continuing");
      await recordMigrationHeartbeat(ctx, SPAN_MIGRATION);
      await ctx.scheduler.runAfter(0, internal.retention.migrateLegacyData, {
        after: last._id,
      });
    } else {
      await markMigrationComplete(ctx, SPAN_MIGRATION);
    }
    return { migratedSpans };
  },
});

export const backfillCorrelationProjections = internalMutation({
  args: { after: v.optional(v.id("spans")) },
  handler: async (ctx, args) => {
    const state = await migrationState(ctx, CORRELATION_MIGRATION);
    if (state?.completedAt !== undefined) return { processedSpans: 0 };
    const after = args.after ?? state?.lastProcessedSpanId;
    const spans = await readMigrationPage(ctx, after);
    const page = spans.slice(0, MIGRATION_BATCH_SIZE);
    for (const span of page) {
      const projected = projectStoredCorrelationAttributes(span.attributes);
      const attributesChanged = projected.attributes.length !== span.attributes.length;
      if (attributesChanged || Object.keys(projected.projections).length > 0) {
        await ctx.db.patch("spans", span._id, {
          ...projected.projections,
          attributes: projected.attributes,
        });
      }
    }

    const processedSpans = (state?.processedSpans ?? 0) + page.length;
    const last = page.at(-1);
    if (spans.length > MIGRATION_BATCH_SIZE) {
      if (!last) throw new Error("Projection page must contain a span before continuing");
      const now = Date.now();
      if (state) {
        await ctx.db.patch("migrationState", state._id, {
          startedAt: state.startedAt ?? now,
          lastScheduledAt: now,
          processedSpans,
          lastProcessedSpanId: last._id,
        });
      } else {
        await ctx.db.insert("migrationState", {
          name: CORRELATION_MIGRATION,
          startedAt: now,
          lastScheduledAt: now,
          processedSpans,
          lastProcessedSpanId: last._id,
        });
      }
      await ctx.scheduler.runAfter(0, internal.retention.backfillCorrelationProjections, {
        after: last._id,
      });
    } else {
      const now = Date.now();
      if (state) {
        await ctx.db.patch("migrationState", state._id, {
          completedAt: now,
          processedSpans,
          ...(last === undefined ? {} : { lastProcessedSpanId: last._id }),
        });
      } else {
        await ctx.db.insert("migrationState", {
          name: CORRELATION_MIGRATION,
          startedAt: now,
          completedAt: now,
          processedSpans,
          ...(last === undefined ? {} : { lastProcessedSpanId: last._id }),
        });
      }
    }
    return { processedSpans: page.length };
  },
});

export const deleteLegacyDeliveries = internalMutation({
  args: {},
  handler: async (ctx) => {
    if ((await migrationState(ctx, DELIVERY_MIGRATION))?.completedAt !== undefined) {
      return { deletedDeliveries: 0 };
    }
    const deliveries = await ctx.db.query("deliveries").take(MIGRATION_BATCH_SIZE);
    for (const delivery of deliveries) await ctx.db.delete("deliveries", delivery._id);
    if (deliveries.length === MIGRATION_BATCH_SIZE) {
      await recordMigrationHeartbeat(ctx, DELIVERY_MIGRATION);
      await ctx.scheduler.runAfter(0, internal.retention.deleteLegacyDeliveries, {});
    } else {
      await markMigrationComplete(ctx, DELIVERY_MIGRATION);
    }
    return { deletedDeliveries: deliveries.length };
  },
});

export const start = internalMutation({
  args: {},
  handler: async (ctx) => {
    const retentionMs = retentionDays() * 24 * 60 * 60 * 1000;
    await ensureMigrationScheduled(ctx, SPAN_MIGRATION, async () =>
      ctx.scheduler.runAfter(0, internal.retention.migrateLegacyData, {}),
    );
    await ensureMigrationScheduled(ctx, DELIVERY_MIGRATION, async () =>
      ctx.scheduler.runAfter(0, internal.retention.deleteLegacyDeliveries, {}),
    );
    await ensureMigrationScheduled(ctx, CORRELATION_MIGRATION, async () =>
      ctx.scheduler.runAfter(0, internal.retention.backfillCorrelationProjections, {}),
    );
    await ctx.scheduler.runAfter(0, internal.retention.deleteExpired, {
      cutoff: Date.now() - retentionMs,
    });
  },
});
