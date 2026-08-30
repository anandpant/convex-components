import { v } from "convex/values";
import { internalMutation, type MutationCtx } from "./_generated/server.js";
import {
  InvalidOtlpDeliveryError,
  OtlpBoundExceededError,
  parseOpenRouterOtlpDelivery,
  type ParsedOpenRouterSpan,
} from "./parser.js";

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

async function admitSpan(ctx: MutationCtx, span: ParsedOpenRouterSpan, receivedAt: number) {
  const existing = await ctx.db
    .query("spans")
    .withIndex("by_trace_span", (query) =>
      query.eq("traceId", span.traceId).eq("spanId", span.spanId),
    )
    .first();
  if (existing) return false;
  await ctx.db.insert("spans", { ...span, receivedAt });
  return true;
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

    const receivedAt = Date.now();
    let admitted = 0;
    for (const span of parsed.spans) {
      if (await admitSpan(ctx, span, receivedAt)) admitted += 1;
    }
    return {
      kind: "accepted",
      admitted,
      duplicates: parsed.spans.length - admitted,
    } as const;
  },
});
