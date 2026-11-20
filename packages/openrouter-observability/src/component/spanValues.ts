import { v } from "convex/values";

export const storedAttribute = v.object({
  key: v.string(),
  valueJson: v.string(),
});

/** Identity, correlation, model, usage and cost: everything a summary or a lookup reads. */
const scalarSpanFields = {
  traceId: v.string(),
  spanId: v.string(),
  parentSpanId: v.optional(v.string()),
  name: v.string(),
  kind: v.optional(v.number()),
  startTimeUnixNano: v.optional(v.string()),
  endTimeUnixNano: v.optional(v.string()),
  serviceName: v.optional(v.string()),
  openrouterTraceId: v.optional(v.string()),
  userId: v.optional(v.string()),
  sessionId: v.optional(v.string()),
  requestId: v.optional(v.string()),
  runId: v.optional(v.string()),
  jobId: v.optional(v.string()),
  rootExecutionId: v.optional(v.string()),
  opencodeSessionId: v.optional(v.string()),
  environment: v.optional(v.string()),
  feature: v.optional(v.string()),
  traceName: v.optional(v.string()),
  spanType: v.optional(v.string()),
  entityType: v.optional(v.string()),
  entityId: v.optional(v.string()),
  requestModel: v.optional(v.string()),
  responseModel: v.optional(v.string()),
  generationId: v.optional(v.string()),
  providerName: v.optional(v.string()),
  finishReason: v.optional(v.string()),
  inputTokens: v.optional(v.number()),
  outputTokens: v.optional(v.number()),
  totalTokens: v.optional(v.number()),
  reasoningTokens: v.optional(v.number()),
  cachedInputTokens: v.optional(v.number()),
  inputCost: v.optional(v.number()),
  outputCost: v.optional(v.number()),
  totalCost: v.optional(v.number()),
  openrouterPromptTokens: v.optional(v.number()),
  openrouterCompletionTokens: v.optional(v.number()),
  openrouterUsageCost: v.optional(v.number()),
  upstreamUsageCost: v.optional(v.number()),
  cacheUsageCost: v.optional(v.number()),
  dataUsageCost: v.optional(v.number()),
  webUsageCost: v.optional(v.number()),
  webFetchUsageCost: v.optional(v.number()),
  upstreamWebFetchUsageCost: v.optional(v.number()),
  fileUsageCost: v.optional(v.number()),
  byokInferenceUsageCost: v.optional(v.number()),
  creditPoolUsageCost: v.optional(v.number()),
  creditPoolId: v.optional(v.string()),
  creditPoolExpiresAt: v.optional(v.string()),
  isByok: v.optional(v.boolean()),
  apiKeyName: v.optional(v.string()),
  streamed: v.optional(v.boolean()),
} as const;

/** Content that lives in the span's archived object, never in the row. */
const payloadFields = {
  input: v.optional(v.string()),
  output: v.optional(v.string()),
  attributes: v.array(storedAttribute),
  resourceAttributes: v.optional(v.array(storedAttribute)),
  eventsJson: v.optional(v.string()),
  linksJson: v.optional(v.string()),
  statusJson: v.optional(v.string()),
} as const;

/** The archived object holding a span's full content: the prepared span as UTF-8 JSON. */
export const spanPayloadRef = v.object({
  key: v.string(),
  sha256: v.string(),
  byteLength: v.number(),
});

/** Payload facts a summary needs, computed once at ingest so listing never loads content. */
const payloadFacts = {
  inputUtf8Bytes: v.optional(v.number()),
  outputUtf8Bytes: v.optional(v.number()),
  attributeCount: v.number(),
  resourceAttributeCount: v.number(),
  cacheCreationInputTokens: v.optional(v.number()),
} as const;

export const parsedSpanFields = { ...scalarSpanFields, ...payloadFields } as const;
export const parsedSpan = v.object(parsedSpanFields);

export const slimSpanFields = {
  ...scalarSpanFields,
  ...payloadFacts,
  payload: spanPayloadRef,
} as const;
export const slimSpan = v.object(slimSpanFields);

/**
 * A stored span. Rows written before 0.5.0 still hold their content inline and have no
 * `payload`; the one-off archive migration moves it out, and 0.6.0 drops these optional forms.
 */
export const storedSpanFields = {
  ...scalarSpanFields,
  ...payloadFacts,
  attributeCount: v.optional(v.number()),
  resourceAttributeCount: v.optional(v.number()),
  payload: v.optional(spanPayloadRef),
  ...payloadFields,
  attributes: v.optional(v.array(storedAttribute)),
  receivedAt: v.number(),
} as const;
