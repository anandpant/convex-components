const OPENROUTER_OTLP_LIMITS = {
  resourceSpans: 64,
  scopeSpans: 64,
  spans: 64,
  attributes: 256,
  events: 16,
  links: 16,
} as const;

export type StoredAttribute = {
  key: string;
  valueJson: string;
};

export type ParsedOpenRouterSpan = {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  name: string;
  kind?: number;
  startTimeUnixNano?: string;
  endTimeUnixNano?: string;
  serviceName?: string;
  openrouterTraceId?: string;
  userId?: string;
  sessionId?: string;
  requestId?: string;
  environment?: string;
  feature?: string;
  traceName?: string;
  spanType?: string;
  entityType?: string;
  entityId?: string;
  requestModel?: string;
  responseModel?: string;
  generationId?: string;
  providerName?: string;
  finishReason?: string;
  inputTokens?: number;
  outputTokens?: number;
  totalTokens?: number;
  reasoningTokens?: number;
  cachedInputTokens?: number;
  inputCost?: number;
  outputCost?: number;
  totalCost?: number;
  openrouterPromptTokens?: number;
  openrouterCompletionTokens?: number;
  openrouterUsageCost?: number;
  upstreamUsageCost?: number;
  cacheUsageCost?: number;
  dataUsageCost?: number;
  webUsageCost?: number;
  webFetchUsageCost?: number;
  upstreamWebFetchUsageCost?: number;
  fileUsageCost?: number;
  byokInferenceUsageCost?: number;
  creditPoolUsageCost?: number;
  creditPoolId?: string;
  creditPoolExpiresAt?: string;
  isByok?: boolean;
  apiKeyName?: string;
  streamed?: boolean;
  input?: string;
  output?: string;
  attributes: Array<StoredAttribute>;
  eventsJson?: string;
  linksJson?: string;
  statusJson?: string;
};

type JsonRecord = Record<string, unknown>;

type OtlpAttribute = {
  key: string;
  value: JsonRecord;
};

export class InvalidOtlpDeliveryError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidOtlpDeliveryError";
  }
}

export class OtlpBoundExceededError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "OtlpBoundExceededError";
  }
}

function invalid(message: string): never {
  throw new InvalidOtlpDeliveryError(message);
}

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireRecord(value: unknown, path: string) {
  if (!isRecord(value)) invalid(`${path} must be an object`);
  return value;
}

function requireArray(value: unknown, path: string) {
  if (!Array.isArray(value)) invalid(`${path} must be an array`);
  return value;
}

function checkCount(count: number, maximum: number, path: string) {
  if (count > maximum) {
    throw new OtlpBoundExceededError(`${path} exceeds the limit of ${maximum}`);
  }
}

function requireString(value: unknown, path: string) {
  if (typeof value !== "string" || value.length === 0) invalid(`${path} must be a string`);
  return value;
}

function optionalString(record: JsonRecord, key: string, path: string) {
  const value = record[key];
  if (value === undefined) return undefined;
  if (typeof value !== "string") invalid(`${path}.${key} must be a string`);
  return value;
}

function optionalNumber(record: JsonRecord, key: string, path: string) {
  const value = record[key];
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isFinite(value)) {
    invalid(`${path}.${key} must be a finite number`);
  }
  return value;
}

function parseAttributes(value: unknown, path: string) {
  if (value === undefined) return [];
  const values = requireArray(value, path);
  checkCount(values.length, OPENROUTER_OTLP_LIMITS.attributes, path);
  return values.map((entry, index): OtlpAttribute => {
    const attribute = requireRecord(entry, `${path}[${index}]`);
    const key = requireString(attribute.key, `${path}[${index}].key`);
    const typedValue = requireRecord(attribute.value, `${path}[${index}].value`);
    return { key, value: typedValue };
  });
}

function hasOnlyKey(value: JsonRecord, key: string) {
  const keys = Object.keys(value);
  return keys.length === 1 && keys[0] === key;
}

function typedString(value: JsonRecord) {
  if (!hasOnlyKey(value, "stringValue")) return undefined;
  return typeof value.stringValue === "string" ? value.stringValue : undefined;
}

function typedBoolean(value: JsonRecord) {
  if (!hasOnlyKey(value, "boolValue")) return undefined;
  return typeof value.boolValue === "boolean" ? value.boolValue : undefined;
}

function parseCanonicalInteger(value: unknown) {
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? value : undefined;
  }
  if (typeof value !== "string" || !/^-?(0|[1-9]\d*)$/.test(value)) return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) return undefined;
  return String(parsed) === value ? parsed : undefined;
}

function typedInteger(value: JsonRecord) {
  if (!hasOnlyKey(value, "intValue")) return undefined;
  return parseCanonicalInteger(value.intValue);
}

function typedDouble(value: JsonRecord) {
  if (!hasOnlyKey(value, "doubleValue")) return undefined;
  return typeof value.doubleValue === "number" && Number.isFinite(value.doubleValue)
    ? value.doubleValue
    : undefined;
}

function typedNumber(value: JsonRecord) {
  return typedDouble(value) ?? typedInteger(value);
}

function matchingIndices(
  attributes: ReadonlyArray<OtlpAttribute>,
  consumed: ReadonlySet<number>,
  key: string,
) {
  const indices = [];
  for (let index = 0; index < attributes.length; index += 1) {
    if (!consumed.has(index) && attributes[index]?.key === key) indices.push(index);
  }
  return indices;
}

function projectValue<T>(
  attributes: ReadonlyArray<OtlpAttribute>,
  consumed: Set<number>,
  key: string,
  decode: (value: JsonRecord) => T | undefined,
) {
  const indices = matchingIndices(attributes, consumed, key);
  if (indices.length !== 1) return undefined;
  const index = indices[0];
  if (index === undefined) return undefined;
  const attribute = attributes[index];
  if (!attribute) return undefined;
  const value = decode(attribute.value);
  if (value === undefined) return undefined;
  consumed.add(index);
  return value;
}

function projectGroup<T extends string | number | boolean>(
  attributes: ReadonlyArray<OtlpAttribute>,
  consumed: Set<number>,
  projections: Readonly<Record<string, string>>,
  decode: (value: JsonRecord) => T | undefined,
) {
  const projected: Record<string, T> = {};
  for (const [field, key] of Object.entries(projections)) {
    const value = projectValue(attributes, consumed, key, decode);
    if (value !== undefined) projected[field] = value;
  }
  return projected;
}

function dedupeContent(
  attributes: ReadonlyArray<OtlpAttribute>,
  consumed: Set<number>,
  keys: ReadonlySet<string>,
) {
  const indices = attributes.flatMap((attribute, index) =>
    keys.has(attribute.key) ? [index] : [],
  );
  if (indices.length === 0) return undefined;
  const values = indices.map((index) => typedString(attributes[index]?.value ?? {}));
  const first = values[0];
  if (first === undefined || !values.every((value) => value === first)) return undefined;
  for (const index of indices) consumed.add(index);
  return first;
}

function dedupeMetadata(attributes: ReadonlyArray<OtlpAttribute>, consumed: Set<number>) {
  const claimedTraceIndices = new Set<number>();
  for (let index = 0; index < attributes.length; index += 1) {
    const attribute = attributes[index];
    if (!attribute?.key.startsWith("span.metadata.")) continue;
    const traceKey = `trace.metadata.${attribute.key.slice("span.metadata.".length)}`;
    const matchingTraceIndex = attributes.findIndex(
      (candidate, candidateIndex) =>
        !claimedTraceIndices.has(candidateIndex) &&
        candidate.key === traceKey &&
        JSON.stringify(candidate.value) === JSON.stringify(attribute.value),
    );
    if (matchingTraceIndex === -1) continue;
    claimedTraceIndices.add(matchingTraceIndex);
    consumed.add(index);
  }
}

const INPUT_KEYS = new Set(["trace.input", "span.input", "gen_ai.prompt"]);
const OUTPUT_KEYS = new Set(["trace.output", "span.output", "gen_ai.completion"]);

const STRING_PROJECTIONS = {
  userId: "user.id",
  sessionId: "session.id",
  requestId: "trace.metadata.request_id",
  environment: "trace.metadata.environment",
  feature: "trace.metadata.feature",
  traceName: "trace.name",
  spanType: "span.type",
  entityType: "trace.metadata.entity_type",
  entityId: "trace.metadata.entity_id",
  requestModel: "gen_ai.request.model",
  responseModel: "gen_ai.response.model",
  generationId: "gen_ai.response.id",
  providerName: "trace.metadata.openrouter.provider_name",
  finishReason: "gen_ai.response.finish_reason",
  apiKeyName: "trace.metadata.openrouter.api_key_name",
  creditPoolId: "trace.metadata.openrouter_generation.credit_pool_id",
  creditPoolExpiresAt: "trace.metadata.openrouter_generation.credit_pool_expires_at",
} as const;

const INTEGER_PROJECTIONS = {
  inputTokens: "gen_ai.usage.input_tokens",
  outputTokens: "gen_ai.usage.output_tokens",
  totalTokens: "gen_ai.usage.total_tokens",
  reasoningTokens: "gen_ai.usage.output_tokens.reasoning",
  cachedInputTokens: "gen_ai.usage.input_tokens.cached",
  openrouterPromptTokens: "trace.metadata.openrouter_generation.tokens_prompt",
  openrouterCompletionTokens: "trace.metadata.openrouter_generation.tokens_completion",
} as const;

const DOUBLE_PROJECTIONS = {
  inputCost: "gen_ai.usage.input_cost",
  outputCost: "gen_ai.usage.output_cost",
  totalCost: "gen_ai.usage.total_cost",
} as const;

const NUMBER_PROJECTIONS = {
  openrouterUsageCost: "trace.metadata.openrouter_generation.usage",
  upstreamUsageCost: "trace.metadata.openrouter_generation.usage_upstream",
  cacheUsageCost: "trace.metadata.openrouter_generation.usage_cache",
  dataUsageCost: "trace.metadata.openrouter_generation.usage_data",
  webUsageCost: "trace.metadata.openrouter_generation.usage_web",
  webFetchUsageCost: "trace.metadata.openrouter_generation.usage_web_fetch",
  upstreamWebFetchUsageCost: "trace.metadata.openrouter_generation.usage_upstream_web_fetch",
  fileUsageCost: "trace.metadata.openrouter_generation.usage_file",
  byokInferenceUsageCost: "trace.metadata.openrouter_generation.byok_usage_inference",
  creditPoolUsageCost: "trace.metadata.openrouter_generation.credit_pool_usage",
} as const;

const BOOLEAN_PROJECTIONS = {
  streamed: "trace.metadata.openrouter_generation.streamed",
  isByok: "trace.metadata.openrouter_generation.is_byok",
} as const;

function extractProjections(attributes: ReadonlyArray<OtlpAttribute>, consumed: Set<number>) {
  return {
    ...projectGroup(attributes, consumed, STRING_PROJECTIONS, typedString),
    ...projectGroup(attributes, consumed, INTEGER_PROJECTIONS, typedInteger),
    ...projectGroup(attributes, consumed, DOUBLE_PROJECTIONS, typedDouble),
    ...projectGroup(attributes, consumed, NUMBER_PROJECTIONS, typedNumber),
    ...projectGroup(attributes, consumed, BOOLEAN_PROJECTIONS, typedBoolean),
  };
}

function serializeOptionalField(record: JsonRecord, key: string, path: string) {
  const value = record[key];
  if (value === undefined) return undefined;
  if (key === "events" || key === "links") {
    const values = requireArray(value, `${path}.${key}`);
    checkCount(values.length, OPENROUTER_OTLP_LIMITS[key], `${path}.${key}`);
  } else {
    requireRecord(value, `${path}.${key}`);
  }
  return JSON.stringify(value);
}

function resourceProjection(attributes: ReadonlyArray<OtlpAttribute>, key: string) {
  const matches = attributes.filter((attribute) => attribute.key === key);
  if (matches.length !== 1) return undefined;
  return typedString(matches[0]?.value ?? {});
}

function extractSpan(
  spanValue: unknown,
  resourceAttributes: ReadonlyArray<OtlpAttribute>,
  path: string,
): ParsedOpenRouterSpan {
  const span = requireRecord(spanValue, path);
  const attributes = parseAttributes(span.attributes, `${path}.attributes`);
  const consumed = new Set<number>();
  const input = dedupeContent(attributes, consumed, INPUT_KEYS);
  const output = dedupeContent(attributes, consumed, OUTPUT_KEYS);
  dedupeMetadata(attributes, consumed);
  const projections = extractProjections(attributes, consumed);
  const eventsJson = serializeOptionalField(span, "events", path);
  const linksJson = serializeOptionalField(span, "links", path);
  const statusJson = serializeOptionalField(span, "status", path);

  return {
    traceId: requireString(span.traceId, `${path}.traceId`),
    spanId: requireString(span.spanId, `${path}.spanId`),
    parentSpanId: optionalString(span, "parentSpanId", path),
    name: requireString(span.name, `${path}.name`),
    kind: optionalNumber(span, "kind", path),
    startTimeUnixNano: optionalString(span, "startTimeUnixNano", path),
    endTimeUnixNano: optionalString(span, "endTimeUnixNano", path),
    serviceName: resourceProjection(resourceAttributes, "service.name"),
    openrouterTraceId: resourceProjection(resourceAttributes, "openrouter.trace.id"),
    ...projections,
    input,
    output,
    attributes: attributes.flatMap((attribute, index) =>
      consumed.has(index)
        ? []
        : [{ key: attribute.key, valueJson: JSON.stringify(attribute.value) }],
    ),
    eventsJson,
    linksJson,
    statusJson,
  };
}

export function parseOpenRouterOtlpDelivery(value: unknown) {
  const delivery = requireRecord(value, "delivery");
  const resourceSpans = requireArray(delivery.resourceSpans, "delivery.resourceSpans");
  checkCount(resourceSpans.length, OPENROUTER_OTLP_LIMITS.resourceSpans, "delivery.resourceSpans");
  const parsedSpans: Array<ParsedOpenRouterSpan> = [];
  let scopeSpanCount = 0;
  for (let resourceIndex = 0; resourceIndex < resourceSpans.length; resourceIndex += 1) {
    const path = `delivery.resourceSpans[${resourceIndex}]`;
    const resourceSpan = requireRecord(resourceSpans[resourceIndex], path);
    const resource =
      resourceSpan.resource === undefined
        ? undefined
        : requireRecord(resourceSpan.resource, `${path}.resource`);
    const resourceAttributes = parseAttributes(resource?.attributes, `${path}.resource.attributes`);
    const scopeSpans = requireArray(resourceSpan.scopeSpans, `${path}.scopeSpans`);
    scopeSpanCount += scopeSpans.length;
    checkCount(scopeSpanCount, OPENROUTER_OTLP_LIMITS.scopeSpans, "delivery scopeSpans");
    for (let scopeIndex = 0; scopeIndex < scopeSpans.length; scopeIndex += 1) {
      const scopePath = `${path}.scopeSpans[${scopeIndex}]`;
      const scopeSpan = requireRecord(scopeSpans[scopeIndex], scopePath);
      const spans = requireArray(scopeSpan.spans, `${scopePath}.spans`);
      checkCount(parsedSpans.length + spans.length, OPENROUTER_OTLP_LIMITS.spans, "delivery spans");
      for (let spanIndex = 0; spanIndex < spans.length; spanIndex += 1) {
        parsedSpans.push(
          extractSpan(spans[spanIndex], resourceAttributes, `${scopePath}.spans[${spanIndex}]`),
        );
      }
    }
  }
  return parsedSpans;
}

export function assertSpanAttributeReconstruction(
  originalAttributes: unknown,
  parsed: ParsedOpenRouterSpan,
) {
  const attributes = parseAttributes(originalAttributes, "span.attributes");
  const consumed = new Set<number>();
  const expectedInput = dedupeContent(attributes, consumed, INPUT_KEYS);
  const expectedOutput = dedupeContent(attributes, consumed, OUTPUT_KEYS);
  dedupeMetadata(attributes, consumed);
  const expectedProjections = extractProjections(attributes, consumed);
  const expectedAttributes = attributes.flatMap((attribute, index) =>
    consumed.has(index) ? [] : [{ key: attribute.key, valueJson: JSON.stringify(attribute.value) }],
  );
  const valuesMatch =
    parsed.input === expectedInput &&
    parsed.output === expectedOutput &&
    Object.entries(expectedProjections).every(
      ([field, value]) => parsed[field as keyof ParsedOpenRouterSpan] === value,
    ) &&
    JSON.stringify(parsed.attributes) === JSON.stringify(expectedAttributes);
  if (!valuesMatch) {
    throw new Error("Parsed span cannot reconstruct the deduplicated source attributes");
  }
}
