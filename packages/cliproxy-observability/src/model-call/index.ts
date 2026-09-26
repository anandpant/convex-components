/** Read contract only: storage and ingestion remain independently owned. */
export type ModelCallSource = "cliproxy" | "openrouter" | "cliproxy_legacy";
export type ModelCallState =
  "in_progress" | "succeeded" | "failed" | "aborted" | "rejected" | "interrupted" | "unknown";
export type EvidenceState = "complete" | "partial" | "unavailable" | "invalid" | "unsupported";
export type ExactCorrelation = Partial<
  Record<
    | "requestId"
    | "runId"
    | "jobId"
    | "traceId"
    | "rootExecutionId"
    | "opencodeSessionId"
    | "operationId"
    | "stepId"
    | "partId"
    | "attemptId",
    string
  >
>;
export type UsageMeasurement = {
  value: number;
  nativeField: string;
  source: "client_protocol" | "openrouter_span";
  scope: "call";
  unit: "tokens";
  finality: "final" | "partial" | "unknown";
  semanticsVersion: string;
};
export type PrivateContentReference = {
  key: string;
  sha256: string;
  byteLength: number;
  contentType: string;
};
/**
 * Where `providerName` came from. Only `observed` is recorded identity. A wire format
 * names an API family, not a vendor account; a model name is a guess.
 */
export type ProviderProvenance =
  "observed" | "derived_from_wire_format" | "derived_from_model" | "unavailable";
export type ModelCallCost =
  | { kind: "unknown" }
  | { kind: "provider_billed" | "proxy_reported"; currency: string; total: string };
/** Who reported `cost`. Nothing estimates cost, so native CLIProxy calls stay `unknown`. */
export type CostProvenance = ModelCallCost["kind"];
/** Per-call token counts, named and counted as the OpenRouter component names them. */
export const TOKEN_FIELDS = [
  "inputTokens",
  "outputTokens",
  "totalTokens",
  "reasoningTokens",
  "cachedInputTokens",
  "cacheCreationInputTokens",
] as const;
export type TokenField = (typeof TOKEN_FIELDS)[number];
export type ModelCallV1 = {
  schemaVersion: 1;
  source: ModelCallSource;
  callId: string;
  sourceDocumentId: string;
  gateway: "cliproxy" | "openrouter";
  deploymentId?: string;
  environment?: "dev" | "prod";
  route?: string;
  configRevision?: string;
  authType: "unknown";
  destinationId?: string;
  instanceId?: string;
  pluginBootId?: string;
  executionId?: string;
  sourceTraceId?: string;
  traceId?: string;
  spanId?: string;
  parentSpanId?: string;
  requestModel?: string;
  responseModel?: string;
  /** Last observed after-auth execution, not inferred provider identity. */
  executionModel?: string;
  selectedAuthId?: string;
  selectedAuthIndex?: string;
  /** CLIProxy's upstream wire format (ToFormat) from the last after-auth event. */
  executionProtocol?: string;
  providerName?: string;
  /** Stored from 0.3.0; reads derive it for older summaries. */
  providerProvenance?: ProviderProvenance;
  streamed?: boolean;
  operation: "generation" | "token_count" | "discovery" | "unknown";
  tokenCount?: number;
  clientProtocol?: "anthropic_messages" | "responses" | "chat_completions" | "unknown";
  state: ModelCallState;
  completionOutcome?: string;
  executionStatusCode?: number;
  protocolTerminal?: string;
  finishReason?: string;
  startTimeUnixNano?: string;
  endTimeUnixNano?: string;
  latencyMs?: number;
  timeToFirstByteMs?: number;
  timeToFirstContentMs?: number;
  timingSource: "plugin_observed" | "openrouter_span" | "unavailable";
  correlation: ExactCorrelation;
  correlationConflicts: string[];
  inputTokens?: number;
  outputTokens?: number;
  totalTokens?: number;
  reasoningTokens?: number;
  cachedInputTokens?: number;
  cacheCreationInputTokens?: number;
  usage: UsageMeasurement[];
  cost: ModelCallCost;
  /** Mirrors `cost.kind`. Stored from 0.3.0; reads derive it for older summaries. */
  costProvenance?: CostProvenance;
  attemptDetail: "unavailable";
  capture: {
    raw: EvidenceState;
    projection: EvidenceState;
    projectionIssue?:
      "malformed_payload" | "truncated_frame" | "frame_limit" | "unsupported_protocol";
    usage: EvidenceState;
    preHook: "unavailable";
    projectedThroughSequence: number;
    persistedThroughSequence: number;
    terminalSequence?: number;
    gaps: string[];
    parserVersion: string;
    pluginVersion?: string;
    redactionVersion?: string;
    capturePolicy?: "hook-body-v1";
    lastObservedAt?: string;
  };
  receivedAt: number;
};
export type ModelCallSummaryV1 = ModelCallV1;

/**
 * CLIProxy upstream wire formats (the after-auth ToFormat), by API family. A format names
 * the protocol CLIProxy speaks upstream, not the vendor account behind it: `claude` also
 * reaches other Anthropic-compatible hosts, and CLIProxy uses `codex` for xAI and Meta too.
 */
export const PROVIDER_BY_WIRE_FORMAT: Readonly<Record<string, string>> = {
  claude: "anthropic",
  openai: "openai",
  "openai-response": "openai",
  codex: "openai",
  gemini: "google",
  "gemini-cli": "google",
  antigravity: "google",
};
/** Requested-model prefixes: a guess from the model name. */
export const PROVIDER_BY_MODEL_PREFIX: ReadonlyArray<readonly [RegExp, string]> = [
  [/^claude-/, "anthropic"],
  [/^gpt-/, "openai"],
  [/^o\d/, "openai"],
  [/^codex/, "openai"],
  [/^gemini-/, "google"],
];
/**
 * Provider identity from one call's recorded facts, never from time or call order: an
 * observed provider, else the wire format's API family, else a guess from the requested
 * model. An absent or unmapped wire format falls back to the model rule.
 */
export function providerIdentity(
  facts: Pick<ModelCallV1, "requestModel" | "executionProtocol"> & {
    /** A provider named by the recording itself, such as an OpenRouter span. */
    observedProvider?: string;
  },
): Pick<ModelCallV1, "providerName" | "providerProvenance"> {
  if (facts.observedProvider)
    return { providerName: facts.observedProvider, providerProvenance: "observed" };
  if (
    facts.executionProtocol !== undefined &&
    Object.hasOwn(PROVIDER_BY_WIRE_FORMAT, facts.executionProtocol)
  )
    return {
      providerName: PROVIDER_BY_WIRE_FORMAT[facts.executionProtocol],
      providerProvenance: "derived_from_wire_format",
    };
  const providerName = PROVIDER_BY_MODEL_PREFIX.find(([prefix]) =>
    prefix.test(facts.requestModel ?? ""),
  )?.[1];
  return providerName
    ? { providerName, providerProvenance: "derived_from_model" }
    : { providerName: undefined, providerProvenance: "unavailable" };
}

/** Structural input: there is no runtime dependency on the OpenRouter component. */
export type OpenRouterSpanInput = {
  _id: string;
  receivedAt: number;
  totalCost?: number;
  openrouterUsageCost?: number;
  traceId?: string;
  spanId?: string;
  parentSpanId?: string;
  requestModel?: string;
  responseModel?: string;
  /** Last observed after-auth execution, not inferred provider identity. */
  executionModel?: string;
  selectedAuthId?: string;
  selectedAuthIndex?: string;
  executionProtocol?: string;
  providerName?: string;
  streamed?: boolean;
  startTimeUnixNano?: string;
  endTimeUnixNano?: string;
  finishReason?: string;
  requestId?: string;
  runId?: string;
  jobId?: string;
  rootExecutionId?: string;
  opencodeSessionId?: string;
  inputTokens?: number;
  outputTokens?: number;
  totalTokens?: number;
  reasoningTokens?: number;
  cachedInputTokens?: number;
  cacheCreationInputTokens?: number;
};
export function fromOpenRouterSpan(span: OpenRouterSpanInput): ModelCallV1 {
  const correlation: ExactCorrelation = {};
  for (const field of [
    "requestId",
    "runId",
    "jobId",
    "rootExecutionId",
    "opencodeSessionId",
  ] as const) {
    if (span[field] !== undefined) correlation[field] = span[field];
  }
  const usage: UsageMeasurement[] = [];
  for (const field of TOKEN_FIELDS) {
    const value = span[field];
    if (value !== undefined && Number.isSafeInteger(value) && value >= 0)
      usage.push({
        value,
        nativeField: field,
        source: "openrouter_span",
        scope: "call",
        unit: "tokens",
        finality: "unknown",
        semanticsVersion: "stored_openrouter_span_v1",
      });
  }
  const reported = span.totalCost ?? span.openrouterUsageCost;
  const cost: ModelCallCost =
    typeof reported === "number" && Number.isFinite(reported) && reported >= 0
      ? { kind: "proxy_reported", currency: "USD", total: String(reported) }
      : { kind: "unknown" };
  return {
    schemaVersion: 1,
    source: "openrouter",
    callId: `openrouter:${span._id}`,
    sourceDocumentId: span._id,
    gateway: "openrouter",
    authType: "unknown",
    traceId: span.traceId,
    spanId: span.spanId,
    parentSpanId: span.parentSpanId,
    requestModel: span.requestModel,
    responseModel: span.responseModel,
    ...providerIdentity({ observedProvider: span.providerName }),
    streamed: span.streamed,
    operation: "generation",
    state: "unknown",
    finishReason: span.finishReason,
    startTimeUnixNano: span.startTimeUnixNano,
    endTimeUnixNano: span.endTimeUnixNano,
    timingSource: "openrouter_span",
    correlation,
    correlationConflicts: [],
    inputTokens: span.inputTokens,
    outputTokens: span.outputTokens,
    totalTokens: span.totalTokens,
    reasoningTokens: span.reasoningTokens,
    cachedInputTokens: span.cachedInputTokens,
    cacheCreationInputTokens: span.cacheCreationInputTokens,
    usage,
    cost,
    costProvenance: cost.kind,
    attemptDetail: "unavailable",
    capture: {
      raw: "unavailable",
      projection: "partial",
      usage: usage.length ? "partial" : "unavailable",
      preHook: "unavailable",
      projectedThroughSequence: 0,
      persistedThroughSequence: 0,
      gaps: ["stream_events_and_attempt_detail_unavailable"],
      parserVersion: "openrouter-structural-v1",
    },
    receivedAt: span.receivedAt,
  };
}
export function fromLegacyCliproxySpan(span: OpenRouterSpanInput): ModelCallV1 {
  const call = fromOpenRouterSpan(span);
  return {
    ...call,
    source: "cliproxy_legacy",
    callId: `cliproxy_legacy:${span._id}`,
    gateway: "cliproxy",
    providerName: undefined,
    providerProvenance: "unavailable",
    timingSource: "unavailable",
    cost: { kind: "unknown" },
    costProvenance: "unknown",
  };
}
