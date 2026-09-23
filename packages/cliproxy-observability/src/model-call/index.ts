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
  providerName?: string;
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
  usage: UsageMeasurement[];
  cost:
    | { kind: "unknown" }
    | { kind: "provider_billed" | "proxy_reported"; currency: string; total: string };
  attemptDetail: "unavailable";
  capture: {
    raw: EvidenceState;
    projection: EvidenceState;
    usage: EvidenceState;
    preHook: "unavailable";
    projectedThroughSequence: number;
    persistedThroughSequence: number;
    terminalSequence?: number;
    gaps: string[];
    parserVersion: string;
    pluginVersion?: string;
    redactionVersion?: string;
    lastObservedAt?: string;
  };
  receivedAt: number;
};
export type ModelCallSummaryV1 = ModelCallV1;

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
  for (const field of [
    "inputTokens",
    "outputTokens",
    "totalTokens",
    "reasoningTokens",
    "cachedInputTokens",
  ] as const) {
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
    providerName: span.providerName,
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
    usage,
    cost:
      typeof (span.totalCost ?? span.openrouterUsageCost) === "number" &&
      Number.isFinite(span.totalCost ?? span.openrouterUsageCost) &&
      (span.totalCost ?? span.openrouterUsageCost)! >= 0
        ? {
            kind: "proxy_reported",
            currency: "USD",
            total: String(span.totalCost ?? span.openrouterUsageCost),
          }
        : { kind: "unknown" },
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
    timingSource: "unavailable",
    cost: { kind: "unknown" },
  };
}
