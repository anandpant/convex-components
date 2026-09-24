import type { ExactCorrelation } from "../model-call/index.js";
export const MAX_ENVELOPE_BYTES = 2 * 1024 * 1024;
export const MAX_CONTENT_BYTES = 1024 * 1024;
const id = /^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,199}$/;
const routes = new Set([
  "GET /v1/models",
  "POST /v1/messages",
  "POST /v1/messages/count_tokens",
  "POST /v1/responses",
  "POST /v1/chat/completions",
]);
export type CaptureObservationV1 = {
  schemaVersion: 1;
  pluginVersion: string;
  /** Historical redacted observations retain their original policy label. */
  redactionVersion?: string;
  capturePolicy?: "hook-body-v1";
  destinationId: string;
  instanceId: string;
  pluginBootId: string;
  requestId: string;
  sequence: number;
  kind:
    | "request"
    | "request_after_auth"
    | "response"
    | "stream_init"
    | "stream_chunk"
    | "completion"
    | "capture_health";
  observedAt: string;
  offsetNs: number;
  route: string;
  configRevision: string;
  sourceFormat?: string;
  requestedModel?: string;
  executionModel?: string;
  selectedAuthId?: string;
  selectedAuthIndex?: string;
  executionProtocol?: string;
  metadataOmissions?: string[];
  sourceTraceId?: string;
  correlation?: ExactCorrelation;
  correlationConflicts?: string[];
  stockChunkIndex?: number;
  body?: string;
  observedBodyBytes?: number;
  bodyFraming?: "stock_json_chunk" | "stock_sse_candidate" | "stock_hook_chunk";
  contentSha256: string;
  contentBytes: number;
  completionOutcome?: string;
  executionStatusCode?: number;
  executionStartedAt?: string;
  executionCompletedAt?: string;
  /** Exact bounded stock completion diagnostic; private event content. */
  error?: string;
  errorPresent?: boolean;
  observedErrorBytes?: number;
  gap?: string;
  bodyFromSequence?: number;
  droppedObservationsTotal?: number;
  scopeConflictsTotal?: number;
};
export async function sha256(bytes: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", Uint8Array.from(bytes));
  return Array.from(new Uint8Array(digest), (v) => v.toString(16).padStart(2, "0")).join("");
}
export async function observationIdentity(o: CaptureObservationV1) {
  return sha256(
    new TextEncoder().encode(
      JSON.stringify([
        o.destinationId,
        o.instanceId,
        o.pluginBootId,
        o.requestId,
        o.sequence,
        o.kind,
      ]),
    ),
  );
}
export async function callIdentity(
  o: Pick<CaptureObservationV1, "destinationId" | "instanceId" | "pluginBootId" | "requestId">,
) {
  return sha256(
    new TextEncoder().encode(
      JSON.stringify([o.destinationId, o.instanceId, o.pluginBootId, o.requestId]),
    ),
  );
}
export function decodeBody(o: CaptureObservationV1): Uint8Array {
  const raw = o.body ?? "";
  if (
    raw.length > Math.ceil(MAX_CONTENT_BYTES / 3) * 4 ||
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(raw)
  )
    throw new Error("invalid body encoding");
  const decoded = atob(raw);
  if (decoded.length > MAX_CONTENT_BYTES || decoded.length !== o.contentBytes)
    throw new Error("body size mismatch");
  return Uint8Array.from(decoded, (c) => c.charCodeAt(0));
}
export async function validateObservation(
  raw: Uint8Array,
  expected: { destinationId: string; instanceIds: readonly string[] },
) {
  if (raw.byteLength > MAX_ENVELOPE_BYTES) throw new Error("envelope limit");
  const value: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("observation required");
  const o = value as CaptureObservationV1;
  const fields = new Set([
    "schemaVersion",
    "pluginVersion",
    "redactionVersion",
    "capturePolicy",
    "destinationId",
    "instanceId",
    "pluginBootId",
    "requestId",
    "sequence",
    "kind",
    "observedAt",
    "offsetNs",
    "route",
    "configRevision",
    "sourceFormat",
    "requestedModel",
    "executionModel",
    "executionProtocol",
    "selectedAuthId",
    "selectedAuthIndex",
    "metadataOmissions",
    "sourceTraceId",
    "correlation",
    "correlationConflicts",
    "stockChunkIndex",
    "body",
    "observedBodyBytes",
    "bodyFraming",
    "contentSha256",
    "contentBytes",
    "completionOutcome",
    "executionStatusCode",
    "executionStartedAt",
    "executionCompletedAt",
    "error",
    "errorPresent",
    "observedErrorBytes",
    "gap",
    "bodyFromSequence",
    "droppedObservationsTotal",
    "scopeConflictsTotal",
  ]);
  if (Object.keys(o).some((key) => !fields.has(key)))
    throw new Error("unsupported observation field");
  if (
    o.schemaVersion !== 1 ||
    o.destinationId !== expected.destinationId ||
    !expected.instanceIds.includes(o.instanceId)
  )
    throw new Error("scope mismatch");
  for (const field of [
    "destinationId",
    "instanceId",
    "pluginBootId",
    "requestId",
    "configRevision",
  ] as const) {
    if (typeof o[field] !== "string" || !id.test(o[field])) throw new Error("invalid identity");
  }
  if (
    !Number.isSafeInteger(o.sequence) ||
    o.sequence < 1 ||
    !Number.isSafeInteger(o.offsetNs) ||
    o.offsetNs < 0 ||
    !Number.isSafeInteger(o.contentBytes) ||
    o.contentBytes < 0 ||
    !routes.has(o.route) ||
    !Number.isFinite(Date.parse(o.observedAt))
  )
    throw new Error("invalid observation metadata");
  if (
    ![
      "request",
      "request_after_auth",
      "response",
      "stream_init",
      "stream_chunk",
      "completion",
    ].includes(o.kind) ||
    !/^[a-f0-9]{64}$/.test(o.contentSha256)
  )
    throw new Error("unsupported observation");
  for (const [key, val] of Object.entries(o.correlation ?? {})) {
    if (
      ![
        "requestId",
        "runId",
        "jobId",
        "traceId",
        "rootExecutionId",
        "opencodeSessionId",
        "operationId",
        "stepId",
        "partId",
        "attemptId",
      ].includes(key) ||
      typeof val !== "string" ||
      val.length > 256
    )
      throw new Error("invalid correlation");
  }
  if (
    o.correlationConflicts &&
    (!Array.isArray(o.correlationConflicts) ||
      o.correlationConflicts.length > 10 ||
      o.correlationConflicts.some((x) => typeof x !== "string" || x.length > 64))
  )
    throw new Error("invalid correlation conflicts");
  for (const field of [
    "pluginVersion",
    "redactionVersion",
    "sourceFormat",
    "requestedModel",
    "executionModel",
    "selectedAuthId",
    "selectedAuthIndex",
    "executionProtocol",
    "sourceTraceId",
    "gap",
    "completionOutcome",
  ] as const) {
    if (o[field] !== undefined && (typeof o[field] !== "string" || o[field]!.length > 256))
      throw new Error("invalid optional metadata");
  }
  if (!o.pluginVersion || (!o.redactionVersion && o.capturePolicy !== "hook-body-v1"))
    throw new Error("capture versions required");
  if (
    o.capturePolicy !== undefined &&
    (o.capturePolicy !== "hook-body-v1" || o.redactionVersion !== undefined)
  )
    throw new Error("invalid capture policy");
  if (
    o.metadataOmissions !== undefined &&
    (!Array.isArray(o.metadataOmissions) ||
      o.metadataOmissions.length > 16 ||
      o.metadataOmissions.some((x) => typeof x !== "string" || x.length > 64))
  )
    throw new Error("invalid metadata omissions");
  if (o.error !== undefined && (typeof o.error !== "string" || o.error.length > 4096))
    throw new Error("invalid error detail");
  if (
    (o.kind === "request_after_auth" || o.bodyFraming === "stock_hook_chunk") &&
    o.capturePolicy !== "hook-body-v1"
  )
    throw new Error("raw hook policy required");
  if (
    o.capturePolicy === "hook-body-v1" &&
    (o.bodyFromSequence !== undefined ||
      (o.kind === "stream_chunk" && o.bodyFraming !== "stock_hook_chunk"))
  )
    throw new Error("raw hook provenance required");
  if (
    o.kind === "completion" &&
    !["succeeded", "failed", "canceled", "rejected"].includes(o.completionOutcome ?? "")
  )
    throw new Error("invalid completion outcome");
  if (
    o.bodyFraming !== undefined &&
    !["stock_json_chunk", "stock_sse_candidate", "stock_hook_chunk"].includes(o.bodyFraming)
  )
    throw new Error("invalid body framing");
  if (
    o.observedBodyBytes !== undefined &&
    (!Number.isSafeInteger(o.observedBodyBytes) ||
      o.observedBodyBytes < 0 ||
      o.observedBodyBytes > MAX_ENVELOPE_BYTES)
  )
    throw new Error("invalid observed byte count");
  if (o.errorPresent !== undefined && typeof o.errorPresent !== "boolean")
    throw new Error("invalid error presence");
  if (
    o.observedErrorBytes !== undefined &&
    (!Number.isSafeInteger(o.observedErrorBytes) ||
      o.observedErrorBytes < 0 ||
      o.observedErrorBytes > MAX_ENVELOPE_BYTES)
  )
    throw new Error("invalid observed diagnostic size");
  const body = decodeBody(o);
  if ((await sha256(body)) !== o.contentSha256) throw new Error("content digest mismatch");
  return {
    observation: o,
    body,
    identity: await observationIdentity(o),
    callId: await callIdentity(o),
    digest: await sha256(raw),
  };
}

/** One immutable NDJSON segment batches observations: no per-token Convex writes. */
export type CaptureSegmentV1 = {
  schemaVersion: 1;
  operation: "segment";
  destinationId: string;
  instanceId: string;
  pluginBootId: string;
  requestId: string;
  firstSequence: number;
  throughSequence: number;
  contentSha256: string;
  contentBytes: number;
  contentBase64: string;
};
export const MAX_SEGMENT_BYTES = 1408 * 1024;
export async function validateSegment(
  raw: Uint8Array,
  expected: { destinationId: string; instanceIds: readonly string[] },
) {
  if (raw.length > MAX_ENVELOPE_BYTES) throw new Error("envelope limit");
  const envelope = JSON.parse(
    new TextDecoder("utf-8", { fatal: true }).decode(raw),
  ) as CaptureSegmentV1;
  if (
    envelope.schemaVersion !== 1 ||
    envelope.operation !== "segment" ||
    envelope.destinationId !== expected.destinationId ||
    !expected.instanceIds.includes(envelope.instanceId) ||
    !Number.isSafeInteger(envelope.firstSequence) ||
    !Number.isSafeInteger(envelope.throughSequence) ||
    envelope.firstSequence < 1 ||
    envelope.throughSequence < envelope.firstSequence ||
    envelope.throughSequence - envelope.firstSequence >= 256 ||
    typeof envelope.contentBase64 !== "string" ||
    envelope.contentBase64.length > Math.ceil(MAX_SEGMENT_BYTES / 3) * 4
  )
    throw new Error("invalid segment");
  const content = Uint8Array.from(atob(envelope.contentBase64), (c) => c.charCodeAt(0));
  if (
    content.byteLength !== envelope.contentBytes ||
    content.byteLength > MAX_SEGMENT_BYTES ||
    (await sha256(content)) !== envelope.contentSha256
  )
    throw new Error("segment integrity failure");
  const lines = new TextDecoder("utf-8", { fatal: true }).decode(content).split("\n");
  if (lines.at(-1) === "") lines.pop();
  if (lines.length !== envelope.throughSequence - envelope.firstSequence + 1)
    throw new Error("segment sequence count mismatch");
  const observations: CaptureObservationV1[] = [];
  for (const [index, line] of lines.entries()) {
    const { observation } = await validateObservation(new TextEncoder().encode(line), expected);
    if (
      observation.instanceId !== envelope.instanceId ||
      observation.pluginBootId !== envelope.pluginBootId ||
      observation.requestId !== envelope.requestId ||
      observation.sequence !== envelope.firstSequence + index
    )
      throw new Error("segment identity mismatch");
    observations.push(observation);
  }
  const first = observations[0]!;
  if (
    observations.some(
      (o, index) =>
        o.route !== first.route ||
        o.configRevision !== first.configRevision ||
        (o.kind === "completion" && index !== observations.length - 1),
    )
  )
    throw new Error("inconsistent segment lifecycle");
  const callId = await callIdentity(first);
  const identity = await sha256(
    new TextEncoder().encode(
      JSON.stringify([
        envelope.destinationId,
        envelope.instanceId,
        envelope.pluginBootId,
        envelope.requestId,
        envelope.firstSequence,
        envelope.throughSequence,
      ]),
    ),
  );
  return { envelope, observations, content, callId, identity, digest: envelope.contentSha256 };
}
