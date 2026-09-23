import type {
  GenericActionCtx,
  GenericDataModel,
  GenericMutationCtx,
  GenericQueryCtx,
  FunctionArgs,
} from "convex/server";
import type { ComponentApi } from "./component/_generated/component.js";
import type { CaptureObservationV1 } from "./capture/index.js";
import { MAX_ENVELOPE_BYTES, validateSegment, sha256 } from "./capture/index.js";
import type { ModelCallV1, PrivateContentReference } from "./model-call/index.js";
import type { PrivateCaptureStorage } from "./content/index.js";
import { nanoTime, protocolForRoute, PARSER_VERSION } from "./protocols/index.js";
export * from "./model-call/index.js";
export { resolveCallBlob, type PrivateCaptureStorage } from "./content/index.js";
export type CliproxyObservabilityComponent = ComponentApi;
type ReadContext = Pick<
  | GenericQueryCtx<GenericDataModel>
  | GenericMutationCtx<GenericDataModel>
  | GenericActionCtx<GenericDataModel>,
  "runQuery"
>;
type ActionContext = Pick<GenericActionCtx<GenericDataModel>, "runMutation" | "runQuery">;
export class CliproxyObservability {
  constructor(readonly component: ComponentApi) {}
  pageRecentSummaries(
    ctx: ReadContext,
    args: FunctionArgs<ComponentApi["queries"]["pageRecentSummaries"]>,
  ) {
    return ctx.runQuery(this.component.queries.pageRecentSummaries, args);
  }
  pageCorrelationSummaries(
    ctx: ReadContext,
    args: FunctionArgs<ComponentApi["queries"]["pageRecentSummaries"]> & {
      correlation: NonNullable<
        FunctionArgs<ComponentApi["queries"]["pageRecentSummaries"]>["correlation"]
      >;
    },
  ) {
    return this.pageRecentSummaries(ctx, args);
  }
  getCall(ctx: ReadContext, args: { destinationId: string; callId: string }) {
    return ctx.runQuery(this.component.queries.getCall, args);
  }
  pageEventSegments(
    ctx: ReadContext,
    args: FunctionArgs<ComponentApi["queries"]["pageEventSegments"]>,
  ) {
    return ctx.runQuery(this.component.queries.pageEventSegments, args);
  }
  getBootHealth(
    ctx: ReadContext,
    args: { destinationId: string; instanceId: string; pluginBootId: string },
  ) {
    return ctx.runQuery(this.component.queries.getBootHealth, args);
  }
  getCaptureCoverage(ctx: ReadContext, args: { destinationId: string }) {
    return ctx.runQuery(this.component.queries.getCaptureCoverage, args);
  }
}
export function initialCall(
  o: CaptureObservationV1,
  callId: string,
  receivedAt: number,
): ModelCallV1 {
  return {
    schemaVersion: 1,
    source: "cliproxy",
    callId,
    sourceDocumentId: "",
    gateway: "cliproxy",
    authType: "unknown",
    route: o.route,
    configRevision: o.configRevision,
    destinationId: o.destinationId,
    instanceId: o.instanceId,
    pluginBootId: o.pluginBootId,
    executionId: o.requestId,
    sourceTraceId: o.sourceTraceId,
    traceId: o.correlation?.traceId,
    requestModel: o.requestedModel?.slice(0, 256),
    operation:
      o.route === "GET /v1/models"
        ? "discovery"
        : o.route === "POST /v1/messages/count_tokens"
          ? "token_count"
          : "generation",
    clientProtocol: protocolForRoute(o.route),
    state: "in_progress",
    timingSource: "plugin_observed",
    startTimeUnixNano: nanoTime(o.observedAt),
    correlation: o.correlation ?? {},
    correlationConflicts: o.correlationConflicts ?? [],
    usage: [],
    cost: { kind: "unknown" },
    attemptDetail: "unavailable",
    capture: {
      raw: "partial",
      projection: "partial",
      usage: "unavailable",
      preHook: "unavailable",
      projectedThroughSequence: 0,
      persistedThroughSequence: 0,
      gaps: [],
      parserVersion: PARSER_VERSION,
      pluginVersion: o.pluginVersion,
      redactionVersion: o.redactionVersion,
      lastObservedAt: o.observedAt,
    },
    receivedAt,
  };
}
export type CaptureHandlerOptions = {
  client: CliproxyObservability;
  destinationId: string;
  deploymentId: string;
  environment: "dev" | "prod";
  instanceIds: readonly string[];
  tokens: readonly string[];
  storage: PrivateCaptureStorage;
  scheduleProjection: (ctx: ActionContext, callId: string) => Promise<void>;
};
function tokenMatches(presented: string, expected: string): boolean {
  let diff = presented.length ^ expected.length;
  for (let i = 0; i < Math.max(presented.length, expected.length); i++)
    diff |= (presented.charCodeAt(i) || 0) ^ (expected.charCodeAt(i) || 0);
  return diff === 0;
}
export async function readBoundedCaptureBody(request: Request): Promise<Uint8Array> {
  if (Number(request.headers.get("Content-Length")) > MAX_ENVELOPE_BYTES)
    throw new RangeError("envelope limit");
  const reader = request.body?.getReader();
  if (!reader) return new Uint8Array();
  let size = 0;
  const chunks: Uint8Array[] = [];
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_ENVELOPE_BYTES) {
        await reader.cancel();
        throw new RangeError("envelope limit");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const out = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}
export async function handleCliproxyCaptureRequest(
  ctx: ActionContext,
  request: Request,
  options: CaptureHandlerOptions,
): Promise<Response> {
  if (request.method !== "POST") return new Response("method not allowed", { status: 405 });
  if (options.tokens.length === 0 || options.tokens.some((x) => x.length < 24))
    return new Response("capture unavailable", { status: 503 });
  const bearer = request.headers.get("Authorization") ?? "";
  if (
    !/^Bearer [^,\r\n]+$/.test(bearer) ||
    !options.tokens.some((token) => tokenMatches(bearer, `Bearer ${token}`))
  )
    return new Response("unauthorized", { status: 401 });
  if (request.headers.get("Content-Type")?.split(";")[0]?.trim() !== "application/json")
    return new Response("unsupported content type", { status: 415 });
  let parsed: Awaited<ReturnType<typeof validateSegment>>;
  try {
    const raw = await readBoundedCaptureBody(request);
    const candidate = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw));
    if (candidate?.operation === "health") {
      if (
        candidate.schemaVersion !== 1 ||
        candidate.destinationId !== options.destinationId ||
        !options.instanceIds.includes(candidate.instanceId)
      )
        return new Response("scope mismatch", { status: 403 });
      return Response.json({
        ready: true,
        destinationId: options.destinationId,
        deploymentId: options.deploymentId,
        instanceId: candidate.instanceId,
        schemaVersion: 1,
      });
    }
    if (candidate?.operation === "health_record") {
      if (
        raw.length > 16384 ||
        candidate.schemaVersion !== 1 ||
        candidate.destinationId !== options.destinationId ||
        !options.instanceIds.includes(candidate.instanceId) ||
        !/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,199}$/.test(candidate.pluginBootId) ||
        candidate.precommitCoverage !== "unknown_before_local_commit"
      )
        return new Response("invalid health scope", { status: 403 });
      const result = await ctx.runMutation(options.client.component.ingest.recordHealth, {
        destinationId: options.destinationId,
        instanceId: candidate.instanceId,
        pluginBootId: candidate.pluginBootId,
        startedAt: candidate.startedAt,
        observedAt: candidate.observedAt,
        observationsTotal: candidate.observationsTotal,
        droppedObservationsTotal: candidate.droppedObservationsTotal,
        lostControlObservationsTotal: candidate.lostControlObservationsTotal,
        scopeConflictsTotal: candidate.scopeConflictsTotal,
        expiredScopesTotal: candidate.expiredScopesTotal,
        activeCalls: candidate.activeCalls,
      });
      return Response.json({
        ...result,
        digest: await sha256(raw),
        destinationId: options.destinationId,
        deploymentId: options.deploymentId,
      });
    }
    parsed = await validateSegment(raw, options);
  } catch (error) {
    return new Response("invalid capture segment", {
      status: error instanceof RangeError ? 413 : 400,
    });
  }
  const { envelope, content, callId, identity, digest, observations } = parsed;
  const first = observations[0]!;
  const reference: PrivateContentReference = {
    key: `observability/cliproxy/v2/${envelope.destinationId}/${envelope.instanceId}/${callId}/${envelope.firstSequence}-${digest}.ndjson`,
    sha256: digest,
    byteLength: content.byteLength,
    contentType: "application/x-ndjson",
  };
  try {
    await options.storage.put(reference, content);
    const correlation = first.correlation ?? {};
    const terminal = observations.find((o) => o.kind === "completion");
    const receipt = await ctx.runMutation(options.client.component.ingest.admit, {
      destinationId: envelope.destinationId,
      instanceId: envelope.instanceId,
      pluginBootId: envelope.pluginBootId,
      executionId: envelope.requestId,
      callId,
      sequence: envelope.firstSequence,
      throughSequence: envelope.throughSequence,
      terminalSequence: terminal?.sequence,
      identity,
      digest,
      reference,
      summaryJson: JSON.stringify({
        ...initialCall(first, callId, Date.now()),
        deploymentId: options.deploymentId,
        environment: options.environment,
      }),
      observedAt: first.observedAt,
      correlation: {
        requestId: correlation.requestId,
        runId: correlation.runId,
        jobId: correlation.jobId,
        rootExecutionId: correlation.rootExecutionId,
        opencodeSessionId: correlation.opencodeSessionId,
      },
    });
    // Scheduling failure returns retryable 503 after durable receipt; retry is idempotent.
    await options.scheduleProjection(ctx, callId);
    return Response.json({ ...receipt, rawCommitted: true, projectionCommitted: false });
  } catch (error) {
    const message = error instanceof Error ? error.message : "";
    return new Response(
      message.includes("capture_digest_conflict") || message.includes("capture_sequence_conflict")
        ? "capture identity conflict"
        : "capture temporarily unavailable",
      {
        status:
          message.includes("capture_digest_conflict") ||
          message.includes("capture_sequence_conflict")
            ? 409
            : 503,
      },
    );
  }
}
export { projectPendingSegments } from "./projection.js";
