import {
  providerIdentity,
  type ModelCallV1,
  type UsageMeasurement,
  type PrivateContentReference,
} from "../model-call/index.js";
import type { CaptureObservationV1 } from "../capture/index.js";
import { decodeBody } from "../capture/index.js";
import { assembleResponsesStream } from "./responses.js";
import { SSEReader, type SSECheckpoint } from "./framing.js";
import { count, list, nanoTime, object, parseObject } from "./values.js";
import { extractUsage, TOKEN_FIELDS } from "./usage.js";
import type { CliproxyProjection, RecordValue } from "./types.js";
/** Bounded semantic state. Emitted text/tools stay in immutable content segments. */
export type ProjectionCheckpoint = {
  sse?: SSECheckpoint;
  sseBlob?: PrivateContentReference;
  usage?: RecordValue;
  terminal?: string;
  usageFinal?: boolean;
  invalid?: boolean;
  truncated?: boolean;
  frameLimit?: boolean;
  unsupported?: boolean;
  rawGap?: boolean;
  streamed?: boolean;
  inputSeen?: boolean;
  chatFinished?: boolean;
};
const usageFields = [
  "input_tokens",
  "output_tokens",
  "total_tokens",
  "prompt_tokens",
  "completion_tokens",
  "cache_read_input_tokens",
  "cache_creation_input_tokens",
] as const;
function boundedUsage(value: unknown): RecordValue {
  const raw = object(value);
  const out: RecordValue = {};
  for (const field of usageFields) {
    if (count(raw[field]) !== undefined) out[field] = raw[field];
  }
  for (const field of [
    "input_tokens_details",
    "output_tokens_details",
    "prompt_tokens_details",
    "completion_tokens_details",
    "cache_creation",
  ]) {
    const detail = object(raw[field]);
    const safe: RecordValue = {};
    for (const name of [
      "cached_tokens",
      "cache_write_tokens",
      "cached_creation_tokens",
      "cache_creation_tokens",
      "reasoning_tokens",
      "thinking_tokens",
      "ephemeral_5m_input_tokens",
      "ephemeral_1h_input_tokens",
    ]) {
      if (count(detail[name]) !== undefined) safe[name] = detail[name];
    }
    if (Object.keys(safe).length) out[field] = safe;
  }
  return out;
}
function semantic(event: RecordValue): boolean {
  const delta = object(event.delta);
  return (
    [delta.text, delta.thinking, delta.partial_json].some(
      (x) => typeof x === "string" && x.length > 0,
    ) ||
    (typeof event.delta === "string" && event.delta.length > 0) ||
    list(event.choices).some((c) => {
      const d = object(object(c).delta);
      return (
        [d.content, d.reasoning_content, d.reasoning].some(
          (x) => typeof x === "string" && x.length > 0,
        ) || list(d.tool_calls).length > 0
      );
    })
  );
}
function observeProtocol(
  call: ModelCallV1,
  state: ProjectionCheckpoint,
  event: RecordValue,
  stream: boolean,
) {
  const protocol = call.clientProtocol;
  const type = event.type;
  if (stream && protocol === "responses") {
    // Shared assembler determines whether the event vocabulary is supported; sequence
    // validation belongs to selected-call content replay, not this scalar checkpoint.
    if (assembleResponsesStream([event]).unsupported) state.unsupported = true;
  }
  if (typeof event.model === "string") call.responseModel = event.model.slice(0, 256);
  if (protocol === "responses") {
    const response = stream ? object(event.response) : event;
    if (
      !stream ||
      ["response.completed", "response.failed", "response.incomplete"].includes(String(type))
    ) {
      const status = String(response.status);
      if (
        response.object !== "response" ||
        typeof response.id !== "string" ||
        !Array.isArray(response.output) ||
        !["completed", "failed", "incomplete"].includes(status) ||
        (stream && type !== `response.${status}`)
      ) {
        state.invalid = true;
        return;
      }
      if (typeof response.model === "string") call.responseModel = response.model.slice(0, 256);
      state.terminal = status;
      state.usage = response.usage === undefined ? undefined : boundedUsage(response.usage);
      state.usageFinal = response.usage !== undefined;
      call.finishReason = status;
    } else if (
      ["response.created", "response.queued", "response.in_progress"].includes(String(type))
    ) {
      if (response.object !== "response" || typeof response.id !== "string") state.invalid = true;
      else if (typeof response.model === "string")
        call.responseModel = response.model.slice(0, 256);
    } else if (type === "error") {
      state.terminal = "failed";
      state.usageFinal = false;
    }
  } else if (protocol === "anthropic_messages") {
    if (!stream || type === "message_start") {
      const message = stream ? object(event.message) : event;
      if (
        message.type !== "message" ||
        typeof message.id !== "string" ||
        !Array.isArray(message.content)
      ) {
        state.invalid = true;
        return;
      }
      if (typeof message.model === "string") call.responseModel = message.model.slice(0, 256);
      state.usage = boundedUsage(message.usage);
      if (!stream) {
        state.terminal = "completed";
        state.usageFinal = message.usage !== undefined;
        call.finishReason =
          typeof message.stop_reason === "string" ? message.stop_reason : undefined;
      }
    }
    if (type === "message_delta") {
      state.usage = { ...state.usage, ...boundedUsage(event.usage) };
      const reason = object(event.delta).stop_reason;
      state.usageFinal =
        typeof reason === "string" && count(object(event.usage).output_tokens) !== undefined;
      if (typeof reason === "string") call.finishReason = reason;
    }
    if (type === "message_stop") state.terminal = "completed";
    if (type === "error") {
      state.terminal = "failed";
      state.usageFinal = false;
    }
  } else if (protocol === "chat_completions") {
    const choices = list(event.choices);
    const finished = choices.every((x) => typeof object(x).finish_reason === "string");
    if (choices.length) {
      state.chatFinished = finished;
      const reason = object(choices[0]).finish_reason;
      if (typeof reason === "string") call.finishReason = reason;
    }
    if (event.usage !== undefined) {
      state.usage = boundedUsage(event.usage);
      state.usageFinal = Array.isArray(event.choices) && finished;
    }
    if (!stream) {
      state.terminal = event.error ? "failed" : finished ? "completed" : undefined;
      state.usageFinal = state.terminal === "completed" && event.usage !== undefined;
    }
    if (event.error) {
      state.terminal = "failed";
      state.usageFinal = false;
    }
  }
}
export function applyObservation(
  call: ModelCallV1,
  state: ProjectionCheckpoint,
  o: CaptureObservationV1,
): void {
  if (o.sequence !== call.capture.projectedThroughSequence + 1)
    throw new Error("projection sequence gap");
  if (o.gap) {
    state.rawGap = true;
    if (!call.capture.gaps.includes(o.gap) && call.capture.gaps.length < 8)
      call.capture.gaps.push(o.gap.slice(0, 128));
  }
  const body = decodeBody(o);
  const decoder = new TextDecoder("utf-8", { fatal: true });
  // A new after-auth event replaces the selection, including unknown fields.
  // Stream initialization may omit auth; an explicit selection replaces the pair.
  if (
    o.kind === "request_after_auth" ||
    (o.kind === "stream_init" &&
      (o.selectedAuthId !== undefined || o.selectedAuthIndex !== undefined))
  ) {
    call.selectedAuthId = o.selectedAuthId;
    call.selectedAuthIndex = o.selectedAuthIndex;
  }
  if (o.kind === "request_after_auth") {
    call.executionModel = o.executionModel;
    call.executionProtocol = o.executionProtocol;
  }
  if (o.kind === "request") {
    state.inputSeen = true;
    if (o.requestedModel) call.requestModel = o.requestedModel.slice(0, 256);
    call.correlation = o.correlation ?? {};
    call.correlationConflicts = o.correlationConflicts ?? [];
    call.traceId = o.correlation?.traceId;
    call.sourceTraceId = o.sourceTraceId;
    try {
      const input = parseObject(decoder.decode(body));
      state.streamed = input.stream === true;
      call.streamed = state.streamed;
    } catch {
      state.invalid = true;
    }
  }
  Object.assign(call, providerIdentity(call));
  if (o.kind === "response" || o.kind === "stream_chunk" || o.kind === "completion") {
    if (call.timeToFirstByteMs === undefined && (o.observedBodyBytes ?? body.length) > 0)
      call.timeToFirstByteMs = o.offsetNs / 1e6;
    try {
      if (o.kind === "response" && body.length) {
        const event = parseObject(decoder.decode(body));
        if (call.operation === "generation") observeProtocol(call, state, event, false);
        else {
          state.terminal = "completed";
          if (call.operation === "token_count") call.tokenCount = count(event.input_tokens);
        }
      }
      if ((o.kind === "stream_chunk" || o.kind === "completion") && body.length) {
        const reader = new SSEReader(state.sse);
        for (const frame of o.bodyFraming === "stock_hook_chunk"
          ? reader.feedStock(body, call.clientProtocol, o.observedAt)
          : reader.feed(body, o.observedAt)) {
          if (frame.data === "[DONE]") {
            if (call.clientProtocol === "chat_completions" && state.chatFinished)
              state.terminal = "completed";
            continue;
          }
          const event = parseObject(frame.data);
          if (call.timeToFirstContentMs === undefined && semantic(event))
            call.timeToFirstContentMs = o.offsetNs / 1e6;
          observeProtocol(call, state, event, true);
        }
        state.sse = reader.checkpoint();
      }
    } catch (error) {
      state.invalid = true;
      if (error instanceof Error && error.message === "SSE frame limit") state.frameLimit = true;
    }
  }
  if (o.kind === "completion") {
    try {
      if (new SSEReader(state.sse).finish().truncated) state.truncated = true;
    } catch {
      state.invalid = true;
    }
    delete state.sse;
    delete state.sseBlob;
    if (
      call.clientProtocol === "chat_completions" &&
      state.chatFinished &&
      o.completionOutcome === "succeeded"
    )
      state.terminal = "completed";
    call.completionOutcome = o.completionOutcome;
    call.executionStatusCode = o.executionStatusCode;
    call.endTimeUnixNano = nanoTime(o.executionCompletedAt);
    const start = nanoTime(o.executionStartedAt);
    if (start) call.startTimeUnixNano = start;
    if (start && call.endTimeUnixNano)
      call.latencyMs = Number(BigInt(call.endTimeUnixNano) - BigInt(start)) / 1e6;
    call.capture.terminalSequence = o.sequence;
    call.state =
      o.completionOutcome === "canceled"
        ? "aborted"
        : o.completionOutcome === "failed"
          ? "failed"
          : o.completionOutcome === "rejected"
            ? "rejected"
            : state.terminal === "completed"
              ? "succeeded"
              : state.terminal === "failed" || state.terminal === "incomplete"
                ? "failed"
                : "unknown";
  }
  call.protocolTerminal = state.terminal;
  const final =
    !!state.terminal &&
    state.usageFinal &&
    !state.invalid &&
    !state.truncated &&
    !state.unsupported &&
    !state.rawGap;
  const projection: CliproxyProjection = {
    version: 1,
    protocol: call.clientProtocol ?? "unknown",
    requestState: "absent",
    responseState: final ? "decoded" : "truncated",
    scalars: {},
    metadata: {},
  };
  if (state.usage && call.operation === "generation") {
    extractUsage(projection, state.usage);
    const measurements: UsageMeasurement[] = [];
    for (const field of usageFields) {
      const value = count(state.usage[field]);
      if (value !== undefined)
        measurements.push({
          value,
          nativeField: field,
          source: "client_protocol",
          scope: "call",
          unit: "tokens",
          finality: final ? "final" : "partial",
          semanticsVersion: String(projection.metadata.tokenSemantics),
        });
    }
    for (const [key, detail] of Object.entries(state.usage)) {
      if (!detail || typeof detail !== "object") continue;
      for (const [field, raw] of Object.entries(detail)) {
        const value = count(raw);
        if (value !== undefined)
          measurements.push({
            value,
            nativeField: `${key}.${field}`,
            source: "client_protocol",
            scope: "call",
            unit: "tokens",
            finality: final ? "final" : "partial",
            semanticsVersion: String(projection.metadata.tokenSemantics),
          });
      }
    }
    call.usage = measurements;
    for (const field of TOKEN_FIELDS) call[field] = final ? projection.scalars[field] : undefined;
  }
  call.capture.lastObservedAt = o.observedAt;
  call.capture.projectedThroughSequence = o.sequence;
  call.capture.persistedThroughSequence = o.sequence;
  call.capture.raw =
    call.capture.terminalSequence === o.sequence && !state.rawGap ? "complete" : "partial";
  call.capture.projectionIssue = state.frameLimit
    ? "frame_limit"
    : state.invalid
      ? "malformed_payload"
      : state.truncated
        ? "truncated_frame"
        : state.unsupported
          ? "unsupported_protocol"
          : undefined;
  call.capture.projection = state.invalid
    ? "invalid"
    : state.truncated
      ? "partial"
      : state.unsupported
        ? "unsupported"
        : state.terminal
          ? "complete"
          : "partial";
  call.capture.usage = call.usage.length ? (final ? "complete" : "partial") : "unavailable";
}
