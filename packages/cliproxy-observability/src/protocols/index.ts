import { assembleResponsesStream } from "./responses.js";
import { assembleChatStream } from "./chat-completions.js";
import { assembleAnthropicStream } from "./anthropic-messages.js";
import { SSEReader } from "./framing.js";
import { object, parseObject } from "./values.js";
import { requestInput, responseOutput } from "./output.js";
import type { CliproxyProjection, RecordValue } from "./types.js";
export { SSEReader } from "./framing.js";
export type { SSEFrame, SSECheckpoint } from "./framing.js";
export type { CliproxyProjection, ProjectionState } from "./types.js";
export { assembleResponsesStream, assembleChatStream, assembleAnthropicStream };
export { nanoTime } from "./values.js";
export const PARSER_VERSION = "client-protocol-v1";
export function protocolForRoute(route: string): CliproxyProjection["protocol"] {
  if (route === "POST /v1/messages" || route === "POST /v1/messages/count_tokens")
    return "anthropic_messages";
  if (route === "POST /v1/responses") return "responses";
  if (route === "POST /v1/chat/completions") return "chat_completions";
  return "unknown";
}
/** Bounded replay helper for fixtures and selected-call export; never fetches data itself. */
export function projectCapturedPayloads(args: {
  route: string;
  request?: Uint8Array;
  response?: Uint8Array;
  chunks?: readonly Uint8Array[];
  complete?: boolean;
}): CliproxyProjection {
  const projection: CliproxyProjection = {
    version: 1,
    protocol: protocolForRoute(args.route),
    requestState: "absent",
    responseState: "absent",
    scalars: {},
    metadata: { parserVersion: PARSER_VERSION, providerIdentity: "unavailable" },
  };
  const decoder = new TextDecoder("utf-8", { fatal: true });
  if (args.request) {
    try {
      const req = parseObject(decoder.decode(args.request));
      projection.input = requestInput(projection.protocol, req);
      projection.requestState = "decoded";
      if (typeof req.model === "string") projection.scalars.requestModel = req.model;
      if (typeof req.stream === "boolean") projection.scalars.streamed = req.stream;
    } catch {
      projection.requestState = "invalid";
    }
  }
  let response: RecordValue = {};
  if (args.response) {
    try {
      response = parseObject(decoder.decode(args.response));
      projection.responseState = "decoded";
      projection.metadata.terminalObserved = true;
      projection.metadata.usageState = response.usage ? "terminal_snapshot" : "absent";
    } catch {
      projection.responseState = "invalid";
    }
  }
  if (args.chunks) {
    const reader = new SSEReader();
    const events: RecordValue[] = [];
    let done = false;
    let invalid = false;
    let bytes = 0;
    for (const chunk of args.chunks) {
      bytes += chunk.byteLength;
      if (bytes > 16 * 1024 * 1024)
        throw new Error("bounded replay limit; use event pages for larger calls");
      try {
        for (const frame of reader.feed(chunk)) {
          if (frame.data === "[DONE]") {
            done = true;
            continue;
          }
          try {
            events.push(parseObject(frame.data));
          } catch {
            invalid = true;
          }
        }
      } catch {
        invalid = true;
      }
    }
    try {
      invalid ||= reader.finish().truncated;
    } catch {
      invalid = true;
    }
    const assembled =
      projection.protocol === "responses"
        ? assembleResponsesStream(events)
        : projection.protocol === "chat_completions"
          ? assembleChatStream(events, done, args.complete === true)
          : assembleAnthropicStream(events);
    response = assembled.response;
    invalid ||= assembled.invalid;
    projection.responseState = invalid
      ? "invalid"
      : !assembled.recognized || assembled.unsupported
        ? "unsupported"
        : assembled.terminal
          ? "decoded"
          : "truncated";
    projection.metadata.terminalObserved = assembled.terminal;
    const final =
      assembled.terminalUsage &&
      assembled.terminal &&
      !invalid &&
      assembled.recognized &&
      !assembled.unsupported;
    projection.metadata.usageState =
      final && response.usage
        ? "terminal_snapshot"
        : response.usage
          ? "partial_snapshot"
          : "absent";
    if (!final) {
      projection.metadata.partialUsage = response.usage;
      delete response.usage;
    }
  }
  if (projection.protocol !== "unknown" && (args.response || args.chunks))
    responseOutput(projection, response);
  if (projection.protocol === "responses" && typeof response.status === "string")
    projection.scalars.statusJson = JSON.stringify({ responseStatus: response.status });
  if (args.route === "POST /v1/messages/count_tokens") {
    projection.metadata.operation = "token_count";
    projection.metadata.tokenCount = object(response).input_tokens;
    projection.scalars.inputTokens = undefined;
    projection.scalars.outputTokens = undefined;
    projection.scalars.totalTokens = undefined;
  }
  if (args.route === "GET /v1/models") {
    projection.metadata.operation = "discovery";
    projection.output = { native: response };
  }
  return projection;
}
