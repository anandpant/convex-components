import { traceBlobRefFromMarker, type TraceBlobRef } from "./blobContent.js";

export type ContentDecodeOutcome<T> =
  | { kind: "absent" }
  | { kind: "invalid"; raw: string; error: string; value?: unknown }
  | { kind: "unsupported"; raw: string; value: unknown }
  | { kind: "decoded"; raw: string; value: unknown; content: T };

export type ToolDefinition = {
  type: "function";
  function: {
    name: string;
    description?: DecodedText;
    parameters?: unknown;
  };
};

export type EmittedToolCall = {
  id: string;
  type: "function";
  function: {
    name: string;
    arguments: DecodedText;
  };
};

export type DecodedText = string | { kind: "blob_text"; ref: TraceBlobRef };

export type TextContentPart = { type: "text"; text: DecodedText; raw: unknown };

export type ImageContentPart = {
  type: "image_url";
  image:
    | { kind: "blob"; ref: TraceBlobRef; detail?: string }
    | { kind: "external_url"; url: string; detail?: string }
    | { kind: "unavailable"; value: unknown; detail?: string };
  raw: unknown;
};

export type OpaqueContentPart = { type: "opaque"; value: unknown; raw: unknown };

export type DecodedContentPart = TextContentPart | ImageContentPart | OpaqueContentPart;

export type DecodedMessageContent =
  DecodedText | null | { kind: "parts"; parts: DecodedContentPart[] };

type MessageBase = {
  content: DecodedMessageContent;
  raw: unknown;
};

export type DecodedMessage =
  | (MessageBase & { role: "system" | "user" })
  | (MessageBase & {
      role: "assistant";
      emittedToolCalls: EmittedToolCall[];
      reasoningDetails?: unknown;
    })
  | (MessageBase & { role: "tool"; toolCallId: string });

export type DecodedInputContent = {
  kind: "messages";
  messages: DecodedMessage[];
};

export type DecodedOutputContent = {
  kind: "response";
  text: DecodedText;
  reasoning?: DecodedText | null;
  requestToolDefinitions: ToolDefinition[];
  rawRequest?: unknown;
};

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function decodedText(value: unknown): DecodedText | undefined {
  if (typeof value === "string") return value;
  const ref = traceBlobRefFromMarker(value);
  return ref ? { kind: "blob_text", ref } : undefined;
}

function parseJson(raw: string | undefined): ContentDecodeOutcome<never> | { value: unknown } {
  if (raw === undefined) return { kind: "absent" };
  try {
    return { value: JSON.parse(raw) as unknown };
  } catch {
    return { kind: "invalid", raw, error: "content is not valid JSON" };
  }
}

function emittedToolCall(value: unknown): EmittedToolCall | undefined {
  if (!isRecord(value) || typeof value.id !== "string" || value.type !== "function") {
    return undefined;
  }
  const fn = value.function;
  const argumentsValue = isRecord(fn) ? decodedText(fn.arguments) : undefined;
  if (!isRecord(fn) || typeof fn.name !== "string" || argumentsValue === undefined) {
    return undefined;
  }
  return { id: value.id, type: "function", function: { name: fn.name, arguments: argumentsValue } };
}

function toolDefinition(value: unknown): ToolDefinition | undefined {
  if (!isRecord(value) || value.type !== "function" || !isRecord(value.function)) {
    return undefined;
  }
  const fn = value.function;
  if (typeof fn.name !== "string") return undefined;
  const description = fn.description === undefined ? undefined : decodedText(fn.description);
  if (fn.description !== undefined && description === undefined) return undefined;
  return {
    type: "function",
    function: {
      name: fn.name,
      ...(description === undefined ? {} : { description }),
      ...(fn.parameters === undefined ? {} : { parameters: fn.parameters }),
    },
  };
}

type MessageDecode =
  { kind: "decoded"; message: DecodedMessage } | { kind: "invalid" } | { kind: "unsupported" };

function decodeMessageContent(value: unknown) {
  const text = decodedText(value);
  if (text !== undefined || value === null) {
    return { kind: "decoded" as const, content: text ?? null };
  }
  if (!Array.isArray(value)) return { kind: "invalid" as const };
  const parts = value.map((part): DecodedContentPart | undefined => {
    if (!isRecord(part)) return { type: "opaque", value: part, raw: part };
    if (part.type === "text") {
      const partText = decodedText(part.text);
      return partText === undefined ? undefined : { type: "text", text: partText, raw: part };
    }
    if (part.type === "image_url" && isRecord(part.image_url)) {
      const detail = typeof part.image_url.detail === "string" ? part.image_url.detail : undefined;
      const ref = traceBlobRefFromMarker(part.image_url.url);
      if (ref)
        return {
          type: "image_url",
          image: { kind: "blob", ref, ...(detail ? { detail } : {}) },
          raw: part,
        };
      if (typeof part.image_url.url === "string") {
        return {
          type: "image_url",
          image: /^https?:\/\//.test(part.image_url.url)
            ? { kind: "external_url", url: part.image_url.url, ...(detail ? { detail } : {}) }
            : { kind: "unavailable", value: part.image_url.url, ...(detail ? { detail } : {}) },
          raw: part,
        };
      }
      return undefined;
    }
    return { type: "opaque", value: part, raw: part };
  });
  if (parts.some((part) => part === undefined)) return { kind: "invalid" as const };
  return {
    kind: "decoded" as const,
    content: { kind: "parts" as const, parts: parts as DecodedContentPart[] },
  };
}

function decodeMessage(value: unknown): MessageDecode {
  if (!isRecord(value) || typeof value.role !== "string") return { kind: "invalid" };
  const decodedContent = decodeMessageContent(value.content);
  if (decodedContent.kind !== "decoded") return decodedContent;
  const base = { content: decodedContent.content, raw: value };
  if (value.role === "system" || value.role === "user") {
    return { kind: "decoded", message: { ...base, role: value.role } };
  }
  if (value.role === "assistant") {
    if (value.tool_calls !== undefined && !Array.isArray(value.tool_calls)) {
      return { kind: "invalid" };
    }
    const calls = (value.tool_calls ?? []).map(emittedToolCall);
    if (calls.some((call) => call === undefined)) return { kind: "invalid" };
    return {
      kind: "decoded",
      message: {
        ...base,
        role: "assistant",
        emittedToolCalls: calls as EmittedToolCall[],
        ...(value.reasoning_details === undefined
          ? {}
          : { reasoningDetails: value.reasoning_details }),
      },
    };
  }
  if (value.role === "tool" && typeof value.tool_call_id === "string") {
    return {
      kind: "decoded",
      message: { ...base, role: "tool", toolCallId: value.tool_call_id },
    };
  }
  return { kind: "unsupported" };
}

export function decodeOpenRouterInput(
  raw: string | undefined,
): ContentDecodeOutcome<DecodedInputContent> {
  const parsed = parseJson(raw);
  if ("kind" in parsed) return parsed;
  if (!isRecord(parsed.value) || !Array.isArray(parsed.value.messages)) {
    return { kind: "unsupported", raw: raw as string, value: parsed.value };
  }
  const messages = parsed.value.messages.map(decodeMessage);
  if (messages.some((message) => message.kind === "unsupported")) {
    return { kind: "unsupported", raw: raw as string, value: parsed.value };
  }
  if (messages.some((message) => message.kind === "invalid")) {
    return {
      kind: "invalid",
      raw: raw as string,
      value: parsed.value,
      error: "messages contains an invalid or unsupported message",
    };
  }
  return {
    kind: "decoded",
    raw: raw as string,
    value: parsed.value,
    content: {
      kind: "messages",
      messages: messages.map((message) => (message as { message: DecodedMessage }).message),
    },
  };
}

export function decodeOpenRouterOutput(
  raw: string | undefined,
): ContentDecodeOutcome<DecodedOutputContent> {
  const parsed = parseJson(raw);
  if ("kind" in parsed) return parsed;
  if (!isRecord(parsed.value) || !("completion" in parsed.value)) {
    return { kind: "unsupported", raw: raw as string, value: parsed.value };
  }
  if (
    decodedText(parsed.value.completion) === undefined ||
    (parsed.value.reasoning !== undefined &&
      parsed.value.reasoning !== null &&
      decodedText(parsed.value.reasoning) === undefined) ||
    (parsed.value.tools !== undefined && !Array.isArray(parsed.value.tools))
  ) {
    return {
      kind: "invalid",
      raw: raw as string,
      value: parsed.value,
      error: "response fields have invalid types",
    };
  }
  const definitions = (parsed.value.tools ?? []).map(toolDefinition);
  if (definitions.some((definition) => definition === undefined)) {
    return {
      kind: "invalid",
      raw: raw as string,
      value: parsed.value,
      error: "tools contains an invalid definition",
    };
  }
  const completion = decodedText(parsed.value.completion);
  if (completion === undefined) throw new Error("validated completion must decode");
  const reasoning =
    parsed.value.reasoning === undefined || parsed.value.reasoning === null
      ? parsed.value.reasoning
      : decodedText(parsed.value.reasoning);
  return {
    kind: "decoded",
    raw: raw as string,
    value: parsed.value,
    content: {
      kind: "response",
      text: completion,
      ...(reasoning === undefined ? {} : { reasoning }),
      requestToolDefinitions: definitions as ToolDefinition[],
      ...(parsed.value.rawRequest === undefined ? {} : { rawRequest: parsed.value.rawRequest }),
    },
  };
}
