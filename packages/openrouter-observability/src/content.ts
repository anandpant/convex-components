export type ContentDecodeOutcome<T> =
  | { kind: "absent" }
  | { kind: "invalid"; raw: string; error: string; value?: unknown }
  | { kind: "unsupported"; raw: string; value: unknown }
  | { kind: "decoded"; raw: string; value: unknown; content: T };

export type ToolDefinition = {
  type: "function";
  function: {
    name: string;
    description?: string;
    parameters?: unknown;
  };
};

export type EmittedToolCall = {
  id: string;
  type: "function";
  function: {
    name: string;
    arguments: string;
  };
};

export type TextContentPart = { type: "text"; text: string; raw: unknown };

export type DecodedMessageContent =
  string | null | { kind: "text_parts"; parts: TextContentPart[] };

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
  text: string;
  reasoning?: string | null;
  requestToolDefinitions: ToolDefinition[];
  rawRequest?: unknown;
};

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
  if (!isRecord(fn) || typeof fn.name !== "string" || typeof fn.arguments !== "string") {
    return undefined;
  }
  return { id: value.id, type: "function", function: { name: fn.name, arguments: fn.arguments } };
}

function toolDefinition(value: unknown): ToolDefinition | undefined {
  if (!isRecord(value) || value.type !== "function" || !isRecord(value.function)) {
    return undefined;
  }
  const fn = value.function;
  if (typeof fn.name !== "string") return undefined;
  if (fn.description !== undefined && typeof fn.description !== "string") return undefined;
  return {
    type: "function",
    function: {
      name: fn.name,
      ...(fn.description === undefined ? {} : { description: fn.description }),
      ...(fn.parameters === undefined ? {} : { parameters: fn.parameters }),
    },
  };
}

type MessageDecode =
  { kind: "decoded"; message: DecodedMessage } | { kind: "invalid" } | { kind: "unsupported" };

function decodeMessageContent(value: unknown) {
  if (typeof value === "string" || value === null) {
    return { kind: "decoded" as const, content: value };
  }
  if (!Array.isArray(value)) return { kind: "invalid" as const };
  const parts = value.map((part): TextContentPart | undefined =>
    isRecord(part) && part.type === "text" && typeof part.text === "string"
      ? { type: "text", text: part.text, raw: part }
      : undefined,
  );
  if (parts.some((part) => part === undefined)) return { kind: "unsupported" as const };
  return {
    kind: "decoded" as const,
    content: { kind: "text_parts" as const, parts: parts as TextContentPart[] },
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
    typeof parsed.value.completion !== "string" ||
    (parsed.value.reasoning !== undefined &&
      parsed.value.reasoning !== null &&
      typeof parsed.value.reasoning !== "string") ||
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
  return {
    kind: "decoded",
    raw: raw as string,
    value: parsed.value,
    content: {
      kind: "response",
      text: parsed.value.completion,
      ...(parsed.value.reasoning === undefined ? {} : { reasoning: parsed.value.reasoning }),
      requestToolDefinitions: definitions as ToolDefinition[],
      ...(parsed.value.rawRequest === undefined ? {} : { rawRequest: parsed.value.rawRequest }),
    },
  };
}
