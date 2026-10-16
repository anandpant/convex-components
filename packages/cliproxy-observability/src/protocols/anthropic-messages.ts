import type { RecordValue } from "./types.js";
import { append, count, list, object } from "./values.js";
export function anthropicMessages(messages: unknown): RecordValue[] {
  const result: RecordValue[] = [];
  for (const value of list(messages)) {
    const message = object(value);
    if (!Array.isArray(message.content)) {
      result.push(message);
      continue;
    }
    let content: unknown[] = [];
    let tools: unknown[] = [];
    const flush = () => {
      if (content.length || tools.length)
        result.push({
          role: message.role,
          content,
          ...(tools.length ? { tool_calls: tools } : {}),
        });
      content = [];
      tools = [];
    };
    for (const raw of message.content) {
      const block = object(raw);
      if (block.type === "tool_use") {
        tools.push({
          id: block.id,
          type: "function",
          function: {
            name: block.name,
            arguments:
              typeof block.partial_json === "string"
                ? block.partial_json
                : JSON.stringify(block.input),
          },
        });
      } else if (block.type === "tool_result") {
        flush();
        result.push({
          role: "tool",
          tool_call_id: block.tool_use_id,
          content: block.content,
          is_error: block.is_error,
        });
      } else content.push(block);
    }
    flush();
  }
  return result;
}

/** Event-local validation shared by full replay and bounded scalar checkpoints. */
export function inspectAnthropicEvent(event: RecordValue) {
  const type = event.type;
  let invalid = false;
  let unsupported = ![
    "message_start",
    "message_delta",
    "message_stop",
    "content_block_start",
    "content_block_delta",
    "content_block_stop",
    "ping",
    "error",
  ].includes(String(type));
  if (String(type).startsWith("content_block_")) invalid = count(event.index) === undefined;
  if (type === "content_block_delta") {
    const delta = object(event.delta);
    unsupported ||= ![
      "text_delta",
      "thinking_delta",
      "signature_delta",
      "input_json_delta",
      "citations_delta",
    ].includes(String(delta.type));
    if (delta.type === "citations_delta")
      invalid ||=
        delta.citation === null ||
        typeof delta.citation !== "object" ||
        Array.isArray(delta.citation);
  }
  return { invalid, unsupported };
}

export function assembleAnthropicStream(events: RecordValue[]) {
  let response: RecordValue = {};
  let invalid = false,
    recognized = false,
    unsupported = false,
    terminal = false,
    terminalUsage = false;
  const blocks = new Map<number, RecordValue>();
  const argumentsByIndex = new Map<number, string>();
  for (const event of events) {
    const inspected = inspectAnthropicEvent(event);
    invalid ||= inspected.invalid;
    unsupported ||= inspected.unsupported;
    const index = count(event.index);
    if (event.type === "message_start") {
      response = { ...object(event.message) };
      recognized = true;
      terminalUsage = false;
    } else if (event.type === "content_block_start" && index !== undefined)
      blocks.set(index, { ...object(event.content_block) });
    else if (event.type === "content_block_delta" && index !== undefined) {
      const block = blocks.get(index);
      const delta = object(event.delta);
      if (!block) {
        invalid = true;
        continue;
      }
      if (delta.type === "citations_delta" && !inspected.invalid)
        block.citations = [...list(block.citations), delta.citation];
      for (const key of ["text", "thinking", "signature"]) append(block, key, delta[key]);
      if (typeof delta.partial_json === "string")
        argumentsByIndex.set(index, (argumentsByIndex.get(index) ?? "") + delta.partial_json);
    } else if (event.type === "message_delta") {
      Object.assign(response, object(event.delta));
      // Message start and final delta are snapshots of different fields, not increments.
      response.usage = { ...object(response.usage), ...object(event.usage) };
      terminalUsage =
        typeof object(event.delta).stop_reason === "string" &&
        count(object(event.usage).output_tokens) !== undefined;
    } else if (event.type === "message_stop") terminal = true;
    else if (event.type === "error") {
      response.error = event.error;
      recognized = true;
      terminal = true;
      terminalUsage = false;
    }
  }
  for (const [index, argumentsText] of argumentsByIndex) {
    const block = blocks.get(index);
    if (block) {
      if (
        argumentsText === "" &&
        block.type === "tool_use" &&
        block.input !== null &&
        typeof block.input === "object" &&
        !Array.isArray(block.input)
      )
        continue;
      try {
        block.input = JSON.parse(argumentsText);
      } catch {
        block.partial_json = argumentsText;
        invalid ||= terminal;
      }
    }
  }
  response.content = [...blocks.entries()].toSorted(([a], [b]) => a - b).map(([, value]) => value);

  return { response, invalid, recognized, unsupported, terminal, terminalUsage };
}
