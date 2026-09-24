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
      } else if (block.type === "text") content.push({ type: "text", text: block.text });
      else content.push(block);
    }
    flush();
  }
  return result;
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
      if (
        !["text_delta", "thinking_delta", "signature_delta", "input_json_delta"].includes(
          String(delta.type),
        )
      )
        unsupported = true;
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
