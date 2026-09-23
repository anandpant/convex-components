import type { CliproxyProjection, RecordValue } from "./types.js";
import { identity, list, object } from "./values.js";
import { extractUsage } from "./usage.js";
import { responsesMessages } from "./responses.js";
import { anthropicMessages } from "./anthropic-messages.js";
export function requestInput(
  protocol: CliproxyProjection["protocol"],
  request: RecordValue,
): RecordValue {
  if (protocol === "anthropic_messages") {
    const messages = anthropicMessages(request.messages);
    if (request.system !== undefined) messages.unshift({ role: "system", content: request.system });
    return { ...request, messages, nativeMessages: request.messages };
  }
  if (protocol === "responses") {
    const { input, ...rest } = request;
    return {
      ...rest,
      messages: [
        ...(typeof request.instructions === "string"
          ? [{ role: "system", content: request.instructions }]
          : []),
        ...(typeof input === "string" ? [{ role: "user", content: input }] : list(input)),
      ],
      nativeInput: input,
    };
  }
  return request;
}
export function responseOutput(projection: CliproxyProjection, response: RecordValue) {
  projection.scalars.responseModel = identity(response.model);
  projection.scalars.generationId = identity(response.id);
  if (projection.responseState === "decoded" && response.usage !== undefined)
    extractUsage(projection, object(response.usage));
  let messages: RecordValue[];
  if (projection.protocol === "chat_completions") {
    const choices = list(response.choices);
    messages = choices.map((raw) => ({
      ...object(object(raw).message),
      choiceIndex: object(raw).index,
      finishReason: object(raw).finish_reason,
    }));
    projection.scalars.finishReason = identity(object(choices[0]).finish_reason);
  } else if (projection.protocol === "responses") {
    messages = responsesMessages(response.output);
    projection.scalars.finishReason = identity(response.status);
  } else {
    messages = anthropicMessages([{ role: "assistant", content: response.content }]);
    projection.scalars.finishReason = identity(response.stop_reason);
  }
  const first = messages[0] ?? {};
  const text: string[] = [];
  const reasoning: string[] = [];
  const selected = projection.protocol === "chat_completions" ? [first] : messages;
  for (const message of selected) {
    if (typeof message.content === "string") text.push(message.content);
    if (typeof message.reasoning_content === "string") reasoning.push(message.reasoning_content);
    if (typeof message.reasoning === "string") reasoning.push(message.reasoning);
    for (const raw of [...list(message.content), ...list(message.summary)]) {
      const block = object(raw);
      if (["text", "output_text"].includes(String(block.type)) && typeof block.text === "string")
        text.push(block.text);
      if (block.type === "thinking" && typeof block.thinking === "string")
        reasoning.push(block.thinking);
      if (block.type === "summary_text" && typeof block.text === "string")
        reasoning.push(block.text);
    }
  }
  projection.output = {
    format: "cliproxy_response_v1",
    protocol: projection.protocol,
    completion: text.join(""),
    reasoning: reasoning.join(""),
    messages,
    // Native blocks preserve signatures, tool results, refusal, unknown content and errors.
    native: response,
  };
}
