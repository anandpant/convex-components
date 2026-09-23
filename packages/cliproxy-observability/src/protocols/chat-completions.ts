import type { RecordValue } from "./types.js";
import { append, count, list, object } from "./values.js";
export function assembleChatStream(events: RecordValue[], done: boolean) {
  const response: RecordValue = {};
  let invalid = false,
    recognized = false,
    unsupported = false,
    terminal = false,
    terminalUsage = false;
  const choices = new Map<number, RecordValue>();
  const tools = new Map<number, Map<number, RecordValue>>();
  for (const event of events) {
    if (Array.isArray(event.choices) || event.error) recognized = true;
    for (const key of ["id", "model", "object"])
      if (event[key] !== undefined) response[key] = event[key];
    if (event.usage !== undefined && event.usage !== null) response.usage = event.usage;
    // Final usage is emitted on an empty choices chunk or with finished choices.
    // A later [DONE] marker cannot turn an earlier running count into final usage.
    if (Array.isArray(event.choices))
      terminalUsage =
        event.usage !== undefined &&
        event.usage !== null &&
        event.choices.every((choice) => typeof object(choice).finish_reason === "string");
    if (event.error) {
      response.error = event.error;
      terminalUsage = false;
    }
    for (const raw of list(event.choices)) {
      const choice = object(raw);
      const index = count(choice.index);
      if (index === undefined) {
        invalid = true;
        continue;
      }
      const saved = choices.get(index) ?? { index, message: { role: "assistant" } };
      const message = object(saved.message);
      const delta = object(choice.delta);
      if (
        Object.keys(delta).some(
          (key) =>
            ![
              "role",
              "content",
              "reasoning_content",
              "reasoning",
              "refusal",
              "tool_calls",
              "reasoning_details",
            ].includes(key),
        )
      )
        unsupported = true;
      for (const key of ["content", "reasoning_content", "reasoning", "refusal"])
        append(message, key, delta[key]);
      if (delta.reasoning_details !== undefined)
        message.reasoning_details = [
          ...list(message.reasoning_details),
          ...list(delta.reasoning_details),
        ];
      const byIndex = tools.get(index) ?? new Map<number, RecordValue>();
      for (const rawTool of list(delta.tool_calls)) {
        const tool = object(rawTool);
        const toolIndex = count(tool.index);
        if (toolIndex === undefined) {
          invalid = true;
          continue;
        }
        const savedTool = byIndex.get(toolIndex) ?? { type: "function", function: {} };
        if (tool.id !== undefined) savedTool.id = tool.id;
        if (tool.type !== undefined) savedTool.type = tool.type;
        for (const key of ["name", "arguments"])
          append(object(savedTool.function), key, object(tool.function)[key]);
        byIndex.set(toolIndex, savedTool);
      }
      tools.set(index, byIndex);
      if (byIndex.size)
        message.tool_calls = [...byIndex.entries()]
          .toSorted(([a], [b]) => a - b)
          .map(([, value]) => value);
      if (choice.finish_reason !== undefined && choice.finish_reason !== null)
        saved.finish_reason = choice.finish_reason;
      choices.set(index, saved);
    }
  }
  response.choices = [...choices.entries()].toSorted(([a], [b]) => a - b).map(([, value]) => value);
  terminal = done || response.error !== undefined;

  return { response, invalid, recognized, unsupported, terminal, terminalUsage };
}
