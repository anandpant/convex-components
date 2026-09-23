import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { projectCapturedPayloads } from "../src/protocols/index.js";
import { decodeCliproxyOutput } from "../src/protocols/content.js";
const fixture = (name: string) =>
  readFileSync(new URL(`./fixtures/responses-${name}.txt`, import.meta.url), "utf8");
// Test-only extraction of the inherited synthetic log fixtures; production accepts native observations.
const parse = (text: string) => {
  const encoder = new TextEncoder();
  const request = text.split("=== REQUEST BODY ===\n")[1]!.split("=== RESPONSE ===")[0]!.trim();
  const response = text.split("=== RESPONSE ===")[1]!;
  const body = response.slice(response.indexOf("\n\n") + 2);
  return projectCapturedPayloads({
    route: "POST /v1/responses",
    request: encoder.encode(request),
    ...(response.includes("text/event-stream")
      ? { chunks: [encoder.encode(body)] }
      : { response: encoder.encode(body) }),
  });
};
it.each(["json", "sse"])("decodes Responses %s text, tool calls and terminal usage", (kind) => {
  const result = parse(fixture(kind));
  expect(result).toMatchObject({
    protocol: "responses",
    requestState: "decoded",
    responseState: "decoded",
    scalars: {
      requestModel: "gpt-5.6",
      responseModel: "gpt-5.6-2026-09-01",
      finishReason: "completed",
      inputTokens: 80,
      outputTokens: 13,
      totalTokens: 93,
      cachedInputTokens: 20,
      reasoningTokens: 3,
    },
    output: {
      completion: "Built.",
      messages: [
        { role: "assistant" },
        {
          tool_calls: [
            {
              id: "call_17",
              type: "function",
              function: { name: "build", arguments: '{"size":2}' },
            },
          ],
        },
      ],
    },
  });
  expect(decodeCliproxyOutput(JSON.stringify(result.output)).state).toBe("decoded");
  expect(JSON.parse(result.scalars.statusJson ?? "{}")).toMatchObject({
    responseStatus: "completed",
  });
});
it("retains partial text and tool arguments after an aborted stream without inventing final usage", () => {
  const result = parse(fixture("aborted"));
  expect(result).toMatchObject({
    responseState: "truncated",
    metadata: { terminalObserved: false },
    output: {
      completion: "Built.",
      messages: [{ role: "assistant" }, { tool_calls: [{ function: { arguments: '{"size":' } }] }],
    },
  });
  expect(result.scalars.totalTokens).toBeUndefined();
  expect(result.scalars.finishReason).toBe("in_progress");
});
it.each(["failed", "incomplete"])("preserves terminal %s status and usage", (status) => {
  const result = parse(
    fixture("sse")
      .replaceAll('"completed"', JSON.stringify(status))
      .replace("event: response.completed", `event: response.${status}`)
      .replace('"type": "response.completed"', `"type": "response.${status}"`),
  );
  expect(result.responseState).toBe("decoded");
  expect(result.scalars.finishReason).toBe(status);
  expect(result.scalars.totalTokens).toBe(93);
});
it("does not finalize on a transport DONE marker or a completed output item", () => {
  const result = parse(fixture("aborted") + "data: [DONE]\n\n");
  expect(result.responseState).toBe("truncated");
  expect(result.scalars.totalTokens).toBeUndefined();
});
it("keeps malformed streams invalid even if a terminal snapshot follows", () => {
  const result = parse(
    fixture("sse").replace(
      "event: response.completed",
      "data: {broken\n\nevent: response.completed",
    ),
  );
  expect(result.responseState).toBe("invalid");
  expect(result.scalars.totalTokens).toBeUndefined();
});

it("retains emitted output when a stream ends with an error event", () => {
  const result = parse(
    fixture("aborted") +
      'event: error\ndata: {"type":"error","code":"server_error","message":"interrupted"}\n\n',
  );
  expect(result).toMatchObject({
    responseState: "decoded",
    scalars: { finishReason: "failed" },
    output: { completion: "Built." },
  });
  expect(result.scalars.totalTokens).toBeUndefined();
});
it("accepts citation annotations without withholding terminal usage", () => {
  const frame =
    'event: response.output_text.annotation.added\ndata: {"type":"response.output_text.annotation.added","output_index":0,"content_index":0,"item_id":"msg_1","annotation_index":0,"annotation":{"type":"url_citation","url":"https://example.test/spec","title":"Spec","start_index":0,"end_index":5}}\n\n';
  const result = parse(
    fixture("sse").replace(
      "event: response.output_text.done",
      frame + "event: response.output_text.done",
    ),
  );
  expect(result.responseState).toBe("decoded");
  expect(result.scalars.totalTokens).toBe(93);
});

it.each([
  { status: "failed", output: [] },
  { status: "completed", output: null },
])("rejects malformed terminal snapshots without replacing emitted evidence: %j", (fields) => {
  const snapshot = {
    id: "resp_corrupt",
    object: "response",
    model: "corrupted-model",
    ...fields,
    usage: { input_tokens: 999, output_tokens: 999, total_tokens: 1998 },
  };
  const result = parse(
    fixture("aborted") +
      `event: response.completed\ndata: ${JSON.stringify({ type: "response.completed", response: snapshot })}\n\n`,
  );
  expect(result).toMatchObject({
    responseState: "invalid",
    scalars: { responseModel: "gpt-5.6-2026-09-01", finishReason: "in_progress" },
    metadata: { terminalObserved: false },
    output: { completion: "Built." },
  });
  expect(result.scalars.totalTokens).toBeUndefined();
  expect(result.metadata.usageState).toBe("absent");
  expect(result.metadata.partialUsage).toBeNull();
});

it.each(["failed", "incomplete"])(
  "retains emitted items omitted by a %s terminal snapshot",
  (status) => {
    const snapshot = {
      id: "resp_123",
      object: "response",
      model: "gpt-5.6-2026-09-01",
      status,
      output: [
        {
          type: "message",
          id: "msg_1",
          role: "assistant",
          content: [{ type: "output_text", text: "Built." }],
        },
      ],
    };
    const result = parse(
      fixture("aborted") +
        `event: response.${status}\ndata: ${JSON.stringify({ type: `response.${status}`, response: snapshot })}\n\n`,
    );
    expect(result).toMatchObject({
      responseState: "decoded",
      scalars: { finishReason: status },
      output: {
        completion: "Built.",
        messages: [{ id: "msg_1" }, { tool_calls: [{ function: { arguments: '{"size":' } }] }],
      },
    });
    expect(result.scalars.totalTokens).toBeUndefined();
  },
);

it.each([
  ["web_search_call", ["in_progress", "searching", "completed"]],
  ["file_search_call", ["in_progress", "searching", "completed"]],
  ["code_interpreter_call", ["in_progress", "interpreting", "completed"]],
  ["image_generation_call", ["in_progress", "generating", "completed"]],
  ["mcp_call", ["in_progress", "completed", "failed"]],
  ["mcp_list_tools", ["in_progress", "completed", "failed"]],
] as const)("retains terminal usage after %s lifecycle events", (tool, statuses) => {
  const frames = statuses
    .map((status) => {
      const type = `response.${tool}.${status}`;
      return `event: ${type}\ndata: ${JSON.stringify({ type, item_id: "tool_1", output_index: 2, sequence_number: 12 })}\n\n`;
    })
    .join("");
  const result = parse(
    fixture("sse").replace("event: response.completed", frames + "event: response.completed"),
  );
  expect(result.responseState).toBe("decoded");
  expect(result.scalars.totalTokens).toBe(93);
  expect(parse(fixture("aborted") + frames).responseState).toBe("truncated");
});

it("keeps native Responses input and normalized messages within the projection bound", () => {
  const input = "x".repeat(2 * 1024 * 1024 + 1024);
  const text = fixture("json").replace(
    /=== REQUEST BODY ===\n[^\n]+/,
    `=== REQUEST BODY ===\n${JSON.stringify({ model: "gpt-5.6", input, instructions: "Build it" })}`,
  );
  const result = parse(text);
  expect(result.input).toMatchObject({
    nativeInput: input,
    messages: [
      { role: "system", content: "Build it" },
      { role: "user", content: input },
    ],
  });
  expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(6 * 1024 * 1024);
});

it("keeps unknown lifecycle events unsupported even with a terminal snapshot", () => {
  const frame =
    'event: response.future_tool.started\ndata: {"type":"response.future_tool.started"}\n\n';
  const result = parse(
    fixture("sse").replace("event: response.completed", frame + "event: response.completed"),
  );
  expect(result.responseState).toBe("unsupported");
  expect(result.scalars.totalTokens).toBeUndefined();
});
