import { expect, it } from "vitest";
import { projectCapturedPayloads } from "../src/protocols/index.js";
import { streamBytes } from "./fixtures/metadata-stream.js";
const project = (route: string, events: object[]) =>
  projectCapturedPayloads({ route, complete: true, chunks: [streamBytes(events)] });

it("retains Chat provider metadata, role, logprobs, reasoning and tool fragments", () => {
  const logprob = { token: "Built", logprob: -0.1, bytes: [66], top_logprobs: [] };
  const result = project("POST /v1/chat/completions", [
    {
      id: "chat-fixture",
      object: "chat.completion.chunk",
      created: 123,
      model: "fixture",
      service_tier: "default",
      system_fingerprint: "fp_fixture",
      choices: [
        {
          index: 0,
          delta: {
            role: "assistant",
            content: "Built",
            reasoning_content: "Check",
            tool_calls: [
              {
                index: 0,
                id: "tool-fixture",
                type: "function",
                function: { name: "build", arguments: '{"size":' },
              },
            ],
          },
          logprobs: { content: [logprob], refusal: null },
        },
      ],
    },
    {
      choices: [
        {
          index: 0,
          delta: {
            content: ".",
            reasoning_content: "ed.",
            tool_calls: [{ index: 0, function: { arguments: "2}" } }],
          },
          logprobs: { content: [{ ...logprob, token: "." }] },
          finish_reason: "tool_calls",
        },
      ],
      usage: { prompt_tokens: 10, completion_tokens: 2, total_tokens: 12 },
    },
    // An empty trailer must not erase already observed terminal usage.
    { choices: [] },
  ]);
  expect(result).toMatchObject({
    responseState: "decoded",
    scalars: { totalTokens: 12 },
    output: {
      completion: "Built.",
      reasoning: "Checked.",
      native: {
        created: 123,
        service_tier: "default",
        system_fingerprint: "fp_fixture",
        choices: [
          {
            message: { tool_calls: [{ function: { name: "build", arguments: '{"size":2}' } }] },
            logprobs: { content: [logprob, { ...logprob, token: "." }] },
          },
        ],
      },
    },
  });
});

const citation = {
  type: "char_location",
  cited_text: "Fixture",
  document_index: 0,
  document_title: "Test",
  start_char_index: 0,
  end_char_index: 7,
};
const messages = [
  {
    type: "message_start",
    message: {
      type: "message",
      id: "message-fixture",
      role: "assistant",
      content: [],
      usage: { input_tokens: 10, output_tokens: 1 },
    },
  },
  { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
  { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "Built." } },
  { type: "content_block_delta", index: 0, delta: { type: "citations_delta", citation } },
  { type: "content_block_stop", index: 0 },
  { type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 2 } },
  { type: "message_stop" },
];
it("retains Anthropic citations in normalized and native text without withholding usage", () => {
  expect(project("POST /v1/messages", messages)).toMatchObject({
    responseState: "decoded",
    scalars: { inputTokens: 10, outputTokens: 2 },
    output: {
      completion: "Built.",
      messages: [{ content: [{ type: "text", text: "Built.", citations: [citation] }] }],
      native: { content: [{ citations: [citation] }] },
    },
  });
});
it("reports unknown Messages events instead of silently treating them as decoded", () => {
  const result = project("POST /v1/messages", [
    ...messages.slice(0, -1),
    { type: "future_event", content: "not decoded" },
    messages.at(-1)!,
  ]);
  expect(result.responseState).toBe("unsupported");
  expect(result.scalars.outputTokens).toBeUndefined();
});
it("keeps malformed citation deltas invalid", () => {
  const result = project(
    "POST /v1/messages",
    messages.map((event) =>
      event.delta?.type === "citations_delta"
        ? { ...event, delta: { type: "citations_delta", citation: null } }
        : event,
    ),
  );
  expect(result.responseState).toBe("invalid");
  expect(result.scalars.outputTokens).toBeUndefined();
});

it.each([
  [
    "POST /v1/responses",
    {
      input: [{ type: "function_call_output", call_id: "fixture-tool", output: "Tool result" }],
      instructions: "Fixture instructions",
    },
    "nativeInput",
  ],
  [
    "POST /v1/messages",
    {
      messages: [
        {
          role: "user",
          content: [
            {
              type: "tool_result",
              tool_use_id: "fixture-tool",
              content: "Tool result",
              is_error: true,
            },
          ],
        },
      ],
    },
    "nativeMessages",
  ],
  [
    "POST /v1/chat/completions",
    { messages: [{ role: "tool", tool_call_id: "fixture-tool", content: "Tool result" }] },
    "messages",
  ],
] as const)("preserves original %s input and tool results", (route, request, nativeKey) => {
  const result = projectCapturedPayloads({ route, request: Buffer.from(JSON.stringify(request)) });
  expect(result.requestState).toBe("decoded");
  expect(result.input?.[nativeKey]).toEqual("input" in request ? request.input : request.messages);
  expect(JSON.stringify(result.input?.messages)).toContain("Tool result");
});
