import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { callIdentity, decodeBody, type CaptureObservationV1 } from "../src/capture/index.js";
import { initialCall } from "../src/client.js";
import { applyObservation, type ProjectionCheckpoint } from "../src/protocols/checkpoint.js";
import { projectCapturedPayloads, SSEReader } from "../src/protocols/index.js";
const encoder = new TextEncoder();
const recording = (name: string) =>
  (
    JSON.parse(readFileSync(new URL(`../fixtures/real/${name}.json`, import.meta.url), "utf8")) as {
      events: CaptureObservationV1[];
    }
  ).events;
// Recorded usage: Messages {input 309, output 7}; Responses adds cached, reasoning and
// total; Chat also reports cache_write_tokens 0. Messages omits both cache counts.
const messagesTokens = { outputTokens: 7 };
const responsesTokens = {
  inputTokens: 309,
  outputTokens: 7,
  totalTokens: 316,
  reasoningTokens: 0,
  cachedInputTokens: 0,
};
const chatTokens = { ...responsesTokens, cacheCreationInputTokens: 0 };
it.each([
  ["messages-sse", messagesTokens],
  ["responses-sse", responsesTokens],
  ["chat-json", chatTokens],
  ["messages-json", messagesTokens],
  ["responses-json", responsesTokens],
  ["chat-sse", chatTokens],
] as const)("replays real %s through summary and full content parsers", async (name, tokens) => {
  const events = recording(name);
  const first = events[0]!;
  const call = initialCall(first, await callIdentity(first), 1);
  const state: ProjectionCheckpoint = {};
  for (const event of events) applyObservation(call, state, event);
  expect(call.state).toBe("succeeded");
  // No recording has an after-auth frame, so gpt-5.6-luna is only a model-name guess.
  expect(call).toMatchObject({
    requestModel: "gpt-5.6-luna",
    providerName: "openai",
    providerProvenance: "derived_from_model",
    cost: { kind: "unknown" },
    costProvenance: "unknown",
  });
  expect({
    inputTokens: call.inputTokens,
    outputTokens: call.outputTokens,
    totalTokens: call.totalTokens,
    reasoningTokens: call.reasoningTokens,
    cachedInputTokens: call.cachedInputTokens,
    cacheCreationInputTokens: call.cacheCreationInputTokens,
  }).toEqual({
    inputTokens: undefined,
    totalTokens: undefined,
    reasoningTokens: undefined,
    cachedInputTokens: undefined,
    cacheCreationInputTokens: undefined,
    ...tokens,
  });
  expect(call.capture.raw).toBe("complete");
  expect(call.capture.projectedThroughSequence).toBe(events.length);
  expect(call.usage.length).toBeGreaterThan(0);
  const projection = projectCapturedPayloads({
    route: first.route,
    complete: true,
    request: decodeBody(first),
    response: events.find((e) => e.kind === "response")
      ? decodeBody(events.find((e) => e.kind === "response")!)
      : undefined,
    chunks: name.endsWith("sse")
      ? events
          .filter(
            (e) => (e.kind === "stream_chunk" || e.kind === "completion") && e.contentBytes > 0,
          )
          .map(decodeBody)
      : undefined,
  });
  expect(projection.responseState).toBe("decoded");
  expect(projection.output?.completion).toBe("CAPTURE_OK");
});
it.each([
  [
    "POST /v1/messages",
    {
      type: "message",
      id: "message",
      content: [],
      stop_reason: "end_turn",
      usage: {
        input_tokens: 10,
        cache_read_input_tokens: 20,
        cache_creation_input_tokens: 30,
        cache_creation: { ephemeral_5m_input_tokens: 25, ephemeral_1h_input_tokens: 5 },
        output_tokens: 5,
      },
    },
    { inputTokens: 60, outputTokens: 5, cachedInputTokens: 20, cacheCreationInputTokens: 30 },
    ["input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"],
  ],
  [
    "POST /v1/responses",
    {
      object: "response",
      id: "response",
      status: "completed",
      output: [],
      usage: {
        input_tokens: 60,
        input_tokens_details: { cached_tokens: 20, cache_write_tokens: 30 },
        output_tokens: 5,
        output_tokens_details: { reasoning_tokens: 2 },
        total_tokens: 65,
      },
    },
    {
      inputTokens: 60,
      outputTokens: 5,
      totalTokens: 65,
      reasoningTokens: 2,
      cachedInputTokens: 20,
      cacheCreationInputTokens: 30,
    },
    ["input_tokens", "input_tokens_details.cache_write_tokens"],
  ],
  [
    "POST /v1/chat/completions",
    {
      choices: [{ index: 0, message: { role: "assistant", content: "" }, finish_reason: "stop" }],
      usage: {
        prompt_tokens: 60,
        prompt_tokens_details: { cached_tokens: 20, cached_creation_tokens: 30 },
        completion_tokens: 5,
        completion_tokens_details: { reasoning_tokens: 2 },
        total_tokens: 65,
      },
    },
    {
      inputTokens: 60,
      outputTokens: 5,
      totalTokens: 65,
      reasoningTokens: 2,
      cachedInputTokens: 20,
      cacheCreationInputTokens: 30,
    },
    ["prompt_tokens", "prompt_tokens_details.cached_creation_tokens"],
  ],
] as const)(
  "normalizes %s usage into OpenRouter token fields beside the native counts",
  async (route, response, tokens, nativeFields) => {
    const base = recording("messages-json")[0]!;
    const observed = (
      sequence: number,
      kind: CaptureObservationV1["kind"],
      body?: unknown,
    ): CaptureObservationV1 => {
      const bytes = body === undefined ? new Uint8Array() : encoder.encode(JSON.stringify(body));
      return {
        ...base,
        route,
        sequence,
        kind,
        body: btoa(String.fromCharCode(...bytes)),
        contentBytes: bytes.length,
        ...(kind === "completion" ? { completionOutcome: "succeeded" } : {}),
      };
    };
    const call = initialCall(observed(1, "request"), "call", 1);
    const state: ProjectionCheckpoint = {};
    for (const o of [
      observed(1, "request", { model: base.requestedModel }),
      observed(2, "response", response),
      observed(3, "completion"),
    ])
      applyObservation(call, state, o);
    expect(call.state).toBe("succeeded");
    expect({
      inputTokens: call.inputTokens,
      outputTokens: call.outputTokens,
      totalTokens: call.totalTokens,
      reasoningTokens: call.reasoningTokens,
      cachedInputTokens: call.cachedInputTokens,
      cacheCreationInputTokens: call.cacheCreationInputTokens,
    }).toEqual({ totalTokens: undefined, reasoningTokens: undefined, ...tokens });
    for (const nativeField of nativeFields)
      expect(call.usage).toContainEqual(
        expect.objectContaining({ nativeField, finality: "final" }),
      );
  },
);
it("preserves partial usage without promoting aborted Messages counters", async () => {
  const events = recording("messages-sse").filter((e) => e.kind !== "completion");
  const terminal = events.findIndex(
    (e) =>
      decodeBody(e).length &&
      new TextDecoder().decode(decodeBody(e)).includes('"type":"message_stop"'),
  );
  const call = initialCall(events[0]!, await callIdentity(events[0]!), 1);
  const state: ProjectionCheckpoint = {};
  for (const event of events.slice(0, terminal)) applyObservation(call, state, event);
  expect(call.inputTokens).toBeUndefined();
  expect(call.outputTokens).toBeUndefined();
  expect(call.usage.every((u) => u.finality === "partial")).toBe(true);
});
it("frames UTF-8 and multiline SSE across every byte split", () => {
  const bytes = new TextEncoder().encode('event: test\r\ndata: {"text":\r\ndata: "é🦉"}\r\n\r\n');
  for (let split = 0; split <= bytes.length; split++) {
    const reader = new SSEReader();
    const frames = [...reader.feed(bytes.slice(0, split)), ...reader.feed(bytes.slice(split))];
    expect(frames).toHaveLength(1);
    expect(JSON.parse(frames[0]!.data)).toEqual({ text: "é🦉" });
    expect(reader.finish().truncated).toBe(false);
  }
});
it("checkpoints an SSE frame split across observations without invalidating usage", async () => {
  const events = recording("messages-sse");
  const index = events.findIndex((o) => o.kind === "stream_chunk" && o.contentBytes > 20);
  const original = events[index]!;
  const bytes = decodeBody(original);
  const parts = [bytes.slice(0, 13), bytes.slice(13)];
  const chunks = parts.map((body) => ({
    ...original,
    body: btoa(String.fromCharCode(...body)),
    contentBytes: body.length,
  }));
  const split = [...events.slice(0, index), ...chunks, ...events.slice(index + 1)].map((o, i) => ({
    ...o,
    sequence: i + 1,
  }));
  const call = initialCall(split[0]!, await callIdentity(split[0]!), 1);
  let state: ProjectionCheckpoint = {};
  for (const event of split) {
    applyObservation(call, state, event);
    state = JSON.parse(JSON.stringify(state)) as ProjectionCheckpoint;
  }
  expect(call.capture.projection).toBe("complete");
  expect(call.capture.usage).toBe("complete");
  expect(call.outputTokens).toBeGreaterThan(0);
});

it("retains a real canceled stream as an aborted call with partial usage", async () => {
  const events = recording("messages-abort");
  const call = initialCall(events[0]!, await callIdentity(events[0]!), 1);
  const state: ProjectionCheckpoint = {};
  for (const event of events) applyObservation(call, state, event);
  expect(call.state).toBe("aborted");
  expect(call.completionOutcome).toBe("canceled");
  expect(call.inputTokens).toBeUndefined();
  expect(call.outputTokens).toBeUndefined();
  expect(call.usage.every((u) => u.finality === "partial")).toBe(true);
  expect(call.capture.usage).toBe("partial");
});
