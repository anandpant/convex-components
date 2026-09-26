import { expect, it } from "vitest";
import { SSEReader, projectCapturedPayloads } from "../src/protocols/index.js";
import { initialCall } from "../src/client.js";
import { applyObservation, type ProjectionCheckpoint } from "../src/protocols/checkpoint.js";
import {
  decodeBody,
  sha256,
  validateObservation,
  type CaptureObservationV1,
} from "../src/capture/index.js";

const encoder = new TextEncoder();
const terminal = `{"type":"response.completed","response":{"object":"response","id":"synthetic","status":"completed","model":"reported-model","output":[],"usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}}`;
async function observation(
  body: string,
  sequence: number,
  kind: CaptureObservationV1["kind"] = "stream_chunk",
): Promise<CaptureObservationV1> {
  const bytes = encoder.encode(body);
  return {
    schemaVersion: 1,
    pluginVersion: "0.2.0",
    capturePolicy: "hook-body-v1",
    destinationId: "dev",
    instanceId: "test",
    pluginBootId: "boot",
    requestId: "req",
    sequence,
    kind,
    observedAt: "2026-09-24T00:00:00Z",
    offsetNs: sequence * 1000,
    route: "POST /v1/responses",
    configRevision: "r1",
    body: btoa(String.fromCharCode(...bytes)),
    contentBytes: bytes.length,
    contentSha256: await sha256(bytes),
    observedBodyBytes: bytes.length,
    ...(kind === "stream_chunk"
      ? { bodyFraming: "stock_hook_chunk", stockChunkIndex: sequence - 2 }
      : {}),
    ...(kind === "completion" ? { completionOutcome: "succeeded" } : {}),
  };
}

it("retains exact JSON, URLs, nested credentials-as-content and inline images", async () => {
  const text =
    " " +
    JSON.stringify(
      {
        token: "word",
        secret: "content",
        authorization: "quoted-example",
        url: "https://example.test/?token=keep",
        nested: JSON.stringify({ password: "example" }),
        image: { type: "image_url", url: "data:image/png;base64,aGVsbG8=" },
      },
      null,
      2,
    ) +
    " ";
  const o = await observation(text, 1, "request");
  const validated = await validateObservation(encoder.encode(JSON.stringify(o)), {
    destinationId: "dev",
    instanceIds: ["test"],
  });
  expect(validated.body).toEqual(encoder.encode(text));
  expect(new TextDecoder().decode(decodeBody(validated.observation))).toBe(text);
  expect(validated.observation.capturePolicy).toBe("hook-body-v1");
  await expect(
    validateObservation(
      encoder.encode(JSON.stringify({ ...o, headers: { Authorization: "must-not-store" } })),
      { destinationId: "dev", instanceIds: ["test"] },
    ),
  ).rejects.toThrow("unsupported observation field");
  await expect(
    validateObservation(encoder.encode(JSON.stringify({ ...o, bodyFromSequence: 1 })), {
      destinationId: "dev",
      instanceIds: ["test"],
    }),
  ).rejects.toThrow("raw hook provenance");
});

it("derives terminal usage across checkpointed stock line callbacks without changing stored bytes", async () => {
  const request = await observation(`{"stream":true}`, 1, "request");
  const call = initialCall(request, "call", 1);
  let state: ProjectionCheckpoint = {};
  const observations = [
    request,
    await observation("event: response.completed", 2),
    await observation(`data: ${terminal}`, 3),
    await observation("", 4, "completion"),
  ];
  for (const o of observations) {
    const before = JSON.stringify(o);
    applyObservation(call, state, o);
    expect(JSON.stringify(o)).toBe(before);
    state = JSON.parse(JSON.stringify(state));
  }
  expect(call).toMatchObject({
    state: "succeeded",
    responseModel: "reported-model",
    totalTokens: 10,
    capture: {
      raw: "complete",
      projection: "complete",
      usage: "complete",
      capturePolicy: "hook-body-v1",
    },
  });
  expect(call.capture.projectionIssue).toBeUndefined();
  expect(call.providerName).toBeUndefined();
  expect(call.authType).toBe("unknown");
});

it.each([
  ["data: {bad}\n\n", "invalid", "malformed_payload"],
  ['data: {"unfinished":', "partial", "truncated_frame"],
])("keeps raw %s while labeling projection separately", async (body, projection, issue) => {
  const request = await observation(`{"stream":true}`, 1, "request");
  const call = initialCall(request, "call", 1);
  const state: ProjectionCheckpoint = {};
  for (const o of [request, await observation(body, 2), await observation("", 3, "completion")])
    applyObservation(call, state, o);
  expect(call.capture).toMatchObject({
    raw: "complete",
    projection,
    projectionIssue: issue,
    usage: "unavailable",
  });
  expect(call.totalTokens).toBeUndefined();
});

it("keeps capture loss explicit even when terminal usage is recorded", async () => {
  const request = await observation(`{"stream":true}`, 1, "request");
  const call = initialCall(request, "call", 1);
  const state: ProjectionCheckpoint = {};
  const dropped = { ...(await observation("", 2)), gap: "observation_body_limit" };
  for (const o of [
    request,
    dropped,
    await observation(`event: response.completed\ndata: ${terminal}`, 3),
    await observation("", 4, "completion"),
  ])
    applyObservation(call, state, o);
  expect(call.capture).toMatchObject({
    raw: "partial",
    usage: "partial",
    gaps: ["observation_body_limit"],
  });
  expect(call.totalTokens).toBeUndefined();
});

it("records after-auth model and selected IDs, deriving the provider only from the execution protocol", async () => {
  const request = {
    ...(await observation(`{"stream":true}`, 1, "request")),
    requestedModel: "claude-opus-5-5",
  };
  const call = initialCall(request, "call", 1);
  const state: ProjectionCheckpoint = {};
  applyObservation(call, state, request);
  expect(call).toMatchObject({
    providerName: "anthropic",
    providerProvenance: "derived_from_model",
  });
  const after = {
    ...(await observation(`{"token":"content"}`, 2, "request_after_auth")),
    executionModel: "selected-model",
    executionProtocol: "openai-response",
    selectedAuthId: "auth-id",
    selectedAuthIndex: "auth-index",
  };
  await validateObservation(encoder.encode(JSON.stringify(after)), {
    destinationId: "dev",
    instanceIds: ["test"],
  });
  applyObservation(call, state, after);
  expect(call).toMatchObject({
    executionModel: "selected-model",
    executionProtocol: "openai-response",
    selectedAuthId: "auth-id",
    selectedAuthIndex: "auth-index",
    attemptDetail: "unavailable",
    correlation: {},
    // The execution outranks the requested model's name.
    providerName: "openai",
    providerProvenance: "derived_from_execution_protocol",
  });
});

it("preserves every split including field-looking strings, UTF-8, CRLF and multiline JSON", () => {
  for (const raw of [
    `event: response.completed\r\ndata: ${terminal}\r\n\r\n`,
    'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"é secret-value data: event: id: retry: :"}\n\n',
    'event: response.output_text.delta\ndata: {\ndata: "delta":"keep token secret",\ndata: "type":"response.output_text.delta"}\n\n',
  ]) {
    const bytes = encoder.encode(raw);
    const want = new SSEReader().feedStock(bytes, "responses");
    for (let split = 0; split <= bytes.length; split++) {
      const reader = new SSEReader();
      const first = reader.feedStock(bytes.slice(0, split), "responses");
      const resumed = new SSEReader(reader.checkpoint());
      expect([...first, ...resumed.feedStock(bytes.slice(split), "responses")]).toEqual(want);
      expect(resumed.finish().truncated).toBe(false);
    }
  }
});

it("keeps arbitrary data-prefixed JSON fragments as content and enforces parser limits", () => {
  const reader = new SSEReader();
  reader.feedStock(encoder.encode("event: response.output_text.delta"), "responses");
  reader.feedStock(encoder.encode('data: {"delta":"literal '), "responses");
  expect(reader.feedStock(encoder.encode('data: field"}'), "responses")[0]?.data).toBe(
    '{"delta":"literal data: field"}',
  );
  expect(() =>
    new SSEReader(undefined, 16).feedStock(encoder.encode("data: " + "x".repeat(20)), "responses"),
  ).toThrow("SSE frame limit");
  expect(() =>
    new SSEReader(undefined, 16).feedStock(
      encoder.encode('{"x":"' + "x".repeat(20) + '"}'),
      "chat_completions",
    ),
  ).toThrow("SSE frame limit");
});

it("projects raw Chat JSON chunks and keeps source strings unchanged", () => {
  const raw =
    '{"model":"reported","choices":[{"index":0,"delta":{"content":"secret-value"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}';
  const result = projectCapturedPayloads({
    route: "POST /v1/chat/completions",
    stockHookChunks: true,
    complete: true,
    chunks: [encoder.encode(raw.slice(0, 60)), encoder.encode(raw.slice(60))],
  });
  expect(result.responseState).toBe("decoded");
  expect(result.scalars.totalTokens).toBe(10);
  expect(result.output?.completion).toBe("secret-value");
});
