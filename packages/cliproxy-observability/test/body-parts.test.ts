/// <reference types="vite/client" />
import { convexTest } from "convex-test";
import { expect, it } from "vitest";
import schema from "../src/component/schema.js";
import { api } from "../src/component/_generated/api.js";
import type { ComponentApi } from "../src/component/_generated/component.js";
import {
  CliproxyObservability,
  handleCliproxyCaptureRequest,
  projectPendingSegments,
  type PrivateCaptureStorage,
} from "../src/client.js";
import {
  MAX_CONTENT_BYTES,
  callIdentity,
  sha256,
  validateObservation,
  validateSegment,
  type CaptureObservationV1,
} from "../src/capture/index.js";
import { validContentHealth } from "../src/capture/content-health.js";
import { BodyPartAssembler, stockHookPieces } from "../src/protocols/index.js";

const encoder = new TextEncoder();
const token = "test-capture-token-at-least-24-characters";
const scope = { destinationId: "dev", instanceIds: ["test"] };
function base64(bytes: Uint8Array) {
  let text = "";
  for (let i = 0; i < bytes.length; i += 0x8000)
    text += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(text);
}
async function record(
  kind: CaptureObservationV1["kind"],
  sequence: number,
  body: Uint8Array,
  extra: Partial<CaptureObservationV1> = {},
): Promise<CaptureObservationV1> {
  return {
    schemaVersion: 1,
    pluginVersion: "0.2.5",
    capturePolicy: "hook-content-block-v1",
    destinationId: "dev",
    instanceId: "test",
    pluginBootId: "boot",
    requestId: "split",
    sequence,
    kind,
    observedAt: "2026-10-11T04:00:00Z",
    offsetNs: sequence * 1000,
    route: "POST /v1/responses",
    configRevision: "r1",
    ...(body.length ? { body: base64(body) } : {}),
    contentBytes: body.length,
    contentSha256: await sha256(body),
    ...(kind === "stream_chunk"
      ? { bodyFraming: "stock_hook_block" as const, stockHookChunkLengths: [body.length] }
      : {}),
    ...(kind === "completion" ? { completionOutcome: "succeeded" } : {}),
    ...extra,
  };
}
/** Producer layout: exact MAX_CONTENT_BYTES parts, consecutive sequences, one manifest. */
async function split(kind: CaptureObservationV1["kind"], first: number, body: Uint8Array) {
  const count = Math.ceil(body.length / MAX_CONTENT_BYTES);
  const totalSha256 = await sha256(body);
  return Promise.all(
    Array.from({ length: count }, (_, index) => {
      const offset = index * MAX_CONTENT_BYTES;
      const part = body.subarray(offset, Math.min(body.length, offset + MAX_CONTENT_BYTES));
      return record(kind, first + index, part, {
        bodyPart: { index, count, offset, totalBytes: body.length, totalSha256 },
        observedBodyBytes: part.length,
      });
    }),
  );
}
const validate = (o: unknown) => validateObservation(encoder.encode(JSON.stringify(o)), scope);
async function segment(observations: CaptureObservationV1[]) {
  const first = observations[0]!;
  const content = encoder.encode(observations.map((o) => JSON.stringify(o)).join("\n") + "\n");
  return {
    schemaVersion: 1,
    operation: "segment",
    destinationId: first.destinationId,
    instanceId: first.instanceId,
    pluginBootId: first.pluginBootId,
    requestId: first.requestId,
    firstSequence: first.sequence,
    throughSequence: observations.at(-1)!.sequence,
    contentBytes: content.length,
    contentSha256: await sha256(content),
    contentBase64: base64(content),
  };
}
const requestBody = encoder.encode(
  JSON.stringify({ model: "gpt-test", stream: true, input: "r".repeat(2 * MAX_CONTENT_BYTES) }),
);

it("validates exact split request parts and rejects inexact manifests", async () => {
  const parts = await split("request", 1, requestBody);
  expect(parts).toHaveLength(3);
  for (const part of parts) expect((await validate(part)).observation).toEqual(part);
  const [first, , last] = parts as [
    CaptureObservationV1,
    CaptureObservationV1,
    CaptureObservationV1,
  ];
  const manifest = first.bodyPart!;
  const cases: Array<[string, unknown]> = [
    ["offset", { ...first, bodyPart: { ...manifest, offset: 1 } }],
    ["count", { ...first, bodyPart: { ...manifest, count: 4 } }],
    ["single", { ...first, bodyPart: { ...manifest, count: 1 } }],
    ["too many", { ...first, bodyPart: { ...manifest, count: 257 } }],
    ["index", { ...first, bodyPart: { ...manifest, index: 3 } }],
    ["fraction", { ...first, bodyPart: { ...manifest, totalBytes: 1.5 } }],
    ["digest", { ...first, bodyPart: { ...manifest, totalSha256: "A".repeat(64) } }],
    ["extra key", { ...first, bodyPart: { ...manifest, extra: 1 } }],
    ["missing key", { ...first, bodyPart: { ...manifest, offset: undefined } }],
    ["kind", { ...first, kind: "completion", completionOutcome: "succeeded" }],
    ["policy", { ...first, capturePolicy: "hook-body-v1" }],
    [
      "short final",
      { ...last, bodyPart: { ...last.bodyPart!, totalBytes: requestBody.length + 1 } },
    ],
  ];
  for (const [, value] of cases) await expect(validate(value)).rejects.toThrow();
  // A non-final part must hold exactly MAX_CONTENT_BYTES.
  const short = await record("request", 1, new Uint8Array(10), { bodyPart: manifest });
  await expect(validate(short)).rejects.toThrow("invalid body part");
}, 60_000);

it("reassembles byte-identical bodies and refuses missing, interleaved or altered parts", async () => {
  const body = Uint8Array.from({ length: 3 * MAX_CONTENT_BYTES + 7 }, (_, i) => (i * 31) % 256);
  const parts = await split("request_after_auth", 5, body);
  const decoded = await Promise.all(parts.map(async (o) => (await validate(o)).body));
  const assembler = new BodyPartAssembler();
  let whole: Uint8Array | undefined;
  for (const [index, part] of parts.entries()) {
    whole = await assembler.push(part, decoded[index]!);
    expect(whole === undefined).toBe(index < parts.length - 1);
  }
  expect(whole).toEqual(body);
  expect(assembler.pending).toBe(false);
  const plain = await record("completion", 9, new Uint8Array());
  expect(await new BodyPartAssembler().push(plain, new Uint8Array())).toEqual(new Uint8Array());

  const missing = new BodyPartAssembler();
  await missing.push(parts[0]!, decoded[0]!);
  await expect(missing.push(parts[2]!, decoded[2]!)).rejects.toThrow("order");
  const interleaved = new BodyPartAssembler();
  await interleaved.push(parts[0]!, decoded[0]!);
  await expect(interleaved.push(plain, new Uint8Array())).rejects.toThrow("interrupted");
  const altered = new BodyPartAssembler();
  for (const [index, part] of parts.slice(0, -1).entries())
    await altered.push(part, decoded[index]!);
  await expect(
    altered.push(
      parts.at(-1)!,
      decoded.at(-1)!.map((b) => b ^ 1),
    ),
  ).rejects.toThrow("digest");
  await expect(
    new BodyPartAssembler(MAX_CONTENT_BYTES).push(parts[0]!, new Uint8Array()),
  ).rejects.toThrow("read limit");
}, 60_000);

it("requires split parts that share a segment to chain", async () => {
  // A full part leaves room for a small final part in the same 1408 KiB segment.
  const parts = await split("response", 1, new Uint8Array(MAX_CONTENT_BYTES + 3));
  const check = async (observations: CaptureObservationV1[]) =>
    validateSegment(encoder.encode(JSON.stringify(await segment(observations))), scope);
  expect((await check(parts)).observations).toHaveLength(2);
  const completion = await record("completion", 2, new Uint8Array());
  await expect(check([parts[0]!, completion])).rejects.toThrow("body part order");
  const whole = await record("response", 1, new Uint8Array(3));
  await expect(check([whole, parts[1]!])).rejects.toThrow("body part order");
  const other = await split("response", 1, new Uint8Array(MAX_CONTENT_BYTES + 4));
  await expect(check([parts[0]!, other[1]!])).rejects.toThrow();
}, 60_000);

it("accepts the native 0.2.5 missed-call counter and older health senders", () => {
  const health = {
    observedBytes: 10,
    committedBytes: 10,
    lostBytes: 0,
    unconfirmedBytes: 0,
    unknownLosses: 0,
    streamCallbacks: 1,
    incompleteCalls: 0,
    capacityRejections: 0,
    retainedChargedBytes: 0,
    peakChargedBytes: 9,
  };
  expect(validContentHealth(health)).toBe(true);
  expect(validContentHealth({ ...health, missedCalls: 2 })).toBe(true);
  expect(validContentHealth({ ...health, missedCalls: -1 })).toBe(false);
  expect(validContentHealth({ ...health, other: 1 })).toBe(false);
  expect(validContentHealth({ ...health, lostBytes: undefined })).toBe(false);
}, 60_000);

function setup() {
  const backend = convexTest(schema, import.meta.glob("../src/component/**/*.ts"));
  const client = new CliproxyObservability(api as unknown as ComponentApi);
  const blobs = new Map<string, Uint8Array>();
  const storage: PrivateCaptureStorage = {
    put: async (ref, body) => {
      blobs.set(ref.key, body);
    },
    get: async (key) =>
      new ReadableStream({
        start(c) {
          c.enqueue(blobs.get(key)!);
          c.close();
        },
      }),
  };
  const ctx = { runMutation: backend.mutation, runQuery: backend.query } as unknown as Parameters<
    typeof handleCliproxyCaptureRequest
  >[0];
  const options = {
    client,
    storage,
    destinationId: "dev",
    deploymentId: "dev-deployment",
    environment: "dev" as const,
    instanceIds: ["test"],
    tokens: [token],
    scheduleProjection: async () => {},
  };
  const post = async (body: unknown) =>
    handleCliproxyCaptureRequest(
      ctx,
      new Request("https://example.test/cliproxy/capture/v1", {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify(body),
      }),
      options,
    );
  return { backend, ctx, options, post };
}
async function project(s: ReturnType<typeof setup>, callId: string) {
  for (let i = 0; i < 32; i++) {
    const result = await projectPendingSegments(s.ctx, { ...s.options, callId });
    if (!result.more) break;
  }
  const call = await s.backend.query(api.queries.getCall, { destinationId: "dev", callId });
  return JSON.parse(call!.summaryJson);
}

it("receives and projects split request and stream bodies across segments", async () => {
  const s = setup();
  const terminal = encoder.encode(
    "data: " +
      JSON.stringify({
        type: "response.completed",
        response: {
          object: "response",
          id: "resp",
          status: "completed",
          output: [{ type: "message", content: [{ type: "output_text", text: "done" }] }],
          usage: { input_tokens: 7, output_tokens: 3, total_tokens: 10 },
        },
      }),
  );
  const request = await split("request", 1, requestBody);
  const after = await record("request_after_auth", 4, encoder.encode('{"model":"gpt-test"}'));
  const created = encoder.encode(
    'data: {"type":"response.created","response":{"object":"response","id":"resp","status":"in_progress","output":[]}}',
  );
  const event = encoder.encode("event: response.completed");
  const block = await record("stream_chunk", 5, Uint8Array.from([...created, ...event]), {
    stockHookChunkLengths: [created.length, event.length],
  });
  const tail = await record("stream_chunk", 6, terminal);
  const completion = await record("completion", 7, new Uint8Array());
  const layout = [[request[0]!], [request[1]!], [request[2]!, after, block, tail, completion]];
  for (const observations of layout)
    expect((await s.post(await segment(observations))).status).toBe(200);
  const callId = await callIdentity(request[0]!);
  expect(await project(s, callId)).toMatchObject({
    state: "succeeded",
    streamed: true,
    totalTokens: 10,
    capture: { raw: "complete", projection: "complete", terminalSequence: 7 },
  });
}, 60_000);

it("projects a split non-stream response and keeps a split stream frame exact", async () => {
  const s = setup();
  const response = encoder.encode(
    JSON.stringify({
      object: "response",
      id: "resp",
      status: "completed",
      output: [
        {
          type: "message",
          content: [{ type: "output_text", text: "x".repeat(MAX_CONTENT_BYTES) }],
        },
      ],
      usage: { input_tokens: 7, output_tokens: 3, total_tokens: 10 },
    }),
  );
  const request = await record("request", 1, encoder.encode('{"model":"gpt-test"}'), {
    requestId: "json",
  });
  const parts = (await split("response", 2, response)).map((o) => ({ ...o, requestId: "json" }));
  const completion = await record("completion", 4, new Uint8Array(), { requestId: "json" });
  for (const observations of [
    [request, parts[0]!],
    [parts[1]!, completion],
  ])
    expect((await s.post(await segment(observations))).status).toBe(200);
  expect(await project(s, await callIdentity(request))).toMatchObject({
    state: "succeeded",
    totalTokens: 10,
    capture: { raw: "complete", projection: "complete" },
  });

  // One oversized stock callback is one exact piece; the receiver's unchanged
  // 1 MiB frame bound reports frame_limit rather than inventing truncation.
  const big = encoder.encode("data: " + "y".repeat(MAX_CONTENT_BYTES + 10));
  const streamParts = await split("stream_chunk", 2, big);
  for (const part of streamParts) expect(part.stockHookChunkLengths).toEqual([part.contentBytes]);
  const assembler = new BodyPartAssembler();
  let whole: Uint8Array | undefined;
  for (const part of streamParts) whole = await assembler.push(part, (await validate(part)).body);
  expect(stockHookPieces(streamParts.at(-1)!, whole!)).toEqual([big]);
  const streamed = await record("request", 1, encoder.encode('{"stream":true}'), {
    requestId: "frame",
  });
  const done = await record("completion", 4, new Uint8Array(), { requestId: "frame" });
  const frame = streamParts.map((o) => ({ ...o, requestId: "frame" }));
  for (const observations of [
    [streamed, frame[0]!],
    [frame[1]!, done],
  ])
    expect((await s.post(await segment(observations))).status).toBe(200);
  expect(await project(s, await callIdentity(streamed))).toMatchObject({
    capture: { raw: "complete", projectionIssue: "frame_limit" },
  });
}, 60_000);

it("rejects a split body interrupted across segment boundaries", async () => {
  const s = setup();
  const parts = await split("request", 1, requestBody);
  const response = await record("response", 2, encoder.encode("{}"));
  const completion = await record("completion", 3, new Uint8Array());
  // Each segment is internally valid; only the projection sees the interruption.
  for (const observations of [[parts[0]!], [response], [completion]])
    expect((await s.post(await segment(observations))).status).toBe(200);
  const callId = await callIdentity(parts[0]!);
  await expect(projectPendingSegments(s.ctx, { ...s.options, callId })).rejects.toThrow(
    "body part interrupted",
  );
  const call = await s.backend.query(api.queries.getCall, { destinationId: "dev", callId });
  expect(call?.projectedThroughSequence).toBe(0);
  expect(call?.projectionFailure?.reason).toBe("projection_processing_failed");
}, 60_000);
