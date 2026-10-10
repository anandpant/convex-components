import { expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { expandStockHookBlock, projectCapturedPayloads } from "../src/protocols/index.js";
import { applyObservation, type ProjectionCheckpoint } from "../src/protocols/checkpoint.js";
import { initialCall } from "../src/client.js";
import { sha256, validateObservation, type CaptureObservationV1 } from "../src/capture/index.js";

const encoder = new TextEncoder();
async function observation(
  kind: CaptureObservationV1["kind"],
  sequence: number,
  pieces: readonly Uint8Array[],
) {
  const body = Uint8Array.from(pieces.flatMap((p) => Array.from(p)));
  const o: CaptureObservationV1 = {
    schemaVersion: 1,
    pluginVersion: "0.2.4",
    capturePolicy: "hook-content-block-v1",
    destinationId: "dev",
    instanceId: "test",
    pluginBootId: "boot",
    requestId: "req",
    sequence,
    kind,
    observedAt: "2026-10-10T16:00:00Z",
    offsetNs: sequence * 1000,
    route: "POST /v1/responses",
    configRevision: "r1",
    contentBytes: body.length,
    contentSha256: await sha256(body),
    body: btoa(String.fromCharCode(...body)),
    ...(kind === "stream_chunk"
      ? { bodyFraming: "stock_hook_block", stockHookChunkLengths: pieces.map((p) => p.length) }
      : {}),
    ...(kind === "completion" ? { completionOutcome: "succeeded" } : {}),
  };
  return o;
}
const terminal = JSON.stringify({
  type: "response.completed",
  response: {
    object: "response",
    id: "resp",
    status: "completed",
    output: [],
    usage: { input_tokens: 7, output_tokens: 3, total_tokens: 10 },
  },
});
const validate = (o: CaptureObservationV1) =>
  validateObservation(encoder.encode(JSON.stringify(o)), {
    destinationId: "dev",
    instanceIds: ["test"],
  });

it("accepts exact native producer fixtures and reconstructs both stock protocols", async () => {
  const lines = readFileSync(
    new URL("../fixtures/content-block-v1.ndjson", import.meta.url),
    "utf8",
  )
    .trim()
    .split("\n");
  expect(lines).toHaveLength(3);
  for (const [index, line] of lines.entries()) {
    const o = JSON.parse(line) as CaptureObservationV1;
    const validated = await validateObservation(encoder.encode(line), {
      destinationId: o.destinationId,
      instanceIds: [o.instanceId],
    });
    if (index < 2) {
      const chunks = expandStockHookBlock(validated.body, o.stockHookChunkLengths!);
      const projected = projectCapturedPayloads({
        route: o.route,
        chunks,
        stockHookChunks: true,
        complete: true,
      });
      expect(projected.responseState).toBe("decoded");
      expect(JSON.stringify(projected.output)).toContain("CAPTURE_OK");
    } else {
      expect(o).toMatchObject({
        kind: "completion",
        captureIncomplete: true,
        gap: "capture_shutdown_incomplete",
        contentBytes: 0,
      });
      expect(o.completionOutcome).toBeUndefined();
      expect(o.lostContentBytes).toBeUndefined();
    }
  }
});

it("recovers exact binary bytes and empty parser boundaries without mutating them", () => {
  const bytes = new Uint8Array([0xff, 0, 0xc3, 0xa9, 0x80]);
  const pieces = expandStockHookBlock(bytes, [1, 0, 2, 2]);
  expect(pieces.map((p) => Array.from(p))).toEqual([[255], [], [0, 195], [169, 128]]);
  expect(Array.from(bytes)).toEqual([255, 0, 195, 169, 128]);
  expect(expandStockHookBlock(new Uint8Array(), [0])).toHaveLength(1);
});
it.each([
  [],
  [1],
  [6],
  [1, -1, 5],
  [1.5, 3.5],
  [NaN],
  [Number.MAX_SAFE_INTEGER],
  Array(1025).fill(0),
])("rejects invalid framing lengths %j", (lengths) => {
  expect(() => expandStockHookBlock(new Uint8Array(5), lengths)).toThrow();
});
it.each([
  [encoder.encode("event: response.completed"), encoder.encode("data: " + terminal)],
  [encoder.encode("event: response.completed\r\ndata: " + terminal + "\r\n\r\n")],
])("projects stock line and JSON callbacks after lossless block expansion", async (...pieces) => {
  const request = await observation("request", 1, [encoder.encode('{"stream":true}')]);
  const block = await observation("stream_chunk", 2, pieces);
  expect((await validate(block)).observation).toEqual(block);
  const call = initialCall(request, "call", 1);
  let state: ProjectionCheckpoint = {};
  for (const o of [request, block, await observation("completion", 3, [])]) {
    applyObservation(call, state, o);
    state = JSON.parse(JSON.stringify(state));
  }
  expect(call).toMatchObject({
    state: "succeeded",
    totalTokens: 10,
    capture: { raw: "complete", projection: "complete", capturePolicy: "hook-content-block-v1" },
  });
});
it("keeps adjacent bare JSON objects distinct for full tool and text reconstruction", async () => {
  const pieces = [
    {
      choices: [
        {
          index: 0,
          delta: {
            tool_calls: [
              {
                index: 0,
                id: "tool",
                type: "function",
                function: { name: "cad", arguments: '{"value":"' },
              },
            ],
          },
          finish_reason: null,
        },
      ],
    },
    {
      choices: [
        {
          index: 0,
          delta: { tool_calls: [{ index: 0, function: { arguments: 'é"}' } }] },
          finish_reason: null,
        },
      ],
    },
    { choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] },
    { choices: [], usage: { prompt_tokens: 7, completion_tokens: 3, total_tokens: 10 } },
  ].map((o) => encoder.encode(JSON.stringify(o)));
  const block = {
    ...(await observation("stream_chunk", 1, pieces)),
    route: "POST /v1/chat/completions",
  };
  const validated = await validate(block);
  const recovered = expandStockHookBlock(validated.body, block.stockHookChunkLengths!);
  expect(recovered).toEqual(pieces);
  const args = { route: block.route, chunks: recovered, stockHookChunks: true, complete: true };
  const projected = projectCapturedPayloads(args);
  expect(projected).toEqual(projectCapturedPayloads({ ...args, chunks: pieces }));
  expect(JSON.stringify(projected.output)).toContain("é");
  expect(projected.responseState).toBe("decoded");
});
it("closes incomplete capture with unknown outcome and exact byte loss", async () => {
  const request = await observation("request", 1, [encoder.encode('{"stream":true}')]);
  const incomplete = {
    ...(await observation("completion", 2, [])),
    completionOutcome: undefined,
    gap: "capture_shutdown_incomplete",
    captureIncomplete: true,
    lostContentBytes: 17,
  };
  await validate(incomplete);
  const call = initialCall(request, "call", 1);
  const state: ProjectionCheckpoint = {};
  applyObservation(call, state, request);
  applyObservation(call, state, incomplete);
  expect(call).toMatchObject({
    state: "unknown",
    capture: { raw: "partial", incomplete: true, lostContentBytes: 17, terminalSequence: 2 },
  });
  expect(call.completionOutcome).toBeUndefined();
  await expect(validate({ ...incomplete, gap: "other" })).rejects.toThrow("completion outcome");
  await expect(validate({ ...incomplete, captureIncomplete: false })).rejects.toThrow();
});
it("rejects policy confusion and retains large supported diagnostics", async () => {
  const block = await observation("stream_chunk", 1, [encoder.encode(terminal)]);
  await expect(validate({ ...block, capturePolicy: "hook-body-v1" })).rejects.toThrow();
  await expect(validate({ ...block, stockHookChunkLengths: [1] })).rejects.toThrow();
  await expect(validate({ ...block, bodyFraming: "stock_hook_chunk" })).rejects.toThrow();
  const completion = {
    ...(await observation("completion", 2, [])),
    error: "x".repeat(5000),
    errorPresent: true,
    observedErrorBytes: 5000,
  };
  expect((await validate(completion)).observation.error).toBe(completion.error);
  await expect(validate({ ...completion, capturePolicy: "hook-body-v1" })).rejects.toThrow(
    "error detail",
  );
});
