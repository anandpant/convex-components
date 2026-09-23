import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { callIdentity, decodeBody, type CaptureObservationV1 } from "../src/capture/index.js";
import { initialCall } from "../src/client.js";
import { applyObservation, type ProjectionCheckpoint } from "../src/protocols/checkpoint.js";
import { projectCapturedPayloads, SSEReader } from "../src/protocols/index.js";
const recording = (name: string) =>
  (
    JSON.parse(readFileSync(new URL(`../fixtures/real/${name}.json`, import.meta.url), "utf8")) as {
      events: CaptureObservationV1[];
    }
  ).events;
it.each(["messages-sse", "responses-sse", "chat-json"])(
  "replays real %s through summary and full content parsers",
  async (name) => {
    const events = recording(name);
    const first = events[0]!;
    const call = initialCall(first, await callIdentity(first), 1);
    const state: ProjectionCheckpoint = {};
    for (const event of events) applyObservation(call, state, event);
    expect(call.state).toBe("succeeded");
    expect(call.providerName).toBeUndefined();
    expect(call.cost).toEqual({ kind: "unknown" });
    expect(call.capture.raw).toBe("complete");
    expect(call.capture.projectedThroughSequence).toBe(events.length);
    expect(call.usage.length).toBeGreaterThan(0);
    const projection = projectCapturedPayloads({
      route: first.route,
      request: decodeBody(first),
      response: events.find((e) => e.kind === "response")
        ? decodeBody(events.find((e) => e.kind === "response")!)
        : undefined,
      chunks: name.endsWith("sse")
        ? events.filter((e) => e.kind === "stream_chunk").map(decodeBody)
        : undefined,
    });
    expect(projection.responseState).toBe("decoded");
    expect(projection.output?.completion).toBe("CAPTURE_OK");
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
