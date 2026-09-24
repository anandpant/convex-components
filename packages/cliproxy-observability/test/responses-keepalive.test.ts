import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { callIdentity, type CaptureObservationV1 } from "../src/capture/index.js";
import { initialCall } from "../src/client.js";
import { applyObservation, type ProjectionCheckpoint } from "../src/protocols/checkpoint.js";
import { projectCapturedPayloads } from "../src/protocols/index.js";

const keepalive = { type: "keepalive", sequence_number: 2 };
const terminal = (status = "completed", usage = true) => ({
  type: `response.${status}`,
  sequence_number: 3,
  response: {
    object: "response",
    id: "response-fixture",
    status,
    output: [],
    ...(usage ? { usage: { input_tokens: 10, output_tokens: 2, total_tokens: 12 } } : {}),
  },
});
const bytes = (events: object[]) =>
  Buffer.from(events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(""));
const project = (events: object[]) =>
  projectCapturedPayloads({
    route: "POST /v1/responses",
    complete: true,
    chunks: [bytes(events)],
  });

it("preserves completed response and usage across every keepalive SSE byte split", () => {
  const body = bytes([keepalive, terminal()]);
  const control = project([terminal()]);
  for (let split = 0; split <= body.length; split++) {
    const result = projectCapturedPayloads({
      route: "POST /v1/responses",
      complete: true,
      chunks: [body.subarray(0, split), body.subarray(split)],
    });
    expect(result.responseState).toBe("decoded");
    expect(result.output).toEqual(control.output);
    expect(result.scalars).toEqual(control.scalars);
    expect(result.metadata.usageState).toBe("terminal_snapshot");
  }
});

it("does not manufacture response identity, terminal state or usage from keepalive alone", () => {
  const result = project([keepalive]);
  expect(result.responseState).toBe("truncated");
  expect(result.metadata.terminalObserved).toBe(false);
  expect(result.scalars.totalTokens).toBeUndefined();
  expect(result.scalars.finishReason).toBeUndefined();
  expect(result.output?.native).not.toHaveProperty("id");
});

it.each([undefined, null, "2", -1, 1.5, Number.MAX_SAFE_INTEGER + 1])(
  "keeps malformed keepalive sequence %j invalid even with a valid terminal",
  (sequence_number) => {
    const result = project([{ type: "keepalive", sequence_number }, terminal()]);
    expect(result.responseState).toBe("invalid");
    expect(result.scalars.totalTokens).toBeUndefined();
  },
);

it("does not clear unknown-event or malformed-JSON diagnostics", () => {
  expect(project([{ type: "fixture.unknown" }, keepalive, terminal()]).responseState).toBe(
    "unsupported",
  );
  const result = projectCapturedPayloads({
    route: "POST /v1/responses",
    complete: true,
    chunks: [Buffer.concat([Buffer.from("data: {broken\n\n"), bytes([keepalive, terminal()])])],
  });
  expect(result.responseState).toBe("invalid");
  expect(result.scalars.totalTokens).toBeUndefined();
});

it("does not repair a malformed terminal snapshot", () => {
  const result = project([
    { type: "response.completed", response: { object: "response", id: "response-fixture" } },
    keepalive,
  ]);
  expect(result.responseState).toBe("invalid");
  expect(result.metadata.terminalObserved).toBe(false);
  expect(result.scalars.totalTokens).toBeUndefined();
});

it.each(["failed", "incomplete"])("preserves %s without inventing usage", (status) => {
  const result = project([keepalive, terminal(status, false)]);
  expect(result.responseState).toBe("decoded");
  expect(result.scalars.finishReason).toBe(status);
  expect(result.metadata.terminalObserved).toBe(true);
  expect(result.scalars.totalTokens).toBeUndefined();
});

it("preserves an error terminal without inventing usage", () => {
  const result = project([keepalive, { type: "error", code: "fixture_error", message: "Failure" }]);
  expect(result.responseState).toBe("decoded");
  expect(result.scalars.finishReason).toBe("failed");
  expect(result.metadata.terminalObserved).toBe(true);
  expect(result.scalars.totalTokens).toBeUndefined();
});

it("keeps public native checkpoint projection and final token scalars complete", async () => {
  const template = (
    JSON.parse(
      readFileSync(new URL("../fixtures/real/responses-sse.json", import.meta.url), "utf8"),
    ) as {
      events: CaptureObservationV1[];
    }
  ).events;
  const body = bytes([keepalive, terminal()]);
  const chunk: CaptureObservationV1 = {
    ...template[1]!,
    kind: "stream_chunk",
    sequence: 3,
    body: body.toString("base64"),
    contentBytes: body.length,
    contentSha256: createHash("sha256").update(body).digest("hex"),
  };
  const call = initialCall(template[0]!, await callIdentity(template[0]!), 1);
  const state: ProjectionCheckpoint = {};
  for (const event of [
    template[0]!,
    template[1]!,
    chunk,
    { ...template.at(-1)!, sequence: 4, bodyFromSequence: 4 },
  ])
    applyObservation(call, state, event);
  expect(call.state).toBe("succeeded");
  expect(call.capture.projection).toBe("complete");
  expect(call.capture.usage).toBe("complete");
  expect(call.inputTokens).toBe(10);
  expect(call.outputTokens).toBe(2);
  expect(call.totalTokens).toBe(12);
});
