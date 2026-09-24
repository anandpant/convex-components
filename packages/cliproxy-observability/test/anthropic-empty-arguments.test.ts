import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { callIdentity, type CaptureObservationV1 } from "../src/capture/index.js";
import { initialCall } from "../src/client.js";
import { applyObservation, type ProjectionCheckpoint } from "../src/protocols/checkpoint.js";
import { decodeCliproxyOutput } from "../src/protocols/content.js";
import { projectCapturedPayloads } from "../src/protocols/index.js";

const start = {
  type: "message_start",
  message: {
    id: "message-fixture",
    type: "message",
    role: "assistant",
    model: "claude-fixture",
    usage: { input_tokens: 2, output_tokens: 0 },
    content: [],
  },
};
const finish = [
  { type: "message_delta", delta: { stop_reason: "tool_use" }, usage: { output_tokens: 4 } },
  { type: "message_stop" },
];
const tool = (input: Record<string, unknown>, fragments: string[], index = 0) => [
  {
    type: "content_block_start",
    index,
    content_block: {
      type: "tool_use",
      id: `tool-${index}`,
      name: "run_part_preflight",
      ...input,
    },
  },
  ...fragments.map((partial_json) => ({
    type: "content_block_delta",
    index,
    delta: { type: "input_json_delta", partial_json },
  })),
  { type: "content_block_stop", index },
];
const bytes = (events: object[]) =>
  new TextEncoder().encode(events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(""));
const project = (events: object[], split?: number) => {
  const body = bytes(events);
  return projectCapturedPayloads({
    route: "POST /v1/messages",
    complete: true,
    chunks: split === undefined ? [body] : [body.slice(0, split), body.slice(split)],
  });
};

it("preserves initial empty tool input through every SSE byte split", () => {
  const events = [start, ...tool({ input: {} }, ["", ""]), ...finish];
  for (let split = 0; split <= bytes(events).length; split++) {
    const result = project(events, split);
    expect(result.responseState).toBe("decoded");
    expect(result.metadata.usageState).toBe("terminal_snapshot");
    expect(result.output).toMatchObject({
      native: { content: [{ type: "tool_use", input: {} }] },
      messages: [{ tool_calls: [{ function: { arguments: "{}" } }] }],
    });
    expect(decodeCliproxyOutput(JSON.stringify(result.output)).state).toBe("decoded");
  }
});

it.each([{}, { input: null }, { input: [] }, { input: "{}" }])(
  "does not invent an object for absent or invalid initial input %j",
  (input) => {
    const result = project([start, ...tool(input, [""]), ...finish]);
    expect(result.responseState).toBe("invalid");
    expect(result.metadata.usageState).toBe("partial_snapshot");
    expect(result.scalars.outputTokens).toBeUndefined();
    expect(result.output).toMatchObject({ native: { content: [{ ...input, partial_json: "" }] } });
    if (!("input" in input)) expect(result.output?.native).not.toHaveProperty("content.0.input");
  },
);

it("retains a supplied nonempty initial object when deltas contribute no characters", () => {
  const result = project([start, ...tool({ input: { supplied: 1 } }, [""]), ...finish]);
  expect(result.responseState).toBe("decoded");
  expect(result.output).toMatchObject({ native: { content: [{ input: { supplied: 1 } }] } });
});

it("ignores empty fragments around valid JSON and keeps multiple tools distinct", () => {
  const result = project([
    start,
    ...tool({ input: {} }, [""]),
    ...tool({ input: {} }, ["", '{"label":"', "", 'é🦉"}', ""], 1),
    ...finish,
  ]);
  expect(result.responseState).toBe("decoded");
  expect(result.output).toMatchObject({
    native: {
      content: [
        { id: "tool-0", input: {} },
        { id: "tool-1", input: { label: "é🦉" } },
      ],
    },
  });
});

it.each(["{broken", '{"size":'])("keeps nonempty invalid arguments %s", (fragment) => {
  const result = project([start, ...tool({ input: {} }, ["", fragment, ""]), ...finish]);
  expect(result.responseState).toBe("invalid");
  expect(result.scalars.outputTokens).toBeUndefined();
  expect(result.output).toMatchObject({ native: { content: [{ partial_json: fragment }] } });
});

it("does not promote an unterminated stream or overwrite signed thinking", () => {
  const result = project([
    start,
    {
      type: "content_block_start",
      index: 0,
      content_block: {
        type: "thinking",
        thinking: "Emitted summary",
        signature: "opaque-signature",
      },
    },
    ...tool({ input: {} }, [""], 1),
  ]);
  expect(result.responseState).toBe("truncated");
  expect(result.metadata.terminalObserved).toBe(false);
  expect(result.scalars.outputTokens).toBeUndefined();
  expect(result.output).toMatchObject({
    native: {
      content: [{ thinking: "Emitted summary", signature: "opaque-signature" }, { input: {} }],
    },
  });
});

it("agrees with the scalar checkpoint for the complete empty-input call", async () => {
  const template = (
    JSON.parse(
      readFileSync(new URL("../fixtures/real/messages-sse.json", import.meta.url), "utf8"),
    ) as {
      events: CaptureObservationV1[];
    }
  ).events;
  const events = [start, ...tool({ input: {} }, [""]), ...finish];
  const body = bytes(events);
  const observation: CaptureObservationV1 = {
    ...template[1]!,
    kind: "stream_chunk",
    sequence: 3,
    body: Buffer.from(body).toString("base64"),
    contentBytes: body.length,
    contentSha256: createHash("sha256").update(body).digest("hex"),
  };
  const call = initialCall(template[0]!, await callIdentity(template[0]!), 1);
  const checkpoint: ProjectionCheckpoint = {};
  for (const event of [
    template[0]!,
    template[1]!,
    observation,
    { ...template.at(-1)!, sequence: 4, bodyFromSequence: 4 },
  ])
    applyObservation(call, checkpoint, event);
  const result = project(events);
  expect(call.capture.projection).toBe("complete");
  expect(call.capture.usage).toBe("complete");
  expect(call.state).toBe("succeeded");
  expect(result.responseState).toBe("decoded");
  expect(result.scalars.inputTokens).toBe(call.inputTokens);
  expect(result.scalars.outputTokens).toBe(call.outputTokens);
});
