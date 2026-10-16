import { expect, it } from "vitest";
import { projectCapturedPayloads } from "../src/protocols/index.js";

import { metadata, terminal, streamBytes } from "./fixtures/metadata-stream.js";
const project = (events: object[]) =>
  projectCapturedPayloads({
    route: "POST /v1/responses",
    chunks: [streamBytes(events)],
    complete: true,
  });

it("preserves metadata events separately from terminal response metadata and all six token counts", () => {
  const control = project([terminal]);
  const result = project([metadata, terminal]);
  expect(result.responseState).toBe("decoded");
  expect(result.scalars).toEqual(control.scalars);
  expect(result.output).toEqual(control.output);
  expect(result.metadata.responseMetadata).toEqual([metadata]);
});

it("preserves repeated metadata updates across every byte split without inventing a terminal", () => {
  const next = { ...metadata, sequence_number: 3, metadata: { routing: { lane: "next" } } };
  const body = streamBytes([metadata, terminal, next]);
  for (let split = 0; split <= body.length; split++) {
    const result = projectCapturedPayloads({
      route: "POST /v1/responses",
      chunks: [body.subarray(0, split), body.subarray(split)],
    });
    expect(result.responseState).toBe("decoded");
    expect(result.metadata.responseMetadata).toEqual([metadata, next]);
    expect(result.scalars.totalTokens).toBe(65);
  }
  const partial = project([metadata]);
  expect(partial.responseState).toBe("truncated");
  expect(partial.metadata.terminalObserved).toBe(false);
  expect(partial.scalars.totalTokens).toBeUndefined();
  expect(partial.metadata.responseMetadata).toEqual([metadata]);
});

it.each([
  { metadata: null },
  { metadata: [] },
  { metadata: "invalid" },
  { response_id: "" },
  { response_id: 2 },
  { response_id: "unrelated" },
  { sequence_number: -1 },
  { sequence_number: 1.5 },
  { sequence_number: undefined },
])("keeps malformed or unrelated metadata invalid: %j", (fields) => {
  for (const events of [
    [{ ...metadata, ...fields }, terminal],
    [terminal, { ...metadata, ...fields }],
  ]) {
    const result = project(events);
    expect(result.responseState).toBe("invalid");
    expect(result.scalars.totalTokens).toBeUndefined();
  }
});

it("retains unknown events, malformed framing and missing terminal diagnostics", () => {
  expect(project([metadata, { type: "response.future" }, terminal]).responseState).toBe(
    "unsupported",
  );
  const result = projectCapturedPayloads({
    route: "POST /v1/responses",
    chunks: [Buffer.concat([streamBytes([metadata, terminal]), Buffer.from("data: {")])],
  });
  expect(result.responseState).toBe("invalid");
  expect(result.scalars.totalTokens).toBeUndefined();
});

it("keeps emitted text, reasoning summaries and unfinished tool arguments with metadata on failure", () => {
  const events = [
    metadata,
    {
      type: "response.output_item.added",
      output_index: 0,
      item: { type: "message", id: "text", content: [] },
    },
    {
      type: "response.output_text.delta",
      output_index: 0,
      item_id: "text",
      content_index: 0,
      delta: "Built.",
    },
    {
      type: "response.output_item.added",
      output_index: 1,
      item: { type: "reasoning", id: "reason", summary: [] },
    },
    {
      type: "response.reasoning_summary_text.delta",
      output_index: 1,
      item_id: "reason",
      summary_index: 0,
      delta: "Checked.",
    },
    {
      type: "response.output_item.added",
      output_index: 2,
      item: {
        type: "function_call",
        id: "tool",
        call_id: "call-fixture",
        name: "build",
        arguments: "",
      },
    },
    {
      type: "response.function_call_arguments.delta",
      output_index: 2,
      item_id: "tool",
      delta: '{"size":',
    },
  ];
  for (const failure of [
    [],
    [{ type: "error", code: "fixture_failure", message: "Interrupted" }],
  ]) {
    const result = project([...events, ...failure]);
    expect(result.responseState).toBe(failure.length ? "decoded" : "truncated");
    expect(result.scalars.totalTokens).toBeUndefined();
    expect(result.output).toMatchObject({
      completion: "Built.",
      reasoning: "Checked.",
      messages: [{}, {}, { tool_calls: [{ function: { arguments: '{"size":' } }] }],
    });
    expect(result.metadata.responseMetadata).toEqual([metadata]);
    if (failure.length)
      expect(result.output?.native).toMatchObject({ error: { code: "fixture_failure" } });
  }
});
