import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { decodeOpenRouterInput, decodeOpenRouterOutput } from "../src/content.js";

describe("OpenRouter content decoding", () => {
  it("decodes string, null, text-part, tool-call, and opaque reasoning-detail messages", () => {
    const value = {
      messages: [
        { role: "system", content: [{ type: "text", text: "system text" }] },
        { role: "user", content: "question" },
        {
          role: "assistant",
          content: null,
          tool_calls: [
            {
              id: "call-1",
              type: "function",
              function: { name: "lookup", arguments: '{"id":1}' },
            },
          ],
          reasoning_details: [
            { type: "reasoning.encrypted", data: "opaque", format: "x", id: "r1", index: 0 },
          ],
        },
        { role: "tool", content: "result", tool_call_id: "call-1" },
      ],
    };
    const raw = JSON.stringify(value);
    const result = decodeOpenRouterInput(raw);

    expect(result).toMatchObject({ kind: "decoded", raw, value });
    if (result.kind !== "decoded") throw new Error("input must decode");
    expect(result.content.messages[0]).toMatchObject({
      role: "system",
      content: {
        kind: "parts",
        parts: [{ type: "text", text: "system text", raw: { type: "text", text: "system text" } }],
      },
    });
    expect(result.content.messages[2]).toMatchObject({
      role: "assistant",
      emittedToolCalls: [{ id: "call-1", function: { name: "lookup" } }],
      reasoningDetails: value.messages[2]?.reasoning_details,
    });
    expect(result.content.messages[3]).toMatchObject({ role: "tool", toolCallId: "call-1" });
  });

  it("decodes the synthetic text-part regression fixture", () => {
    const raw = readFileSync(
      fileURLToPath(new URL("test-fixtures/content-text-parts.json", import.meta.url)),
      "utf8",
    );
    const result = decodeOpenRouterInput(raw);
    expect(result).toMatchObject({
      kind: "decoded",
      content: {
        messages: [
          {
            role: "system",
            content: {
              kind: "parts",
              parts: [{ type: "text", text: "Synthetic system instruction" }],
            },
          },
          { role: "user", content: "Synthetic request" },
        ],
      },
    });
  });

  it("keeps request definitions separate from historical emitted calls", () => {
    const raw = JSON.stringify({
      completion: "",
      reasoning: null,
      tools: [
        {
          type: "function",
          function: {
            name: "lookup",
            description: "Look up a record",
            parameters: { type: "object" },
          },
        },
      ],
      rawRequest: { model: "example/model" },
    });
    const result = decodeOpenRouterOutput(raw);

    expect(result).toMatchObject({
      kind: "decoded",
      raw,
      content: {
        text: "",
        reasoning: null,
        requestToolDefinitions: [{ function: { name: "lookup" } }],
      },
    });
    expect(JSON.stringify(result)).not.toContain("emittedToolCalls");
  });

  it("distinguishes absent, invalid JSON, unsupported shapes, and invalid known shapes", () => {
    expect(decodeOpenRouterInput(undefined)).toEqual({ kind: "absent" });
    expect(decodeOpenRouterInput("not-json")).toMatchObject({ kind: "invalid", raw: "not-json" });
    expect(
      decodeOpenRouterInput(JSON.stringify({ messages: [{ role: "developer", content: "x" }] })),
    ).toMatchObject({ kind: "unsupported" });
    expect(
      decodeOpenRouterInput(
        JSON.stringify({ messages: [{ role: "user", content: [{ type: "image", url: "x" }] }] }),
      ),
    ).toMatchObject({
      kind: "decoded",
      content: { messages: [{ content: { parts: [{ type: "opaque" }] } }] },
    });
    expect(
      decodeOpenRouterInput(JSON.stringify({ messages: [{ role: "tool", content: "x" }] })),
    ).toMatchObject({ kind: "unsupported" });
    expect(decodeOpenRouterOutput(JSON.stringify({ completion: 1 }))).toMatchObject({
      kind: "invalid",
    });
    expect(decodeOpenRouterOutput(JSON.stringify({ choices: [] }))).toMatchObject({
      kind: "unsupported",
    });
  });
});
