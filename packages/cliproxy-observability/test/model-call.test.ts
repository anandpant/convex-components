import { expect, it } from "vitest";
import {
  fromOpenRouterSpan,
  fromLegacyCliproxySpan,
  providerIdentity,
} from "../src/model-call/index.js";
it("preserves stored OpenRouter scalars without inventing stream or attempt evidence", () => {
  const call = fromOpenRouterSpan({
    _id: "span-doc",
    receivedAt: 10,
    traceId: "app-trace",
    spanId: "span",
    providerName: "provider",
    inputTokens: 0,
    totalCost: 0.004,
    runId: "exact-run",
  });
  expect(call).toMatchObject({
    source: "openrouter",
    sourceDocumentId: "span-doc",
    traceId: "app-trace",
    providerName: "provider",
    providerProvenance: "observed",
    inputTokens: 0,
    correlation: { runId: "exact-run" },
    cost: { kind: "proxy_reported", currency: "USD", total: "0.004" },
    costProvenance: "proxy_reported",
    attemptDetail: "unavailable",
    capture: { raw: "unavailable" },
  });
  expect(call.usage[0]).toMatchObject({ value: 0, unit: "tokens", finality: "unknown" });
  expect(call.outputTokens).toBeUndefined();
  expect(call.timeToFirstContentMs).toBeUndefined();
});
it("carries OpenRouter cache writes and never guesses a provider or cost it did not report", () => {
  const call = fromOpenRouterSpan({
    _id: "span-doc",
    receivedAt: 10,
    requestModel: "gpt-5.6-luna",
    cacheCreationInputTokens: 12,
  });
  expect(call).toMatchObject({
    providerName: undefined,
    providerProvenance: "unavailable",
    cacheCreationInputTokens: 12,
    cost: { kind: "unknown" },
    costProvenance: "unknown",
  });
  expect(call.usage).toMatchObject([{ nativeField: "cacheCreationInputTokens", value: 12 }]);
});
it("labels legacy gateway rows and leaves OAuth cost unknown", () => {
  expect(
    fromLegacyCliproxySpan({
      _id: "legacy",
      receivedAt: 1,
      providerName: "cliproxy",
      totalCost: 0,
    }),
  ).toMatchObject({
    source: "cliproxy_legacy",
    gateway: "cliproxy",
    providerName: undefined,
    providerProvenance: "unavailable",
    cost: { kind: "unknown" },
    costProvenance: "unknown",
  });
});
it.each([
  [
    "a recorded provider",
    { observedProvider: "OpenAI", executionProtocol: "claude" },
    "OpenAI",
    "observed",
  ],
  [
    "Claude execution",
    { executionProtocol: "claude", requestModel: "gpt-5" },
    "anthropic",
    "derived_from_execution_protocol",
  ],
  [
    "Messages execution",
    { executionProtocol: "messages" },
    "anthropic",
    "derived_from_execution_protocol",
  ],
  [
    "Responses execution",
    { executionProtocol: "openai-response" },
    "openai",
    "derived_from_execution_protocol",
  ],
  [
    "responses execution",
    { executionProtocol: "responses" },
    "openai",
    "derived_from_execution_protocol",
  ],
  [
    "Codex execution",
    { executionProtocol: "codex", requestModel: "claude-opus-5-5" },
    "openai",
    "derived_from_execution_protocol",
  ],
  ["Chat execution", { executionProtocol: "chat" }, "openai", "derived_from_execution_protocol"],
  [
    "an unmapped execution",
    { executionProtocol: "gemini", requestModel: "claude-opus-5-5" },
    undefined,
    "unavailable",
  ],
  ["a prototype-named execution", { executionProtocol: "toString" }, undefined, "unavailable"],
  [
    "an execution model alone",
    { executionModel: "claude-opus-5-5", requestModel: "claude-opus-5-5" },
    undefined,
    "unavailable",
  ],
  ["a Claude model", { requestModel: "claude-opus-5-5" }, "anthropic", "derived_from_model"],
  ["a GPT model", { requestModel: "gpt-5.6-luna" }, "openai", "derived_from_model"],
  ["an o-series model", { requestModel: "o4-mini" }, "openai", "derived_from_model"],
  ["a Codex model", { requestModel: "codex-mini-latest" }, "openai", "derived_from_model"],
  ["a Gemini model", { requestModel: "gemini-3-pro" }, "google", "derived_from_model"],
  ["an alias outside every prefix", { requestModel: "opus" }, undefined, "unavailable"],
  [
    "a vendor-qualified model",
    { requestModel: "anthropic/claude-opus-5-5" },
    undefined,
    "unavailable",
  ],
  ["no recorded facts", {}, undefined, "unavailable"],
] as const)(
  "derives provider identity from %s",
  (_name, facts, providerName, providerProvenance) => {
    expect(providerIdentity(facts)).toEqual({ providerName, providerProvenance });
  },
);
