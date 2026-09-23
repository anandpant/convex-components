import { expect, it } from "vitest";
import { fromOpenRouterSpan, fromLegacyCliproxySpan } from "../src/model-call/index.js";
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
    inputTokens: 0,
    correlation: { runId: "exact-run" },
    cost: { kind: "proxy_reported", currency: "USD", total: "0.004" },
    attemptDetail: "unavailable",
    capture: { raw: "unavailable" },
  });
  expect(call.usage[0]).toMatchObject({ value: 0, unit: "tokens", finality: "unknown" });
  expect(call.outputTokens).toBeUndefined();
  expect(call.timeToFirstContentMs).toBeUndefined();
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
    cost: { kind: "unknown" },
  });
});
