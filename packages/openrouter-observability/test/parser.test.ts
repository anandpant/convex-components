import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  assertSpanAttributeReconstruction,
  InvalidOtlpDeliveryError,
  OtlpBoundExceededError,
  parseOpenRouterOtlpDelivery,
  type ParsedOpenRouterSpan,
} from "../src/component/parser.js";

type FixtureAttribute = { key: string; value: Record<string, unknown> };
type FixtureSpan = {
  attributes: Array<FixtureAttribute>;
  name: string;
  spanId: string;
  traceId: string;
};
type Fixture = {
  resourceSpans: Array<{
    scopeSpans: Array<{ spans: Array<FixtureSpan> }>;
  }>;
};

const FIXTURES = [
  {
    file: "001-smoke.json",
    generationId: "gen-1787971242-ZNNkqbzHERdScNd64gYK",
    model: "openai/gpt-5.4-nano",
    requestId: "fixture-req-001",
    finishReason: "stop",
    usage: [13, 5, 18, 0, 0, 0.0000026, 0.00000625, 0.00000885],
    openrouterTokens: [9, 2],
  },
  {
    file: "002-tool-call.json",
    generationId: "gen-1787972736-9ngMnzfRAnNHIroUIO90",
    model: "openai/gpt-5.4-nano",
    requestId: "fixture-req-002",
    finishReason: "tool_calls",
    usage: [54, 18, 72, 0, 0, 0.0000108, 0.0000225, 0.0000333],
    openrouterTokens: [69, 7],
  },
  {
    file: "003-reasoning-luna.json",
    generationId: "gen-1787972747-Wd6lkgLcnjcUaU3Y1Tgz",
    model: "openai/gpt-5.4-nano",
    requestId: "fixture-req-003",
    finishReason: "stop",
    usage: [16, 6, 22, 0, 0, 0.0000032, 0.0000075, 0.0000107],
    openrouterTokens: [8, 1],
  },
  {
    file: "004-reasoning-nano.json",
    generationId: "gen-1787972760-gCWdFfH6D3r9CfswQztg",
    model: "openai/gpt-5.6-luna",
    requestId: "fixture-req-004",
    finishReason: "stop",
    usage: [16, 6, 22, 0, 0, 0.0000032, 0.0000072, 0.0000104],
    openrouterTokens: [8, 1],
  },
] as const;

const PROJECTION_FIELDS = [
  "userId",
  "sessionId",
  "requestId",
  "environment",
  "feature",
  "traceName",
  "spanType",
  "entityType",
  "entityId",
  "requestModel",
  "responseModel",
  "generationId",
  "providerName",
  "finishReason",
  "inputTokens",
  "outputTokens",
  "totalTokens",
  "reasoningTokens",
  "cachedInputTokens",
  "inputCost",
  "outputCost",
  "totalCost",
  "openrouterPromptTokens",
  "openrouterCompletionTokens",
  "openrouterUsageCost",
  "upstreamUsageCost",
  "cacheUsageCost",
  "dataUsageCost",
  "webUsageCost",
  "webFetchUsageCost",
  "upstreamWebFetchUsageCost",
  "fileUsageCost",
  "byokInferenceUsageCost",
  "creditPoolUsageCost",
  "creditPoolId",
  "creditPoolExpiresAt",
  "isByok",
  "apiKeyName",
  "streamed",
] as const satisfies ReadonlyArray<keyof ParsedOpenRouterSpan>;

const EMPTY_BILLING_PROJECTIONS = {
  openrouterPromptTokens: undefined,
  openrouterCompletionTokens: undefined,
  openrouterUsageCost: undefined,
  upstreamUsageCost: undefined,
  cacheUsageCost: undefined,
  dataUsageCost: undefined,
  webUsageCost: undefined,
  webFetchUsageCost: undefined,
  upstreamWebFetchUsageCost: undefined,
  fileUsageCost: undefined,
  byokInferenceUsageCost: undefined,
  creditPoolUsageCost: undefined,
  creditPoolId: undefined,
  creditPoolExpiresAt: undefined,
  isByok: undefined,
} as const;

function loadFixture(file: string) {
  const path = fileURLToPath(new URL(`test-fixtures/${file}`, import.meta.url));
  return JSON.parse(readFileSync(path, "utf8")) as Fixture;
}

function fixtureSpans(fixture: Fixture) {
  return fixture.resourceSpans.flatMap((resource) =>
    resource.scopeSpans.flatMap((scope) => scope.spans),
  );
}

function projectionValues(span: ParsedOpenRouterSpan) {
  return Object.fromEntries(PROJECTION_FIELDS.map((field) => [field, span[field]]));
}

function assertRemainderOrder(
  original: ReadonlyArray<FixtureAttribute>,
  parsed: ParsedOpenRouterSpan,
) {
  let searchFrom = 0;
  for (const remainder of parsed.attributes) {
    const index = original.findIndex(
      (attribute, candidateIndex) =>
        candidateIndex >= searchFrom &&
        attribute.key === remainder.key &&
        JSON.stringify(attribute.value) === remainder.valueJson,
    );
    expect(index).toBeGreaterThanOrEqual(searchFrom);
    searchFrom = index + 1;
  }
}

function expectedRootProjection(fixture: (typeof FIXTURES)[number]) {
  const [
    inputTokens,
    outputTokens,
    totalTokens,
    reasoningTokens,
    cachedInputTokens,
    inputCost,
    outputCost,
    totalCost,
  ] = fixture.usage;
  const [openrouterPromptTokens, openrouterCompletionTokens] = fixture.openrouterTokens;
  return {
    userId: "fixture-capture-smoke",
    sessionId: "fixture-capture-session-001",
    requestId: fixture.requestId,
    environment: "dev",
    feature: "observability_incubation",
    traceName: "Fixture Capture Smoke",
    spanType: "generation",
    entityType: undefined,
    entityId: undefined,
    requestModel: fixture.model,
    responseModel: fixture.model,
    generationId: fixture.generationId,
    providerName: "OpenAI",
    finishReason: fixture.finishReason,
    inputTokens,
    outputTokens,
    totalTokens,
    reasoningTokens,
    cachedInputTokens,
    inputCost,
    outputCost,
    totalCost,
    ...EMPTY_BILLING_PROJECTIONS,
    openrouterPromptTokens,
    openrouterCompletionTokens,
    apiKeyName: "api-key_TEST",
    streamed: true,
  };
}

describe("OpenRouter Broadcast OTLP parser", () => {
  it.each(FIXTURES)("parses and losslessly deduplicates $file", (fixtureDefinition) => {
    const fixture = loadFixture(fixtureDefinition.file);
    const originalSpans = fixtureSpans(fixture);
    const parsed = parseOpenRouterOtlpDelivery(fixture);

    expect(fixture.resourceSpans).toHaveLength(2);
    expect(originalSpans.map((span) => span.attributes.length)).toEqual([97, 10]);
    expect(parsed).toHaveLength(2);
    const [root, provider] = parsed;
    expect(root).toBeDefined();
    expect(provider).toBeDefined();
    if (!root || !provider) throw new Error("fixture must contain two spans");

    expect(projectionValues(root)).toEqual(expectedRootProjection(fixtureDefinition));
    expect(projectionValues(provider)).toEqual({
      userId: undefined,
      sessionId: undefined,
      requestId: undefined,
      environment: undefined,
      feature: undefined,
      traceName: undefined,
      spanType: "span",
      entityType: undefined,
      entityId: undefined,
      requestModel: undefined,
      responseModel: undefined,
      generationId: `${fixtureDefinition.generationId}:attempt-0`,
      providerName: "OpenAI",
      finishReason: undefined,
      inputTokens: undefined,
      outputTokens: undefined,
      totalTokens: undefined,
      reasoningTokens: undefined,
      cachedInputTokens: undefined,
      inputCost: undefined,
      outputCost: undefined,
      totalCost: undefined,
      ...EMPTY_BILLING_PROJECTIONS,
      apiKeyName: undefined,
      streamed: undefined,
    });
    expect(root.serviceName).toBe("openrouter");
    expect(root.openrouterTraceId).toBe(fixtureDefinition.generationId);
    expect(root.input).toBeTypeOf("string");
    expect(root.output).toBeTypeOf("string");
    expect(root.attributes.map(({ key }) => key)).not.toEqual(
      expect.arrayContaining([
        "trace.input",
        "span.input",
        "gen_ai.prompt",
        "trace.output",
        "span.output",
        "gen_ai.completion",
      ]),
    );

    for (const [index, span] of parsed.entries()) {
      const original = originalSpans[index];
      if (!original) throw new Error("missing original fixture span");
      assertSpanAttributeReconstruction(original.attributes, span);
      assertRemainderOrder(original.attributes, span);
    }
  });

  it("projects the OpenRouter cost catalog with its observed OTLP value types", () => {
    const attributes: Array<FixtureAttribute> = [
      { key: "gen_ai.usage.input_tokens", value: { intValue: 13 } },
      { key: "gen_ai.usage.output_tokens", value: { intValue: 7 } },
      { key: "gen_ai.usage.total_cost", value: { doubleValue: 0.000_011_35 } },
      {
        key: "trace.metadata.openrouter_generation.tokens_prompt",
        value: { intValue: 9 },
      },
      {
        key: "trace.metadata.openrouter_generation.tokens_completion",
        value: { intValue: 4 },
      },
      {
        key: "trace.metadata.openrouter_generation.usage",
        value: { doubleValue: 0.000_011_35 },
      },
      {
        key: "trace.metadata.openrouter_generation.usage_upstream",
        value: { doubleValue: 0.000_01 },
      },
      {
        key: "trace.metadata.openrouter_generation.usage_cache",
        value: { intValue: 0 },
      },
      {
        key: "trace.metadata.openrouter_generation.usage_data",
        value: { doubleValue: -0.000_001 },
      },
      {
        key: "trace.metadata.openrouter_generation.usage_web",
        value: { doubleValue: 0.000_002 },
      },
      {
        key: "trace.metadata.openrouter_generation.usage_web_fetch",
        value: { doubleValue: 0.000_003 },
      },
      {
        key: "trace.metadata.openrouter_generation.usage_upstream_web_fetch",
        value: { doubleValue: 0.000_002_5 },
      },
      {
        key: "trace.metadata.openrouter_generation.usage_file",
        value: { doubleValue: 0.000_004 },
      },
      {
        key: "trace.metadata.openrouter_generation.byok_usage_inference",
        value: { intValue: 0 },
      },
      {
        key: "trace.metadata.openrouter_generation.credit_pool_usage",
        value: { doubleValue: 0.000_005 },
      },
      {
        key: "trace.metadata.openrouter_generation.credit_pool_id",
        value: { stringValue: "pool_test" },
      },
      {
        key: "trace.metadata.openrouter_generation.credit_pool_expires_at",
        value: { stringValue: "2026-09-01T00:00:00Z" },
      },
      {
        key: "trace.metadata.openrouter_generation.is_byok",
        value: { boolValue: false },
      },
    ];
    const delivery = {
      resourceSpans: [
        {
          resource: { attributes: [] },
          scopeSpans: [
            {
              spans: [{ traceId: "trace", spanId: "span", name: "cost", attributes }],
            },
          ],
        },
      ],
    };

    const [span] = parseOpenRouterOtlpDelivery(delivery);
    expect(span).toMatchObject({
      inputTokens: 13,
      outputTokens: 7,
      totalCost: 0.000_011_35,
      openrouterPromptTokens: 9,
      openrouterCompletionTokens: 4,
      openrouterUsageCost: 0.000_011_35,
      upstreamUsageCost: 0.000_01,
      cacheUsageCost: 0,
      dataUsageCost: -0.000_001,
      webUsageCost: 0.000_002,
      webFetchUsageCost: 0.000_003,
      upstreamWebFetchUsageCost: 0.000_002_5,
      fileUsageCost: 0.000_004,
      byokInferenceUsageCost: 0,
      creditPoolUsageCost: 0.000_005,
      creditPoolId: "pool_test",
      creditPoolExpiresAt: "2026-09-01T00:00:00Z",
      isByok: false,
      attributes: [],
    });
    if (!span) throw new Error("delivery must contain one span");
    assertSpanAttributeReconstruction(attributes, span);
  });

  it("keeps every content copy when byte identity is not proven", () => {
    const delivery = {
      resourceSpans: [
        {
          resource: { attributes: [] },
          scopeSpans: [
            {
              spans: [
                {
                  traceId: "trace",
                  spanId: "span",
                  name: "generation",
                  attributes: [
                    { key: "trace.input", value: { stringValue: "one" } },
                    { key: "span.input", value: { stringValue: "two" } },
                  ],
                },
              ],
            },
          ],
        },
      ],
    };

    const [span] = parseOpenRouterOtlpDelivery(delivery);
    expect(span?.input).toBeUndefined();
    expect(span?.attributes.map(({ key }) => key)).toEqual(["trace.input", "span.input"]);
  });

  it("preserves unprojected resource attributes in source order", () => {
    const delivery = {
      resourceSpans: [
        {
          resource: {
            attributes: [
              { key: "service.name", value: { stringValue: "openrouter" } },
              { key: "cloud.region", value: { stringValue: "us-east-1" } },
              { key: "openrouter.trace.id", value: { stringValue: "generation-123" } },
              { key: "deployment.environment", value: { stringValue: "preview" } },
            ],
          },
          scopeSpans: [
            {
              spans: [{ traceId: "trace", spanId: "span", name: "resource metadata" }],
            },
          ],
        },
      ],
    };

    const [span] = parseOpenRouterOtlpDelivery(delivery);
    expect(span).toMatchObject({
      serviceName: "openrouter",
      openrouterTraceId: "generation-123",
      resourceAttributes: [
        { key: "cloud.region", valueJson: JSON.stringify({ stringValue: "us-east-1" }) },
        {
          key: "deployment.environment",
          valueJson: JSON.stringify({ stringValue: "preview" }),
        },
      ],
    });
  });

  it.each([
    {},
    { resourceSpans: [{}] },
    { resourceSpans: [{ scopeSpans: [{ spans: [{ name: "missing ids" }] }] }] },
    {
      resourceSpans: [
        {
          scopeSpans: [
            {
              spans: [
                {
                  traceId: "trace",
                  spanId: "span",
                  name: "bad",
                  attributes: [null],
                },
              ],
            },
          ],
        },
      ],
    },
  ])("rejects malformed OTLP without partial output", (delivery) => {
    expect(() => parseOpenRouterOtlpDelivery(delivery)).toThrow(InvalidOtlpDeliveryError);
  });

  it("distinguishes structural bounds from malformed OTLP", () => {
    const delivery = {
      resourceSpans: Array.from({ length: 65 }, () => ({ scopeSpans: [] })),
    };
    expect(() => parseOpenRouterOtlpDelivery(delivery)).toThrow(OtlpBoundExceededError);
  });
});
