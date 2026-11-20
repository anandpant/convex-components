/// <reference types="vite/client" />

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { convexTest } from "convex-test";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  handleOpenRouterTraceRequest,
  loadFullSpan,
  readBoundedTraceBody,
  type SpanCursor,
  type TraceBlobStorage,
} from "../src/client.js";
import { api } from "../src/component/_generated/api.js";
import type { ComponentApi } from "../src/component/_generated/component.js";
import { parseOpenRouterOtlpDelivery } from "../src/component/parser.js";
import schema from "../src/component/schema.js";
import { memoryStorage } from "./memoryStorage.js";

const modules = import.meta.glob("../src/component/**/*.ts");
const TOKEN = "test-openrouter-observability-token";
const JSON_HEADERS = {
  authorization: `Bearer ${TOKEN}`,
  "content-type": "application/json",
};

function createBackend() {
  return convexTest(schema, modules);
}

function loadFixture(file = "001-smoke.json") {
  return readFileSync(fileURLToPath(new URL(`test-fixtures/${file}`, import.meta.url)), "utf8");
}

async function storedSpans(backend: ReturnType<typeof createBackend>) {
  return await backend.run(async (ctx) => await ctx.db.query("spans").collect());
}

async function post(
  backend: ReturnType<typeof createBackend>,
  body: string,
  headers: HeadersInit = JSON_HEADERS,
  storage: TraceBlobStorage = { put: async () => undefined },
  method: "POST" | "PUT" = "POST",
) {
  return await handleOpenRouterTraceRequest(
    {
      runMutation: async (_reference, args) =>
        await backend.mutation(api.ingest.admitPrepared, args as never),
    },
    new Request("https://example.test/openrouter/traces", { method, headers, body }),
    {
      component: api as unknown as ComponentApi,
      bearerToken: TOKEN,
      blobStorage: storage,
      spanPrefix: "test/spans",
    },
  );
}

function envelope(...spans: Record<string, unknown>[]) {
  return { resourceSpans: [{ scopeSpans: [{ spans }] }] };
}

describe("OpenRouter observability component", () => {
  afterEach(() => vi.useRealTimers());

  it.each([
    ["missing authorization", { "content-type": "application/json" }, 401],
    ["wrong bearer token", { ...JSON_HEADERS, authorization: "Bearer wrong" }, 401],
    ["wrong content type", { ...JSON_HEADERS, "content-type": "text/plain" }, 415],
  ])("fails closed for %s", async (_name, headers, status) => {
    const backend = createBackend();
    const response = await post(backend, loadFixture(), headers);
    expect(response.status).toBe(status);
    expect(await storedSpans(backend)).toHaveLength(0);
  });

  it("fails closed when the webhook token is not configured", async () => {
    const backend = createBackend();
    const response = await handleOpenRouterTraceRequest(
      { runMutation: async () => Promise.reject(new Error("must not run")) },
      new Request("https://example.test/traces", {
        method: "POST",
        headers: JSON_HEADERS,
        body: loadFixture(),
      }),
      {
        component: api as unknown as ComponentApi,
        bearerToken: undefined,
        blobStorage: { put: async () => undefined },
      },
    );
    expect(response.status).toBe(401);
    expect(await storedSpans(backend)).toHaveLength(0);
  });

  it("rejects malformed JSON and malformed OTLP without writes", async () => {
    const backend = createBackend();
    expect((await post(backend, "{not-json")).status).toBe(400);
    expect((await post(backend, JSON.stringify({ resourceSpans: [{}] }))).status).toBe(400);
    expect(await storedSpans(backend)).toHaveLength(0);
  });

  it("rejects HTTP and structural bounds without writes", async () => {
    const backend = createBackend();
    const oversized = `"${"a".repeat(8 * 1024 * 1024)}"`;
    expect((await post(backend, oversized)).status).toBe(413);
    const spans = Array.from({ length: 65 }, (_, index) => ({
      traceId: "trace",
      spanId: `span-${index}`,
      name: "bounded",
    }));
    expect((await post(backend, JSON.stringify(envelope(...spans)))).status).toBe(413);
    const expandedResourceAttributes = {
      resourceSpans: [
        {
          resource: {
            attributes: [{ key: "large.resource", value: { stringValue: "x".repeat(200 * 1024) } }],
          },
          scopeSpans: [{ spans: spans.slice(0, 64) }],
        },
      ],
    };
    expect((await post(backend, JSON.stringify(expandedResourceAttributes))).status).toBe(413);
    expect(await storedSpans(backend)).toHaveLength(0);
  });

  it("counts streamed bytes, cancels oversized bodies, and ignores a misleading short length", async () => {
    const backend = createBackend();
    let pulls = 0;
    let cancelled = false;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        pulls += 1;
        controller.enqueue(new Uint8Array(3 * 1024 * 1024));
      },
      cancel() {
        cancelled = true;
      },
    });
    const response = await handleOpenRouterTraceRequest(
      { runMutation: async () => Promise.reject(new Error("must not run")) },
      new Request("https://example.test/traces", {
        method: "POST",
        headers: { ...JSON_HEADERS, "content-length": "1" },
        body: stream,
        duplex: "half",
      } as RequestInit),
      {
        component: api as unknown as ComponentApi,
        bearerToken: TOKEN,
        blobStorage: { put: async () => undefined },
      },
    );

    expect(response.status).toBe(413);
    expect(pulls).toBeLessThanOrEqual(5);
    expect(cancelled).toBe(true);
    expect(await storedSpans(backend)).toHaveLength(0);
  });

  it("rejects an oversized declared length before reading or writing", async () => {
    let pulled = false;
    const request = new Request("https://example.test/traces", {
      method: "POST",
      headers: { "content-length": String(8 * 1024 * 1024 + 1) },
      body: new ReadableStream({
        pull(controller) {
          pulled = true;
          controller.enqueue(new Uint8Array([123, 125]));
          controller.close();
        },
      }),
      duplex: "half",
    } as RequestInit);

    expect(await readBoundedTraceBody(request, 8 * 1024 * 1024)).toEqual({ kind: "too_large" });
    expect(pulled).toBe(false);
  });

  it("decodes UTF-8 characters split across body chunks", async () => {
    const utf8 = new TextEncoder().encode('{"message":"café"}');
    const split = utf8.indexOf(0xc3) + 1;
    const request = new Request("https://example.test/traces", {
      method: "POST",
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(utf8.slice(0, split));
          controller.enqueue(utf8.slice(split));
          controller.close();
        },
      }),
      duplex: "half",
    } as RequestInit);

    const result = await readBoundedTraceBody(request, 1024);
    expect(result.kind).toBe("body");
    if (result.kind !== "body") throw new Error("body must be accepted");
    expect(new TextDecoder().decode(result.bytes)).toBe('{"message":"café"}');
  });

  it("accepts authenticated Test Connection with no write", async () => {
    const backend = createBackend();
    const storage = memoryStorage();
    const response = await post(
      backend,
      JSON.stringify({ resourceSpans: [] }),
      { ...JSON_HEADERS, "x-test-connection": "true" },
      storage.adapter,
    );
    expect(response.status).toBe(204);
    expect(await storedSpans(backend)).toHaveLength(0);
    expect(storage.objects.size).toBe(0);
  });

  it.each(["POST", "PUT"] as const)(
    "ingests and archives a real fixture over %s",
    async (method) => {
      const backend = createBackend();
      const storage = memoryStorage();
      const fixture = loadFixture();
      const original = parseOpenRouterOtlpDelivery(JSON.parse(fixture));
      const response = await post(backend, fixture, JSON_HEADERS, storage.adapter, method);
      expect(response.status).toBe(202);
      const rows = await storedSpans(backend);
      expect(rows).toHaveLength(2);
      expect(rows).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ requestId: "fixture-req-001", spanType: "generation" }),
          expect.objectContaining({ parentSpanId: "a44772891b0e7515", spanType: "span" }),
        ]),
      );
      expect(storage.objects.size).toBe(rows.length);

      for (const { _id: spanDocumentId } of rows) {
        const row = await backend.query(api.queries.getSpan, { spanDocumentId });
        if (!row?.payload) throw new Error("expected a span with a payload reference");
        for (const field of ["input", "output", "attributes", "resourceAttributes"]) {
          expect(row).not.toHaveProperty(field);
        }
        const { key, sha256, byteLength } = row.payload;
        expect(key).toBe(`test/spans/${row.traceId}/${row.spanId}/${sha256}.json`);
        const object = storage.objects.get(key);
        if (!object) throw new Error("expected an archived object");
        expect(object).toMatchObject({
          key,
          sha256,
          byteLength,
          kind: "trace_blob",
          contentType: "application/json",
          encoding: "utf8",
        });
        expect(object.bytes.byteLength).toBe(byteLength);
        expect(createHash("sha256").update(object.bytes).digest("hex")).toBe(sha256);
        const expected = original.find(
          (span) => span.traceId === row.traceId && span.spanId === row.spanId,
        );
        if (!expected) throw new Error("expected a matching fixture span");
        const full = await loadFullSpan(storage.reader, row, { maxBytes: byteLength });
        expect(full).toMatchObject({ attributes: expected.attributes });
        expect(full?.input).toBe(expected.input);
        expect(full?.output).toBe(expected.output);
      }
    },
  );

  it("deduplicates sequential and concurrent redelivery", async () => {
    const backend = createBackend();
    const rawBody = loadFixture();
    expect((await post(backend, rawBody)).status).toBe(202);
    expect((await post(backend, rawBody)).status).toBe(204);
    expect(await storedSpans(backend)).toHaveLength(2);

    const freshBackend = createBackend();
    const results = await Promise.all([post(freshBackend, rawBody), post(freshBackend, rawBody)]);
    expect(results.map(({ status }) => status).sort()).toEqual([202, 204]);
    expect(await storedSpans(freshBackend)).toHaveLength(2);
  });

  it("keeps every differing redelivery as a variant and skips identical ones", async () => {
    const backend = createBackend();
    const storage = memoryStorage();
    const deliver = async (name: string) =>
      (
        await post(
          backend,
          JSON.stringify(envelope({ traceId: "trace", spanId: "span", name })),
          JSON_HEADERS,
          storage.adapter,
        )
      ).status;

    expect(await deliver("first")).toBe(202);
    expect(await deliver("first")).toBe(204);
    for (let index = 0; index < 10; index++) expect(await deliver(`changed ${index}`)).toBe(204);
    expect(await deliver("changed 0")).toBe(204);

    const [row] = await storedSpans(backend);
    if (!row?.payload) throw new Error("expected an archived span");
    expect(row.name).toBe("first");
    expect(row.variantCount).toBe(10);
    expect(row.variants).toHaveLength(8);
    for (const variant of row.variants ?? []) {
      const object = storage.objects.get(variant.key);
      expect(object?.sha256).toBe(variant.sha256);
      expect(variant.sha256).not.toBe(row.payload.sha256);
    }
    expect(storage.objects.size).toBe(11);
  });

  it("does not collapse distinct trace and span ID pairs", async () => {
    const backend = createBackend();
    const body = JSON.stringify(
      envelope(
        { traceId: "a\u0000b", spanId: "c", name: "first" },
        { traceId: "a", spanId: "b\u0000c", name: "second" },
      ),
    );
    expect((await post(backend, body)).status).toBe(202);
    expect(await storedSpans(backend)).toHaveLength(2);
  });

  it("does not invent missing content for a privacy-redacted delivery", async () => {
    const backend = createBackend();
    const storage = memoryStorage();
    const body = JSON.stringify(envelope({ traceId: "private", spanId: "span", name: "redacted" }));
    expect((await post(backend, body, JSON_HEADERS, storage.adapter)).status).toBe(202);
    const [row] = await storedSpans(backend);
    if (!row) throw new Error("expected a span");
    const full = await loadFullSpan(storage.reader, row, { maxBytes: 1024 });
    expect(full).toMatchObject({ traceId: "private", spanId: "span", attributes: [] });
    expect(full).not.toHaveProperty("input");
    expect(full).not.toHaveProperty("output");
  });

  it.each([
    { kind: "request", requestId: "fixture-request" },
    { kind: "run", runId: "fixture-run" },
    { kind: "job", jobId: "fixture-job" },
  ] as const)("pages $kind correlations completely across tied timestamps", async (correlation) => {
    vi.useFakeTimers();
    const backend = createBackend();
    const attributes = [
      { key: "trace.metadata.request_id", value: { stringValue: "fixture-request" } },
      { key: "trace.metadata.run_id", value: { stringValue: "fixture-run" } },
      { key: "trace.metadata.job_id", value: { stringValue: "fixture-job" } },
    ];
    const name = "\u0000\n\t😀".repeat(200);
    for (let batch = 0; batch < 2; batch += 1) {
      vi.setSystemTime(100 + batch);
      const spans = Array.from({ length: 4 }, (_, index) => ({
        traceId: "correlated-trace",
        spanId: `span-${batch * 4 + index}`,
        name,
        attributes,
      }));
      expect((await post(backend, JSON.stringify(envelope(...spans)))).status).toBe(202);
    }
    expect(
      (
        await post(
          backend,
          JSON.stringify(
            envelope({
              traceId: "correlated-trace",
              spanId: "unrelated",
              name: "unrelated",
            }),
          ),
        )
      ).status,
    ).toBe(202);

    const spanIds: string[] = [];
    let cursor: SpanCursor | undefined;
    for (let index = 0; index < 3; index += 1) {
      const result = await backend.query(api.queries.pageCorrelationSummaries, {
        correlation,
        limit: 3,
        cursor,
      });
      expect(result.page).toHaveLength(index === 2 ? 2 : 3);
      expect(result.done).toBe(index === 2);
      expect(new TextEncoder().encode(JSON.stringify(result)).length).toBeLessThanOrEqual(
        32 * 1024,
      );
      for (const span of result.page) {
        expect(span.name).toBe(name.slice(0, 128));
        expect(span.truncatedFields).toContain("name");
        spanIds.push(span.spanId);
      }
      const last = result.page.at(-1);
      expect(result.cursor).toEqual(
        result.done
          ? undefined
          : {
              receivedAt: last?.receivedAt,
              _creationTime: last?.creationTime,
            },
      );
      cursor = result.cursor;
    }
    expect(spanIds).toEqual(Array.from({ length: 8 }, (_, index) => `span-${7 - index}`));
    await expect(
      backend.query(api.queries.pageCorrelationSummaries, { correlation, limit: 8 }),
    ).rejects.toThrow("limit must be a positive integer no greater than 7");
  });

  it("summarizes usage, cost, and content bytes from slim facts", async () => {
    const backend = createBackend();
    const fixture = loadFixture("003-reasoning-luna.json");
    const traceId = "b633b7fd35845c40143e92b76c02a978";
    const byokTraceId = "d633b7fd35845c40143e92b76c02a978";
    const byok = JSON.parse(fixture.replaceAll(traceId, byokTraceId)) as {
      resourceSpans: Array<{
        scopeSpans: Array<{
          spans: Array<{ name: string; attributes?: Array<{ key: string; value: unknown }> }>;
        }>;
      }>;
    };
    const input = '{"messages":[{"role":"user","content":"café"}]}';
    const output = '{"completion":"🌕"}';
    const overrides: Record<string, unknown> = {
      "trace.input": { stringValue: input },
      "span.input": { stringValue: input },
      "gen_ai.prompt": { stringValue: input },
      "trace.output": { stringValue: output },
      "span.output": { stringValue: output },
      "gen_ai.completion": { stringValue: output },
      "gen_ai.usage.output_tokens.reasoning": { intValue: 4 },
      "gen_ai.usage.input_tokens.cached": { intValue: 12 },
    };
    const added = [
      { key: "gen_ai.usage.input_tokens.cache_write", value: { intValue: 759 } },
      { key: "trace.metadata.openrouter_generation.cache_write_tokens", value: { intValue: 759 } },
      { key: "trace.metadata.openrouter_generation.is_byok", value: { boolValue: true } },
      {
        key: "trace.metadata.openrouter_generation.byok_usage_inference",
        value: { doubleValue: 0.0003885 },
      },
    ];
    for (const { scopeSpans } of byok.resourceSpans)
      for (const { spans } of scopeSpans)
        for (const span of spans)
          if (span.name === "fixture-reasoning") {
            span.attributes = [
              ...(span.attributes ?? []).filter(({ key }) => !(key in overrides)),
              ...Object.entries(overrides).map(([key, value]) => ({ key, value })),
              ...added,
            ];
          }
    expect((await post(backend, fixture)).status).toBe(202);
    expect((await post(backend, JSON.stringify(byok))).status).toBe(202);

    const { page } = await backend.query(api.queries.pageCorrelationSummaries, {
      correlation: { kind: "request", requestId: "fixture-req-003" },
    });
    const counted = page.find(
      (span) => span.traceId === byokTraceId && span.name === "fixture-reasoning",
    );
    if (!counted) throw new Error("expected the BYOK generation summary");
    const facts = {
      reasoningTokens: 4,
      cachedInputTokens: 12,
      cacheCreationInputTokens: 759,
      isByok: true,
      byokInferenceUsageCost: 0.0003885,
      inputUtf8Bytes: Buffer.byteLength(input),
      outputUtf8Bytes: Buffer.byteLength(output),
    };
    expect(counted).toMatchObject(facts);
    const row = await backend.query(api.queries.getSpan, {
      spanDocumentId: counted.spanDocumentId,
    });
    expect(row).toMatchObject(facts);
    for (const field of ["input", "output", "attributes"]) {
      expect(row).not.toHaveProperty(field);
      expect(counted).not.toHaveProperty(field);
    }
    const plain = page.find(
      (span) => span.traceId === traceId && span.name === "fixture-reasoning",
    );
    expect(plain).toMatchObject({ reasoningTokens: 0, cachedInputTokens: 0 });
    expect(plain).not.toHaveProperty("cacheCreationInputTokens");
    expect(plain).not.toHaveProperty("isByok");
    expect(plain).not.toHaveProperty("byokInferenceUsageCost");
  });

  it("summarizes a legacy inline row and loads it unchanged without storage", async () => {
    const backend = createBackend();
    const input = '{"prompt":"café"}';
    const output = '{"completion":"🌕"}';
    const spanDocumentId = await backend.run(
      async (ctx) =>
        await ctx.db.insert("spans", {
          traceId: "legacy-trace",
          spanId: "legacy-span",
          requestId: "legacy-request",
          name: "legacy",
          receivedAt: 100,
          input,
          output,
          attributes: [
            { key: "gen_ai.usage.input_tokens.cache_write", valueJson: '{"intValue":7}' },
          ],
          resourceAttributes: [{ key: "resource", valueJson: '{"stringValue":"legacy"}' }],
        }),
    );
    const { page } = await backend.query(api.queries.pageCorrelationSummaries, {
      correlation: { kind: "request", requestId: "legacy-request" },
    });
    expect(page).toEqual([
      expect.objectContaining({
        spanDocumentId,
        inputUtf8Bytes: Buffer.byteLength(input),
        outputUtf8Bytes: Buffer.byteLength(output),
        attributeCount: 1,
        resourceAttributeCount: 1,
        cacheCreationInputTokens: 7,
      }),
    ]);
    const row = await backend.query(api.queries.getSpan, { spanDocumentId });
    if (!row) throw new Error("expected the legacy row");
    expect(row).toMatchObject({ input, output });
    expect(row).not.toHaveProperty("payload");
    const reader = { get: vi.fn(async () => null) };
    expect(await loadFullSpan(reader, row, { maxBytes: 1024 })).toEqual(row);
    expect(reader.get).not.toHaveBeenCalled();
  });
});
