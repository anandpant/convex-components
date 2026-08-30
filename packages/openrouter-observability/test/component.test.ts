/// <reference types="vite/client" />

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { convexTest } from "convex-test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api, internal } from "../src/component/_generated/api.js";
import { parseOpenRouterOtlpDelivery } from "../src/component/parser.js";
import schema from "../src/component/schema.js";

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
) {
  return await backend.fetch("/traces", { method: "POST", headers, body });
}

function envelope(span: Record<string, unknown>) {
  return { resourceSpans: [{ scopeSpans: [{ spans: [span] }] }] };
}

describe("OpenRouter observability component", () => {
  beforeEach(() => {
    process.env.WEBHOOK_TOKEN = TOKEN;
    delete process.env.RETENTION_DAYS;
  });

  afterEach(() => {
    vi.useRealTimers();
    delete process.env.WEBHOOK_TOKEN;
    delete process.env.RETENTION_DAYS;
  });

  it("serves its health route", async () => {
    const response = await createBackend().fetch("/health");
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("ok");
  });

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
    delete process.env.WEBHOOK_TOKEN;
    const backend = createBackend();
    const response = await post(backend, loadFixture());
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
    const oversized = `"${"a".repeat(900 * 1024)}"`;
    expect((await post(backend, oversized)).status).toBe(413);
    const tooManySpans = {
      resourceSpans: [
        {
          scopeSpans: [
            {
              spans: Array.from({ length: 65 }, (_, index) => ({
                traceId: "trace",
                spanId: `span-${index}`,
                name: "bounded",
              })),
            },
          ],
        },
      ],
    };
    expect((await post(backend, JSON.stringify(tooManySpans))).status).toBe(413);
    const expandedResourceAttributes = {
      resourceSpans: [
        {
          resource: {
            attributes: [{ key: "large.resource", value: { stringValue: "x".repeat(200 * 1024) } }],
          },
          scopeSpans: [
            {
              spans: Array.from({ length: 64 }, (_, index) => ({
                traceId: "trace",
                spanId: `span-${index}`,
                name: "expanded resource metadata",
              })),
            },
          ],
        },
      ],
    };
    expect((await post(backend, JSON.stringify(expandedResourceAttributes))).status).toBe(413);
    expect(await storedSpans(backend)).toHaveLength(0);
  });

  it("accepts a duplicate-only expanded delivery without rewriting spans", async () => {
    const backend = createBackend();
    const expandedDelivery = {
      resourceSpans: [
        {
          resource: {
            attributes: [{ key: "large.resource", value: { stringValue: "x".repeat(200 * 1024) } }],
          },
          scopeSpans: [
            {
              spans: Array.from({ length: 64 }, (_, index) => ({
                traceId: "trace",
                spanId: `span-${index}`,
                name: "expanded resource metadata",
              })),
            },
          ],
        },
      ],
    };
    const parsed = parseOpenRouterOtlpDelivery(expandedDelivery);
    await backend.run(async (ctx) => {
      await ctx.db.insert("migrationState", {
        name: "prismantix-spans-v1",
        startedAt: Date.now(),
        completedAt: Date.now(),
      });
      for (const span of parsed) {
        const spanDocumentId = await ctx.db.insert("spans", { ...span, receivedAt: 0 });
        await ctx.db.insert("spanKeys", {
          traceId: span.traceId,
          spanId: span.spanId,
          spanDocumentId,
        });
      }
    });

    expect((await post(backend, JSON.stringify(expandedDelivery))).status).toBe(204);
    expect(await storedSpans(backend)).toHaveLength(64);
  });

  it("accepts authenticated Test Connection with no write", async () => {
    const backend = createBackend();
    const response = await post(backend, JSON.stringify({ resourceSpans: [] }), {
      ...JSON_HEADERS,
      "x-test-connection": "true",
    });
    expect(response.status).toBe(204);
    expect(await storedSpans(backend)).toHaveLength(0);
  });

  it.each(["POST", "PUT"] as const)("atomically ingests a real fixture over %s", async (method) => {
    const backend = createBackend();
    const response = await backend.fetch("/traces", {
      method,
      headers: JSON_HEADERS,
      body: loadFixture(),
    });
    expect(response.status).toBe(202);
    expect(await storedSpans(backend)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ requestId: "fixture-req-001", spanType: "generation" }),
        expect.objectContaining({ parentSpanId: "a44772891b0e7515", spanType: "span" }),
      ]),
    );
  });

  it("deduplicates sequential and concurrent redelivery", async () => {
    const backend = createBackend();
    const rawBody = loadFixture();
    expect((await post(backend, rawBody)).status).toBe(202);
    expect((await post(backend, rawBody)).status).toBe(204);

    const freshBackend = createBackend();
    const results = await Promise.all([
      freshBackend.mutation(internal.ingest.admit, { rawBody, isTestConnection: false }),
      freshBackend.mutation(internal.ingest.admit, { rawBody, isTestConnection: false }),
    ]);
    expect(
      results.reduce(
        (count, result) => count + (result.kind === "accepted" ? result.admitted : 0),
        0,
      ),
    ).toBe(2);
    expect(await storedSpans(freshBackend)).toHaveLength(2);
  });

  it("does not collapse distinct trace and span ID pairs", async () => {
    const backend = createBackend();
    const body = JSON.stringify({
      resourceSpans: [
        {
          scopeSpans: [
            {
              spans: [
                { traceId: "a\u0000b", spanId: "c", name: "first" },
                { traceId: "a", spanId: "b\u0000c", name: "second" },
              ],
            },
          ],
        },
      ],
    });

    expect((await post(backend, body)).status).toBe(202);
    expect(await storedSpans(backend)).toHaveLength(2);
  });

  it("does not invent missing content for a privacy-redacted delivery", async () => {
    const backend = createBackend();
    const body = JSON.stringify(envelope({ traceId: "private", spanId: "span", name: "redacted" }));
    expect((await post(backend, body)).status).toBe(202);
    const spans = await storedSpans(backend);
    expect(spans).toEqual([expect.objectContaining({ traceId: "private", spanId: "span" })]);
    expect(spans[0]).not.toHaveProperty("input");
    expect(spans[0]).not.toHaveProperty("output");
  });

  it("supports every bounded correlation query and exclusive cursors", async () => {
    const backend = createBackend();
    const root = parseOpenRouterOtlpDelivery(JSON.parse(loadFixture()))[0];
    if (!root) throw new Error("fixture must contain a root span");
    await backend.run(async (ctx) => {
      for (let index = 0; index < 3; index += 1) {
        await ctx.db.insert("spans", {
          ...root,
          traceId: "correlated-trace",
          spanId: `span-${index}`,
          entityType: "record",
          entityId: "record-123",
          receivedAt: 100,
        });
      }
    });

    expect(await backend.query(api.queries.getTrace, { traceId: "correlated-trace" })).toHaveLength(
      3,
    );
    expect(
      await backend.query(api.queries.getSpan, {
        traceId: "correlated-trace",
        spanId: "span-0",
      }),
    ).toMatchObject({ spanId: "span-0" });

    const firstPage = await backend.query(api.queries.listBySession, {
      sessionId: root.sessionId ?? "",
      limit: 2,
    });
    const last = firstPage.at(-1);
    if (!last) throw new Error("first page must contain a cursor row");
    const secondPage = await backend.query(api.queries.listBySession, {
      sessionId: root.sessionId ?? "",
      limit: 2,
      before: { receivedAt: last.receivedAt, _creationTime: last._creationTime },
    });
    expect([...firstPage, ...secondPage]).toHaveLength(3);
    expect(await backend.query(api.queries.listByUser, { userId: root.userId ?? "" })).toHaveLength(
      3,
    );
    expect(
      await backend.query(api.queries.listByRequest, { requestId: root.requestId ?? "" }),
    ).toHaveLength(3);
    expect(
      await backend.query(api.queries.listByEntity, {
        entityType: "record",
        entityId: "record-123",
      }),
    ).toHaveLength(3);
    expect(await backend.query(api.queries.listRecent, {})).toHaveLength(3);
    await expect(backend.query(api.queries.listRecent, { limit: 9 })).rejects.toThrow(
      "limit must be a positive integer no greater than 8",
    );
  });

  it("deletes expired rows across bounded retention batches", async () => {
    vi.useFakeTimers();
    const backend = createBackend();
    await backend.run(async (ctx) => {
      for (let index = 0; index < 257; index += 1) {
        const spanDocumentId = await ctx.db.insert("spans", {
          traceId: `expired-trace-${index}`,
          spanId: `expired-span-${index}`,
          name: "expired",
          attributes: [],
          resourceAttributes: [],
          receivedAt: 1,
        });
        await ctx.db.insert("spanKeys", {
          traceId: `expired-trace-${index}`,
          spanId: `expired-span-${index}`,
          spanDocumentId,
        });
      }
      const spanDocumentId = await ctx.db.insert("spans", {
        traceId: "current-trace",
        spanId: "current-span",
        name: "current",
        attributes: [],
        resourceAttributes: [],
        receivedAt: 10_000,
      });
      await ctx.db.insert("spanKeys", {
        traceId: "current-trace",
        spanId: "current-span",
        spanDocumentId,
      });
    });

    expect(await backend.mutation(internal.retention.deleteExpired, { cutoff: 5_000 })).toEqual({
      deletedSpans: 8,
    });
    await backend.finishAllScheduledFunctions(() => vi.runAllTimers());
    expect((await storedSpans(backend)).map((span) => span.spanId)).toEqual(["current-span"]);
    expect(await backend.run(async (ctx) => await ctx.db.query("spanKeys").collect())).toHaveLength(
      1,
    );
  });

  it("migrates legacy spans and removes legacy raw deliveries in bounded batches", async () => {
    vi.useFakeTimers();
    const backend = createBackend();
    await backend.run(async (ctx) => {
      const stale = Date.now() - 6 * 60 * 1000;
      await ctx.db.insert("migrationState", {
        name: "prismantix-spans-v1",
        startedAt: stale,
        lastScheduledAt: stale,
      });
      for (let index = 0; index < 3; index += 1) {
        await ctx.db.insert("spans", {
          traceId: `legacy-trace-${index}`,
          spanId: `legacy-span-${index}`,
          name: "legacy",
          attributes: [],
          receivedAt: index,
        });
        await ctx.db.insert("deliveries", {
          byteLength: 2,
          rawBody: "{}",
          receivedAt: index,
        });
      }
    });

    const duplicateResponse = await post(
      backend,
      JSON.stringify(
        envelope({ traceId: "legacy-trace-0", spanId: "legacy-span-0", name: "legacy" }),
      ),
    );
    expect(duplicateResponse.status).toBe(503);
    expect(await storedSpans(backend)).toHaveLength(3);

    await backend.finishAllScheduledFunctions(() => vi.runAllTimers());
    expect(
      await post(
        backend,
        JSON.stringify(
          envelope({ traceId: "legacy-trace-0", spanId: "legacy-span-0", name: "legacy" }),
        ),
      ),
    ).toHaveProperty("status", 204);
    expect(await backend.mutation(internal.retention.deleteLegacyDeliveries, {})).toEqual({
      deletedDeliveries: 2,
    });
    await backend.finishAllScheduledFunctions(() => vi.runAllTimers());

    expect(await storedSpans(backend)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ resourceAttributes: [] }),
        expect.objectContaining({ resourceAttributes: [] }),
        expect.objectContaining({ resourceAttributes: [] }),
      ]),
    );
    expect(await backend.run(async (ctx) => await ctx.db.query("spanKeys").collect())).toHaveLength(
      3,
    );
    expect(
      await backend.run(async (ctx) => await ctx.db.query("deliveries").collect()),
    ).toHaveLength(0);
    expect(
      await backend.run(async (ctx) =>
        (await ctx.db.query("migrationState").collect()).map(({ name }) => name).sort(),
      ),
    ).toEqual(["prismantix-deliveries-v1", "prismantix-spans-v1"]);

    await backend.run(async (ctx) => {
      await ctx.db.insert("spans", {
        traceId: "post-migration-trace",
        spanId: "post-migration-span",
        name: "post-migration",
        attributes: [],
        receivedAt: Date.now(),
      });
    });
    await backend.mutation(internal.retention.start, {});
    await backend.finishAllScheduledFunctions(() => vi.runAllTimers());
    const postMigrationSpans = await storedSpans(backend);
    expect(postMigrationSpans).toEqual([
      expect.objectContaining({ spanId: "post-migration-span" }),
    ]);
    expect(postMigrationSpans[0]).not.toHaveProperty("resourceAttributes");
  });
});
