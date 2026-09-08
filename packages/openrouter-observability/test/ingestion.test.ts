/// <reference types="vite/client" />

import { convexTest } from "convex-test";
import { describe, expect, it } from "vitest";
import {
  findTraceBlobRefs,
  resolveTraceBlob,
  TRACE_BLOB_MARKER,
  TraceContentInvalidError,
  type TraceBlobPut,
  type TraceBlobRef,
} from "../src/blobContent.js";
import type { ComponentApi } from "../src/component/_generated/component.js";
import { api } from "../src/component/_generated/api.js";
import schema from "../src/component/schema.js";
import { decodeOpenRouterInput, decodeOpenRouterOutput } from "../src/content.js";
import { handleOpenRouterTraceRequest } from "../src/ingestion.js";

const modules = import.meta.glob("../src/component/**/*.ts");
const TOKEN = "test-token";

function backend() {
  return convexTest(schema, modules);
}

function inputAttribute(input: unknown) {
  return { key: "trace.input", value: { stringValue: JSON.stringify(input) } };
}

function delivery(input: unknown, extraAttributes: unknown[] = []) {
  return {
    resourceSpans: [
      {
        scopeSpans: [
          {
            spans: [
              {
                traceId: "trace-multimodal",
                spanId: "span-multimodal",
                name: "generation",
                attributes: [inputAttribute(input), ...extraAttributes],
              },
            ],
          },
        ],
      },
    ],
  };
}

function deliveryWithRawInput(rawInput: string) {
  return {
    resourceSpans: [
      {
        scopeSpans: [
          {
            spans: [
              {
                traceId: "trace-exact-json",
                spanId: "span-exact-json",
                name: "generation",
                attributes: [{ key: "trace.input", value: { stringValue: rawInput } }],
              },
            ],
          },
        ],
      },
    ],
  };
}

function memoryStorage() {
  const objects = new Map<string, TraceBlobPut>();
  return {
    objects,
    adapter: {
      put: async (object: TraceBlobPut) => {
        objects.set(object.key, object);
      },
    },
    reader: {
      get: async (reference: TraceBlobRef, options: { maxBytes: number }) => {
        const object = objects.get(reference.key);
        if (!object || object.bytes.byteLength > options.maxBytes) return null;
        return { bytes: object.bytes, contentType: object.contentType };
      },
    },
  };
}

async function ingest(
  testBackend: ReturnType<typeof backend>,
  rawBody: string,
  storage: ReturnType<typeof memoryStorage>["adapter"],
) {
  return await handleOpenRouterTraceRequest(
    {
      runMutation: async (_reference, args) =>
        await testBackend.mutation(api.ingest.admitPrepared, args as never),
    },
    new Request("https://example.test/openrouter/traces", {
      method: "POST",
      headers: { authorization: `Bearer ${TOKEN}`, "content-type": "application/json" },
      body: rawBody,
    }),
    {
      component: api as unknown as ComponentApi,
      bearerToken: TOKEN,
      blobPrefix: "observability/openrouter",
      blobStorage: storage,
    },
  );
}

async function onlySpan(testBackend: ReturnType<typeof backend>) {
  const spans = await testBackend.run(async (ctx) => await ctx.db.query("spans").collect());
  expect(spans).toHaveLength(1);
  const span = spans[0];
  if (!span) throw new Error("expected one span");
  return span;
}

describe("host-owned blob ingestion", () => {
  it("accepts a body above the former 900 KiB limit and preserves mixed content order", async () => {
    const testBackend = backend();
    const storage = memoryStorage();
    const imageBytes = Uint8Array.from([137, 80, 78, 71, 13, 10, 26, 10, 1, 2, 3, 4]);
    let imageBinary = "";
    for (const byte of imageBytes) imageBinary += String.fromCharCode(byte);
    const imageDataUrl = `data:image/png;base64,${btoa(imageBinary)}`;
    const largeText = "large trace text ".repeat(70_000);
    const reservedShape = {
      [TRACE_BLOB_MARKER]: {
        kind: "trace_blob",
        key: "attacker-chosen-key",
        sha256: "0".repeat(64),
        byteLength: 1,
        contentType: "image/png",
        encoding: "binary",
      },
    };
    const input = {
      messages: [
        {
          role: "user",
          content: [
            { type: "text", text: "before" },
            { type: "image_url", image_url: { url: imageDataUrl, detail: "high" } },
            { type: "future_part", payload: reservedShape },
            { type: "future_image", b64_json: "AQIDBA==" },
            { type: "image_url", image_url: { url: "redacted" } },
            { type: "image_url", image_url: { url: "https://example.test/original.png" } },
            { type: "text", text: largeText },
          ],
        },
      ],
    };
    const rawBody = JSON.stringify(
      delivery(input, [
        { key: "unknown.large", value: { stringValue: "opaque".repeat(20_000) } },
        {
          key: "trace.output",
          value: { stringValue: JSON.stringify({ completion: largeText }) },
        },
      ]),
    );
    expect(new TextEncoder().encode(rawBody).byteLength).toBeGreaterThan(900 * 1024);

    expect((await ingest(testBackend, rawBody, storage.adapter)).status).toBe(202);
    const span = await onlySpan(testBackend);
    const persisted = JSON.stringify(span);
    expect(persisted).not.toContain("data:image/png;base64");
    expect(persisted).not.toContain(btoa(imageBinary));
    expect(persisted).not.toContain(largeText.slice(0, 1_000));
    expect(persisted).not.toContain("attacker-chosen-key");
    expect(decodeOpenRouterOutput(span.output)).toMatchObject({
      kind: "decoded",
      content: { text: { kind: "blob_text" } },
    });

    const decoded = decodeOpenRouterInput(span.input);
    expect(decoded.kind).toBe("decoded");
    if (decoded.kind !== "decoded") throw new Error("input must decode");
    const content = decoded.content.messages[0]?.content;
    expect(content).toMatchObject({
      kind: "parts",
      parts: [
        { type: "text", text: "before" },
        { type: "image_url", image: { kind: "blob", detail: "high" } },
        { type: "opaque" },
        { type: "opaque" },
        { type: "image_url", image: { kind: "unavailable", value: "redacted" } },
        {
          type: "image_url",
          image: { kind: "external_url", url: "https://example.test/original.png" },
        },
        { type: "text", text: { kind: "blob_text" } },
      ],
    });

    const references = findTraceBlobRefs(span);
    expect(references.length).toBeGreaterThanOrEqual(4);
    const imageReference = references.find((reference) => reference.contentType === "image/png");
    if (!imageReference) throw new Error("expected image reference");
    const resolved = await resolveTraceBlob(storage.reader, span, imageReference, {
      maxBytes: 1024,
    });
    expect(resolved?.bytes).toEqual(imageBytes);

    const forgedReference: TraceBlobRef = {
      kind: "trace_blob",
      key: "attacker-chosen-key",
      sha256: "0".repeat(64),
      byteLength: 1,
      contentType: "image/png",
      encoding: "binary",
    };
    await expect(
      resolveTraceBlob(storage.reader, span, forgedReference, { maxBytes: 1024 }),
    ).rejects.toThrow("does not belong");
  });

  it("writes no span on storage failure and reuses deterministic keys on retry", async () => {
    const testBackend = backend();
    const storage = memoryStorage();
    const body = JSON.stringify(
      delivery({
        messages: [
          {
            role: "user",
            content: [{ type: "image_url", image_url: { url: "data:image/png;base64,AQIDBA==" } }],
          },
        ],
      }),
    );
    const attemptedKeys: string[] = [];
    expect(
      (
        await ingest(testBackend, body, {
          put: async (object) => {
            attemptedKeys.push(object.key);
            throw new Error("simulated storage outage");
          },
        })
      ).status,
    ).toBe(503);
    expect(
      await testBackend.run(async (ctx) => await ctx.db.query("spans").collect()),
    ).toHaveLength(0);

    expect((await ingest(testBackend, body, storage.adapter)).status).toBe(202);
    expect([...storage.objects.keys()]).toEqual(attemptedKeys);
    expect((await ingest(testBackend, body, storage.adapter)).status).toBe(204);
    expect([...storage.objects.keys()]).toEqual(attemptedKeys);
  });

  it("rejects malformed explicit base64 before storage or database mutation", async () => {
    const testBackend = backend();
    const storage = memoryStorage();
    const body = JSON.stringify(
      delivery({
        messages: [
          {
            role: "user",
            content: [
              { type: "image_url", image_url: { url: "data:image/png;base64,not valid!" } },
            ],
          },
        ],
      }),
    );
    expect((await ingest(testBackend, body, storage.adapter)).status).toBe(400);
    expect(storage.objects.size).toBe(0);
    expect(
      await testBackend.run(async (ctx) => await ctx.db.query("spans").collect()),
    ).toHaveLength(0);
  });

  it("keeps ordinary text that begins with data", async () => {
    const testBackend = backend();
    const storage = memoryStorage();
    const body = JSON.stringify(
      delivery({ messages: [{ role: "user", content: "data: inspect" }] }),
    );
    expect((await ingest(testBackend, body, storage.adapter)).status).toBe(202);
    expect((await onlySpan(testBackend)).input).toContain("data: inspect");
    expect(storage.objects.size).toBe(0);
  });

  it("externalizes JSON with unsafe number lexemes without rewriting its bytes", async () => {
    const testBackend = backend();
    const storage = memoryStorage();
    const exactInput =
      '{"first":9007199254740993,"quoted":"9007199254740993","escaped":"\\"9007199254740993","dup":1,"dup":2,"hugeExponent":1e400,"last":true}';
    expect(
      (await ingest(testBackend, JSON.stringify(deliveryWithRawInput(exactInput)), storage.adapter))
        .status,
    ).toBe(202);

    const span = await onlySpan(testBackend);
    const references = findTraceBlobRefs(span);
    expect(references).toHaveLength(1);
    const reference = references[0];
    if (!reference) throw new Error("expected exact JSON reference");
    expect(reference.contentType).toBe("application/json");
    const resolved = await resolveTraceBlob(storage.reader, span, reference, { maxBytes: 2048 });
    expect(new TextDecoder().decode(resolved?.bytes)).toBe(exactInput);
  });

  it("rejects direct prepared writes that still contain externalizable content", async () => {
    const testBackend = backend();
    const parsed = {
      traceId: "trace",
      spanId: "span",
      name: "unsafe",
      input: JSON.stringify({ image: "data:image/png;base64,AQIDBA==" }),
      attributes: [],
      resourceAttributes: [],
    };
    await expect(
      testBackend.mutation(api.ingest.admitPrepared, { spans: [parsed] }),
    ).resolves.toMatchObject({ kind: "rejected", status: 413 });
    expect(
      await testBackend.run(async (ctx) => await ctx.db.query("spans").collect()),
    ).toHaveLength(0);
  });

  it("fails bounded retrieval on oversize, wrong membership, length, and digest", async () => {
    const reference: TraceBlobRef = {
      kind: "trace_blob",
      key: "observability/openrouter/key",
      sha256: "0".repeat(64),
      byteLength: 4,
      contentType: "application/octet-stream",
      encoding: "binary",
    };
    const span = { input: JSON.stringify({ [TRACE_BLOB_MARKER]: reference }) };
    const reader = {
      get: async () => ({ bytes: Uint8Array.from([1, 2, 3, 4]) }),
    };
    await expect(resolveTraceBlob(reader, span, reference, { maxBytes: 3 })).rejects.toThrow(
      "byte limit",
    );
    await expect(resolveTraceBlob(reader, {}, reference, { maxBytes: 4 })).rejects.toThrow(
      "does not belong",
    );
    await expect(resolveTraceBlob(reader, span, reference, { maxBytes: 4 })).rejects.toBeInstanceOf(
      TraceContentInvalidError,
    );
  });
});
