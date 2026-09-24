/// <reference types="vite/client" />
import { readFileSync } from "node:fs";
import { convexTest } from "convex-test";
import { expect, it } from "vitest";
import schema from "../src/component/schema.js";
import { api } from "../src/component/_generated/api.js";
import type { ComponentApi } from "../src/component/_generated/component.js";
import {
  CliproxyObservability,
  handleCliproxyCaptureRequest,
  projectPendingSegments,
  resolveCallBlob,
  type PrivateCaptureStorage,
} from "../src/client.js";
import { sha256, callIdentity, type CaptureObservationV1 } from "../src/capture/index.js";
const events = (
  JSON.parse(
    readFileSync(new URL("../fixtures/real/messages-sse.json", import.meta.url), "utf8"),
  ) as { events: CaptureObservationV1[] }
).events;
const token = "test-capture-token-at-least-24-characters";
const headers = { authorization: `Bearer ${token}`, "content-type": "application/json" };
const encode = new TextEncoder();
async function envelope(observations = events) {
  const first = observations[0]!;
  const content = encode.encode(observations.map((o) => JSON.stringify(o)).join("\n") + "\n");
  return {
    schemaVersion: 1,
    operation: "segment",
    destinationId: first.destinationId,
    instanceId: first.instanceId,
    pluginBootId: first.pluginBootId,
    requestId: first.requestId,
    firstSequence: first.sequence,
    throughSequence: observations.at(-1)!.sequence,
    contentBytes: content.length,
    contentSha256: await sha256(content),
    contentBase64: btoa(String.fromCharCode(...content)),
  };
}
function setup() {
  const backend = convexTest(schema, import.meta.glob("../src/component/**/*.ts"));
  const client = new CliproxyObservability(api as unknown as ComponentApi);
  const blobs = new Map<string, Uint8Array>();
  const storage: PrivateCaptureStorage = {
    put: async (ref, body) => {
      blobs.set(ref.key, body);
    },
    get: async (key) =>
      new ReadableStream({
        start(c) {
          c.enqueue(blobs.get(key)!);
          c.close();
        },
      }),
  };
  const ctx = { runMutation: backend.mutation, runQuery: backend.query } as unknown as Parameters<
    typeof handleCliproxyCaptureRequest
  >[0];
  const options = {
    client,
    storage,
    destinationId: events[0]!.destinationId,
    deploymentId: "dev-deployment",
    environment: "dev" as const,
    instanceIds: [events[0]!.instanceId],
    tokens: [token],
    scheduleProjection: async () => {},
  };
  const post = async (body: unknown, requestHeaders: HeadersInit = headers) =>
    handleCliproxyCaptureRequest(
      ctx,
      new Request("https://example.test/cliproxy/capture/v1", {
        method: "POST",
        headers: requestHeaders,
        body: JSON.stringify(body),
      }),
      options,
    );
  return { backend, client, ctx, blobs, storage, options, post };
}
it.each([
  ["unauthenticated", { "content-type": "application/json" }, 401],
  ["wrong content type", { ...headers, "content-type": "text/plain" }, 415],
  ["oversized", { ...headers, "content-length": String(2 * 1024 * 1024 + 1) }, 413],
] as const)("rejects %s before persistence", async (_name, requestHeaders, status) => {
  const s = setup();
  expect((await s.post(await envelope(), requestHeaders)).status).toBe(status);
  expect(s.blobs.size).toBe(0);
});
it("commits raw independently, deduplicates exact retries, and projects from owned blobs", async () => {
  const s = setup();
  const body = await envelope();
  const first = await s.post(body);
  expect(first.status).toBe(200);
  const firstAck = await first.json();
  expect(firstAck).toMatchObject({
    destinationId: s.options.destinationId,
    deploymentId: s.options.deploymentId,
    callId: await callIdentity(events[0]!),
    duplicate: false,
    rawCommitted: true,
    projectionCommitted: false,
  });
  const duplicate = await s.post(body);
  const duplicateAck = await duplicate.json();
  expect(duplicateAck).toMatchObject({
    destinationId: s.options.destinationId,
    deploymentId: s.options.deploymentId,
    identity: firstAck.identity,
    digest: firstAck.digest,
    callId: firstAck.callId,
    duplicate: true,
    rawCommitted: true,
    projectionCommitted: false,
  });
  const callId = await callIdentity(events[0]!);
  const args = { destinationId: events[0]!.destinationId, callId };
  const raw = await s.backend.query(api.queries.getCall, args);
  expect(raw?.persistedThroughSequence).toBe(events.length);
  expect(raw?.projectedThroughSequence).toBe(0);
  expect(await projectPendingSegments(s.ctx, { ...s.options, callId })).toMatchObject({
    progressed: true,
    more: false,
  });
  const projected = await s.backend.query(api.queries.getCall, args);
  expect(JSON.parse(projected!.summaryJson)).toMatchObject({
    state: "succeeded",
    capture: { raw: "complete" },
  });
  const page = await s.backend.query(api.queries.pageRecentSummaries, {
    destinationId: args.destinationId,
  });
  expect(page.calls).toHaveLength(1);
  expect(page.done).toBe(true);
  expect(await s.backend.run((ctx) => ctx.db.query("receipts").collect())).toHaveLength(1);
});
it("persists unmodified hook NDJSON and projects line callbacks across separate segments", async () => {
  const s = setup();
  const terminal = {
    type: "response.completed",
    response: {
      object: "response",
      id: "synthetic",
      model: "reported",
      status: "completed",
      output: [],
      usage: { input_tokens: 7, output_tokens: 3, total_tokens: 10 },
    },
  };
  const bodies = [
    ' { "stream":true, "token":"content", "image":{"url":"data:image/png;base64,aGVsbG8="} } ',
    '{"secret":"executed tool argument"}',
    "event: response.completed",
    "data: " + JSON.stringify(terminal),
    "",
  ];
  const observations: CaptureObservationV1[] = [];
  for (const [index, body] of bodies.entries()) {
    const bytes = encode.encode(body);
    const o: CaptureObservationV1 = {
      ...events[0]!,
      redactionVersion: undefined,
      capturePolicy: "hook-body-v1",
      pluginVersion: "0.2.0",
      route: "POST /v1/responses",
      sourceFormat: "openai-response",
      sequence: index + 1,
      kind:
        index === 0
          ? "request"
          : index === 1
            ? "request_after_auth"
            : index === 4
              ? "completion"
              : "stream_chunk",
      body: btoa(String.fromCharCode(...bytes)),
      contentBytes: bytes.length,
      contentSha256: await sha256(bytes),
      ...(index === 1
        ? {
            executionModel: "selected",
            executionProtocol: "openai-response",
            selectedAuthId: "auth-id",
            selectedAuthIndex: "auth-index",
          }
        : {}),
      ...(index === 2 || index === 3
        ? { bodyFraming: "stock_hook_chunk", stockChunkIndex: index - 2 }
        : {}),
      ...(index === 4 ? { completionOutcome: "succeeded" } : {}),
    };
    observations.push(o);
    const segment = await envelope([o]);
    expect((await s.post(segment)).status).toBe(200);
    const callId = await callIdentity(o);
    expect(await projectPendingSegments(s.ctx, { ...s.options, callId })).toMatchObject({
      progressed: true,
    });
    expect(
      [...s.blobs.values()].some(
        (raw) => new TextDecoder().decode(raw) === JSON.stringify(o) + "\n",
      ),
    ).toBe(true);
  }
  const callId = await callIdentity(observations[0]!);
  const stored = await s.backend.query(api.queries.getCall, {
    destinationId: s.options.destinationId,
    callId,
  });
  expect(JSON.parse(stored!.summaryJson)).toMatchObject({
    state: "succeeded",
    totalTokens: 10,
    executionModel: "selected",
    selectedAuthId: "auth-id",
    capture: {
      raw: "complete",
      projection: "complete",
      usage: "complete",
      capturePolicy: "hook-body-v1",
    },
  });
});
it("rejects changed content under the same sequence identity", async () => {
  const s = setup();
  expect((await s.post(await envelope())).status).toBe(200);
  const changed = events.map((o) => ({ ...o, requestedModel: "changed" }));
  expect((await s.post(await envelope(changed))).status).toBe(409);
});
it("isolates destinations and rejects digest tampering", async () => {
  const s = setup();
  const body = await envelope();
  expect((await s.post({ ...body, destinationId: "production" })).status).toBe(400);
  expect((await s.post({ ...body, contentSha256: "0".repeat(64) })).status).toBe(400);
  expect(s.blobs.size).toBe(0);
});
it("waits for missing segments and resumes without duplicate projections", async () => {
  const s = setup();
  const callId = await callIdentity(events[0]!);
  expect((await s.post(await envelope(events.slice(3)))).status).toBe(200);
  expect(await projectPendingSegments(s.ctx, { ...s.options, callId })).toMatchObject({
    waitingForGap: true,
  });
  expect((await s.post(await envelope(events.slice(0, 3)))).status).toBe(200);
  expect(await projectPendingSegments(s.ctx, { ...s.options, callId })).toMatchObject({
    progressed: true,
  });
  expect(await projectPendingSegments(s.ctx, { ...s.options, callId })).toMatchObject({
    progressed: false,
  });
});
it("requires manifest ownership and verifies private blob integrity", async () => {
  const s = setup();
  await s.post(await envelope());
  const callId = await callIdentity(events[0]!);
  const page = await s.backend.query(api.queries.pageEventSegments, {
    destinationId: events[0]!.destinationId,
    callId,
  });
  const ref = page.segments[0]!.reference;
  await expect(resolveCallBlob(s.storage, [], ref)).rejects.toThrow("unowned");
  s.blobs.set(ref.key, encode.encode("tampered"));
  await expect(resolveCallBlob(s.storage, [ref], ref)).rejects.toThrow("integrity");
});
it("retains raw admission when scheduling fails, then recovers on an exact retry", async () => {
  const s = setup();
  s.options.scheduleProjection = async () => {
    throw new Error("scheduler unavailable");
  };
  const body = await envelope();
  expect((await s.post(body)).status).toBe(503);
  expect(await s.backend.run((ctx) => ctx.db.query("receipts").collect())).toHaveLength(1);
  s.options.scheduleProjection = async () => {};
  expect((await s.post(body)).status).toBe(200);
  expect(await s.backend.run((ctx) => ctx.db.query("receipts").collect())).toHaveLength(1);
});
it("paginates equal-timestamp summaries without duplicate or missing calls", async () => {
  const s = setup();
  for (let i = 0; i < 9; i++)
    expect(
      (await s.post(await envelope(events.map((o) => ({ ...o, requestId: `execution-${i}` })))))
        .status,
    ).toBe(200);
  await s.backend.run(async (ctx) => {
    for (const row of await ctx.db.query("calls").collect())
      await ctx.db.patch("calls", row._id, { receivedAt: 100 });
  });
  const ids = new Set<string>();
  let cursor: { receivedAt: number; creationTime: number } | undefined;
  for (let i = 0; i < 5; i++) {
    const page = await s.backend.query(api.queries.pageRecentSummaries, {
      destinationId: events[0]!.destinationId,
      cursor,
    });
    expect(encode.encode(JSON.stringify(page)).length).toBeLessThan(32 * 1024);
    for (const raw of page.calls) {
      const call = raw as { callId: string };
      expect(ids.has(call.callId)).toBe(false);
      ids.add(call.callId);
    }
    if (page.done) break;
    expect(page.cursor).not.toEqual(cursor);
    cursor = page.cursor ?? undefined;
  }
  expect(ids.size).toBe(9);
});
it("records projection failure without losing the raw receipt and clears it on recovery", async () => {
  const s = setup();
  await s.post(await envelope());
  const callId = await callIdentity(events[0]!);
  const key = [...s.blobs.keys()][0]!;
  const original = s.blobs.get(key)!;
  s.blobs.set(key, encode.encode("broken"));
  await expect(projectPendingSegments(s.ctx, { ...s.options, callId })).rejects.toThrow();
  const args = { destinationId: events[0]!.destinationId, callId };
  expect(await s.backend.query(api.queries.getCall, args)).toMatchObject({
    persistedThroughSequence: events.length,
    projectedThroughSequence: 0,
    projectionFailure: { reason: "projection_processing_failed" },
  });
  s.blobs.set(key, original);
  await projectPendingSegments(s.ctx, { ...s.options, callId });
  expect((await s.backend.query(api.queries.getCall, args))?.projectionFailure).toBeUndefined();
});
it("indexes exact request correlations when an earlier segment arrives last", async () => {
  const s = setup();
  await s.post(await envelope(events.slice(3).map((o) => ({ ...o, correlation: undefined }))));
  await s.post(
    await envelope(
      events.slice(0, 3).map((o) => ({ ...o, correlation: { runId: "late-request-run" } })),
    ),
  );
  const page = await s.backend.query(api.queries.pageRecentSummaries, {
    destinationId: events[0]!.destinationId,
    correlation: { kind: "runId", value: "late-request-run" },
  });
  expect(page.calls).toHaveLength(1);
});
it("stores large pending SSE framing privately across projection revisions", async () => {
  const s = setup();
  const index = events.findIndex((o) => o.kind === "stream_chunk" && o.contentBytes > 0);
  const original = events[index]!;
  const body = encode.encode(
    atob(original.body!).replace("{", '{"padding":"' + "x".repeat(30000) + '",'),
  );
  const parts = [body.slice(0, 20000), body.slice(20000)];
  const split = await Promise.all(
    parts.map(async (bytes) => ({
      ...original,
      body: btoa(String.fromCharCode(...bytes)),
      contentBytes: bytes.length,
      contentSha256: await sha256(bytes),
    })),
  );
  const all = [...events.slice(0, index), ...split, ...events.slice(index + 1)].map((o, i) => ({
    ...o,
    sequence: i + 1,
  }));
  const callId = await callIdentity(all[0]!);
  expect((await s.post(await envelope(all.slice(0, index + 1)))).status).toBe(200);
  await projectPendingSegments(s.ctx, { ...s.options, callId });
  const state = await s.backend.query(api.queries.getProcessingState, {
    destinationId: events[0]!.destinationId,
    callId,
  });
  expect(state!.checkpointJson.length).toBeLessThan(4096);
  expect(JSON.parse(state!.checkpointJson).sseBlob).toBeDefined();
  expect((await s.post(await envelope(all.slice(index + 1)))).status).toBe(200);
  await projectPendingSegments(s.ctx, { ...s.options, callId });
  const result = await s.backend.query(api.queries.getCall, {
    destinationId: events[0]!.destinationId,
    callId,
  });
  expect(JSON.parse(result!.summaryJson).capture.usage).toBe("complete");
});
it("authenticates content-free health and retains boot loss counters without regressing on delayed data", async () => {
  const s = setup();
  const first = events[0]!;
  const handshake = await s.post({
    schemaVersion: 1,
    operation: "health",
    destinationId: first.destinationId,
    instanceId: first.instanceId,
  });
  expect(await handshake.json()).toMatchObject({ ready: true, deploymentId: "dev-deployment" });
  expect(s.blobs.size).toBe(0);
  await s.post(await envelope(events.slice(0, 2)));
  const time = new Date(Date.parse(first.observedAt) + 60000).toISOString();
  const health = {
    schemaVersion: 1,
    operation: "health_record",
    destinationId: first.destinationId,
    instanceId: first.instanceId,
    pluginBootId: "new-boot",
    startedAt: time,
    observedAt: time,
    observationsTotal: 10,
    droppedObservationsTotal: 3,
    lostControlObservationsTotal: 1,
    scopeConflictsTotal: 0,
    expiredScopesTotal: 0,
    activeCalls: 1,
    precommitCoverage: "unknown_before_local_commit",
  };
  expect((await s.post(health)).status).toBe(200);
  await s.post(await envelope(events.slice(2, 3)));
  const coverage = await s.backend.query(api.queries.getCaptureCoverage, {
    destinationId: first.destinationId,
  });
  expect(coverage.sources[0]?.pluginBootId).toBe("new-boot");
  expect(JSON.parse(coverage.sources[0]!.healthJson!)).toMatchObject({
    counterScope: "plugin_boot",
    lostControlObservationsTotal: 1,
  });
  const call = await s.backend.query(api.queries.getCall, {
    destinationId: first.destinationId,
    callId: await callIdentity(first),
  });
  expect(JSON.parse(call!.summaryJson)).toMatchObject({
    state: "unknown",
    capture: { gaps: ["completion_unobserved_prior_boot"] },
  });
});
