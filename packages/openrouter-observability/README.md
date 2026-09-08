# OpenRouter observability for Convex

`@shpitdev/convex-openrouter-observability` receives OpenRouter Broadcast OTLP JSON inside your existing Convex deployment. It stores typed spans in component-owned tables so your host Convex functions can query traces by user, session, request, or application entity.

The component is specific to OpenRouter Broadcast traces. It is not a general OTLP collector.

## Install

```sh
pnpm add @shpitdev/convex-openrouter-observability
```

Register the component and pass the webhook token from the host deployment:

```ts
// convex/convex.config.ts
import openrouterObservability from "@shpitdev/convex-openrouter-observability/convex.config.js";
import { defineApp } from "convex/server";
import { v } from "convex/values";

const app = defineApp({
  env: { OPENROUTER_OBSERVABILITY_TOKEN: v.optional(v.string()) },
});

app.use(openrouterObservability, {
  name: "openrouterObservability",
  httpPrefix: "/openrouter/",
  env: {
    WEBHOOK_TOKEN: app.env.OPENROUTER_OBSERVABILITY_TOKEN,
    RETENTION_DAYS: "30",
  },
});

export default app;
```

The optional binding lets preview deployments install before their secret is provisioned. The
ingestion routes fail closed with `401` until the token is set.

Configure an OpenRouter Broadcast webhook at:

```text
https://<deployment>.convex.site/openrouter/traces
```

Set its custom header to:

```json
{ "Authorization": "Bearer <deployment-specific-secret>" }
```

The mounted routes are:

| Method | Path      | Result            |
| ------ | --------- | ----------------- |
| `POST` | `/traces` | Ingest a delivery |
| `PUT`  | `/traces` | Ingest a delivery |
| `GET`  | `/health` | Return `ok`       |

`RETENTION_DAYS` is optional and defaults to 30. It accepts integer strings from 1 through 3650. A daily job deletes expired spans in indexed batches.
Invalid retention configuration makes the cleanup job fail. Treat that as an operational configuration error and alert on failed Convex cron runs. `/health` is only a liveness route and does not report retention readiness.

The initial release also performs a bounded, idempotent migration for deployments upgrading from
the former Prismantix-local component: it adds compact deduplication keys, fills the newly separated
resource-attribute field, and removes the incubator's raw-delivery rows. Ingestion returns `503`
while legacy span keys are being prepared so duplicate checks remain within Convex transaction limits.

## Compact discovery and full export

The query contract separates discovery from content export:

- `pageCorrelationSummaries` searches the indexed request, session, user, entity, run, job, root-execution, or explicit OpenCode-session field.
- `pageTraceSummaries` walks one trace.
- `pageRecentSummaries` supports administrator discovery across owners.
- `exportFullSpan` returns one full span by the `spanDocumentId` from a summary.

Every page returns `cursor` and `done`. Correlation pages emit at most seven summaries, trace pages at most six, and every call reads at most eight full documents including lookahead and trace-cursor resolution. Summary strings are clipped to 128 characters and named in `truncatedFields`. The serialized page stays at or below 32 KiB and advances its cursor from the last emitted row when the byte limit shortens a page. Content, raw attributes, events, and links appear only in `exportFullSpan`.

The explicit `opencodeSession` correlation reads only `trace.metadata.opencode_session_id`. It does not treat every OpenRouter `session.id` as an OpenCode session. New run, job, root-execution, and OpenCode-session indexes return `status: "not_ready"` until the bounded historical backfill reports ready through `getCorrelationProjectionCoverage`. Ingestion starts the backfill immediately on an upgraded deployment; the daily maintenance job retries it. Empty deployments are ready without waiting for the cron. This prevents a partial index from looking like an empty result.

Component query functions are internal references from the host's perspective. The executable [`example/convex/traces.ts`](example/convex/traces.ts) exposes only `internalQuery` wrappers for CLI use. A product-facing wrapper must authorize its host record before it passes an opaque correlation value to the component.

Run a compact request page directly against a mounted component:

```sh
pnpm exec convex run \
  --component openrouterObservability \
  queries:pageCorrelationSummaries \
  '{"correlation":{"kind":"request","requestId":"<opaque-request-id>"}}' \
  --typecheck disable --codegen disable --deployment <reference>
```

Do not add `--push` during inspection. Put full exports in a permission-restricted file instead of printing them into a terminal transcript:

```sh
umask 077
pnpm exec convex run \
  --component openrouterObservability \
  queries:exportFullSpan \
  '{"spanDocumentId":"<id-from-summary>"}' \
  --typecheck disable --codegen disable --deployment <reference> \
  > .memory/openrouter-span.json
```

The `.memory` directory must stay ignored. Delete exports when the diagnosis is complete. Shell arguments can remain in history, so use only opaque IDs and never place prompts, completions, tokens, or signed URLs in the command itself.

Production-read validation against one Meshix request found 36 generation spans across two traces and one session. The full traversal was 7,902,710 UTF-8 bytes, with a maximum span of 308,224 bytes. Request lookup included the preprocessing span that session lookup omitted. This is evidence for compact request-first discovery, not a provider-general shape guarantee or deployed proof of these new queries.

## Typed content decoding

`decodeOpenRouterInput` and `decodeOpenRouterOutput` return explicit `absent`, `invalid`, `unsupported`, or `decoded` outcomes while retaining the original string and parsed value. Supported input messages include system, user, assistant, and tool roles with string, null, or text-part-array content. Assistant `tool_calls` are emitted calls from message history. Output `tools` are request tool definitions and never become emitted calls. `reasoning_details` remains opaque, including encrypted entries.

Valid JSON with an unfamiliar role or content part is `unsupported`; the decoder does not discard unknown multimodal data. A production-read replay decoded all 36 inputs and outputs in the sampled Meshix request, including the text-part input on the Gemini preprocessing span. That replay validates the decoder locally against production-read data. It is not deployed query proof.

## Ingestion behavior

- Bearer tokens are compared through fixed-length SHA-256 digests.
- Only JSON bodies up to 900 KiB are accepted. The HTTP action counts bytes while reading the stream and cancels it as soon as the limit is exceeded. `Content-Length` can reject an obviously oversized body early but never replaces the streamed count.
- Empty authenticated OpenRouter Test Connection envelopes return `204` without writes.
- OTLP resource spans, scope spans, spans, attributes, events, and links have explicit count limits.
- Expanded stored spans are size-checked before admission so shared resource metadata cannot exceed Convex document or transaction limits through fan-out.
- A delivery writes all new spans in one mutation. Invalid input writes nothing.
- `(traceId, spanId)` identifies duplicates. New deliveries return `202`; duplicate-only deliveries return `204`.
- Known correlation, model, token, and cost values get typed columns. Unknown span and resource attributes retain their order and original OTLP typed values as JSON.
- The component does not keep raw webhook bodies.

## OpenRouter request metadata

Send Broadcast correlation through the OpenRouter request body, not AI SDK telemetry configuration:

```ts
const model = openrouter(modelId, {
  user: opaqueUserId,
  extraBody: {
    session_id: workflowSessionId,
    trace: {
      request_id: requestId,
      entity_type: "project_record",
      entity_id: recordId,
    },
  },
});
```

OpenRouter maps `session_id` to `session.id`, `user` to `user.id`, and custom `trace` keys to `trace.metadata.*`. The component projects the generic entity type and ID pair for indexed host lookups. AI SDK telemetry metadata is a separate channel and does not populate Broadcast metadata.

The parser keeps provider-native `gen_ai.usage.*` token counts separate from OpenRouter's normalized prompt and completion token counts. Optional billing costs accept both OTLP integer and double encodings because zero and non-zero values can use different numeric variants. Ambiguous duplicate values remain in the ordered attribute remainder instead of being guessed.

OpenRouter destination settings control content exposure before delivery. Enable Privacy Mode when prompt and completion content must not reach the component. Limit each destination to the intended API keys; a destination with no selected API keys receives traffic for every key in the workspace. Use opaque IDs and avoid direct personal data in trace attributes.

Broadcast fixtures have shown request tool definitions in output metadata, but that does not prove the model emitted tool calls. The component does not infer emitted tool calls from those definitions.

Prismantix still mounts its incubating local component. After this package is published, its cutover must install the package and remove the old local component and raw-delivery table in the same change. This repository does not perform that application change.

## Test utilities

`@shpitdev/convex-openrouter-observability/test` exports `register`, `schema`, and `modules` for `convex-test` host integration suites.
