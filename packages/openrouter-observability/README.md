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

## Query from the host app

Component functions cannot read host auth or host tables. Keep authorization and joins in host functions:

```ts
// convex/traces.ts
import { OpenRouterObservability } from "@shpitdev/convex-openrouter-observability";
import { v } from "convex/values";
import { components } from "./_generated/api.js";
import { query } from "./_generated/server.js";

const observability = new OpenRouterObservability(components.openrouterObservability);

export const byRequest = query({
  args: { requestRecordId: v.id("traceRequests") },
  handler: async (ctx, args) => {
    const identity = await ctx.auth.getUserIdentity();
    if (!identity) throw new Error("Unauthorized");
    const request = await ctx.db.get("traceRequests", args.requestRecordId);
    if (!request || request.ownerSubject !== identity.subject) {
      throw new Error("Forbidden");
    }
    return await observability.listByRequest(ctx, {
      requestId: request.observabilityRequestId,
    });
  },
});
```

Checking only that a caller is signed in is not authorization. Resolve a host record from a host `Id`, verify its tenant or owner, and only then pass the stored opaque correlation ID to the component. The complete executable example is in `example/convex/traces.ts`. Keep `listRecent` behind an internal query or an explicit administrator check because it reads across owners.

The client exposes `getTrace`, `getSpan`, `listBySession`, `listByUser`, `listByRequest`, `listByEntity`, and `listRecent`. `getTrace` returns `{ page, cursor, done }`; pass a non-final `cursor` back as `afterSpanId` until `done` is true. Trace pages contain at most seven rows and use one additional row for lookahead, so each call reads no more than eight documents. The other list calls return at most eight rows and accept an exclusive `{ receivedAt, _creationTime }` cursor. These caps keep reads below Convex's 16 MiB transaction limit even when stored spans approach the 1 MiB document limit.

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
