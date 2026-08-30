# OpenRouter observability for Convex

`@anandpant/convex-openrouter-observability` receives OpenRouter Broadcast OTLP JSON inside your existing Convex deployment. It stores typed spans in component-owned tables so your host Convex functions can query traces by user, session, request, or application entity.

The component is specific to OpenRouter Broadcast traces. It is not a general OTLP collector.

## Install

```sh
pnpm add @anandpant/convex-openrouter-observability
```

Register the component and pass the webhook token from the host deployment:

```ts
// convex/convex.config.ts
import openrouterObservability from "@anandpant/convex-openrouter-observability/convex.config.js";
import { defineApp } from "convex/server";
import { v } from "convex/values";

const app = defineApp({
  env: { OPENROUTER_OBSERVABILITY_TOKEN: v.string() },
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

## Query from the host app

Component functions cannot read host auth or host tables. Keep authorization and joins in host functions:

```ts
// convex/traces.ts
import { OpenRouterObservability } from "@anandpant/convex-openrouter-observability";
import { v } from "convex/values";
import { components } from "./_generated/api.js";
import { query } from "./_generated/server.js";

const observability = new OpenRouterObservability(components.openrouterObservability);

export const byRequest = query({
  args: { requestId: v.string() },
  handler: async (ctx, args) => {
    const identity = await ctx.auth.getUserIdentity();
    if (!identity) throw new Error("Unauthorized");
    return await observability.listByRequest(ctx, args);
  },
});
```

The client exposes `getTrace`, `getSpan`, `listBySession`, `listByUser`, `listByRequest`, `listByEntity`, and `listRecent`. List calls return at most eight rows and accept an exclusive `{ receivedAt, _creationTime }` cursor. The eight-row cap keeps reads below Convex's 16 MiB transaction limit even when stored spans approach the 1 MiB document limit.

## Ingestion behavior

- Bearer tokens are compared through fixed-length SHA-256 digests.
- Only JSON bodies up to 900 KiB are accepted.
- Empty authenticated OpenRouter Test Connection envelopes return `204` without writes.
- OTLP resource spans, scope spans, spans, attributes, events, and links have explicit count limits.
- A delivery writes all new spans in one mutation. Invalid input writes nothing.
- `(traceId, spanId)` identifies duplicates. New deliveries return `202`; duplicate-only deliveries return `204`.
- Known correlation, model, token, and cost values get typed columns. Unknown attributes retain their order and original OTLP typed values as JSON.
- The component does not keep raw webhook bodies.

Host applications should use opaque identifiers for OpenRouter `user`, `session_id`, request, and entity metadata. Avoid putting direct personal data in trace attributes.

## Test utilities

`@anandpant/convex-openrouter-observability/test` exports `register`, `schema`, and `modules` for `convex-test` host integration suites.
