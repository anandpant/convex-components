# Changelog

## 0.5.2

- Each recorded variant is also stored as its own row, and `pageVariantsSince` (client `pageVariantsSince`) pages them in creation order, including variants beyond the row's cap of 8, so a downstream copy can pick up variants added to old spans.

## 0.5.1

- `handleOpenRouterTraceRequest` takes an optional `onStored` hook. After admission it receives every span the component stored, as a new row or a new variant, with its full prepared content, so a host can forward it downstream without reading the archived object back. A hook failure is logged and never fails the delivery.
- `admitPrepared` also returns `stored`: the row id, trace and span ids, content digest, receipt time and kind of each stored span.
- `pageStoredSince` (client `pageStoredSince`) pages stored rows in creation order, 200 at most per page, for reconciling a downstream copy.

## 0.5.0

- Each span is stored as a slim row plus one archived object holding its full content. The host handler writes the object through the existing `TraceBlobStorage` at `<spanPrefix>/<traceId>/<spanId>/<sha256>.json` (new `spanPrefix` option), then admits the row. Rows record the object's key, digest and length in `payload`, with content byte counts, attribute counts and cache-write tokens precomputed, so listing never reads content.
- A redelivered span whose content differs keeps the first row and records the new object in `variants` (up to 8, with `variantCount` counting all), so no delivered content goes unreferenced. Summaries carry `variantCount` and `contentBytes`, the size a full read loads.
- `loadFullSpan(reader, row, { maxBytes })` returns a span's full content in an action and checks the object's length and digest. `archiveSpan` lets a host admit a span it prepared itself.
- `getSpan` replaces `exportFullSpan` and returns the stored row. `pageCorrelationSummaries` keeps the request, run, and job correlations and returns up to 7 summaries per page (raised once 0.6.0 removes inline content).
- Removed: `pageTraceSummaries`, `pageRecentSummaries`, `getCorrelationProjectionCoverage`, the session, user, entity, root-execution and OpenCode-session correlations and their indexes, the `by_received` index, retention (`RETENTION_DAYS` and its cron), and the Prismantix and correlation migrations. Spans are kept indefinitely. Deduplication uses the span table's own `(traceId, spanId)` index instead of `spanKeys`.
- Rows stored before 0.5.0 keep their content inline and still summarize and load. The temporary `archiveMigration` functions let a host script page those rows and mark each one archived after it has written and confirmed its object; 0.6.0 removes them with the inline form and the unused `spanKeys`, `deliveries` and `migrationState` tables.

Breaking change: remove `env.RETENTION_DAYS` from `app.use`, pass `spanPrefix`, and read content through `getSpan` and `loadFullSpan` in an action.

## 0.4.2

- Summaries carry `cacheCreationInputTokens`, read from the span's stored `gen_ai.usage.input_tokens.cache_write` attribute (or `trace.metadata.openrouter_generation.cache_write_tokens`), so spans stored before this release report it too. 0.4.1 said OpenRouter records no cache writes; it does, and `inputTokens` already includes them.
- Summaries carry `isByok` and `byokInferenceUsageCost`. On a BYOK call `totalCost` is only OpenRouter's fee; `byokInferenceUsageCost` is the inference billed to the caller's own provider key.
- A field the span did not record stays absent. Stored spans and the schema are unchanged.

## 0.4.1

- Summaries from `pageTraceSummaries`, `pageCorrelationSummaries`, and `pageRecentSummaries` now carry `reasoningTokens` and `cachedInputTokens`. Every span already stored them; only a full-span export returned them before. A count the span did not record stays absent, never 0.
- OpenRouter reports no cache-write count, so neither spans nor summaries have one.

## 0.4.0

- Accepts `RETENTION_DAYS: "indefinite"` to keep spans and their deduplication keys indefinitely. Cleanup is not scheduled, and already-queued cleanup continuations delete nothing and stop.
- Keeps migrations and correlation backfills running under indefinite retention. The default of 30 days and numeric range of 1–3650 days are unchanged.
- Documents that external blob retention is configured separately in the host's object store.

## 0.3.0

- Moves authenticated trace ingestion into a host HTTP handler with a host-owned blob storage adapter.
- Externalizes inline data URLs, recognized binary fields, and oversized strings before the component mutation so Convex stores durable references instead of base64 or large content.
- Decodes mixed text, image, and unknown message parts in order. External images, owned blob references, and unavailable upstream placeholders remain distinct.
- Adds bounded blob retrieval with stored-span membership, byte-length, and SHA-256 checks.
- Raises the bounded HTTP request limit from 900 KiB to 8 MiB while keeping per-blob, stored-document, and transaction limits.

Breaking change: mount `handleOpenRouterTraceRequest` in the host HTTP router and provide `TraceBlobStorage`. The component no longer mounts `/traces` or `/health`, and it no longer accepts `WEBHOOK_TOKEN`. Hosts retain the same public URLs themselves.

## 0.2.0

- Adds typed OpenRouter input and output decoding with explicit absent, invalid, unsupported, and decoded results.
- Replaces full-document discovery methods with compact cursor pages capped by row, read, string, and serialized-output limits.
- Adds explicit single-span export and fixed indexed correlation selectors, including user, request, session, entity, run, job, root execution, and OpenCode session.
- Backfills new correlation projections in bounded batches and reports not-ready coverage until historical rows are complete.

Breaking change: remove calls to `getTrace`, `getSpan`, `listBySession`, `listByUser`, `listByRequest`, `listByEntity`, and `listRecent`. Use `pageTraceSummaries`, `pageCorrelationSummaries`, `pageRecentSummaries`, and `exportFullSpan` instead.
