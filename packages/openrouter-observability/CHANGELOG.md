# Changelog

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
