# Changelog

## 0.2.0

- Adds typed OpenRouter input and output decoding with explicit absent, invalid, unsupported, and decoded results.
- Replaces full-document discovery methods with compact cursor pages capped by row, read, string, and serialized-output limits.
- Adds explicit single-span export and fixed indexed correlation selectors, including user, request, session, entity, run, job, root execution, and OpenCode session.
- Backfills new correlation projections in bounded batches and reports not-ready coverage until historical rows are complete.

Breaking change: remove calls to `getTrace`, `getSpan`, `listBySession`, `listByUser`, `listByRequest`, `listByEntity`, and `listRecent`. Use `pageTraceSummaries`, `pageCorrelationSummaries`, `pageRecentSummaries`, and `exportFullSpan` instead.
