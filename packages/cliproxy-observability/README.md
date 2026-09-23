# CLIProxy observability

Private model-call capture for stock CLIProxyAPI 7.3.5. A separate native plugin and durable exporter feed this Convex component; the component stores bounded summaries and immutable private-content references. TypeScript owns client-protocol normalization.

This package is a release candidate. Native delivery and receiver normalization are implemented; publication, Meshix adoption and DEV activation require their separate release/deployment gates.

| Layer       | Coverage                                                                                                                   | CI                                       |
| ----------- | -------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| Unit        | Protocol framing, recorded replay, inherited Responses regressions                                                         | `check`                                  |
| Integration | Authenticated receiver, Convex receipts/projection, private blob ownership                                                 | `check`                                  |
| e2e api     | Isolated nginx + official stock binary + simulated provider; six real success fixtures and one real cancellation recording | `native-capture` (offline playback only) |
| e2e web     | None; host Ops integration is pending                                                                                      | No                                       |

No deployed receiver or browser proof is claimed by these tests.

## Host integration

Mount `@shpitdev/convex-cliproxy-observability/convex.config` with `app.use`. Construct `CliproxyObservability(components.cliproxyObservability)` in the host backend. Expose a dedicated HTTP action through `handleCliproxyCaptureRequest`; use private receiver credentials, exact destination/instance enrollment and private blob storage. Never pass receiver credentials to inference sandboxes.

The host supplies `PrivateCaptureStorage.put/get` and a durable `scheduleProjection` callback. Projection actions call `projectPendingSegments`, rescheduling when `more` is true. Scheduler failure returns 503 after the raw receipt, making exporter retries safe. The host must monitor and recover failed projection actions; persisted raw content and receipts remain independently readable. No TTL or cleanup cron removes calls, receipts or content by default.

The receiver accepts `CaptureSegmentV1` from the `/capture` export: immutable NDJSON batches of up to 256 contiguous observations. A segment is at most 1408 KiB; the outer JSON envelope is at most 2 MiB. Each observation payload is at most 1 MiB. The exporter must persist batch boundaries before retrying. Identical identities and digests deduplicate; changed digests or overlapping ranges conflict.

## Reads and evidence

- `pageRecentSummaries` and `pageCorrelationSummaries` return at most three summaries under a 32 KiB response budget, using indexed keyset cursors.
- `getCall` reads one bounded record; `pageEventSegments` pages private references. `resolveCallBlob` requires membership in the host-authorized call manifest and checks size/hash while streaming.
- Raw admission and projection have separate sequence watermarks. Missing segments stop projection; invalid protocol data and missing usage remain explicit. Boot health retains loss counters and labels incomplete prior-boot calls unknown.
- `ModelCallV1` is the common read contract. `fromOpenRouterSpan` is a structural adapter with no runtime dependency on the OpenRouter component. `fromLegacyCliproxySpan` labels historical rows distinctly.
- Gateway identity is known. Selected provider, attempt detail and OAuth cost are unknown unless supported evidence exists. Pre-hook capture is unavailable; previews are not enrolled.

`projectCapturedPayloads` reconstructs selected-call content from bounded pages. Its 16 MiB replay limit is a read-operation bound, not a retention limit. Larger calls remain available through raw event pages. Summary checkpoints retain scalar state and private references for large unfinished frames; immutable segments retain text, reasoning, tools and native usage snapshots.

## Parser provenance

The client-protocol assemblers and Responses regressions were adapted from Meshix commit `737ea929919f82921accaa4171d6abed5c85a1cb` (the reviewed #1371 stack). Synthetic log fixtures are extracted only in tests; this package does not ingest legacy text logs. Sanitized real recordings and their provenance live in `fixtures/real`.

Run `pnpm --filter @shpitdev/convex-cliproxy-observability test`, `typecheck`, and `pack:check`. The latter installs the tarball in an isolated consumer with the minimum supported Convex version and checks exported APIs plus executable component test modules.

Copyright 2026 Anand Pant. Apache-2.0; the native ABI adapter includes its upstream MIT notice.
