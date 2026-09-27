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
- Gateway identity is known. Provider identity is derived with explicit provenance (see below), attempt detail is unavailable, and cost is never estimated. Pre-hook capture is unavailable; previews are not enrolled.

For `capturePolicy: hook-body-v1`, decode each observation body from base64 and verify `contentBytes`/`contentSha256` before presenting it. Keep callbacks separate for raw evidence; their bytes may be partial JSON or UTF-8. `request_after_auth` is executed-request evidence, not the original client request. `executionModel`, `executionProtocol`, `selectedAuthId` and `selectedAuthIndex` are exact stock fields, not inferred provider or attempt identities. Header collections and opaque auth metadata are excluded. Model payload content is unredacted and must remain private.

`projectCapturedPayloads` reconstructs selected-call content from bounded pages. Pass `stockHookChunks: true` only for the new hook-body policy; historical observations already contain canonical framing. Its 16 MiB replay limit is a read-operation bound, not a retention limit. Larger calls remain available through raw event pages. Summary checkpoints retain scalar state and private references for large unfinished frames; immutable segments retain text, reasoning, tools and native usage snapshots.

## Provider, tokens and cost

`providerIdentity` derives `providerName` and `providerProvenance` from one call's recorded facts, never from time or call order. The first rule that applies wins:

1. `observed`: the recording itself names the provider. Only the OpenRouter adapter sets this today, and reads keep any provider name an older summary stored. For CLIProxy calls, the only exact source would be the native plugin recording the selected auth's provider on `request.intercept_after`; that is a follow-up.
2. `derived_from_wire_format`: `executionProtocol`, the upstream wire format CLIProxy recorded after auth (its ToFormat), is in `PROVIDER_BY_WIRE_FORMAT`. `claude` maps to `anthropic`; `openai`, `openai-response` and `codex` to `openai`; `gemini`, `gemini-cli` and `antigravity` to `google`. This names the API family, not the vendor account: `claude` also reaches other Anthropic-compatible hosts, and CLIProxy uses `codex` for xAI and Meta as well.
3. `derived_from_model`: `PROVIDER_BY_MODEL_PREFIX` matches the requested model, including when the wire format is absent or unmapped (such as `interactions`). `claude-` maps to `anthropic`; `gpt-`, `o` plus a digit, and `codex` to `openai`; `gemini-` to `google`. CLIProxy can route any alias to any upstream, so this is only a guess.
4. `unavailable` when no rule applies.

The recordings in `fixtures/real` have no after-auth frames, so they derive `openai` from `gpt-5.6-luna` although their recorded upstream was a gateway.

Token fields use the OpenRouter names and unit (tokens per call): `inputTokens`, `outputTokens`, `totalTokens`, `reasoningTokens`, `cachedInputTokens` (cache reads) and `cacheCreationInputTokens` (cache writes). They come only from terminal client-protocol usage and are set only when that usage is final. The rule is one table in `src/protocols/usage.ts`:

- Input includes cache reads and writes. Anthropic reports `cache_read_input_tokens` and `cache_creation_input_tokens` apart from `input_tokens`, so its input is their sum. Anthropic and CLIProxy's translators omit a cache count when no cache is used, so an absent one counts as 0; `input_tokens` itself is required.
- `totalTokens` is the reported `total_tokens`. Anthropic Messages reports none, and this package never sums one.
- Cache reads: Anthropic `cache_read_input_tokens`, Responses `input_tokens_details.cached_tokens`, Chat `prompt_tokens_details.cached_tokens`.
- Cache writes: Anthropic `cache_creation_input_tokens` (its `cache_creation.ephemeral_*` split stays raw), Responses `input_tokens_details.cache_write_tokens` or `cache_creation_tokens`, Chat `prompt_tokens_details.cache_write_tokens`, `cached_creation_tokens` or `cache_creation_tokens`. The first reported alias wins.
- Reasoning: Anthropic `output_tokens_details.thinking_tokens`, Responses `output_tokens_details.reasoning_tokens`, Chat `completion_tokens_details.reasoning_tokens`.

Every reported native count also stays in `usage` as a `UsageMeasurement` with its `nativeField`, `finality` and `semanticsVersion: "client_protocol_usage_v2"`.

Cost is never estimated. Native calls keep `cost: { kind: "unknown" }` with `costProvenance: "unknown"`, because CLIProxy's client protocols report no price. `fromOpenRouterSpan` keeps OpenRouter's charge as a `proxy_reported` USD cost.

`providerProvenance` and `costProvenance` are optional in `ModelCallV1`. Summaries projected by 0.3.0 or later store them. For older summaries, `getCall` and `pageRecentSummaries` derive them from the facts those summaries recorded. The same reads apply the current token rule to an older summary's final native counts, which adds `cacheCreationInputTokens` and counts absent Messages cache counts as 0. Stored rows are not rewritten.

Projected summaries of the real recordings measure 1,783 to 2,934 bytes. A streamed Opus call with an after-auth frame, all ten correlations and every Anthropic usage count measures 3,771 bytes, under the 8 KiB summary cap. With every bounded string at its validator maximum a summary still reaches 9,583 bytes (10,253 before 0.3.0), so the cap is not a guarantee for adversarial metadata.

### Differences from the OpenRouter component

- The OpenRouter component stores costs as bare numbers with no currency; `fromOpenRouterSpan` labels them USD. CLIProxy records no cost.
- Provenance exists only in `ModelCallV1`. The OpenRouter component's own summaries carry neither provider nor cost provenance.
- OpenRouter's `providerName` is the upstream it routed to, as OpenRouter names it (for example `OpenAI`). CLIProxy's is a lowercase API family or model-name guess. It is not authoritative until the native plugin records the selected auth's provider.
- The OpenRouter recordings here carry no cache-write count, so `cacheCreationInputTokens` stays unset for OpenRouter calls. The structural `OpenRouterSpanInput` accepts it if a host supplies one.
- OpenRouter summaries expose input, output and total tokens; its cached and reasoning counts are only on full spans (`exportFullSpan`).

## Parser provenance

The client-protocol assemblers and Responses regressions were adapted from Meshix commit `737ea929919f82921accaa4171d6abed5c85a1cb` (the reviewed #1371 stack). Synthetic log fixtures are extracted only in tests; this package does not ingest legacy text logs. Sanitized real recordings and their provenance live in `fixtures/real`.

Run `pnpm --filter @shpitdev/convex-cliproxy-observability test`, `typecheck`, and `pack:check`. The latter installs the tarball in an isolated consumer with the minimum supported Convex version and checks exported APIs plus executable component test modules.

Copyright 2026 Anand Pant. Apache-2.0; the native ABI adapter includes its upstream MIT notice.
