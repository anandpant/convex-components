# Changelog

## 0.3.0

- Derive `providerName` with `providerProvenance` (`observed`, `derived_from_execution_protocol`, `derived_from_model` or `unavailable`) from one call's recorded facts through `providerIdentity` and the exported `PROVIDER_BY_EXECUTION_PROTOCOL` and `PROVIDER_BY_MODEL_PREFIX` tables. The model-name rule is a guess used only when no execution was recorded. `fromOpenRouterSpan` marks OpenRouter's provider `observed`.
- Add `costProvenance`. Cost is never estimated: native calls stay `unknown`, and the OpenRouter adapter keeps `proxy_reported`.
- Add `cacheCreationInputTokens` from Anthropic `cache_creation_input_tokens` and the Responses and Chat cache-write details, and keep those details as raw usage measurements. One table now states the normalization rule; `totalTokens` stays reported-only. Measurements name `client_protocol_usage_v2`.
- Derive the new fields on read for summaries projected before 0.3.0 without rewriting stored rows. The structural `OpenRouterSpanInput` accepts `cacheCreationInputTokens`.
- Native plugin/exporter source, native reported version (0.2.0), capture schema and ABI are unchanged. Updating the npm package does not require a native host upgrade.

## 0.2.2

- Recognize valid Responses keepalive control frames without downgrading otherwise complete capture projection and terminal usage. Malformed sequence numbers and unrelated unknown or malformed events remain diagnostic.
- Native plugin/exporter source, native reported version (0.2.0), capture schema, and ABI remain unchanged. This package update does not rewrite historical projections or require a native host upgrade.

## 0.2.1

- Preserve recorded selected-auth identity when stream initialization omits auth metadata. Explicit selections still replace the pair; new after-auth events retain unknown fields rather than inheriting a previous selection.
- Preserve supplied tool-input objects when Messages stream argument deltas contain zero characters. Missing initial input and malformed or truncated nonempty arguments remain diagnostic.
- Native plugin/exporter source and the native reported version (0.2.0), capture schema, and ABI are unchanged. Updating the npm package does not require a native host upgrade or rewrite historical summaries.

## 0.2.0

- Preserve exact bounded hook payload bytes with `hook-body-v1`; remove native content redaction and semantic withholding.
- Derive stock stream framing in the receiver, distinguishing complete raw capture from malformed/truncated projection.
- Capture after-auth request bodies, execution model/protocol and exact selected auth IDs without collecting credential headers or opaque metadata.

## 0.1.1

- Include the receiver-bound destination and deployment IDs in segment admission and duplicate ACKs so the native exporter can verify its delivery target.

## 0.1.0

- Stock CLIProxy native capture proof and private Convex component.
- Immutable batched raw admission, exact receipts, bounded queries and separate projection progress.
- ModelCallV1, structural OpenRouter/legacy adapters and TypeScript Messages, Responses and Chat parsers.
- Per-destination durable delivery, exact ACK/replay, private health and reserved control gaps.
- Fragment-aware credential redaction, all six protocol variants and real cancellation evidence.
- Checksummed Linux artifact packaging and exact-version npm release checks.
- No automatic retention expiry. Live activation remains a separate deployment gate.
