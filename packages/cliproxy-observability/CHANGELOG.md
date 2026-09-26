# Changelog

## 0.3.0

- `ModelCallV1` contract change: native CLIProxy calls now carry a derived `providerName`, which was always absent for them before. New optional fields are `providerProvenance` (`observed`, `derived_from_wire_format`, `derived_from_model` or `unavailable`), `costProvenance` (mirrors `cost.kind`) and `cacheCreationInputTokens`. Anthropic Messages `inputTokens` now counts an absent cache read or write as 0 instead of leaving input unknown, and usage measurements name `client_protocol_usage_v2`. Code that constructs `ModelCallV1` needs no new fields.
- `providerIdentity` uses the first rule that applies: a provider the recording names; the after-auth wire format through the exported `PROVIDER_BY_WIRE_FORMAT` table, which names an API family rather than a vendor account; then a guess from the requested model through `PROVIDER_BY_MODEL_PREFIX`, which also covers an absent or unmapped wire format. `fromOpenRouterSpan` marks OpenRouter's provider `observed`.
- Cost is never estimated: native calls stay `unknown`, and the OpenRouter adapter keeps `proxy_reported`.
- `cacheCreationInputTokens` comes from Anthropic `cache_creation_input_tokens` and the Responses and Chat cache-write details, which are also kept as raw usage measurements. One table states the normalization rule; `totalTokens` stays reported-only. The structural `OpenRouterSpanInput` accepts `cacheCreationInputTokens`.
- For summaries projected before 0.3.0, reads derive provider identity and cost provenance, keep any stored provider name as `observed`, and apply the current token rule to their final native counts. Stored rows are not rewritten.
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
