# Changelog

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
