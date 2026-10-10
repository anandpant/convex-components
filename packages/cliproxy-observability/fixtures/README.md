# Protocol recordings

`content-block-v1.ndjson` contains three exact native 0.2.4 producer observations
for consuming-reader tests: a Chat block containing three bare JSON callbacks, a
Responses block containing ten event/data callbacks, and a shutdown completion
with `captureIncomplete: true` and no provider outcome. The first two were
captured through official stock 8.0.23 with simulated provider playback on CT101;
the last came through the same plugin's C ABI against a private temporary outbox.
These are synthetic content, separate from the real provider recordings below.
Plugin SHA-256: `592168393cfcfad9df599bfc40a0d9889f9035aa4455f491b45d917394e2663a`.
Exporter SHA-256: `a5b3d4017cd52cd52819149665535cd165de7498ec702176d151ca106ee28321`.
Keep the raw lines and Base64 bodies intact; the package test validates their
digests and reconstructs both protocols through the shared splitter.

Lengths count bytes, including empty callbacks. On known capacity loss the
producer emits a metadata-only `stock_hook_block` gap with lengths `[0]`,
`captureIncomplete: true`, and cumulative `lostContentBytes`; later content is
preserved with `gap: "prior_capture_gap"`. Shutdown can be incomplete with zero
known lost bytes, so the omitted `lostContentBytes` in the third line means zero
known byte loss, not complete capture. Never assemble complete tool arguments
across a gap. `completionOutcome` describes the provider only when observed.

`real/` contains plugin-produced, sanitized observations from six harmless dev protocol variants and one real cancellation on September 23, 2026 UTC. The isolated official CLIProxy 7.3.5
ARM64 process used the existing private Meshix dev gateway as its configured
OpenAI-compatible upstream. The existing gateway retained ownership of OAuth
state. These are real provider responses, with an extra gateway explicitly
recorded in provenance; the selected upstream provider remains unknown.

Each fixture records the actual client protocol, requested model, stock/plugin
hashes, versions, event order/times, stock RequestID, destination and content
hashes. Stock completion is separate from protocol terminal evidence. Bodies are
Base64-encoded redacted hook observations, including observed Responses SSE
candidates before the stock final framer. They are not byte-exact network captures.
No request or provider credentials, cookies, account addresses, auth filenames or
prior user content were exported. Only the minimal fixture prompt was submitted.

`native/harness/record.py` produced these files from the plugin's outbox. It is
explicitly invoked with private dev credentials and is never run in CI. Two earlier
successful Messages calls failed the recorder's initial token-boundary assertion,
and one attempt returned HTTP 502; those are not promoted as successful fixtures.
The initial three recordings came from the final bounded slice-1 invocation. Slice 3 adds Messages JSON, Responses JSON, Chat SSE and a real Messages cancellation. One earlier Chat SSE call succeeded at inference but exposed an uncaptured stock JSON-chunk framing path; that incomplete recording was not promoted. The pinned-source framing fix and replay regression passed before the successful Chat recording. Each fixture keeps its own actual plugin hash and redaction version.

Synthetic provider playback in `native/harness/proof.py` is separate from these
recordings. Derived split-frame and malformed cases identify their source fixture/transformation in tests. `messages-abort.json` is the separately recorded real client cancellation, not a synthetic truncation.
