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

`body-part-v1.ndjson` contains the two exact native 0.2.5 `request` records of one
streamed `/v1/responses` call whose 1,100,069-byte client body exceeds the 1 MiB
record bound. They came through official stock 8.0.23 (SHA-256
`085c1bd627c952f2c8db9b5af60bf4aeb1772c3fb102a241bf5ab7662d47f727`) with simulated
provider playback in CT101's private release qualification. The first record holds
exactly 1,048,576 body bytes and the second the remaining 51,493; both carry the same
`bodyPart` manifest (`count: 2`, `totalBytes: 1100069`, `totalSha256`). Feed them in
order to `/protocols`' `BodyPartAssembler`, which verifies order, offsets, length and
the whole-body digest and returns the client's exact JSON body. Plugin SHA-256:
`e5ec8b5e181a68e9f9bb7dbaab86bd66317b92fd30cee0110480e8b90af724fb`. Exporter SHA-256:
`3b85a2c36e8b59279975165431e78abd2514c335f343833929a04d0662d2fd47`. Synthetic content.

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
