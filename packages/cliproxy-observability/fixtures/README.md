# Protocol recordings

`real/` contains plugin-produced, sanitized observations from three harmless dev
protocol calls on September 23, 2026 UTC. The isolated official CLIProxy 7.3.5
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
The three checked-in recordings came from the final bounded invocation.

Synthetic provider playback in `native/harness/proof.py` is separate from these
recordings. Derived abort, split-frame and malformed fixtures must identify the
original fixture and transformation; do not label them as real abort recordings.
