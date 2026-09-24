# Stock CLIProxy capture

Go native capture for official CLIProxyAPI 7.3.5, ABI 1 / RPC schema 6. The serving binary is unmodified and hash-verified. TypeScript in the component owns protocol normalization; the plugin scopes and timestamps exact hook-body observations.

This source is a release candidate. Live installation requires the separately owned Meshix receiver and my-nix deployment, closure of the device-endpoint bypass, resource preflight and DEV verification before production enrollment.

## Capture and durability

Before-selection hooks bind stock RequestID to the exact configured inference key, destination, route revision and deployment cross-check. Model listing uses its verified response-only entry point. Personal/preview traffic, unknown completions and half-observed streams cannot create calls. The plugin clears the internal capture headers before forwarding.

Callbacks perform bounded copying/enqueueing, never disk or network waits. Each destination has its own data and reserved metadata-control queues. Workers retain observations until a private Unix-socket receipt proves SQLite FULL/WAL admission. A full queue emits a metadata-only gap at the allocated sequence when control capacity remains; lost control records and process-loss uncertainty remain explicit in durable boot health.

The exporter persists immutable, checksummed NDJSON segments before sending. Files are fsynced and renamed before their SQLite references commit; startup removes unreferenced files and recovers delivering batches. Retries preserve boundaries, content and recipient bindings. Exact raw receipts permit local payload replica reclamation; compact receipts remain. Raw remote receipt does not claim projection completion.

Remote workers are independent per destination. Connection failures, 408/429/5xx use bounded backoff; authorization, schema, redirect and identity conflicts quarantine delivery. A 413 splits only at observation boundaries; an indivisible observation remains quarantined. TLS verification is enabled and redirects are forbidden. Recipient identity is checked by a content-free handshake and every segment receipt. Correct configuration first, then use the private `POST /resume` endpoint with a destination ID to retry quarantined records. No retry invokes a model.

## Private configuration

The plugin reads `CLIPROXY_CAPTURE_CONFIG` (JSON, mode 0600): `enabled`, `instanceId`, `revision`, `socket`, `bindings` containing exact `key`, `destinationId`, `deployment`, `environment`; optional `queueBytes` and `maxActive`. Changes require controlled restart. Only allowlisted correlation headers are retained.

The exporter accepts `--socket`, `--db`, `--budget-bytes`, `--reserve-bytes`, and optional `--delivery-config`. Its private delivery JSON contains `destinations`, each with `destinationId`, immutable `deploymentId`, HTTPS `url` ending `/cliproxy/capture/v1`, `instanceId` and dedicated receiver `token`. Tokens stay on the collector host. An empty delivery configuration provides local admission only.

Socket/state directories are 0700; sockets, databases and private configuration are 0600. The exporter holds a process lock. Default limits are a 16 MiB data queue, 128 reserved control slots, 1024 active scopes, 1 MiB observation bodies, 2 MiB HTTP envelopes, 1408 KiB remote segments and 256 observations per segment. Queue budgets are divided across enrolled destinations; each destination has 128 control slots. Idle scopes expire after 24 hours with unknown coverage. The receiver separately bounds unfinished protocol frames at 1 MiB.

The default outbox budget is 2 GiB with a 2 GiB filesystem reserve. Per-destination payload allowances reserve room for other receivers and segment replicas. Ordinary admission preserves a SQLite control reserve; remote ACKs reclaim payloads and incrementally compact free pages. Existing non-vacuum outboxes undergo a one-time startup VACUUM after a free-space check, preserving pending data; insufficient migration space stops the exporter before admission. Segment headroom scales with the configured budget. Finite disk cannot cover an unlimited outage or unlimited compact receipts. Capacity exhaustion never evicts unacknowledged data or blocks inference.

## Evidence boundaries

`capturePolicy: hook-body-v1` stores each bounded request, after-auth request, response and stream callback body byte-for-byte. NDJSON encodes `body` as base64; `contentBytes` and `contentSha256` describe those exact decoded bytes. Strings named token/secret, URLs, nested JSON, tool arguments and inline images are content and are not rewritten. Malformed JSON, partial UTF-8 and truncated streams remain raw evidence. Oversized callbacks and queue loss still produce explicit gaps.

Transport header collections, cookies and opaque stock metadata are never persisted. Only named correlation headers and selected auth ID/index scalars are admitted. Identity/model scalars containing a configured routing key are omitted, with field-only omission/conflict metadata; no replacement identity is fabricated. Body content and the stock completion diagnostic text are not scanned or redacted. Diagnostics retain their exact string up to 4096 bytes; larger diagnostics report `errorPresent`, `observedErrorBytes`, `metadataOmissions: ["error:limit"]` and a `diagnostic_text_limit` gap. Capture remains private and authenticated.

Stock hooks run before final framing and are not downstream wire bytes or proof of client receipt. Stream observations use `bodyFraming: stock_hook_chunk`, preserving callback sequence and byte count. Responses may deliver separate event/data lines; Chat may deliver raw JSON. The receiver derives frames for projection without changing stored observations. First-body timing uses observed byte presence; semantic timing uses a complete decoded event. Malformed/truncated/frame-limit projection is separate from raw capture completeness. Historical redacted observations retain their original policy labels and framing.

After-auth observations record the exact executed request body, `executionModel` and `executionProtocol`; selected auth ID/index come only from the pinned host's selected-auth scalars. Repeated after-auth callbacks remain separate observations, not invented attempt records. Summary execution fields describe the last observed selection. Missing scalar metadata stays absent.

Boot-health counters have plugin-boot scope and must not be summed across destinations. Incomplete prior-boot calls read as unknown, not inferred success/failure. Pre-hook failures, actual selected provider/auth type, upstream attempts and OAuth cost remain unavailable.

## Verification and capacity

```sh
cd packages/cliproxy-observability/native
go test -race ./...
docker build -t cliproxy-capture-proof -f harness/Dockerfile .
docker run --rm --cpus 2 --memory 4g cliproxy-capture-proof
```

Default/CI playback is simulated and never uses real credentials. It checks all six generation JSON/SSE variants, models/count_tokens, post-hook failure, pre-hook unavailability, exact scope/exclusion, internal-header removal, capture hashes/gaps, restart and exporter outage. Native tests cover crash/ACK replay, wrong receipts, resegmentation, redirects, destination fairness, queue/disk saturation, exact payload fidelity and fragmented stream bytes. Receiver tests cover derived stock framing, usage and explicit projection failures.

`harness/record.py` is explicitly invoked outside CI. It uses a private dev-key handoff at `/run/dev-key.json` and exports private hook-body observations to `/recordings` only after a separate credential/account scan. `RECORD_VARIANTS=counterparts`, `chat` or `cancel` selects additional bounded recordings. The isolated stock server relays through the existing dev gateway; OAuth state is never copied. Seven real fixtures cover the six protocol variants and cancellation, with selected provider unknown. No CAD generations are involved.

`harness/results/` contains historical redacting-policy local Docker measurements, not measurements of `hook-body-v1` or CT101 capacity proof. `measure-hooks.sh` temporarily adds a test to the exact upstream source to measure host clones and native RPC; it never rebuilds the serving binary. Client first-byte time is separate from semantic TTFT. The hardened slice-3 callback p50/p95/p99 was 0.73/16.36/24.59 ms at concurrency 8 / 65,579-byte requests / 2 CPUs, exceeding the provisional 1 ms target (initial p99: 18.52 ms). Playback latency p99 increased from 75.39 to 154.50 ms; throughput decreased from 142.28 to 85.22 calls/s. These are bounded local measurements, not production capacity guarantees. Added RAM alone cannot fix callback tail latency.

Allocate at least 4096 MiB RAM to CT101 before activation. Hardened stock/plugin peak RSS was 83.66 MiB and exporter peak RSS was 18.13 MiB at the tested load, leaving substantial headroom. Initial read-only preflight found about 131 GiB host RAM available and 26 GiB container root space available; recheck before applying resources. The 2 GiB outbox plus free-space reserve fit the existing 32 GiB root at that preflight.

Apache-2.0. The native ABI adapter's upstream MIT notice is in `plugin/UPSTREAM-LICENSE`.
