# Stock CLIProxy capture

Go native capture for official CLIProxyAPI 7.3.5, ABI 1 / RPC schema 6. The serving binary is unmodified and hash-verified. TypeScript in the component owns protocol normalization; the plugin scopes, timestamps, redacts and frames observations.

This source is a release candidate. Live installation requires the separately owned Meshix receiver and my-nix deployment, closure of the device-endpoint bypass, resource preflight and DEV verification before production enrollment.

## Capture and durability

Before-selection hooks bind stock RequestID to the exact configured inference key, destination, route revision and deployment cross-check. Model listing uses its verified response-only entry point. Personal/preview traffic, unknown completions and half-observed streams cannot create calls. The plugin clears the internal capture headers before forwarding.

Callbacks perform bounded copying/enqueueing, never disk or network waits. Each destination has its own data and reserved metadata-control queues. Workers retain observations until a private Unix-socket receipt proves SQLite FULL/WAL admission. A full queue emits a metadata-only gap at the allocated sequence when control capacity remains; lost control records and process-loss uncertainty remain explicit in durable boot health.

The exporter persists immutable, checksummed NDJSON segments before sending. Files are fsynced and renamed before their SQLite references commit; startup removes unreferenced files and recovers delivering batches. Retries preserve boundaries, content and recipient bindings. Exact raw receipts permit local payload replica reclamation; compact receipts remain. Raw remote receipt does not claim projection completion.

Remote workers are independent per destination. Connection failures, 408/429/5xx use bounded backoff; authorization, schema, redirect and identity conflicts quarantine delivery. A 413 splits only at observation boundaries; an indivisible observation remains quarantined. TLS verification is enabled and redirects are forbidden. Recipient identity is checked by a content-free handshake and every segment receipt. Correct configuration first, then use the private `POST /resume` endpoint with a destination ID to retry quarantined records. No retry invokes a model.

## Private configuration

The plugin reads `CLIPROXY_CAPTURE_CONFIG` (JSON, mode 0600): `enabled`, `instanceId`, `revision`, `socket`, `bindings` containing exact `key`, `destinationId`, `deployment`, `environment`; optional `redactions`, `queueBytes` and `maxActive`. Changes require controlled restart. Only allowlisted correlation headers are retained.

The exporter accepts `--socket`, `--db`, `--budget-bytes`, `--reserve-bytes`, and optional `--delivery-config`. Its private delivery JSON contains `destinations`, each with `destinationId`, immutable `deploymentId`, HTTPS `url` ending `/cliproxy/capture/v1`, `instanceId` and dedicated receiver `token`. Tokens stay on the collector host. An empty delivery configuration provides local admission only.

Socket/state directories are 0700; sockets, databases and private configuration are 0600. The exporter holds a process lock. Default limits are a 16 MiB data queue, 128 reserved control slots, 1024 active scopes, 1 MiB observation bodies, 2 MiB HTTP envelopes, 1408 KiB remote segments and 256 observations per segment. Queue/redaction budgets are divided across enrolled destinations; each destination has 128 control slots. Pending serialized-argument redaction is bounded at 1 MiB / 8192 frames. Idle scopes and abandoned redactor state expire after 24 hours with unknown coverage.

The default outbox budget is 2 GiB with a 2 GiB filesystem reserve. Per-destination payload allowances reserve room for other receivers and segment replicas. Ordinary admission preserves a SQLite control reserve; remote ACKs reclaim payloads and incrementally compact free pages. Existing non-vacuum outboxes undergo a one-time startup VACUUM after a free-space check, preserving pending data; insufficient migration space stops the exporter before admission. Segment headroom scales with the configured budget. Finite disk cannot cover an unlimited outage or unlimited compact receipts. Capacity exhaustion never evicts unacknowledged data or blocks inference.

## Evidence boundaries

JSON/SSE redaction removes configured values and credential fields, including nested serialized JSON and values split across text/tool-argument deltas. Ambiguous fragments wait in bounded redaction state; malformed, truncated or over-limit content is withheld with explicit gaps. Unknown secrets in ordinary prose remain a limitation.

Stock hooks run before final framing. Responses candidates can omit delimiters or arrive as separate `event:` and `data:` lines; capture restores that field boundary without inserting newlines inside JSON fragments. One OpenAI Chat translation emits JSON chunks and drops transport `[DONE]`. `bodyFraming` labels these cases; canonical framing is added only to carry sanitized observations. `bodyFromSequence` preserves the earliest contributing sequence. Retained content is not byte-exact downstream traffic or proof of client receipt. First-body timing uses observed byte presence; semantic timing uses the first complete retained event.

Boot-health counters have plugin-boot scope and must not be summed across destinations. Incomplete prior-boot calls read as unknown, not inferred success/failure. Pre-hook failures, actual selected provider/auth identity, upstream attempts and OAuth cost remain unavailable.

## Verification and capacity

```sh
cd packages/cliproxy-observability/native
go test -race ./...
docker build -t cliproxy-capture-proof -f harness/Dockerfile .
docker run --rm --cpus 2 --memory 4g cliproxy-capture-proof
```

Default/CI playback is simulated and never uses real credentials. It checks all six generation JSON/SSE variants, models/count_tokens, post-hook failure, pre-hook unavailability, exact scope/exclusion, internal-header removal, capture hashes/gaps, restart and exporter outage. Native tests cover crash/ACK replay, wrong receipts, resegmentation, redirects, destination fairness, queue/disk saturation and fragmented secrets.

`harness/record.py` is explicitly invoked outside CI. It uses a private dev-key handoff at `/run/dev-key.json` and exports sanitized observations to `/recordings`. `RECORD_VARIANTS=counterparts`, `chat` or `cancel` selects additional bounded recordings. The isolated stock server relays through the existing dev gateway; OAuth state is never copied. Seven real fixtures cover the six protocol variants and cancellation, with selected provider unknown. No CAD generations are involved.

`harness/results/` contains local Docker measurements, not CT101 capacity proof. `measure-hooks.sh` temporarily adds a test to the exact upstream source to measure host clones and native RPC; it never rebuilds the serving binary. Client first-byte time is separate from semantic TTFT. The hardened slice-3 callback p50/p95/p99 was 0.73/16.36/24.59 ms at concurrency 8 / 65,579-byte requests / 2 CPUs, exceeding the provisional 1 ms target (initial p99: 18.52 ms). Playback latency p99 increased from 75.39 to 154.50 ms; throughput decreased from 142.28 to 85.22 calls/s. These are bounded local measurements, not production capacity guarantees. Added RAM alone cannot fix callback tail latency.

Allocate at least 4096 MiB RAM to CT101 before activation. Hardened stock/plugin peak RSS was 83.66 MiB and exporter peak RSS was 18.13 MiB at the tested load, leaving substantial headroom. Initial read-only preflight found about 131 GiB host RAM available and 26 GiB container root space available; recheck before applying resources. The 2 GiB outbox plus free-space reserve fit the existing 32 GiB root at that preflight.

Apache-2.0. The native ABI adapter's upstream MIT notice is in `plugin/UPSTREAM-LICENSE`.
