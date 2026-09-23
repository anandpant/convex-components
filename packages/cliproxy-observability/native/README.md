# CLIProxy capture: stock-host proof

This is slice 1 of the capture system: a Go native plugin for unmodified
CLIProxyAPI 7.3.5 and a private Unix-socket exporter with durable SQLite admission.
It is not ready for live installation. Receiver delivery, durable health accounting,
release artifacts and deployment hardening follow in later slices.

The official serving binary is downloaded and SHA256-verified by the harness.
The plugin uses native ABI 1 and RPC schema 6. It does not install an executor,
change model payloads or use the unkeyed usage hook. Before-selection hooks bind
stock RequestID to an exact configured key, destination, route and deployment
cross-check. Completion and chunks use that binding; unknown completions and
half-observed streams cannot create calls. Personal and preview traffic is excluded.

The callback clears the three internal capture headers, bounds decoded payloads,
and enqueues allowlisted observations. It never waits for disk or the collector.
The worker redacts and sends observations; the exporter acknowledges only after a
SQLite FULL/WAL commit. Replay with the same identity/digest is idempotent. A
conflicting digest is rejected. Queued events retain their destination across retries.

## Run the isolated proof

```sh
cd packages/cliproxy-observability/native
go test -race ./...
docker build -t cliproxy-capture-proof -f harness/Dockerfile .
docker run --rm --cpus 2 --memory 4g cliproxy-capture-proof
```

The default harness uses **simulated provider playback**, temporary private files,
a separate stock server and nginx stamp/scrub listeners. It never uses real keys
or makes model calls. CI runs this path on Linux amd64. On an ARM Mac, explicitly
select `--platform linux/amd64` to test the release architecture under emulation;
label those timing and RSS results accordingly.

It checks registration, Requests/Responses/Chat capture, both credential carriers,
case-sensitive matching, destination spoofing, personal/preview exclusion,
credential conflicts, missing deployment, provider-header removal, content hashes,
exporter restart and inference while the exporter is unavailable. It reports
p50/p95/p99 client first-byte and full-response latency, throughput, and Linux
VmHWM/RSS for the stock host with plugin and exporter under concurrency 8 with
64 KiB request content. Client first-byte time is not semantic TTFT.

`harness/measure-hooks.sh` adds a temporary test to a checkout of the exact
upstream commit to measure host cloning and real native RPC callbacks. It does
not modify or rebuild the serving binary. `harness/hook-bench.sh` supplies the
private simulated scope/outbox when run in the build image. End-to-end latency
and callback measurements are separate evidence.

`harness/record.py` is an explicitly invoked real-recording tool, never a CI step.
It takes an existing private dev inference-key handoff at `/run/dev-key.json`,
makes three harmless protocol calls, and exports only the plugin's sanitized
outbox observations to `/recordings`. The isolated stock server uses the existing
dev gateway as an OpenAI-compatible upstream. The existing gateway remains the
OAuth owner; no refresh-token state is copied. Fixture provenance records this
extra gateway and leaves the selected provider unknown. The fixture's inference
key is not a delivery credential. No CAD generations are involved.

## Contract and limits

The private config is JSON, mode 0600: `enabled`, `instanceId`, `revision`,
`socket`, `bindings` (exact `key`, `destinationId`, `deployment`, `environment`),
optional `redactions`, `queueBytes` (default 16 MiB), and `maxActive` (default
1024). The plugin reads `CLIPROXY_CAPTURE_CONFIG`; credentials never appear in
registration metadata. Scope changes require a controlled restart in this slice.

The socket directory is mode 0700 and the socket 0600. The exporter holds a
process lock, rejects non-socket replacement, limits envelopes to 2 MiB and
bodies to 1 MiB, and reserves filesystem space. Outbox rows have no expiry.
SQLite files are local replicas; remote retention/delivery is not implemented yet.
The current prototype uses inline SQLite payloads; immutable segment files and
full crash-boundary recovery belong to delivery hardening.

Capture is fail-open. Events lost before a durable local commit cannot be
reconstructed. Queue loss counters are attached to later observations; a quiet
or crashed process can leave an unknown interval. Durable heartbeats, reserved
control delivery, abandoned-scope recovery and remote retry/quarantine are release
gates, not claims made by this prototype. The current worker intentionally waits
on one unacknowledged observation and cannot yet provide per-destination fairness.

Only approved correlation fields are retained; arbitrary headers/metadata are
never serialized. JSON/SSE framing is used only for redaction. Malformed,
oversized or truncated frames are withheld with explicit gap reasons. Complete
Responses candidate frames may lack delimiters because stock interception occurs
before its final SSE validator. Captured bodies are redacted observations, not
proof of exact downstream bytes or client receipt. Protocol/usage normalization
belongs exclusively to the receiver's TypeScript parsers.

Redaction removes known values and credential fields, including nested serialized
JSON. Unknown secrets embedded in ordinary prose remain a limit. Comprehensive
semantic delta redaction, adversarial depth/size handling and real cancellation
fixtures must pass before release. These limits prevent activation of slice 1.

Provider identity, OAuth cost, actual upstream attempts and pre-hook failures are
unavailable. No usage/provider/attempt association is inferred. Response status
and completion outcome remain separate from capture completeness.

The ABI declarations derive from the pinned upstream example; its MIT notice is
in `plugin/UPSTREAM-LICENSE`. The rest of this repository retains its own license.

## Measured capacity

The checked-in `harness/results/` reports are local ARM64 Docker measurements with
2 CPUs and a 4 GiB cap, not CT101 measurements. The pinned-host callback benchmark
(200 calls, concurrency 8, 65,579-byte requests) measured p50 0.765 ms, p95 10.32 ms,
and p99 18.52 ms. The provisional p99 <1 ms target is not met under this load.
The native playback report contains complete latency/throughput and process RSS
high-water measurements. `VmHWM` supplements 10 ms RSS samples; neither proves
unbounded-outage memory behavior or production capacity.

For CT101, allocate at least 4096 MiB RAM before activation. The plugin queue and
redactor working sets are each bounded at 16 MiB by default. Observed combined
stock/plugin/exporter RSS was below 120 MiB in these runs, so 4 GiB provides
substantial headroom over the current 1 GiB allocation. Read-only Proxmox preflight
found 133,898 MiB available on the host and 27,065 MiB free on the container root;
the proposed 2 GiB outbox and 2 GiB free-space reserve fit the existing 32 GiB disk.
Recheck these live values before applying resources. No resource change is part
of this source-only slice. Callback tail latency remains a delivery-hardening
measurement/optimization item; extra RAM alone does not prove the latency target.
