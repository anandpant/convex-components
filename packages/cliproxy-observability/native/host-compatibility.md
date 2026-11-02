# Native 0.2.6 host registration

Native 0.2.6 accepts `plugin.register` and `plugin.reconfigure` requests whose
host maximum `schema_version` is at least 6 (an unsigned 32-bit integer). It replies with producer schema **6**, ABI **1**,
and the same request, response, stream and completion capabilities. Schemas
below 6, missing/non-integer/out-of-range versions and malformed JSON fail before
capture initialization. A rejected request leaves an existing engine intact.
Private configuration validation and reconfigure behavior are unchanged.

## Why the host schema can differ

[Official stock 8.0.51](https://github.com/router-for-me/CLIProxyAPI/releases/tag/v8.0.51)
is source `7cad1cb7df48c6c5ffb7ff0b34ef8198806115bc`. The reviewed contract is:

- [`sdk/pluginabi/types.go`](https://github.com/router-for-me/CLIProxyAPI/blob/v8.0.51/sdk/pluginabi/types.go): ABI 1, host RPC schema 10. Schemas 7–10 add opt-in body omission, raw body patching, multiple model catalogs and multiple auth providers.
- [`internal/pluginhost/rpc_client.go`](https://github.com/router-for-me/CLIProxyAPI/blob/v8.0.51/internal/pluginhost/rpc_client.go): both lifecycle methods send the host's maximum schema, then retain the plugin's returned schema for adapters. A schema-6 reply is accepted.
- [`internal/pluginhost/adapters_interceptors.go`](https://github.com/router-for-me/CLIProxyAPI/blob/v8.0.51/internal/pluginhost/adapters_interceptors.go): body omission requires schema 7 and an explicit capability; raw body patching requires schema 8 and an explicit capability. Capture keeps schema 6 and advertises neither. Stream request/history omission thresholds remain 3/5, as before.

Native 0.2.5 rejected the initial schema-10 request before the host could use its
schema-6 reply. Negotiating schema 6 fixes that admission step. The
host maximum describes available support, not the selected callback format. An
upper cap would reject a later additive maximum even when the host still supports
schema 6. The lifecycle field uses the same `uint32` type as stock; negative,
overflowing and malformed values fail. The plugin never advertises a callback
schema newer than 6 or new capabilities.

[`sdk/pluginapi/types.go`](https://github.com/router-for-me/CLIProxyAPI/blob/v8.0.51/sdk/pluginapi/types.go)
explicitly describes the returned version as the negotiated plugin contract.
The official [simple C example](https://github.com/router-for-me/CLIProxyAPI/blob/v8.0.51/examples/plugin/simple/c/src/plugin.c)
returns its own older schema on both lifecycle methods without capping the host
maximum. Tests exercise larger synthetic host maxima (11, 42 and `2^32-1`) and
verify the same schema-6 reply/capabilities; this is negotiation coverage, not
qualification of a future stock release.

This relies on hosts honoring the negotiated schema. An ABI change is rejected
by native initialization; a conforming host that removes schema-6 support must
reject the plugin reply rather than send newer callback formats. A host that
violates that contract remains a risk. Keep exact stock-upgrade protocol/content
and load qualification as the deployment gate; registration success alone is
insufficient.

## Artifact and activation boundaries

On November 1, 2026, the candidate Linux amd64 pair passed the fixed isolated
fixture's explicitly labeled `protocol_only` mode against official 8.0.51.
The official archive SHA-256 was
`15de160e1fd97a0f9f7e0577582c765b14e040ab6a8314f514af9fae8a2c4700`;
the executable was
`19eb8f65b0a7066af51771ee1f2be8b37f0dd93c0e94f40de437c1db04a7a5f1`.
All six JSON/SSE variants preserved exact request/after-auth bytes and response
content, including complete tool arguments. Callback boundaries reconstruct
exact HTTP stream bytes: Chat's writer adds `data:` framing and `[DONE]`, and
Responses adds a final newline. Failed-request and completion-error bytes also
matched exactly. Scope/exclusion, header stripping, restart, exporter outage,
and graceful final loss/unconfirmed/incomplete accounting passed. Published
native 0.2.5 rejected schema 10 at the C registration entry point while accepting
6; the candidate accepted both and replied with schema 6.

The unpaced 8-worker/160-call/64-KiB load **failed** under local amd64 emulation
and parent-operated isolated native AMD64 at 2 CPU / 4 GiB. The matched native
8.0.33 / native 0.2.5 control also failed (193 drops, 39 lost-control records,
4,950,802 lost bytes); the candidate failed with two destinations (191/15/
4,819,488) and one destination (276/0/7,132,364). The global 256-data-record
ceiling remains unchanged; one destination does not yield 512 slots. All
terminals in the one-destination run did not make its gapped content complete.
This class of burst limit predates negotiation; identical production-shaped
throughput across host versions has not been established.

The coordinator accepted a schema-only compatibility scope with exact-artifact
protocol/content and bounded operational acceptance, retaining the stress
failures as a separate capacity limit. The exact pair also passed the parent's
isolated native AMD64 8.0.51 protocol-only fixture with 16,315 observed=committed
bytes and zero loss/uncertainty counters. Neither protocol-only nor CI's 7.3.5
baseline is a latest-stock full-stress pass. Queue/resource/storage/default
changes require a separate scope. Bounded live capture loss still requires
rollback regardless of the old control; this document is not activation proof.

Package 0.3.9 carries native 0.2.6. The exporter entry point, all capture/storage/
delivery code, module dependencies and pinned build image match package 0.3.7
and 0.3.8 except the shared `capture.Version` identity (0.2.5 → 0.2.6).
This is source parity, not binary identity: both native binaries are rebuilt
with the existing pinned Go bookworm image. Preserve previous release artifacts
for rollback; do not replace their manifests or checksums.

Receiver/parser, capture framing, FULL/WAL, storage schema, retention, resource
limits, queues and backpressure are unchanged. Synthetic disposable-container
proof does not qualify current-data reopen/write/delivery, prior-exporter
rollback, live OAuth/provider traffic or receiver projection. The coordinator
owns those qualification and activation gates. Timed blocks do not establish
zero loss before local commit or on a crash.

# Historical native 0.2.0 with CLIProxyAPI 7.3.15

The unchanged released Linux amd64 native 0.2.0 plugin and exporter passed a
bounded compatibility check with the official plugin-enabled CLIProxyAPI 7.3.15
binary on September 24, 2026. This is supplemental host qualification; the
historical 0.2.0 archive and its 7.3.5 manifest remain unchanged. It does not
qualify arm64, a rebuilt plugin, other stock releases, or live provider access.

## Exact pair

| Artifact                                                                                                 | SHA-256                                                            |
| -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| [Official 7.3.15 Linux amd64 archive](https://github.com/router-for-me/CLIProxyAPI/releases/tag/v7.3.15) | `801c3a23061d57a830e67fcd033fda26e96c2bfe93e1b2e34e4428ed7defc7e5` |
| Extracted `cli-proxy-api`                                                                                | `370d68b028b2906493eee0cc37562f295c74cc188e8114373776ee52d31f459b` |
| Released 0.2.0 `cliproxy-capture.so`                                                                     | `d81a7b20ff601ea0915d6aea6bcce43f590d2bf928f9160aa9382a022b5cd3aa` |
| Released 0.2.0 `cliproxy-capture-exporter`                                                               | `fdd19da4bb3940df3680351eaaeb758d60e5a0b93b2ea3f9f273d9054c45d7a2` |

Stock source is `673131f57484517c3a1eae7e36c4cfa7b9bb4efc`;
capture source is `dfbe71344853caa237233c2d869b8353180a3ec4`.
ABI definitions, loader and interceptor adapter match 7.3.5. RPC schema 6 adds
an optional scheduler capability unused by capture; the whole schema is not
byte-identical. This static comparison alone is insufficient for qualification.

## Verification and limits

The existing `harness/proof.py` ran once per stock version on native x86_64,
with the exact artifacts above, the same Debian glibc 2.36 filesystem, a private
network namespace with only loopback, and 2 CPU / 4 GiB limits. Only the candidate
report's version label changed. Both 7.3.5 and 7.3.15 passed the 8-worker,
160-call, 64 KiB load plus protocol, scope, authority-stripping, restart,
exporter-outage and deliberate error checks. Each retained 1,209 observations
across 178 calls, with no gaps, body-hash errors or sorted-sequence errors.

A separate local native Messages fixture passed 43 checks over four fake-key
calls: streaming thinking/signature and opaque blocks, continuation preservation,
an explicit over-limit body gap, and a failed completion. This exercised the
Claude executor, but not OAuth or the real provider's validation. Source-level
race tests also passed; those are distinct from testing the released binaries.

One earlier amd64-emulated candidate run lost bodies: 47 queue-limit and 103
subsequent gap observations. The reversed-order candidate run passed; both
emulated 7.3.5 controls passed. The native pair found no difference in capture
completeness. The original failure remains an unresolved saturation result,
not proof of a deterministic stock regression or of lossless capture. Queue and
body limits were not raised. Twelve out-of-order arrivals in the failed run had
contiguous sequence numbers after sorting; they were not missing observations.

The standard Dockerfile and packaging script still describe the original 7.3.5
release build. Running them unchanged does not test this supplementary pair.
To repeat this check, mount the verified 7.3.15 executable and exact released
capture binaries into that fixture instead of rebuilding instrumentation.

Deployment remains a separate operation with drain, rollback and receipt checks.
Live OAuth acceptance, actual provider reasoning/usage, remote receiver delivery
and projection require explicit live verification. Capture retains hook bodies;
post-hook cache-control transformations mean after-auth bytes are not necessarily
the final wire request. Known size, queue and projection limitations remain.
