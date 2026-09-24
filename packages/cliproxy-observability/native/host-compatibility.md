# Native 0.2.0 with CLIProxyAPI 7.3.15

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
