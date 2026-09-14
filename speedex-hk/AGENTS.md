# HK proxy — deployment and observer evidence

Inherit [root rules](../../AGENTS.md). Read [proxy](../../docs/proxy.md) and
[HK timing](../../docs/hk-proxy-timing.md); installation, health and measurement acceptance are distinct.

- Ops hosts come only from the trusted node registry; editable product proxy URLs never become SSH targets.
  Do not expose proxy credentials or CA keys. Use protected auth files, restrictive permissions and no secrets
  in argv/logs/environment. Report sanitized health/version/permission facts, never credential contents.
- The selective-decrypt scaffolding (`selective-decrypt.overlay.example`, `speedex-mitm-exp.service`,
  `SPEEDEX_HK_INSTANCE=experiment` in install.sh) is default-OFF and never wired to production ports/paths;
  enabling any GMGN/OKX decryption requires the SOP in `docs/plans/hk-selective-mitm.md` plus explicit
  per-run authorization. The experiment unit stays loopback-only with diagnostics-minimal logging
  (no auth headers, tokens, full query strings, signed bodies or full captures).
- Verify actual allow_hosts/interception support. A passthrough platform has no HK transaction observation;
  a separate RTT probe is not HK-L1a'/L3'. Keep host matching exact or dot-delimited subdomain matching.
- Bind each leg to an immutable active window and independent request/response anchor. Success IDs cannot
  self-prove correlation; reject stale/replay-suspect/cross-entry/hash-conflicting evidence. Buffered events
  retain source time and require the same freshness/anchor checks as live events. Freeze first observations.
- Hook entry time is observer time, not physical wire time. Retain clock domain, anchorRole and method.
  Separate receipt observation from exact block/slot proof and preserve explicit missing reasons.
- Count authorized slots, attempts and observations separately; multiple requests/previews are not multiple
  authorized attempts. Keep superseded history without double counting; bound active-window collection.
- A healthy new instance does not preserve an old run. Recheck node/instance/run identity and child liveness
  after await; reload/tunnel loss must surface degraded/missing evidence, never a successful empty collection.
- Changes must propagate through Python → client → orchestrator → private artifact → public DTO → API/UI/export.
  Persist actual runtime/build identity per batch; health-only fields cannot reconstruct historical environment.
- Use offline synthetic fixtures and supported runtime tests. Live sampling, installation/restart and deployment
  follow root authorization rules; tests or an old deployment snapshot do not authorize a new rollout.
