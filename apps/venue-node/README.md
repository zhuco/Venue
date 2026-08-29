# venue-node fixed binaries

This package owns exactly six account-node artifacts. Build one binary with only its matching
feature:

```text
cargo build -p venue-node --no-default-features --features bybit --bin venue-node-bybit
```

Every launch requires a canonical account UUID, canonical `BASE/QUOTE`, an absolute artifact base,
and a mode spelled exactly `TEST` or `LIVE`. The final root is derived in memory as
`<base>/<venue>/<mode>/<trading_account_id>`; callers cannot supply or override it.

Binance, Gate.io, and Bitget accept existing fixed Stage 7 commands after `--`. Omit the legacy
`--artifacts-root` because the node injects the derived root. Only `LIVE` delegates to the existing
runtime; `TEST` fails closed because those physical clients remain production-only.

```text
venue-node-gate --mode LIVE --trading-account-id <uuid> --symbol DOGE/USDT \
  --artifacts-base <absolute-path> -- --config venue.gate.example.toml grid-stop
```

Bybit, OKX, and Hyperliquid currently validate only their secret-free binding, selected adapter
endpoints, credential environment namespace, and derived root. They then fail closed before reading
credentials, networking, or creating artifacts. Do not enable them until the shared runtime has
integrated and verified Owner, WAL, the unique account writer fence, signed readback, UNKNOWN
reconciliation, Stop/Flatten, and operator-confirmed Canary evidence.

`venue_node::NodeSafetyHost` is the exchange-neutral composition boundary for that integration. It
reuses the canonical account-root fence, writer lease, and command WAL from `venue-execution`,
routes every command through one exact `StrategyBinding`, and gives an injected `PhysicalGateway`
one non-cloneable dispatch permit only after WAL, capability, writer, binding, and lifecycle checks;
risk-increasing LIVE commands additionally require fresh Canary evidence bound to that exact
durable command identity. Startup and ambiguous
dispatches require a newer adapter-verified signed readback; UNKNOWN
commands are settled from their durable family/client identity and are never resubmitted. Stop and
Flatten accept exact operator evidence and complete only from a request-newer full-family receipt;
Stop retains symbol custody while a signed nonzero position remains. The fixed binaries are not yet
wired to this host, so their existing fail-closed behavior is unchanged.

Run `scripts/verify_venue_node_binaries.ps1` with a target directory outside the worktree to build
all six exact feature compositions and scan each executable for foreign endpoint, credential, and
binding markers. A successful build is packaging evidence only; it never authorizes LIVE takeover.
