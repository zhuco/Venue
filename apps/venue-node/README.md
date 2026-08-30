# venue-node fixed binaries

This package owns exactly six account-node artifacts. Build one binary with only its matching
feature:

```text
cargo build -p venue-node --no-default-features --features bybit --bin venue-node-bybit
```

Every launch requires a stable internal trading-account ID (not an exchange-issued UUID), canonical `BASE/QUOTE`, an absolute artifact base,
and a mode spelled exactly `LIVE`. The final root is derived in memory as
`<base>/<venue>/<mode>/<trading_account_id>`; callers cannot supply or override it.

Binance, Gate.io, and Bitget accept existing fixed Stage 7 commands after `--`. Omit the legacy
`--artifacts-root` because the node injects the derived root. Non-production modes are rejected
before endpoint, credential, or artifact setup.

```text
venue-node-gate --mode LIVE --trading-account-id <uuid> --symbol DOGE/USDT \
  --artifacts-base <absolute-path> -- --config venue.gate.example.toml grid-stop
```

Bybit, OKX, and Hyperliquid expose only three production subcommands after `--`: `preflight`,
`canary-place`, and `canary-cancel`. Each requires `--confirm-live <lowercase-venue>`. The binaries
load credentials from the root `.env`; they never print credential values.

Bybit and OKX use `DOGE/USDT`; Hyperliquid perpetuals use `DOGE/USDC`. A fixed binary rejects a
non-canonical quote before gateway construction.

```text
venue-node-okx --mode LIVE --trading-account-id <internal-id> --symbol DOGE/USDT \
  --artifacts-base G:\Venue\artifacts -- preflight --confirm-live okx
```

All production mutations pass through `venue_execution::AccountMutationHost`: one process lock per
account, one `commands.jsonl`, Owner embedded in each command, `Submitted` persisted before the
adapter receives a non-cloneable permit, and ambiguous results frozen as `Unknown` without retry.
Risk-increasing commands are post-only limit orders. The account's cumulative nominal position
cannot exceed 10 USDT; an existing uncancelled entry or non-zero venue position fences another
entry. OKX accepts canonical base quantity but converts it to whole/lot-aligned contracts using
live instrument rules.
Market, stop, and reduce-only commands remain unavailable in this MVP path.

The artifact root is `G:\Venue\artifacts\<venue>\LIVE\<trading_account_id>`. Append is refused once
`commands.jsonl` reaches 5 MiB and no file may exceed 10 MiB; Hyperliquid `nonce.json` is capped at
4 KiB. Archive only terminal, reconciled history; never remove unresolved `Submitted/Unknown` state.

Run `scripts/verify_venue_node_binaries.ps1` with a target directory outside the worktree to build
all six exact feature compositions and scan each executable for foreign endpoint, credential, and
binding markers. A successful build is packaging evidence only; it never authorizes LIVE takeover.
