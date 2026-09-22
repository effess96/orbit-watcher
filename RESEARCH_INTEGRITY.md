# Research integrity upgrade

The watcher detects candidate dislocations. It does not establish executable profit.
This release preserves the existing dashboard colors and monitoring architecture.

## Changed behavior

- Current liquidity alone no longer produces a positive depth label for Whirlpool,
  CLMM or DLMM routes. Tick/bin traversal is not implemented. Cross-currency depth
  is also unknown because the conversion leg's price impact is not modeled.
- Constant-product estimates remain available, with assumed fees and costs. The
  legacy `tradable=yes` field means model-positive only. It is never execution proof.
- Defaults of 10 seconds and 8 slots bound the age/lag of every compared account,
  including a required SOL/USDC reference. Idle periods expire open observations.
  These are conservative observation gates: unchanged accounts may be excluded.
  Passing does not establish transaction-consistent state.
- Stale gaps are interrupted, not logged as market closures. Opposite directions
  are separate observations. Reconnects invalidate cached pool state.
- Snapshot batches retain their own RPC context slot instead of assigning the
  newest batch's slot to every account.
- New CSV columns are `depth_status`, `depth_reason`, and `execution_verified`.
  Historical records migrate by column name and retain their original values.
- Background tasks and recording handles close during orderly server shutdown.

## Reproduction and downloads

The authenticated dashboard now offers **Replay settings (no credentials)**.
It exports an allowlist of pool mappings and research parameters. RPC URLs and
service credentials are excluded; it is not a full Railway backup.

New raw logs contain session settings, a watcher source hash, evaluation timestamps,
tip slots and interruption markers. Replay honors these boundaries, including
multiple evaluations in one slot. Embedded settings take precedence over the
supplied config for new sessions. Legacy data needs the deployed configuration
and remains approximate because its evaluation timing was never recorded.

`raw_updates.1.jsonl` can be downloaded through the authenticated route if present.
When rotation splits a session, concatenate retained segments in chronological
order before replay. If the session start is no longer retained, exact replay is
not available: begin a new recording session and archive it before rotation.
Future checkpointed rotation is still needed for indefinite replay retention.

Use a fresh output folder. Findings across session boundaries are retained in CSV;
the returned engine's counters describe the final session, not all sessions.
Malformed JSON inside a recorded session is rejected rather than silently skipped.
The source hash identifies the original implementation; replay under changed code
is a new analysis, even when event ordering is identical.

## Validation and rollout

Run `python -m unittest discover -v`. Tests use local fake RPC/WebSocket services
and artificial account balances. No external messaging credentials are required.

Validation on 22 September 2026: 71 tests passed; dashboard JavaScript syntax
passed; all 116 supplied historical gap rows retained every original field through
CSV migration. The complete 555,571-update file was audited, not exactly replayed.
Visual browser validation remains incomplete: Chromium was unavailable and its
download failed. No new live-network or Railway study was run for this upgrade.

The overnight audit can be regenerated with:

```sh
python audit_results.py /path/to/exported/files > overnight-audit.json
```

Back up `/data` before deployment. Applying this branch to the Railway-linked
branch may trigger deployment. This release appends evidence columns and migrates
existing CSV headers; retain the backup if rolling back to an older writer.

After deployment, collect a bounded fresh window, download its settings and logs,
then compare replayed rows with the live CSV. Expect fewer depth-positive labels;
that reflects removed unsupported estimates, not measured market deterioration.

## Remaining execution work

Capture transaction-consistent state and all tick/bin accounts for a candidate;
use the DEX's actual quoting math or transaction simulation; test both legs with
an atomic final-balance guard and complete costs; then repeat at affordable sizes.
The fixed 0.25/1/5 SOL research sizes are not a $300 allocation plan. They must not
be treated as instructions to commit capital. No transaction execution is added.
