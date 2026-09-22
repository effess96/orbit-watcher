# Orbit Dislocation Watcher

A **read-only** research tool. It answers one question with real data:

> When a big trade knocks one pool's price out of line, how big is the gap after fees, and how fast does someone close it?

Short-lived gaps are difficult to act on. Observed duration alone does not establish execution feasibility or identify why a gap closed.

**It never trades.** There's no wallet, no signing, no transaction sending, and no private key anywhere. A test checks that the code contains none of that.

## Two ways to use it

1. **Hosted private dashboard (recommended).** Runs around the clock on Railway behind a password. Follow **[DEPLOY.md](DEPLOY.md)**.
2. **Command line on any computer.** Follow the steps below.

To try the dashboard on your own computer first:

```sh
# macOS/Linux
export ADMIN_PASSWORD='choose-a-long-password'
python3 server.py
# Windows PowerShell
$env:ADMIN_PASSWORD='choose-a-long-password'
python server.py
```

Then open http://localhost:8080.

---

## What you need

- **Python 3.10 or newer.** Nothing else: no `pip install`. Check your version with `python --version`.
- An internet connection. The free public Solana address works to start. For long runs, a free RPC key (from Helius, QuickNode or Triton) is more reliable. See *Using your own RPC* below.

## Step 1: find pools for a token

Pick a token that trades in **several Raydium or PumpSwap pools** against SOL or USDC. Copy its mint address (from Solscan or DexScreener), then run:

```sh
python watcher.py discover <TOKEN_MINT> --write
```

This command:

1. asks DexScreener which pools exist for the token;
2. checks which program runs each pool and keeps the supported types (Raydium AMM v4, CPMM, CLMM; PumpSwap; Orca Whirlpool; Meteora DLMM) above $10k liquidity;
3. finds each pool's two reserve vaults automatically;
4. saves everything to `config.json`.

It also tells you which pools it skipped and why (dust liquidity, unsupported type, or not paired with SOL/USDC).

You can run `discover` several times with different tokens. Each token becomes its own "watch" in `config.json`.

## Step 2: watch

```sh
python watcher.py watch
```

It loads the current balances, then streams every change live. Once a minute it prints a status line, for example:

```
[14:02:11]   3.0 min | updates 412 | slot 448712003 | shocks 2 | profitable gaps closed 0 | open now 0 | TOKEN/SOL gap 0.084%
```

Stop it with **Ctrl+C**. A report prints when it stops. To stop automatically after a set time: `python watcher.py watch --minutes 120`.

Leave it running for a day or more. Busy hours and quiet hours behave differently.

## Step 3: read the report

```sh
python watcher.py report
```

It shows:

- **Shocks.** The number of times one pool's price jumped by at least `shock_pct` in a single update, and how often that left a profitable gap visible to you.
- **Profitable gaps.** How many were seen, the share that closed within 1 and 2 slots, the median time open, and the size of the theoretical best profit.
- **What this means.** A plain-language reading of the numbers.

## Step 4 (optional): replay with different assumptions

Every raw balance change is saved in `data/raw_updates.jsonl`. For legacy logs, supply the settings used to collect them in `config.json`, then run:

```sh
python watcher.py replay
```

This re-analyses saved data (results go to `data_replay/`) without reconnecting. New recordings embed redacted session settings and actual evaluation/interrupt boundaries; replay uses those settings. Legacy logs use approximate slot grouping and print a warning. Use a fresh output directory to avoid appending duplicate findings. See [research integrity notes](RESEARCH_INTEGRITY.md).

---

## Settings in config.json

| Setting | Meaning |
|---|---|
| `cost_quote` | Your assumed cost per attempt (network fee plus priority fee plus Jito tip), in the quote token. Default: 0.0005 SOL or 0.10 USDC. **This is a guess. Adjust it.** |
| `min_profit_quote` | A gap only counts if the best profit after `cost_quote` is at least this much. |
| `shock_pct` | The price jump in one update that counts as a "shock". Default: 0.5%. |
| `fee` (per pool) | The pool's swap fee. Defaults: 0.25% (Raydium), 0.30% (PumpSwap). Raydium CPMM pools can be 0.25%, 1%, 2% or 4%: check each pool on Raydium's site. |
| `commitment` | `processed` gives the fastest view of changes. |
| `max_state_age_s` | Maximum observed account-state age, default 10 seconds. Quiet unchanged accounts can be conservatively excluded. |
| `max_state_slot_lag` | Maximum account lag against observed tip/latest update, default 8 slots. This is not proof of transaction-consistent state. |
| `evaluate_delay_ms` | How long to wait after an update before comparing pools, so that both vaults of a swap have arrived. |

## Using your own RPC

Set these in the terminal before running, so your key never goes into a file.

macOS/Linux:
```sh
export SOLANA_RPC_HTTP='https://YOUR-RPC-URL'
export SOLANA_RPC_WS='wss://YOUR-RPC-URL'
```
Windows PowerShell:
```powershell
$env:SOLANA_RPC_HTTP='https://YOUR-RPC-URL'
$env:SOLANA_RPC_WS='wss://YOUR-RPC-URL'
```

## Output files (folder `data/`)

- `dislocations.csv`: one row per profitable gap: which pools, when it opened, slots and seconds open, peak gap %, peak net profit, best trade size.
- `shocks.csv`: one row per price jump, and whether a profitable gap was visible.
- `raw_updates.jsonl`: every vault balance change (used by `replay`).

## Evidence limits

- All outputs are research signals. No account is funded and no trade is executed.
- Constant-product depth outputs are model estimates using observed vault balances and configured fees, not exact DEX execution. Reserve adjustments, fee changes, transfer restrictions and actual landing costs still require verification.
- Concentrated-liquidity and cross-currency routes retain price-gap discovery, but their depth is **unverified** until tick/bin state and conversion price impact are modeled. They do not receive a positive depth classification.
- The legacy CSV column `tradable` is retained for compatibility. `yes` means only model-positive. New columns explain depth status and explicitly record `execution_verified=no`. Historical rows keep their original values and are not revalidated.
- Stale state interrupts a gap observation; it does not count as a market closure. Freshness limits are conservative and can exclude unchanged but valid pools. Fresh data also does not guarantee all accounts reflect a single transaction.
- Gap closure does not prove another searcher traded, and peak model returns may overlap. Never sum peaks as earned or achievable profit.
- The raw log is rotated at 200 MB; CSV history is cumulative. These can cover different windows. New partial recordings without a session boundary are refused for exact replay.

## Checking it works

```sh
python -m unittest -v
```

The offline tests run against a fake Solana server on your own computer, with no internet needed. They cover pool maths, vault discovery, the WebSocket client (including pings and reconnects), gap timing, replay, the report and the dashboard (login, lockout, security headers, adding and removing tokens, downloads), and they check that no trading code exists.

The original overnight exports were audited offline; see [the findings](research/OVERNIGHT_REVIEW.md). This upgrade has not been validated in a new Railway observation window.

### Mac certificate error

If you see `CERTIFICATE_VERIFY_FAILED` on a Mac, run the `Install Certificates.command` file in your Python folder under Applications, once.
