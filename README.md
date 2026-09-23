# Orbit Dislocation Watcher

A **read-only** research tool. It answers one question with real data:

> When a big trade knocks one pool's price out of line, how big is the gap after fees, and how fast does someone close it?

If most gaps close within one or two Solana slots (0.4–0.8 seconds), a home-built bot can't win them. If some last longer, you can inspect them one by one.

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

Every raw balance change is saved in `data/raw_updates.jsonl`. Change the cost or threshold numbers in `config.json`, then run:

```sh
python watcher.py replay
```

This re-analyses the saved data (results go to `data_replay/`) without reconnecting to anything.

---

## Settings in config.json

| Setting | Meaning |
|---|---|
| `cost_quote` | Your assumed cost per attempt (network fee plus priority fee plus Jito tip), in the quote token. Default: 0.0005 SOL or 0.10 USDC. **This is a guess. Adjust it.** |
| `min_profit_quote` | A gap only counts if the best profit after `cost_quote` is at least this much. |
| `shock_pct` | The price jump in one update that counts as a "shock". Default: 0.5%. |
| `fee` (per pool) | The pool's swap fee. Defaults: 0.25% (Raydium), 0.30% (PumpSwap). Raydium CPMM pools can be 0.25%, 1%, 2% or 4%: check each pool on Raydium's site. |
| `commitment` | `processed` gives the fastest view of changes. |
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

## Pool earnings (liquidity-provider research)

For every Orca Whirlpool and Raydium CLMM pool you watch (plus the SOL/USDC reference pool, if it is one of
those kinds), Orbit keeps two pretend liquidity positions of 100 SOL/USDC each: one over a price range of
±5% and one over ±20%, centred on the price when tracking started. No money is involved.

- **Fees earned** come from the pool's own on-chain fee counter, so they are what a real position that size
  would have collected while the price was inside its range.
- **Price-move loss** compares the position with simply holding the coins it started with (often called
  impermanent loss).
- **Net vs holding** = fees + price-move loss. Positive means providing liquidity beat holding.
- **Worst so far** is the lowest that position has been, so a bad stretch is not hidden by an average.
- **On your SOL / day** applies today's pace to your own capital (set `ORBIT_LP_CAPITAL_SOL`, default 2.5 SOL).
- **Days to break even** compares that with the cost of opening and closing the position: network fees plus
  the swaps in and out (the pool's own fee on your capital).

Positions are saved to `data/lp_state.json` every 30 seconds, so restarts don't reset them. Odd pool
readings (a fee counter that appears to move backwards, an impossible one-update jump, or the same slot
seen twice) are ignored and counted as "odd pool readings ignored"; a position whose numbers stop making
sense is started again automatically. "Reset tracking" on the dashboard starts every position from scratch. Results show on
the dashboard ("Pool earnings") and in the report. Judge them after several days: one sharp price move
can wipe out weeks of fees, especially on memecoins. Meteora and constant-product pools are not measured.

## Output files (folder `data/`)

- `dislocations.csv`: one row per profitable gap: which pools, when it opened, slots and seconds open, peak gap %, peak net profit, best trade size.
- `shocks.csv`: one row per price jump, and whether a profitable gap was visible.
- `raw_updates.jsonl`: every vault balance change (used by `replay`).

## Honest limits

- **You can't see gaps that open and close inside one slot.** Updates arrive at most once per slot per account, and a searcher can close a gap in the same block. Seeing *zero* gaps is therefore a meaningful result: it means nothing lasted long enough for you.
- "Best net" is a ceiling from pool maths minus your cost guess. It isn't a fill. Other traders, slippage, token transfer taxes and frozen tokens can all make a gap untradable.
- Supported pools: Raydium AMM v4, Raydium CPMM and PumpSwap (exact profit maths), plus Orca Whirlpool, Raydium CLMM and Meteora DLMM (price and live fee read from the pool; gap after fees only, no size estimate). Meteora DAMM pools aren't supported.
- SOL and USDC pools of the same token are compared through a live SOL/USDC reference pool, including that pool's swap fee.
- A concentrated pool whose price lands more than 50% away from the others is excluded and reported in the log, as a guard against decoding errors.
- Raydium AMM v4 vault balances can include small amounts that aren't tradable reserves. Treat tiny gaps on those pools with caution.
- A lost connection drops any gap that was open at the time, instead of timing it wrongly.
- The public Solana RPC may limit subscriptions. If you see repeated reconnects, use your own RPC.

## Checking it works

```sh
python -m unittest -v
```

The 40 offline tests run against a fake Solana server on your own computer, with no internet needed. They cover pool maths, vault discovery, the WebSocket client (including pings and reconnects), gap timing, replay, the report and the dashboard (login, lockout, security headers, adding and removing tokens, downloads), and they check that no trading code exists.

**Not yet tested against the real Solana network.** The build environment had no internet access. Your first real `discover` and `watch` runs are that test. If something fails, send me the error message.

### Mac certificate error

If you see `CERTIFICATE_VERIFY_FAILED` on a Mac, run the `Install Certificates.command` file in your Python folder under Applications, once.
