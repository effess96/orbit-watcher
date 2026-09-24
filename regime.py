"""
Market mood for Orbit: is the crypto market risk-on or risk-off, and how calm are the tokens you watch?

Two parts:

1. Crypto regime (0-100). The six scoring components and the composite come unchanged from the
   crypto-regime-analyzer skill in github.com/tradermonty/claude-trading-skills (MIT licence,
   copyright 2026 TraderMonty; see regime_skill.py). Only the data fetching is rewritten here,
   using Python's standard library so Orbit still needs no pip installs.

2. Solana "LP weather" (Orbit's own addition). For every token you watch, how often did its price
   (against SOL and against the dollar) stay inside +/-5% and +/-20% over 3-day and 7-day windows
   in the last 90 days? That is a history-based guide to how long a paper liquidity position in that
   range would have kept earning fees. It uses daily closes, so real intraday swings were larger:
   treat the figures as optimistic.

Everything here describes the market. None of it is a buy or sell signal, and nothing here trades.

Data: CoinGecko's free public API (no key) and perpetual funding from Binance, or OKX when Binance
refuses the server's region. The first run of a day makes ~40 slow, rate-limited requests (about
5-6 minutes, in the background); later runs that day use the cache.
"""
from __future__ import annotations

import json
import math
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import regime_skill  # noqa: E402  (the skill's scoring code, in one file)

regime_skill.install()

from calculators.alt_breadth_calculator import calculate_alt_breadth          # noqa: E402
from calculators.btc_trend_calculator import calculate_btc_trend              # noqa: E402
from calculators.dominance_calculator import calculate_dominance_regime       # noqa: E402
from calculators.drawdown_vol_calculator import calculate_drawdown_vol        # noqa: E402
from calculators.funding_calculator import calculate_funding_regime           # noqa: E402
from calculators.momentum_thrust_calculator import calculate_momentum_thrust  # noqa: E402
from scorer import COMPONENT_LABELS, COMPONENT_WEIGHTS, calculate_composite_score  # noqa: E402

COINGECKO = "https://api.coingecko.com/api/v3"
BINANCE_FUNDING = "https://fapi.binance.com/fapi/v1/premiumIndex"
OKX_FUNDING = "https://www.okx.com/api/v5/public/funding-rate?instId={sym}-USDT-SWAP"
USER_AGENT = "orbit-watcher/1.4 (read-only research)"
REQUEST_DELAY_S = 8.0          # CoinGecko's free tier allows roughly 5-15 requests a minute
MAX_RETRIES = 4
BACKOFF_BASE_S = 15
HISTORY_DAYS = 365
TOP_N = 20
WEATHER_DAYS = 90
WINDOWS = (3, 7)               # 3 days = Orbit's 72-hour test; 7 days = a week
BANDS = (0.05, 0.20)           # Orbit's two paper ranges
BTC_CONSTRUCTIVE_THRESHOLD = 60

STABLECOIN_IDS = {"tether", "usd-coin", "dai", "first-digital-usd", "ethena-usde", "usds", "paypal-usd",
                  "true-usd", "frax", "binance-usd", "usd1-wlfi"}
WRAPPED_OR_STAKED_IDS = {"wrapped-bitcoin", "wrapped-steth", "staked-ether", "weth", "coinbase-wrapped-btc",
                         "wrapped-eeth", "rocket-pool-eth", "wrapped-beacon-eth", "susds"}
NON_CRYPTO_BETA_IDS = {"figure-heloc", "blackrock-usd-institutional-digital-liquidity-fund"}
EXCLUDED = STABLECOIN_IDS | WRAPPED_OR_STAKED_IDS | NON_CRYPTO_BETA_IDS
WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def http_json(url: str, timeout: float = 30):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def utc_day(t: float | None = None) -> str:
    return datetime.fromtimestamp(time.time() if t is None else t, timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------------------------
# Data fetching (standard library version of the skill's data client, plus Orbit's extras)
# ---------------------------------------------------------------------------------------------
class RegimeData:
    def __init__(self, cache_dir: Path, get=http_json, sleep=time.sleep, top_n: int = TOP_N,
                 delay: float = REQUEST_DELAY_S, log=print):
        self.dir = Path(cache_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.get, self.sleep, self.top_n, self.delay, self.log = get, sleep, top_n, delay, log
        self.requests = 0
        self.funding_source = "none"

    # cache: one file per name per UTC day; old days are pruned
    def _path(self, name: str) -> Path:
        return self.dir / f"{utc_day()}_{name}.json"

    def _cached(self, name: str):
        p = self._path(name)
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                return None
        return None

    def _store(self, name: str, data) -> None:
        tmp = self._path(name).with_suffix(".tmp")
        tmp.write_text(json.dumps(data), encoding="utf-8")
        tmp.replace(self._path(name))

    def prune(self, keep_days: int = 2) -> None:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
        for f in self.dir.glob("20??-??-??_*.json"):
            if f.name[:10] < cutoff:
                f.unlink(missing_ok=True)

    def _get(self, url: str):
        """GET with 429-aware backoff (respects Retry-After), like the skill's client."""
        for attempt in range(MAX_RETRIES):
            self.requests += 1
            try:
                return self.get(url)
            except urllib.error.HTTPError as e:
                if e.code != 429 or attempt == MAX_RETRIES - 1:
                    raise
                ra = (e.headers or {}).get("Retry-After", "") if hasattr(e, "headers") else ""
                wait = float(ra) if str(ra).replace(".", "", 1).isdigit() else BACKOFF_BASE_S * 2 ** attempt
                self.log(f"Market mood: CoinGecko rate limit, waiting {wait:.0f}s")
                self.sleep(wait)
        raise RuntimeError("unreachable")

    def universe(self) -> list[dict]:
        cached = self._cached(f"universe_top{self.top_n}")
        if cached:
            return cached
        per_page = self.top_n + len(EXCLUDED)
        raw = self._get(f"{COINGECKO}/coins/markets?vs_currency=usd&order=market_cap_desc"
                        f"&per_page={per_page}&page=1&sparkline=false")
        uni = [{"id": c["id"], "symbol": str(c["symbol"]).upper()} for c in raw if c["id"] not in EXCLUDED]
        uni = uni[: self.top_n]
        self._store(f"universe_top{self.top_n}", uni)
        return uni

    def history(self, coin_id: str) -> list[float]:
        """Daily closes, oldest first."""
        cached = self._cached(f"hist_{coin_id}")
        if cached:
            return cached
        raw = self._get(f"{COINGECKO}/coins/{coin_id}/market_chart?vs_currency=usd&days={HISTORY_DAYS}"
                        f"&interval=daily")
        closes = [float(p[1]) for p in raw.get("prices", []) if p and p[1] and float(p[1]) > 0]
        self._store(f"hist_{coin_id}", closes)
        self.sleep(self.delay)
        return closes

    def dominance(self) -> float:
        cached = self._cached("dominance_now")
        if cached is not None:
            return cached
        dom = float(self._get(f"{COINGECKO}/global")["data"]["market_cap_percentage"]["btc"])
        self._store("dominance_now", dom)
        hist = self._dominance_history()
        hist[utc_day()] = dom
        (self.dir / "dominance_history.json").write_text(json.dumps(hist, indent=1), encoding="utf-8")
        self.sleep(self.delay)
        return dom

    def _dominance_history(self) -> dict:
        try:
            return json.loads((self.dir / "dominance_history.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def dominance_series(self) -> tuple[list[float], int]:
        """31 consecutive daily readings (the skill's requirement) and how many days are stored so far."""
        hist = self._dominance_history()
        today = datetime.now(timezone.utc).date()
        keys = [(today - timedelta(days=o)).isoformat() for o in range(30, -1, -1)]
        series = [hist[k] for k in keys] if all(k in hist for k in keys) else []
        return series, len(hist)

    def funding(self, symbols: list[str]) -> dict:
        """Latest 8h funding per perp. Binance first; OKX when Binance blocks the server's region."""
        cached = self._cached("funding")
        if cached and cached.get("source", "none") != "none":     # a failed day is retried on the next run
            self.funding_source = cached.get("source", "none")
            return cached.get("rates", {})
        rates, source = {}, "none"
        try:
            by = {r["symbol"]: r for r in self._get(BINANCE_FUNDING)}
            rates = {f"{s}USDT": float(by[f"{s}USDT"]["lastFundingRate"]) for s in symbols if f"{s}USDT" in by}
            source = "Binance"
        except Exception as e:
            self.log(f"Market mood: Binance funding unavailable ({type(e).__name__}: {e}); trying OKX")
        if len(rates) < 2:
            rates = {}
            for s in symbols:
                try:
                    d = self._get(OKX_FUNDING.format(sym=s)).get("data") or []
                    if d and d[0].get("fundingRate") not in (None, ""):
                        rates[f"{s}USDT"] = float(d[0]["fundingRate"])
                except Exception:
                    continue
                self.sleep(0.3)
            source = "OKX" if len(rates) >= 2 else "none"
        rates = {k: v for k, v in rates.items() if math.isfinite(v) and abs(v) < 1}
        self.funding_source = source
        self._store("funding", {"source": source, "rates": rates})
        return rates

    def coin_id_for_mint(self, mint: str) -> str | None:
        """CoinGecko id for a Solana token mint; remembered forever once found."""
        known = {WSOL: "solana", USDC: "usd-coin"}
        if mint in known:
            return known[mint]
        path = self.dir / "mint_ids.json"
        try:
            ids = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            ids = {}
        if mint in ids:
            return ids[mint] or None
        try:
            cid = self._get(f"{COINGECKO}/coins/solana/contract/{mint}").get("id")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
            cid = ""                                   # CoinGecko doesn't list it (typical for brand-new tokens)
        self.sleep(self.delay)
        ids[mint] = cid or ""
        path.write_text(json.dumps(ids, indent=1), encoding="utf-8")
        return cid or None


# ---------------------------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------------------------
def regime_from_snapshot(series: dict, dominance: list, funding: dict) -> dict:
    """The skill's six components and composite, unchanged."""
    btc = series.get("BTC", [])
    alts = {s: c for s, c in series.items() if s != "BTC"}
    trend = calculate_btc_trend(btc)
    up = trend.get("data_available", False) and trend["score"] >= BTC_CONSTRUCTIVE_THRESHOLD
    comps = {"btc_trend": trend,
             "alt_breadth": calculate_alt_breadth(alts),
             "dominance": calculate_dominance_regime(dominance, up),
             "funding": calculate_funding_regime(funding),
             "drawdown_vol": calculate_drawdown_vol(btc),
             "momentum_thrust": calculate_momentum_thrust(series)}
    return {"components": comps, "composite": calculate_composite_score(comps)}


def realised_vol(closes: list[float], days: int = 30) -> float | None:
    rets = [math.log(b / a) for a, b in zip(closes[-days - 1:-1], closes[-days:]) if a > 0 and b > 0]
    return round(statistics.pstdev(rets) * math.sqrt(365) * 100, 1) if len(rets) >= 10 else None


def range_weather(prices: list[float], days: int = WEATHER_DAYS) -> dict | None:
    """How often the price stayed within each band over each window, in the last `days` days."""
    if len(prices) < max(WINDOWS) + 10:
        return None
    last = prices[-(days + max(WINDOWS)):]
    out = {"days": len(last) - max(WINDOWS)}
    for w in WINDOWS:
        segs = [last[i:i + w + 1] for i in range(len(last) - w)]
        for b in BANDS:
            inside = sum(all(abs(x / s[0] - 1) <= b for x in s) for s in segs)
            out[f"in_{w}d_{round(b * 100)}"] = round(100 * inside / len(segs), 1)
        moves = sorted(abs(s[-1] / s[0] - 1) * 100 for s in segs)
        out[f"median_move_{w}d"] = round(moves[len(moves) // 2], 2)
    out["vol30"] = realised_vol(prices)
    return out


def ratio(a: list[float], b: list[float]) -> list[float]:
    n = min(len(a), len(b))
    return [x / y for x, y in zip(a[-n:], b[-n:]) if y > 0]


CALM_PCT, CHOPPY_PCT = 80, 50


def lp_weather_label(w: dict | None, band: int = 20) -> str:
    """Plain words for one range over 3 days (Orbit's 72-hour test): calm 80%+, choppy 50-80%, stormy under 50%."""
    if not w or f"in_3d_{band}" not in w:
        return "unknown"
    x = w[f"in_3d_{band}"]
    return "calm" if x >= CALM_PCT else "choppy" if x >= CHOPPY_PCT else "stormy"


def analyse(data: RegimeData, watches: list[dict]) -> dict:
    t0 = time.time()
    universe = data.universe()
    series, ids = {}, {}
    for coin in universe:
        try:
            closes = data.history(coin["id"])
            if closes:
                series[coin["symbol"]] = closes
                ids[coin["id"]] = closes
        except Exception as e:
            data.log(f"Market mood: {coin['symbol']} history failed ({type(e).__name__}); skipped")
    try:
        data.dominance()
    except Exception as e:
        data.log(f"Market mood: dominance failed ({type(e).__name__})")
    dom, dom_days = data.dominance_series()
    funding = data.funding([c["symbol"] for c in universe[:10]])
    reg = regime_from_snapshot(series, dom, funding)

    sol = ids.get("solana") or data.history("solana")
    sol_trend = calculate_btc_trend(sol) if sol else {"data_available": False, "signal": "no data"}
    sol_view = {"price": round(sol[-1], 2) if sol else None, "score": sol_trend.get("score"),
                "signal": str(sol_trend.get("signal", "")).replace("BTC", "SOL"),
                "data_available": bool(sol_trend.get("data_available")),
                "vol30": realised_vol(sol) if sol else None,
                "change_30d_pct": round((sol[-1] / sol[-31] - 1) * 100, 1) if sol and len(sol) > 31 else None,
                "weather_usd": range_weather(sol) if sol else None}

    tokens = []
    for w in watches:
        row = {"watch": w["label"].strip(), "mint": w["mint"], "coingecko": None,
               "vs_sol": None, "vs_usd": None, "note": ""}
        try:
            cid = data.coin_id_for_mint(w["mint"])
        except Exception as e:
            cid, row["note"] = None, f"lookup failed ({type(e).__name__})"
        if cid:
            row["coingecko"] = cid
            try:
                closes = ids.get(cid) or data.history(cid)
                if "SOL" in w["quotes"] and sol and cid != "solana":
                    row["vs_sol"] = range_weather(ratio(closes, sol))
                if "USDC" in w["quotes"] or cid == "solana":
                    row["vs_usd"] = range_weather(closes)
                if not (row["vs_sol"] or row["vs_usd"]):
                    row["note"] = "too little history"
            except Exception as e:
                row["note"] = f"history failed ({type(e).__name__})"
        elif not row["note"]:
            row["note"] = "not listed on CoinGecko"
        main = row["vs_sol"] or row["vs_usd"]
        row["weather_5"], row["weather_20"] = lp_weather_label(main, 5), lp_weather_label(main, 20)
        for vs in ("vs_sol", "vs_usd"):
            if row[vs]:
                row[vs]["weather_5"], row[vs]["weather_20"] = lp_weather_label(row[vs], 5), lp_weather_label(row[vs], 20)
        tokens.append(row)

    comps = {cid: {"label": COMPONENT_LABELS[cid], "weight": COMPONENT_WEIGHTS[cid],
                   "score": c.get("score"), "signal": c.get("signal", ""),
                   "data_available": bool(c.get("data_available"))} for cid, c in reg["components"].items()}
    data.prune()
    return {"t": round(time.time()), "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "composite": reg["composite"], "components": comps, "universe": len(series),
            "dominance_days": dom_days, "funding_source": data.funding_source,
            "sol": sol_view, "tokens": tokens, "requests": data.requests,
            "seconds": round(time.time() - t0)}


def watches_for_mood(cfg: dict) -> list[dict]:
    """Your own tokens (not the hunter's temporary ones), with the quotes their pools use."""
    out = []
    for w in cfg.get("watches", []):
        if w.get("auto_until") or not w.get("token_mint"):
            continue
        quotes = set()
        for p in w.get("pools", []):
            q = p.get("quote_mint", w.get("quote_mint"))
            quotes.add("SOL" if q == WSOL else "USDC" if q == USDC else "?")
        out.append({"label": w["label"], "mint": w["token_mint"], "quotes": sorted(quotes)})
    return out
