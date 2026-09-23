"""Tests for the market mood module, against a fake CoinGecko / Binance / OKX (no network)."""
import contextlib
import io
import json
import math
import os
import tempfile
import unittest
import urllib.error
from pathlib import Path

os.environ.setdefault("ORBIT_AUDIT", "0")

import regime as R
import server as S
import watcher as W

BONK = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
WIF = "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"
NEWT = "NewTokenNotOnCoinGecko1111111111111111111111"


def closes(start, daily, n=400, wobble=0.0):
    return [start * (1 + daily) ** i * (1 + wobble * math.sin(i)) for i in range(n)]


class FakeWeb:
    """Answers the URLs RegimeData asks for, in the real APIs' shapes."""

    def __init__(self, binance_blocked=False, rate_limit_once=False):
        self.calls, self.binance_blocked, self.rate_limit_once = [], binance_blocked, rate_limit_once
        coins = ["bitcoin", "ethereum", "tether", "solana", "ripple", "dogecoin", "cardano", "tron",
                 "chainlink", "avalanche-2", "sui", "wrapped-bitcoin"]
        self.markets = [{"id": c, "symbol": {"bitcoin": "btc", "ethereum": "eth", "solana": "sol"}.get(c, c[:4])}
                        for c in coins]
        self.hist = {c: closes(100, 0.002, wobble=0.01) for c in coins}
        self.hist["solana"] = closes(150, 0.001, wobble=0.005)
        self.hist["bonk"] = closes(1e-5, 0.001, wobble=0.004)           # calm against SOL
        self.hist["dogwifcoin"] = closes(2, 0.0, wobble=0.25)           # wild

    def __call__(self, url):
        self.calls.append(url)
        if self.rate_limit_once and len(self.calls) == 1:
            raise urllib.error.HTTPError(url, 429, "Too Many", {"Retry-After": "1"}, None)
        if "/coins/markets" in url:
            return self.markets
        if "/market_chart" in url:
            cid = url.split("/coins/")[1].split("/")[0]
            return {"prices": [[i, p] for i, p in enumerate(self.hist[cid])]}
        if url.endswith("/global"):
            return {"data": {"market_cap_percentage": {"btc": 57.3}}}
        if "/contract/" in url:
            mint = url.rsplit("/", 1)[1]
            ids = {BONK: "bonk", WIF: "dogwifcoin"}
            if mint not in ids:
                raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
            return {"id": ids[mint]}
        if "binance" in url:
            if self.binance_blocked:
                raise urllib.error.HTTPError(url, 451, "Unavailable For Legal Reasons", {}, None)
            return [{"symbol": "BTCUSDT", "lastFundingRate": "0.0001"}, {"symbol": "ETHUSDT", "lastFundingRate": "0.00008"},
                    {"symbol": "SOLUSDT", "lastFundingRate": "0.00005"}]
        if "okx" in url:
            sym = url.split("instId=")[1].split("-")[0]
            return {"data": [{"fundingRate": "0.00012"}]} if sym in ("BTC", "ETH", "SOL") else {"data": []}
        raise AssertionError(url)


WATCHES = [{"label": "Bonk", "mint": BONK, "quotes": ["SOL", "USDC"]},
           {"label": "$WIF", "mint": WIF, "quotes": ["SOL"]},
           {"label": "NEW", "mint": NEWT, "quotes": ["SOL"]}]


class MoodTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.slept = []

    def tearDown(self):
        self.tmp.cleanup()

    def data(self, web):
        return R.RegimeData(self.dir / "cache", get=web, sleep=self.slept.append, delay=0, log=lambda m: None)

    def test_full_run_scores_regime_and_lp_weather(self):
        web = FakeWeb()
        m = R.analyse(self.data(web), WATCHES)
        self.assertEqual(m["universe"], 10)                               # tether + wrapped BTC left out
        self.assertIn(m["composite"]["zone"], ("RISK_ON", "NEUTRAL", "RISK_OFF"))
        self.assertIsNotNone(m["composite"]["score"])
        self.assertFalse(m["components"]["dominance"]["data_available"])   # needs 31 days of readings first
        self.assertEqual(m["dominance_days"], 1)
        self.assertEqual(m["funding_source"], "Binance")
        self.assertTrue(m["components"]["funding"]["data_available"])
        self.assertAlmostEqual(m["sol"]["price"], round(web.hist["solana"][-1], 2))
        rows = {t["watch"]: t for t in m["tokens"]}
        self.assertEqual(rows["Bonk"]["weather"], "calm")
        self.assertEqual(rows["$WIF"]["weather"], "stormy")
        self.assertIsNotNone(rows["Bonk"]["vs_usd"])                     # it has a USDC pool too
        self.assertIsNone(rows["$WIF"]["vs_usd"])
        self.assertEqual(rows["NEW"]["note"], "not listed on CoinGecko")
        self.assertEqual(rows["NEW"]["weather"], "unknown")

    def test_second_run_same_day_uses_the_cache(self):
        web = FakeWeb()
        R.analyse(self.data(web), WATCHES)
        n = len(web.calls)
        R.analyse(self.data(web), WATCHES)
        self.assertEqual(len(web.calls), n)

    def test_funding_falls_back_to_okx_when_binance_blocks_the_region(self):
        m = R.analyse(self.data(FakeWeb(binance_blocked=True)), WATCHES)
        self.assertEqual(m["funding_source"], "OKX")
        self.assertTrue(m["components"]["funding"]["data_available"])

    def test_rate_limit_waits_and_retries(self):
        web = FakeWeb(rate_limit_once=True)
        R.analyse(self.data(web), WATCHES)
        self.assertIn(1.0, self.slept)                                    # honoured Retry-After

    def test_range_weather_maths(self):
        flat = [100.0] * 100
        w = R.range_weather(flat)
        self.assertEqual((w["in_3d_5"], w["in_7d_20"], w["median_move_7d"]), (100.0, 100.0, 0.0))
        jumpy = [100.0 if i % 2 else 110.0 for i in range(100)]            # 10% swings every day
        w = R.range_weather(jumpy)
        self.assertEqual(w["in_3d_5"], 0.0)
        self.assertEqual(w["in_3d_20"], 100.0)
        self.assertIsNone(R.range_weather([1.0] * 5))
        self.assertEqual(R.lp_weather_label({"in_3d_20": 95}), "calm")
        self.assertEqual(R.lp_weather_label({"in_3d_20": 80}), "choppy")
        self.assertEqual(R.lp_weather_label({"in_3d_20": 50}), "stormy")

    def test_dominance_history_turns_on_after_31_days(self):
        d = self.data(FakeWeb())
        from datetime import datetime, timedelta, timezone
        today = datetime.now(timezone.utc).date()
        (d.dir / "dominance_history.json").write_text(json.dumps(
            {(today - timedelta(days=o)).isoformat(): 55 + o * 0.1 for o in range(31)}))
        series, n = d.dominance_series()
        self.assertEqual((len(series), n), (31, 31))

    def test_hunter_tokens_are_left_out(self):
        cfg = {"watches": [{"label": "A", "token_mint": BONK, "pools": [{"quote_mint": R.WSOL}]},
                           {"label": "NEW X", "token_mint": NEWT, "auto_until": 1, "pools": []}]}
        self.assertEqual(R.watches_for_mood(cfg), [{"label": "A", "mint": BONK, "quotes": ["SOL"]}])

    def test_server_saves_logs_reports_and_alerts_on_zone_change(self):
        sent = []
        app = S.App(self.dir / "app", "x" * 12, notifier=S.Notifier(sender=lambda *a: sent.append(a)))
        app.notifier.enabled = True
        app.save_config({"watches": []})
        with contextlib.redirect_stdout(io.StringIO()):
            m = app.run_mood_once(self.data(FakeWeb()))
        self.assertTrue((app.out / "regime.json").exists())
        self.assertEqual(len((app.out / "regime_log.jsonl").read_text().splitlines()), 1)
        self.assertIn("MARKET MOOD", W.report(app.out))
        self.assertEqual(app.snapshot()["mood"]["composite"], m["composite"])
        app.mood["composite"]["zone"] = "SOMETHING_ELSE"
        with contextlib.redirect_stdout(io.StringIO()):
            app.run_mood_once(self.data(FakeWeb()))
        self.assertEqual(len(sent), 1)                                    # one alert for the change
        self.assertIn("market mood changed", str(sent[0]))

    def test_vendored_scoring_is_the_skills_own(self):
        self.assertEqual(sum(R.COMPONENT_WEIGHTS.values()), 1.0)
        self.assertTrue((Path(R.__file__).parent / "regime_skill" / "NOTICE").exists())


if __name__ == "__main__":
    unittest.main()
