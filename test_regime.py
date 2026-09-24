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
        self.assertEqual((rows["Bonk"]["weather_5"], rows["Bonk"]["weather_20"]), ("calm", "calm"))
        self.assertEqual((rows["$WIF"]["weather_5"], rows["$WIF"]["weather_20"]), ("stormy", "stormy"))
        self.assertEqual(rows["Bonk"]["vs_sol"]["weather_5"], "calm")
        self.assertIsNotNone(rows["Bonk"]["vs_usd"])                     # it has a USDC pool too
        self.assertIsNone(rows["$WIF"]["vs_usd"])
        self.assertEqual(rows["NEW"]["note"], "not listed on CoinGecko")
        self.assertEqual(rows["NEW"]["weather_5"], "unknown")

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
        self.assertEqual(R.lp_weather_label({"in_3d_20": 70}), "choppy")
        self.assertEqual(R.lp_weather_label({"in_3d_20": 40}), "stormy")
        # real readings from the first live run: Bonk vs SOL was 35% at +/-5% and 97% at +/-20%
        live = {"in_3d_5": 35, "in_3d_20": 97}
        self.assertEqual((R.lp_weather_label(live, 5), R.lp_weather_label(live, 20)), ("stormy", "calm"))

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
        import regime_skill
        up = Path("/tmp/claude-0/-home-claude/2033b762-bf82-5962-a954-4059d8a56af0/scratchpad/repos/cts/skills/"
                  "crypto-regime-analyzer/scripts")
        if up.exists():                                   # word-for-word copy of the skill's code
            self.assertEqual(regime_skill.SOURCES["scorer"], (up / "scorer.py").read_text())
            for f in (up / "calculators").glob("*_calculator.py"):
                self.assertEqual(regime_skill.SOURCES[f"calculators.{f.stem}"], f.read_text())


if __name__ == "__main__":
    unittest.main()


class FakeMintRpc:
    """getAccountInfo / getTokenLargestAccounts in jsonParsed shape."""

    def __init__(self, mint_auth=None, freeze=None, ext=None, largest=None, supply=1000):
        self.info = {"value": {"owner": W.TOKEN_2022 if ext else "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                               "data": {"parsed": {"info": {"supply": str(supply), "mintAuthority": mint_auth,
                                                            "freezeAuthority": freeze,
                                                            "extensions": [{"extension": e} for e in (ext or [])]}}}}}
        self.largest = largest or []

    def call(self, method, params):
        if method == "getAccountInfo":
            return self.info
        if method == "getTokenLargestAccounts":
            return {"value": [{"address": a, "amount": str(n)} for a, n in self.largest]}
        raise AssertionError(method)


class V14Tests(unittest.TestCase):
    def test_token_safety_levels(self):
        ok = W.token_safety(FakeMintRpc(largest=[("vault", 600), ("a", 100), ("b", 50)]), "m", {"vault"})
        self.assertEqual((ok["level"], ok["top10_pct"]), ("ok", 15.0))              # the pool's own vault left out
        risky = W.token_safety(FakeMintRpc(freeze="X"), "m")
        self.assertEqual(risky["level"], "risky")
        self.assertIn("frozen", risky["flags"][0])
        caution = W.token_safety(FakeMintRpc(ext=["transferFeeConfig"], largest=[("a", 700)]), "m")
        self.assertEqual((caution["level"], caution["program"]), ("caution", "Token-2022"))
        self.assertEqual(len(caution["flags"]), 2)                                  # fee + concentration

    def test_idle_pools_are_left_out_of_live_gaps_only(self):
        import test_watcher as TW
        with tempfile.TemporaryDirectory() as d:
            rec = W.Recorder(Path(d), keep_raw=False)
            e = W.Engine(TW.config(), rec)
            w = e.watches[0]
            a, b = w.pools
            a.base, a.quote, b.base, b.quote = 10 ** 12, 10 ** 12, 10 ** 12, 11 * 10 ** 11   # B is 10% dearer
            self.assertAlmostEqual(e.max_gap_pct(w, active_only=True), 10.0, places=6)
            b.last_update -= W.IDLE_POOL_S + 1                                              # B stops updating
            self.assertIsNone(e.max_gap_pct(w, active_only=True))                           # left out on the dashboard
            self.assertAlmostEqual(e.max_gap_pct(w), 10.0, places=6)                        # detection unchanged
            rec.close()

    def test_spread_is_the_average_over_positions_with_an_hour(self):
        rows = [{"range_pct": 5, "hours": 20, "net_vs_hold_pct": 2.0, "net_sol_per_day": 0.05},
                {"range_pct": 5, "hours": 20, "net_vs_hold_pct": -4.0, "net_sol_per_day": -0.1},
                {"range_pct": 5, "hours": 0.2, "net_vs_hold_pct": 9.0},
                {"range_pct": 20, "hours": 20, "net_vs_hold_pct": -0.5, "net_sol_per_day": -0.01}]
        sp = W.lp_spread(rows)
        self.assertEqual((sp["5"]["n"], sp["5"]["ahead"], sp["5"]["avg_net_pct"]), (2, 1, -1.0))
        self.assertEqual(sp["20"]["avg_net_pct"], -0.5)
        self.assertNotIn("5 auto", sp)
        rows.append({"range_pct": 5, "hours": 5, "net_vs_hold_pct": 1.0, "strategy": "rebalance"})
        self.assertEqual(W.lp_spread(rows)["5 auto"]["avg_net_pct"], 1.0)
        self.assertIn("SPREAD EVENLY", "\n".join(W.lp_spread_lines([("x", r) for r in rows])))

    def test_rebalancing_position_recentres_after_ten_minutes_and_pays_for_it(self):
        import test_watcher as TW
        pe = TW.PoolEarningsTests()
        p = pe.pool()
        static, auto = W.LpSim(p, 0.05, 0.0), W.LpSim(p, 0.05, 0.0, rebalance=True)
        pe.move(p, 1.1)                                     # price +21%: far outside a +/-5% range
        for t in (10.0, 300.0, 700.0):
            static.observe(t)
            auto.observe(t)
        self.assertEqual((static.rebalances, auto.rebalances), (0, 1))
        self.assertTrue(auto.sa <= p.sq <= auto.sb)         # back in range around the new price
        r = auto.result()
        self.assertEqual(r["strategy"], "rebalance")
        self.assertGreater(r["rebalance_cost_pct"], 0)
        auto.observe(2000.0)
        self.assertEqual(auto.rebalances, 1)                # in range now: no further moves
        again = W.LpSim(p, 0.05, 0.0, auto.state(), rebalance=True)   # survives a restart
        self.assertEqual((again.rebalances, again.rebalance), (1, True))
        old = static.state()                                # positions saved before v1.4 stay static
        self.assertFalse(W.LpSim(p, 0.05, 0.0, old).rebalance)

    def test_watchdog_alerts_once_on_stall_and_on_recovery(self):
        sent = []
        with tempfile.TemporaryDirectory() as d:
            app = S.App(Path(d), "x" * 12, notifier=S.Notifier(sender=lambda *a: sent.append(a)))
            app.notifier.enabled = True

            class E:
                updates = 5
            app.engine = E()
            with contextlib.redirect_stdout(io.StringIO()):
                app.check_health(now=1000)
                app.check_health(now=1000 + S.STALL_ALERT_S - 1)
                self.assertEqual(sent, [])
                app.check_health(now=1000 + S.STALL_ALERT_S + 1)
                app.check_health(now=1000 + S.STALL_ALERT_S + 100)
                self.assertEqual(len(sent), 1)
                self.assertIn("no pool updates", str(sent[0]))
                E.updates = 9
                app.check_health(now=5000)
                self.assertEqual(len(sent), 2)
                self.assertIn("recovered", str(sent[1]))
                app.mood_status["fails"] = S.MOOD_FAILS_ALERT
                app.check_health(now=5001)
                app.check_health(now=5002)
                self.assertEqual(len(sent), 3)


class VerdictTests(unittest.TestCase):
    @staticmethod
    def pos(rng, final, day1, strat="static", hours=72.5):
        return {"range_pct": rng, "strategy": strat, "hours": hours, "net_vs_hold_pct": final, "day1_net_pct": day1}

    def test_pending_until_72_hours(self):
        out = "\n".join(W.lp_verdict([self.pos(5, 1.0, 1.0, hours=30)]))
        self.assertIn("Pending", out)
        self.assertIn("30.0 of 72", out)

    def test_fail_when_every_strategy_is_behind_holding(self):
        rows = [self.pos(5, -2.0, -1.0) for _ in range(8)] + [self.pos(20, -0.5, -0.2) for _ in range(8)]
        out = "\n".join(W.lp_verdict(rows))
        self.assertIn("RESULT: FAIL", out)

    def test_pass_needs_a_positive_strategy_and_leaders_that_persist(self):
        rows = [self.pos(20, 2 * (0.5 + i * 0.1), 0.5 + i * 0.1) for i in range(8)]   # leaders pull further ahead
        self.assertIn("RESULT: PASS for +/-20%", "\n".join(W.lp_verdict(rows)))
        flipped = [self.pos(20, 2.0 - i * 0.2, 0.5 + i * 0.1) for i in range(8)]  # leaders fall back
        out = "\n".join(W.lp_verdict(flipped))
        self.assertIn("did not stay ahead", out)
        self.assertIn("RESULT: FAIL", out)

    def test_day1_checkpoint_and_vs_keeping_quote(self):
        import test_watcher as TW
        pe = TW.PoolEarningsTests()
        p = pe.pool()
        sim = W.LpSim(p, 0.2, 0.0)
        sim.observe(3600.0)
        self.assertIsNone(sim.result()["day1_net_pct"])
        sim.observe(W.DAY1_S + 1)
        r = sim.result()
        self.assertIsNotNone(r["day1_net_pct"])
        self.assertAlmostEqual(r["vs_quote_pct"], 0.0, places=6)                 # price unchanged, no fees
        pe.move(p, 0.95)                                                         # token falls ~10% vs SOL
        sim.observe(W.DAY1_S + 100)
        r = sim.result()
        self.assertLess(r["vs_quote_pct"], r["net_vs_hold_pct"])                 # worse than keeping SOL
