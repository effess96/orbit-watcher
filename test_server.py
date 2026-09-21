"""
Offline tests for the hosted dashboard (server.py). Standard library only.
A fake Solana RPC, fake DexScreener and a local WebSocket server replace the network.
Run:  python -m unittest -v
"""
import asyncio
import base64
import contextlib
import io
import http.client
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

import server as S
import watcher as W
from test_watcher import (POOL_A, POOL_B, SCRIPT, TOKEN, VA_Q, VA_T, VB_Q, VB_T, FakeRpc, FakeSolanaWs,
                          start_accounts)

PASSWORD = "correct horse battery"


def pool_raw(owner, vt, vq):
    data = bytes(8) + W.b58decode(vt) + W.b58decode(vq) + bytes(16)
    return {"owner": owner, "data": [base64.b64encode(data).decode(), "base64"]}


RAW = {POOL_A: pool_raw("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", VA_T, VA_Q),
       POOL_B: pool_raw("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", VB_T, VB_Q)}


def fake_fetch(url):
    def pair(addr, liq):
        return {"chainId": "solana", "pairAddress": addr, "liquidity": {"usd": liq},
                "baseToken": {"address": TOKEN, "symbol": "TEST"},
                "quoteToken": {"address": W.WSOL, "symbol": "SOL"}}
    return [pair(POOL_A, 5e5), pair(POOL_B, 3e5)]


class Dashboard(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.loop = asyncio.new_event_loop()
        ready = threading.Event()

        def run():
            asyncio.set_event_loop(cls.loop)
            cls.fake_ws = FakeSolanaWs(SCRIPT)
            ws_url = cls.loop.run_until_complete(cls.fake_ws.start())
            cls.app = S.App(Path(cls.tmp.name), PASSWORD, fetch=fake_fetch,
                            rpc_factory=lambda url: FakeRpc(start_accounts(), RAW),
                            connect=lambda url: W.WebSocket.connect(ws_url),
                            first_backoff=0.2, eval_delay=0.05)
            cls.task = cls.loop.create_task(cls.app.supervise())
            ready.set()
            cls.loop.run_forever()

        cls.thread = threading.Thread(target=run, daemon=True)
        cls.thread.start()
        ready.wait(5)
        cls.httpd = S.ThreadingHTTPServer(("127.0.0.1", 0), S.make_handler(cls.app))
        cls.port = cls.httpd.server_address[1]
        threading.Thread(target=cls.httpd.serve_forever, daemon=True).start()
        time.sleep(0.3)

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.loop.call_soon_threadsafe(cls.task.cancel)
        cls.loop.call_soon_threadsafe(cls.loop.stop)
        cls.thread.join(3)
        cls.tmp.cleanup()

    # -- helpers --------------------------------------------------------------
    def req(self, method, path, body=None, cookie=None, headers=None, form=False):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        h = dict(headers or {})
        if cookie:
            h["Cookie"] = f"orbit_session={cookie}"
        data = None
        if body is not None:
            data = body.encode() if form else json.dumps(body).encode()
            h["Content-Type"] = "application/x-www-form-urlencoded" if form else "application/json"
        c.request(method, path, data, h)
        r = c.getresponse()
        out = (r.status, dict(r.getheaders()), r.read())
        c.close()
        return out

    def login(self, ip="10.0.0.1"):
        st, h, _ = self.req("POST", "/login", f"password={PASSWORD.replace(' ', '+')}", form=True,
                            headers={"X-Forwarded-For": ip})
        self.assertEqual(st, 303)
        return h["Set-Cookie"].split(";")[0].split("=", 1)[1]

    def api(self, path, body, cookie):
        return self.req("POST", path, body, cookie, {"X-Orbit": "1"})

    def state(self, cookie):
        st, _, b = self.req("GET", "/api/state", cookie=cookie)
        self.assertEqual(st, 200)
        return json.loads(b)

    def wait_for(self, cookie, cond, seconds=10):
        end = time.time() + seconds
        while time.time() < end:
            s = self.state(cookie)
            if cond(s):
                return s
            time.sleep(0.2)
        self.fail(f"condition not met; last state: {s['state']} stats={s['stats']}")

    # -- tests (run in name order) -------------------------------------------
    def test_1_requires_login(self):
        st, h, _ = self.req("GET", "/")
        self.assertEqual((st, h["Location"]), (303, "/login"))
        self.assertEqual(self.req("GET", "/api/state")[0], 401)
        self.assertEqual(self.req("GET", "/download/dislocations.csv")[0], 401)
        self.assertEqual(self.req("GET", "/healthz")[:1], (200,))
        st, h, b = self.req("GET", "/login")
        self.assertEqual(st, 200)
        self.assertNotIn(b"{{NONCE}}", b)
        self.assertIn("nonce-", h["Content-Security-Policy"])

    def test_2_bad_password_and_lockout(self):
        for _ in range(5):
            st, h, _ = self.req("POST", "/login", "password=nope", form=True, headers={"X-Forwarded-For": "9.9.9.9"})
            self.assertEqual((st, h["Location"]), (303, "/login?e=1"))
        st, _, _ = self.req("POST", "/login", f"password={PASSWORD}", form=True, headers={"X-Forwarded-For": "9.9.9.9"})
        self.assertEqual(st, 429)

    def test_3_cookie_is_protected(self):
        st, h, _ = self.req("POST", "/login", f"password={PASSWORD.replace(' ', '+')}", form=True,
                            headers={"X-Forwarded-Proto": "https"})
        cookie = h["Set-Cookie"]
        for flag in ("HttpOnly", "SameSite=Strict", "Secure"):
            self.assertIn(flag, cookie)
        token = cookie.split(";")[0].split("=", 1)[1]
        exp, sig = token.split(".")
        self.assertEqual(self.req("GET", "/api/state", cookie=f"{int(exp) + 999}.{sig}")[0], 401)

    def test_4_dashboard_page(self):
        cookie = self.login()
        st, h, b = self.req("GET", "/", cookie=cookie)
        self.assertEqual(st, 200)
        self.assertNotIn(b"{{NONCE}}", b)
        nonce = h["Content-Security-Policy"].split("nonce-")[1].split("'")[0]
        self.assertIn(f'nonce="{nonce}"'.encode(), b)
        self.assertEqual(h["X-Frame-Options"], "DENY")
        self.assertIn("idle", self.state(cookie)["state"])

    def test_5_post_needs_same_origin_header(self):
        cookie = self.login()
        self.assertEqual(self.req("POST", "/api/discover", {"mint": TOKEN}, cookie)[0], 403)
        st, _, _ = self.req("POST", "/api/discover", {"mint": TOKEN}, cookie,
                            {"X-Orbit": "1", "Origin": "https://evil.example", "Host": f"127.0.0.1:{self.port}"})
        self.assertEqual(st, 403)
        st, _, b = self.api("/api/discover", {"mint": "not a mint!"}, cookie)
        self.assertEqual(st, 400)

    def test_6_discover_add_stream_download(self):
        cookie = self.login()
        st, _, b = self.api("/api/discover", {"mint": TOKEN}, cookie)
        self.assertEqual(st, 200, b)
        found = json.loads(b)
        self.assertEqual(found["watches"][0]["label"], "TEST")
        st, _, b = self.api("/api/watches/add", {"mint": TOKEN}, cookie)
        self.assertEqual((st, json.loads(b)), (200, {"added": ["TEST"]}))
        s = self.wait_for(cookie, lambda s: s["stats"].get("dislocations", 0) >= 1)
        self.assertEqual(s["state"], "running")
        self.assertEqual(s["slot"], 103)
        self.assertEqual(len(s["dislocations"]), 1)
        self.assertEqual(s["dislocations"][0]["slots_open"], "2")
        self.assertIsNotNone(s["watches"][0]["pools"][0]["price"])
        st, h, b = self.req("GET", "/download/dislocations.csv", cookie=cookie)
        self.assertEqual(st, 200)
        self.assertIn(b"TEST", b)
        self.assertIn("attachment", h["Content-Disposition"])
        self.assertEqual(self.req("GET", "/download/..%2Fconfig.json", cookie=cookie)[0], 404)
        self.assertEqual(self.req("GET", "/download/.session_secret", cookie=cookie)[0], 404)
        st, _, b = self.req("GET", "/api/report", cookie=cookie)
        self.assertIn(b"Profitable gaps seen and closed: 1", b)

    def test_7_update_and_remove(self):
        cookie = self.login()
        st, _, b = self.api("/api/watches/update", {"label": "TEST", "cost_sol": 2,
                                                    "fees": {"Raydium AMM v4 " + POOL_A[:4]: 0.01}}, cookie)
        self.assertEqual(st, 200, b)
        cfg = json.loads((Path(self.tmp.name) / "config.json").read_text())
        self.assertEqual(cfg["watches"][0]["cost_sol"], 2.0)
        self.assertEqual(cfg["watches"][0]["pools"][0]["fee"], 0.01)
        self.assertEqual(self.api("/api/watches/update", {"label": "TEST", "shock_pct": 500}, cookie)[0], 400)
        st, _, b = self.api("/api/watches/update", {"label": "TEST",
                                                    "remove_pools": ["PumpSwap " + POOL_B[:4]]}, cookie)
        self.assertEqual(st, 400)
        self.assertIn(b"at least 2 pools", b)
        self.assertEqual(self.api("/api/watches/update", {"label": "TEST", "remove_pools": ["nope"]},
                                  cookie)[0], 400)
        self.assertEqual(self.api("/api/watches/remove", {"label": "nope"}, cookie)[0], 400)
        self.assertEqual(self.api("/api/watches/remove", {"label": "TEST"}, cookie)[0], 200)
        s = self.wait_for(cookie, lambda s: "idle" in s["state"])
        self.assertEqual(s["watches"], [])

    def test_7b_hunter_toggle_and_test_alert(self):
        cookie = self.login()
        self.assertEqual(self.api("/api/hunter", {"enabled": True}, cookie)[0], 200)
        self.assertTrue(self.state(cookie)["hunter"]["enabled"])
        self.api("/api/hunter", {"enabled": False}, cookie)
        self.assertFalse(self.state(cookie)["hunter"]["enabled"])
        st, _, b = self.api("/api/test-alert", {}, cookie)
        self.assertEqual(json.loads(b)["enabled"], False)

    def test_8_logout(self):
        cookie = self.login()
        st, h, _ = self.api("/api/logout", {}, cookie)
        self.assertEqual(st, 200)
        self.assertIn("Max-Age=0", h["Set-Cookie"])


def gecko(pool, base, quote, liq):
    return {"data": [{"id": "solana_" + pool, "type": "pool",
                      "attributes": {"address": pool, "name": "TEST / SOL", "reserve_in_usd": str(liq)},
                      "relationships": {"base_token": {"data": {"id": "solana_" + base}},
                                        "quote_token": {"data": {"id": "solana_" + quote}},
                                        "dex": {"data": {"id": "raydium"}}}}]}


class HunterAndAlerts(unittest.TestCase):
    def app(self, gecko_data):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.sent = []
        fetch = lambda url: gecko_data if "geckoterminal" in url else fake_fetch(url)
        return S.App(Path(tmp.name), PASSWORD, fetch=fetch, rpc_factory=lambda url: FakeRpc(start_accounts(), RAW),
                     notifier=S.Notifier("t", "c", sender=self.sent.append))

    def test_hunter_adds_temporary_watch_once_and_expires_it(self):
        app = self.app(gecko(POOL_A, TOKEN, W.WSOL, 50_000))
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(app.hunt_once(), ["NEW TEST"])
            self.assertEqual(app.hunt_once(), [])                             # same token not re-added
        cfg = app.config()
        self.assertTrue(cfg["watches"][0]["auto_until"] > time.time())
        self.assertEqual(app.hunter["recent"][0]["result"], "watching 2 pools for 30 min")
        cfg["watches"][0]["auto_until"] = time.time() - 1
        app.save_config(cfg)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(app.expire_auto(), ["NEW TEST"])
        self.assertEqual(app.config()["watches"], [])

    def test_hunter_skips_small_and_non_sol_pools(self):
        app = self.app(gecko(POOL_A, TOKEN, W.WSOL, 500))
        self.assertEqual(app.hunt_once(), [])
        app = self.app(gecko(POOL_A, TOKEN, W.b58encode(bytes([99] * 32)), 1e6))
        self.assertEqual(app.hunt_once(), [])

    def test_alerts_for_tradable_gap_and_big_shock(self):
        app = self.app({"data": []})
        row = {"watch": "X", "buy_pool": "A", "sell_pool": "B", "peak_net_gap_pct": "0.5", "slots_open": "3",
               "seconds_open": "1.2", "depth_net_sol_0_25": "0.001", "depth_net_sol_1": "0.004",
               "depth_net_sol_5": "-0.2", "tradable": "yes"}
        with contextlib.redirect_stdout(io.StringIO()):
            app.handle_event("gap_closed", row)
            app.handle_event("gap_closed", {**row, "tradable": "no"})            # not alerted
            app.notifier._last = 0
            app.handle_event("big_shock", {"watch": "X", "pool": "A", "move_pct": 5.1, "slot": 9, "gap_visible": False})
        time.sleep(0.2)
        self.assertEqual(len(self.sent), 2)
        self.assertIn("0.00400 SOL", self.sent[0])
        self.assertIn("moved 5.1%", self.sent[1])
        self.assertEqual(len(app.alerts), 2)

    def test_notifier_off_without_credentials(self):
        self.assertFalse(S.Notifier("", "").send("x"))


class Helpers(unittest.TestCase):
    def test_tail_csv_large_file(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "x.csv"
            with open(p, "w") as fh:
                fh.write("a,b\n" + "".join(f"{i},{'x' * 50}\n" for i in range(20000)))
            rows = S.tail_csv(p, 3)
            self.assertEqual([r["a"] for r in rows], ["19999", "19998", "19997"])

    def test_raw_log_rotation(self):
        with tempfile.TemporaryDirectory() as d:
            app = S.App(Path(d), PASSWORD)
            app.recorder = W.Recorder(app.out)
            app.recorder.raw_fh.write("x" * 100)
            app.recorder.raw_fh.flush()
            old = S.RAW_ROTATE_BYTES
            S.RAW_ROTATE_BYTES = 10
            try:
                app.maintain()
            finally:
                S.RAW_ROTATE_BYTES = old
            self.assertTrue((app.out / "raw_updates.1.jsonl").exists())
            app.recorder.raw(1.0, "v", 1, 2)
            app.recorder.close()
            self.assertEqual((app.out / "raw_updates.jsonl").read_text().count("\n"), 1)

    def test_refuses_short_password(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, {"ADMIN_PASSWORD": "short"}):
            with self.assertRaises(SystemExit):
                S.main()


if __name__ == "__main__":
    unittest.main()
