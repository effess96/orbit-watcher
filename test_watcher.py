"""
Offline tests for watcher.py. Standard library only; no network needed.
A fake Solana RPC and a local WebSocket server stand in for the real network.
Run:  python -m unittest -v
"""
import asyncio
import base64
import csv
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import watcher as W

TOKEN = W.b58encode(bytes([7] * 32))
POOL_A, POOL_B = W.b58encode(bytes([11] * 32)), W.b58encode(bytes([12] * 32))
VA_T, VA_Q = W.b58encode(bytes([21] * 32)), W.b58encode(bytes([22] * 32))
VB_T, VB_Q = W.b58encode(bytes([31] * 32)), W.b58encode(bytes([32] * 32))


def token_account(mint, amount, decimals):
    return {"data": {"parsed": {"type": "account", "info": {
        "mint": mint, "tokenAmount": {"amount": str(amount), "decimals": decimals}}}}}


def config(cost=0.0005, minimum=0.0005, shock=0.5):
    return {"watches": [{
        "label": "TEST/SOL", "token_mint": TOKEN, "quote_mint": W.WSOL,
        "token_decimals": 6, "quote_decimals": 9, "cost_quote": cost,
        "min_profit_quote": minimum, "shock_pct": shock,
        "pools": [
            {"name": "A", "address": POOL_A, "base_vault": VA_T, "quote_vault": VA_Q, "fee": 0.0025},
            {"name": "B", "address": POOL_B, "base_vault": VB_T, "quote_vault": VB_Q, "fee": 0.0025}]}]}


# Both pools start at 1,000,000 tokens / 1,000 SOL => price 0.001 SOL.
START = {VA_T: 10 ** 12, VA_Q: 10 ** 12, VB_T: 10 ** 12, VB_Q: 10 ** 12}


class FakeRpc:
    """Answers getMultipleAccounts / getAccountInfo from a dictionary."""

    def __init__(self, accounts, raw_accounts=None, slot=100):
        self.accounts, self.raw, self.slot, self.calls = accounts, raw_accounts or {}, slot, []

    def call(self, method, params):
        self.calls.append(method)
        if method == "getMultipleAccounts":
            return {"context": {"slot": self.slot}, "value": [self.accounts.get(k) for k in params[0]]}
        if method == "getAccountInfo":
            return {"context": {"slot": self.slot}, "value": self.raw.get(params[0])}
        raise AssertionError(method)


def start_accounts():
    return {VA_T: token_account(TOKEN, START[VA_T], 6), VA_Q: token_account(W.WSOL, START[VA_Q], 9),
            VB_T: token_account(TOKEN, START[VB_T], 6), VB_Q: token_account(W.WSOL, START[VB_Q], 9)}


class TempDirCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def rows(self, name, folder=None):
        with open((folder or self.dir) / name, newline="") as fh:
            return list(csv.DictReader(fh))


class Base58(unittest.TestCase):
    def test_roundtrip_real_addresses(self):
        for addr in (W.USDC, W.WSOL, "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"):
            raw = W.b58decode(addr)
            self.assertEqual(len(raw), 32)
            self.assertEqual(W.b58encode(raw), addr)

    def test_all_zero_key(self):
        self.assertEqual(W.b58encode(bytes(32)), "1" * 32)


class PoolMaths(unittest.TestCase):
    def test_equal_pools_no_profit(self):
        self.assertEqual(W.best_round_trip((1e6, 1000, .0025), (1e6, 1000, .0025)), (0.0, 0.0))

    def test_small_gap_eaten_by_fees(self):
        # 0.3% gap < two 0.25% fees
        self.assertEqual(W.best_round_trip((1e6, 1000, .0025), (1e6, 1003, .0025))[1], 0.0)

    def test_real_gap_found_and_optimal(self):
        cheap, rich = (1e6, 1000, .0025), (1e6, 1050, .0025)
        size, profit = W.best_round_trip(cheap, rich)
        self.assertGreater(profit, 0)
        # neighbours of the chosen size must not do better
        f = lambda q: W.swap_out(W.swap_out(q, 1000, 1e6, .0025), 1e6, 1050, .0025) - q
        self.assertGreaterEqual(profit + 1e-9, f(size * 0.9))
        self.assertGreaterEqual(profit + 1e-9, f(size * 1.1))


class Engine(TempDirCase):
    def engine(self, **kw):
        rec = W.Recorder(self.dir)
        self.addCleanup(rec.close)
        e = W.Engine(config(**kw), rec)
        for v, a in START.items():
            e.on_vault(v, a, 100, 1000.0)
        e.evaluate(1000.0)
        return e

    def test_shock_opens_and_later_close_is_timed(self):
        e = self.engine()
        # A big sell into pool A: +5% tokens, -4.8% SOL -> A becomes cheap
        e.on_vault(VA_T, 1_050_000 * 10 ** 6, 101, 1000.4)
        e.on_vault(VA_Q, 952_380_952_381, 101, 1000.4)
        e.evaluate(1000.5)
        self.assertEqual(e.open_count(), 1)
        self.assertEqual(e.stats["shocks"], 1)
        # an arbitrageur rebalances: B moves to the same price two slots later
        e.on_vault(VB_T, 1_050_000 * 10 ** 6, 103, 1001.2)
        e.on_vault(VB_Q, 952_380_952_381, 103, 1001.2)
        e.evaluate(1001.3)
        self.assertEqual(e.open_count(), 0)
        e.rec.dislocations.fh.flush()
        d = self.rows("dislocations.csv")
        self.assertEqual(len(d), 1)
        self.assertEqual(d[0]["buy_pool"], "A")
        self.assertEqual(d[0]["sell_pool"], "B")
        self.assertEqual(int(d[0]["slots_open"]), 2)
        self.assertAlmostEqual(float(d[0]["seconds_open"]), 0.8, places=3)
        self.assertGreater(float(d[0]["peak_net_quote"]), 0)
        s = self.rows("shocks.csv")
        self.assertEqual(s[0]["profitable_gap_visible"], "yes")

    def test_small_move_is_not_a_gap(self):
        e = self.engine()
        e.on_vault(VA_Q, int(START[VA_Q] * 1.004), 101, 1000.4)   # +0.4% price
        e.evaluate(1000.5)
        self.assertEqual(e.open_count(), 0)
        self.assertEqual(e.stats["shocks"], 0)  # below 0.5% shock threshold

    def test_cost_assumption_blocks_gap(self):
        e = self.engine(cost=100.0)
        e.on_vault(VA_T, 1_050_000 * 10 ** 6, 101, 1000.4)
        e.evaluate(1000.5)
        self.assertEqual(e.open_count(), 0)
        self.assertEqual(e.stats["shocks"], 1)
        e.rec.shocks.fh.flush()
        self.assertEqual(self.rows("shocks.csv")[0]["profitable_gap_visible"], "no")

    def test_out_of_order_update_ignored(self):
        e = self.engine()
        e.on_vault(VA_T, 5, 99, 1000.4)
        self.assertEqual(e.watches[0].pools[0].base, START[VA_T])

    def test_interrupt_drops_open_gaps(self):
        e = self.engine()
        e.on_vault(VA_T, 1_050_000 * 10 ** 6, 101, 1000.4)
        e.evaluate(1000.5)
        e.interrupt()
        self.assertEqual(e.open_count(), 0)
        self.assertEqual(e.stats["interrupted"], 1)


class VaultDiscovery(unittest.TestCase):
    def test_finds_largest_vault_per_mint_in_unaligned_layout(self):
        decoy = W.b58encode(bytes([41] * 32))     # token account of another mint
        small = W.b58encode(bytes([42] * 32))     # small token account, same mint
        # PumpSwap-like layout: 8 discriminator + 1 + 2, then addresses (offset 11, unaligned)
        data = bytes(8) + b"\x01" + b"\x02\x00" + b"".join(W.b58decode(k) for k in
                                                        (decoy, TOKEN, W.WSOL, small, VA_T, VA_Q)) + bytes(40)
        accts = {VA_T: token_account(TOKEN, 900, 6), VA_Q: token_account(W.WSOL, 800, 9),
                 small: token_account(TOKEN, 5, 6), decoy: token_account(W.USDC, 10 ** 9, 6)}
        found = W.find_vaults(FakeRpc(accts), data, {TOKEN, W.WSOL})
        self.assertEqual(found[TOKEN], (VA_T, 900, 6))
        self.assertEqual(found[W.WSOL], (VA_Q, 800, 9))


class Discover(unittest.TestCase):
    def test_builds_watch_and_skips_unsupported(self):
        def pool_raw(owner, vt, vq):
            data = bytes(8) + W.b58decode(vt) + W.b58decode(vq) + bytes(16)
            return {"owner": owner, "data": [base64.b64encode(data).decode(), "base64"]}

        dlmm = W.b58encode(bytes([13] * 32))
        raw = {POOL_A: pool_raw("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", VA_T, VA_Q),
               POOL_B: pool_raw("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", VB_T, VB_Q),
               dlmm: {"owner": "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", "data": ["", "base64"]}}

        def pair(addr, liq):
            return {"chainId": "solana", "pairAddress": addr, "liquidity": {"usd": liq},
                    "baseToken": {"address": TOKEN, "symbol": "TEST"},
                    "quoteToken": {"address": W.WSOL, "symbol": "SOL"}}

        fetch = lambda url: [pair(POOL_A, 5e5), pair(POOL_B, 3e5), pair(dlmm, 9e5)]
        watches, notes = W.discover(TOKEN, FakeRpc(start_accounts(), raw), fetch)
        self.assertEqual(len(watches), 1)
        w = watches[0]
        self.assertEqual(w["label"], "TEST")
        self.assertEqual(w["token_decimals"], 6)
        self.assertEqual([p["quote_decimals"] for p in w["pools"]], [9, 9])
        self.assertEqual([p["base_vault"] for p in w["pools"]], [VA_T, VB_T])
        self.assertEqual([p["fee"] for p in w["pools"]], [0.0025, 0.0030])
        self.assertTrue(any("Meteora DLMM" in n for n in notes))
        W.Watch(w)  # the produced config is valid

    def test_dust_pools_skipped(self):
        def pair(addr, liq):
            return {"chainId": "solana", "pairAddress": addr, "liquidity": {"usd": liq},
                    "baseToken": {"address": TOKEN, "symbol": "TEST"},
                    "quoteToken": {"address": W.WSOL, "symbol": "SOL"}}
        rpc = FakeRpc(start_accounts(), {})
        watches, notes = W.discover(TOKEN, rpc, lambda url: [pair(POOL_A, 23), pair(POOL_B, 9_999)])
        self.assertEqual(watches, [])
        self.assertEqual(sum("below $10,000" in n for n in notes), 2)
        self.assertNotIn("getAccountInfo", rpc.calls)  # dust is skipped before any RPC work


class WebSocketFrames(unittest.TestCase):
    def test_roundtrip_all_length_forms(self):
        async def go():
            for n in (5, 300, 70000):
                payload = os.urandom(n)
                for mask in (True, False):
                    reader = asyncio.StreamReader()
                    reader.feed_data(W.ws_encode_frame(0x2, payload, mask))
                    fin, op, got = await W.ws_read_frame(reader)
                    self.assertEqual((fin, op, got), (True, 0x2, payload))
        asyncio.run(go())


class FakeSolanaWs:
    """Local WebSocket server that behaves like Solana's accountSubscribe stream."""

    def __init__(self, script, drop_first=False):
        self.script, self.drop_first, self.connections, self.pongs = script, drop_first, 0, 0

    async def start(self):
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        return f"ws://127.0.0.1:{self.server.sockets[0].getsockname()[1]}/"

    async def send(self, writer, obj):
        writer.write(W.ws_encode_frame(0x1, json.dumps(obj).encode(), False))
        await writer.drain()

    async def handle(self, reader, writer):
        self.connections += 1
        head = (await reader.readuntil(b"\r\n\r\n")).decode()
        key = [l.split(":", 1)[1].strip() for l in head.split("\r\n") if l.lower().startswith("sec-websocket-key")][0]
        writer.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                      f"Sec-WebSocket-Accept: {W.ws_accept_key(key)}\r\n\r\n").encode())
        await writer.drain()
        subs = {}
        try:
            got_slot = False
            while len(subs) < 4 or not got_slot:
                _, op, payload = await W.ws_read_frame(reader)
                if op == 0x1:
                    m = json.loads(payload)
                    if m["method"] == "slotSubscribe":
                        got_slot = True
                        await self.send(writer, {"jsonrpc": "2.0", "id": m["id"], "result": 999})
                        continue
                    assert m["method"] == "accountSubscribe"
                    subs[m["params"][0]] = 500 + m["id"]
                    await self.send(writer, {"jsonrpc": "2.0", "id": m["id"], "result": 500 + m["id"]})
            writer.write(W.ws_encode_frame(0x9, b"hi", False))   # server ping
            await writer.drain()
            if self.drop_first and self.connections == 1:
                writer.close()
                return
            for vault, amount, slot, pause in self.script:
                await asyncio.sleep(pause)
                await self.send(writer, {"jsonrpc": "2.0", "method": "slotNotification",
                                         "params": {"subscription": 999, "result": {"slot": slot + 1}}})
                await self.send(writer, {"jsonrpc": "2.0", "method": "accountNotification", "params": {
                    "subscription": subs[vault],
                    "result": {"context": {"slot": slot}, "value": token_account(
                        TOKEN if vault in (VA_T, VB_T) else W.WSOL, amount, 6 if vault in (VA_T, VB_T) else 9)}}})
            while True:
                _, op, _ = await W.ws_read_frame(reader)
                if op == 0xA:
                    self.pongs += 1
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass


SCRIPT = [  # a shock on A, then B catches up two slots later
    (VA_T, 1_050_000 * 10 ** 6, 101, 0.2),
    (VA_Q, 952_380_952_381, 101, 0.0),
    (VB_T, 1_050_000 * 10 ** 6, 103, 0.5),
    (VB_Q, 952_380_952_381, 103, 0.0),
]


class LiveStreamEndToEnd(TempDirCase):
    def run_stream(self, drop_first=False, seconds=2.5):
        async def go():
            server = FakeSolanaWs(SCRIPT, drop_first)
            url = await server.start()
            rec = W.Recorder(self.dir)
            with redirect_stdout(io.StringIO()) as out:
                engine = await W.run_watch(config(), FakeRpc(start_accounts()), rec, url, stop_after=seconds,
                                           first_backoff=0.2, eval_delay=0.05, status_every=1)
            rec.close()
            server.server.close()
            return engine, server, out.getvalue()
        return asyncio.run(go())

    def test_stream_records_gap_and_answers_ping(self):
        engine, server, out = self.run_stream()
        self.assertEqual(engine.stats["dislocations"], 1)
        self.assertEqual(engine.latency_stats()["median_slots"], 1)            # tip announced one slot ahead
        self.assertEqual(server.pongs, 1)
        d = self.rows("dislocations.csv")
        self.assertEqual(int(d[0]["slots_open"]), 2)
        self.assertIn("Read-only", out)
        self.assertIn("slot 103", out)

    def test_reconnects_after_drop(self):
        engine, server, out = self.run_stream(drop_first=True, seconds=3.5)
        self.assertGreaterEqual(server.connections, 2)
        self.assertIn("Reconnecting", out)
        self.assertEqual(engine.stats["dislocations"], 1)

    def test_replay_matches_live_and_report_reads(self):
        engine, _, _ = self.run_stream()
        out = self.dir / "replay"
        again = W.replay(config(), self.dir / "raw_updates.jsonl", out)
        self.assertEqual(again.stats["dislocations"], engine.stats["dislocations"])
        self.assertEqual(self.rows("dislocations.csv", out)[0]["slots_open"],
                         self.rows("dislocations.csv")[0]["slots_open"])
        text = W.report(self.dir)
        self.assertIn("Candidate gaps seen and closed (CSV history): 1", text)
        self.assertIn("closed within 2 slots (~0.8 s): 100%", text)


# ---------------------------------------------------------------------------
# Concentrated-liquidity pools and SOL/USDC comparison
# ---------------------------------------------------------------------------
WHIRL, CLMM_POOL, DLMM_POOL, CLMM_CFG = (W.b58encode(bytes([n] * 32)) for n in (51, 52, 53, 54))
VU_T, VU_Q = W.b58encode(bytes([61] * 32)), W.b58encode(bytes([62] * 32))      # USDC pool vaults
REF_S, REF_U = W.b58encode(bytes([71] * 32)), W.b58encode(bytes([72] * 32))    # SOL/USDC ref vaults


def whirl_bytes(mint_a, mint_b, price_raw, fee_rate=3000, liquidity=0):
    b = bytearray(653)
    b[45:47] = fee_rate.to_bytes(2, "little")
    b[49:65] = int(liquidity).to_bytes(16, "little")
    b[65:81] = int(price_raw ** 0.5 * 2 ** 64).to_bytes(16, "little")
    b[101:133], b[181:213] = W.b58decode(mint_a), W.b58decode(mint_b)
    return bytes(b)


def clmm_bytes(cfg, mint0, mint1, price_raw):
    b = bytearray(1544)
    b[9:41], b[73:105], b[105:137] = W.b58decode(cfg), W.b58decode(mint0), W.b58decode(mint1)
    b[253:269] = int(price_raw ** 0.5 * 2 ** 64).to_bytes(16, "little")
    return bytes(b)


def dlmm_bytes(mint_x, mint_y, active_id, bin_step=25, base_factor=10000, vfc=0, va=0, power=0):
    b = bytearray(904)
    b[8:10], b[16:20], b[34] = base_factor.to_bytes(2, "little"), vfc.to_bytes(4, "little"), power
    b[40:44], b[76:80] = va.to_bytes(4, "little"), active_id.to_bytes(4, "little", signed=True)
    b[80:82] = bin_step.to_bytes(2, "little")
    b[88:120], b[120:152] = W.b58decode(mint_x), W.b58decode(mint_y)
    return bytes(b)


def b64acct(data, owner="x"):
    return {"owner": owner, "data": [base64.b64encode(data).decode(), "base64"]}


class Decoders(unittest.TestCase):
    def pool(self, kind, token_is_a, qdec=9):
        return W.Pool({"name": kind, "kind": kind, "address": WHIRL, "quote_mint": W.WSOL,
                       "quote_decimals": qdec, "token_is_a": token_is_a, "fee": 0.003}, 6)

    def test_whirlpool_both_orientations_and_fee(self):
        # token (6 dp) worth 0.001 SOL (9 dp): 1 raw token = 1 raw lamport when token is A
        price, fee = self.pool("whirlpool", True).decode_state(whirl_bytes(TOKEN, W.WSOL, 1.0, 3000))
        self.assertAlmostEqual(price, 0.001, places=9)
        self.assertAlmostEqual(fee, 0.003)
        price, _ = self.pool("whirlpool", False).decode_state(whirl_bytes(W.WSOL, TOKEN, 1.0))
        self.assertAlmostEqual(price, 0.001, places=9)

    def test_clmm_price(self):
        price, fee = self.pool("clmm", True).decode_state(clmm_bytes(CLMM_CFG, TOKEN, W.WSOL, 2.0))
        self.assertAlmostEqual(price, 0.002, places=9)
        self.assertIsNone(fee)

    def test_dlmm_price_and_fee(self):
        # (1 + 25/10000)^active = 1  ->  active 0
        price, fee = self.pool("dlmm", True).decode_state(dlmm_bytes(TOKEN, W.WSOL, 0))
        self.assertAlmostEqual(price, 0.001, places=12)
        self.assertAlmostEqual(fee, 0.005)                        # 0.25% fee + 0.25% bin width
        price, _ = self.pool("dlmm", True).decode_state(dlmm_bytes(TOKEN, W.WSOL, 100))
        self.assertAlmostEqual(price, 0.001 * 1.0025 ** 100, places=12)
        vol = W.dlmm_fee(dlmm_bytes(TOKEN, W.WSOL, 0, vfc=7500, va=10000))
        expected = (2_500_000 + -(-(7500 * (10000 * 25) ** 2) // 100_000_000_000)) / 1e9
        self.assertAlmostEqual(vol, min(expected, 0.1))
        self.assertAlmostEqual(W.live_fee("dlmm", dlmm_bytes(TOKEN, W.WSOL, 0, bin_step=100)), 0.02)

    def test_layout_mints(self):
        self.assertEqual(W.layout_mints("dlmm", dlmm_bytes(TOKEN, W.WSOL, 0)), (TOKEN, W.WSOL))
        with self.assertRaises(ValueError):
            W.layout_mints("whirlpool", b"short")


def mixed_config(extra_pools=(), ref=False, min_gap=0.2):
    pools = [{"name": "A", "kind": "cp", "quote_mint": W.WSOL, "quote_decimals": 9,
              "base_vault": VA_T, "quote_vault": VA_Q, "fee": 0.0025},
             {"name": "Orca", "kind": "whirlpool", "address": WHIRL, "quote_mint": W.WSOL,
              "quote_decimals": 9, "token_is_a": True, "fee": 0.003}] + list(extra_pools)
    w = {"label": "TEST", "token_mint": TOKEN, "token_decimals": 6, "cost_sol": 0.0005,
         "min_profit_sol": 0.0005, "min_net_gap_pct": min_gap, "shock_pct": 0.5, "pools": pools}
    if ref:
        w["sol_usdc"] = {"name": "ref", "kind": "cp", "quote_mint": W.USDC, "quote_decimals": 6,
                         "base_vault": REF_S, "quote_vault": REF_U, "fee": 0.0025}
    return {"watches": [w]}


USDC_POOL = {"name": "U", "kind": "cp", "quote_mint": W.USDC, "quote_decimals": 6,
             "base_vault": VU_T, "quote_vault": VU_Q, "fee": 0.0025}


class MixedEngine(TempDirCase):
    def engine(self, cfg):
        rec = W.Recorder(self.dir)
        self.addCleanup(rec.close)
        return W.Engine(cfg, rec)

    def test_cp_vs_whirlpool_gap_is_timed(self):
        e = self.engine(mixed_config())
        e.on_vault(VA_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VA_Q, 10 ** 12, 100, 1000.0)                                  # A: 0.001 SOL
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.0)), 100, 1000.0)  # Orca: 0.001
        e.evaluate(1000.0)
        self.assertEqual(e.open_count(), 0)
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.02)), 101, 1000.4)  # Orca +2%
        e.evaluate(1000.5)
        self.assertEqual(e.open_count(), 1)
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.0)), 104, 1001.6)
        e.evaluate(1001.7)
        e.rec.dislocations.fh.flush()
        d = self.rows("dislocations.csv")[0]
        self.assertEqual((d["method"], d["buy_pool"], d["sell_pool"], d["slots_open"]), ("gap", "A", "Orca", "3"))
        self.assertAlmostEqual(float(d["peak_net_gap_pct"]), (1.02 * 0.9975 * 0.997 - 1) * 100, places=3)
        self.assertEqual(d["peak_net_quote"], "")

    def test_gap_below_fees_ignored(self):
        e = self.engine(mixed_config())
        e.on_vault(VA_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VA_Q, 10 ** 12, 100, 1000.0)
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.006)), 100, 1000.0)  # 0.6% < fees+0.2%
        e.evaluate(1000.0)
        self.assertEqual(e.open_count(), 0)

    def test_usdc_pool_compared_through_reference(self):
        e = self.engine(mixed_config([USDC_POOL], ref=True))
        for v, a in ((VA_T, 10 ** 12), (VA_Q, 10 ** 12), (REF_S, 10 ** 12), (REF_U, 150 * 10 ** 9)):
            e.on_vault(v, a, 100, 1000.0)                                        # SOL = 150 USDC
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.0)), 100, 1000.0)
        e.on_vault(VU_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VU_Q, 150_000 * 10 ** 6, 100, 1000.0)                        # 0.15 USDC = 0.001 SOL
        e.evaluate(1000.0)
        w = e.watches[0]
        self.assertAlmostEqual(e.to_sol(w, w.pools[2]), 0.001)
        self.assertEqual(e.open_count(), 0)
        e.on_vault(VU_Q, 156_000 * 10 ** 6, 101, 1000.4)                        # USDC pool +4%
        e.evaluate(1000.5)
        self.assertEqual(e.open_count(), 2)                                      # vs A and vs Orca
        e.on_vault(VU_Q, 150_000 * 10 ** 6, 102, 1000.8)
        e.evaluate(1000.9)
        e.rec.dislocations.fh.flush()
        rows = self.rows("dislocations.csv")
        self.assertEqual({r["method"] for r in rows}, {"gap"})
        self.assertTrue(all(r["sell_pool"] == "U" for r in rows))

    def test_wrong_price_is_excluded(self):
        e = self.engine(mixed_config())
        e.on_vault(VA_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VA_Q, 10 ** 12, 100, 1000.0)
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 3.0)), 100, 1000.0)   # 3x off
        with redirect_stdout(io.StringIO()) as out:
            e.evaluate(1000.0)
        self.assertTrue(e.watches[0].pools[1].suspect)
        self.assertEqual(e.open_count(), 0)
        self.assertIn("excluding it", out.getvalue())

    def test_mixed_quotes_need_reference(self):
        with self.assertRaises(ValueError):
            W.Watch(mixed_config([USDC_POOL])["watches"][0])

    def test_replay_reproduces_state_updates(self):
        e = self.engine(mixed_config())
        e.on_vault(VA_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VA_Q, 10 ** 12, 100, 1000.0)
        for slot, raw in ((100, 1.0), (101, 1.03), (103, 1.0)):
            e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, raw)), slot, 1000.0 + (slot - 100) / 10)
            e.evaluate(1000.0 + (slot - 100) / 10)
        e.rec.raw_fh.flush()
        again = W.replay(mixed_config(), self.dir / "raw_updates.jsonl", self.dir / "replay")
        self.assertEqual(again.stats["dislocations"], e.stats["dislocations"])
        self.assertEqual(again.stats["dislocations"], 1)


class DiscoverMixed(unittest.TestCase):
    def test_all_pool_types_and_reference(self):
        cp_raw = b64acct(bytes(8) + W.b58decode(VA_T) + W.b58decode(VA_Q) + bytes(16),
                         "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8")
        ref_raw = b64acct(bytes(8) + W.b58decode(REF_S) + W.b58decode(REF_U) + bytes(16),
                          "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8")
        cfg = bytearray(80)
        cfg[47:51] = (10000).to_bytes(4, "little")                               # 1% CLMM fee tier
        raw = {POOL_A: cp_raw,
               WHIRL: b64acct(whirl_bytes(W.WSOL, TOKEN, 1.0, 1600), "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"),
               CLMM_POOL: b64acct(clmm_bytes(CLMM_CFG, TOKEN, W.USDC, 1.0), "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"),
               DLMM_POOL: b64acct(dlmm_bytes(TOKEN, W.WSOL, 0), "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"),
               CLMM_CFG: b64acct(bytes(cfg)), W.SOL_USDC_FALLBACK: ref_raw}
        mint = lambda d: {"data": {"parsed": {"type": "mint", "info": {"decimals": d}}}}
        accts = {**start_accounts(), TOKEN: mint(6), W.WSOL: mint(9), W.USDC: mint(6),
                 REF_S: token_account(W.WSOL, 10 ** 12, 9), REF_U: token_account(W.USDC, 150 * 10 ** 9, 6)}

        def pair(addr, quote, liq):
            return {"chainId": "solana", "pairAddress": addr, "liquidity": {"usd": liq},
                    "baseToken": {"address": TOKEN, "symbol": "TEST"}, "quoteToken": {"address": quote}}

        def fetch(url):
            if W.WSOL in url:
                return []                                                        # force the fallback ref
            return [pair(POOL_A, W.WSOL, 9e6), pair(WHIRL, W.WSOL, 1e6), pair(CLMM_POOL, W.USDC, 5e5),
                    pair(DLMM_POOL, W.WSOL, 2e5)]

        watches, notes = W.discover(TOKEN, FakeRpc(accts, raw), fetch)
        self.assertEqual(len(watches), 1, notes)
        w = watches[0]
        self.assertEqual([p["kind"] for p in w["pools"]], ["cp", "whirlpool", "clmm", "dlmm"])
        self.assertEqual([p.get("token_is_a") for p in w["pools"]], [None, False, True, True])
        self.assertEqual([p["fee"] for p in w["pools"]], [0.0025, 0.0016, 0.01, 0.005])
        self.assertEqual(w["pools"][2]["quote_mint"], W.USDC)
        self.assertEqual(w["sol_usdc"]["base_vault"], REF_S)
        self.assertTrue(any("SOL/USDC reference" in n for n in notes))
        W.Watch(w)


class CsvMigration(TempDirCase):
    def test_old_header_with_mixed_rows_is_repaired(self):
        old = ["opened_utc", "watch", "buy_pool", "sell_pool", "open_slot", "close_slot", "slots_open",
               "seconds_open", "peak_gap_pct", "peak_net_quote", "best_size_quote", "quote"]
        new_row = {"opened_utc": "t2", "watch": "Bonk", "buy_pool": "X", "sell_pool": "Y", "method": "gap",
                   "open_slot": "449026290", "close_slot": "449026439", "slots_open": "149",
                   "seconds_open": "59.6", "peak_gap_pct": "0.8", "peak_net_gap_pct": "0.63",
                   "peak_net_quote": "", "best_size_quote": "", "quote": "SOL"}
        p = self.dir / "dislocations.csv"
        with open(p, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(old)
            w.writerow(["t1", "TEST/SOL", "A", "B", "100", "102", "2", "0.8", "5.0", "1.03", "40", "SOL"])
            w.writerow([new_row[k] for k in W._D2])                    # appended by v0.2 under old header
        rec = W.Recorder(self.dir)
        rec.close()
        rows = self.rows("dislocations.csv")
        self.assertEqual(list(rows[0].keys()), W.DISLOCATION_FIELDS)
        self.assertEqual((rows[0]["slots_open"], rows[0]["peak_net_quote"], rows[0]["method"]), ("2", "1.03", ""))
        self.assertEqual((rows[1]["slots_open"], rows[1]["peak_net_gap_pct"], rows[1]["method"]), ("149", "0.63", "gap"))
        self.assertIn("median time open: 76 slots", W.report(self.dir))
        self.assertEqual(rows[1]["tradable"], "")


class DepthAndSignals(TempDirCase):
    def engine(self, cfg=None):
        rec = W.Recorder(self.dir)
        self.addCleanup(rec.close)
        e = W.Engine(cfg or mixed_config(), rec)
        self.events = []
        e.on_event = lambda kind, data: self.events.append((kind, data))
        return e

    def test_clmm_swap_maths_matches_price_for_small_trades(self):
        p = W.Pool({"name": "O", "kind": "whirlpool", "address": WHIRL, "quote_mint": W.WSOL, "quote_decimals": 9,
                    "token_is_a": True, "fee": 0.0}, 6)
        p.state_price, p.L, p.sq = 0.001, 10 ** 15, 1.0
        self.assertAlmostEqual(p.buy_token(0.001) / 1.0, 1.0, places=5)       # 0.001 SOL buys ~1 token
        self.assertAlmostEqual(p.sell_token(1.0), 0.001, places=8)
        p.fee = 0.003
        self.assertAlmostEqual(p.sell_token(p.buy_token(1.0)), 0.997 ** 2, places=5)  # round trip pays two fees
        q = W.Pool({"name": "O2", "kind": "whirlpool", "address": WHIRL, "quote_mint": W.WSOL, "quote_decimals": 9,
                    "token_is_a": False, "fee": 0.0}, 6)
        q.state_price, q.L, q.sq = 0.001, 10 ** 15, 1.0                       # token is B: same raw price
        self.assertAlmostEqual(q.buy_token(0.001), 1.0, places=5)

    def feed_gap(self, e, liquidity):
        e.on_vault(VA_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VA_Q, 10 ** 12, 100, 1000.0)                                  # A: 0.001 SOL, 1000 SOL deep
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.0, 3000, liquidity)), 100, 1000.0)
        e.evaluate(1000.0)
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.03, 3000, liquidity)), 101, 1000.4)  # Orca +3%
        e.evaluate(1000.5)
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.0, 3000, liquidity)), 103, 1001.0)
        e.evaluate(1001.1)
        e.rec.dislocations.fh.flush()
        return self.rows("dislocations.csv")[-1]

    def test_deep_concentrated_gap_still_requires_ticks(self):
        row = self.feed_gap(self.engine(), 10 ** 16)
        self.assertEqual(row["tradable"], "unknown")
        self.assertEqual(row["depth_reason"], "tick_or_bin_depth_unavailable")
        self.assertEqual(row["execution_verified"], "no")
        self.assertEqual(row["depth_net_sol_1"], "")
        self.assertEqual([k for k, _ in self.events], ["big_shock", "gap_closed"])   # +3% is also a big shock

    def test_thin_concentrated_gap_has_no_unsupported_depth_estimate(self):
        row = self.feed_gap(self.engine(), 10 ** 9)                             # almost no liquidity at the price
        self.assertEqual(row["tradable"], "unknown")
        self.assertEqual(row["depth_net_sol_0_25"], "")

    def test_dlmm_gap_depth_unknown(self):
        cfg = mixed_config([{"name": "Met", "kind": "dlmm", "address": DLMM_POOL, "quote_mint": W.WSOL,
                             "quote_decimals": 9, "token_is_a": True, "fee": 0.005}], min_gap=0.2)
        cfg["watches"][0]["pools"] = [cfg["watches"][0]["pools"][0], cfg["watches"][0]["pools"][2]]
        e = self.engine(cfg)
        e.on_vault(VA_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VA_Q, 10 ** 12, 100, 1000.0)
        e.on_account(DLMM_POOL, b64acct(dlmm_bytes(TOKEN, W.WSOL, 0)), 100, 1000.0)
        e.evaluate(1000.0)
        e.on_account(DLMM_POOL, b64acct(dlmm_bytes(TOKEN, W.WSOL, 12)), 101, 1000.4)   # +3.0%
        e.evaluate(1000.5)
        e.on_account(DLMM_POOL, b64acct(dlmm_bytes(TOKEN, W.WSOL, 0)), 102, 1000.8)
        e.evaluate(1000.9)
        e.rec.dislocations.fh.flush()
        self.assertEqual(self.rows("dislocations.csv")[-1]["tradable"], "unknown")

    def test_dlmm_single_bin_is_not_a_shock_but_big_move_is(self):
        cfg = mixed_config([{"name": "Met", "kind": "dlmm", "address": DLMM_POOL, "quote_mint": W.WSOL,
                             "quote_decimals": 9, "token_is_a": True, "fee": 0.005}])
        e = self.engine(cfg)
        e.on_vault(VA_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VA_Q, 10 ** 12, 100, 1000.0)
        e.on_account(DLMM_POOL, b64acct(dlmm_bytes(TOKEN, W.WSOL, 0, bin_step=80)), 100, 1000.0)
        e.evaluate(1000.0)
        e.on_account(DLMM_POOL, b64acct(dlmm_bytes(TOKEN, W.WSOL, 1, bin_step=80)), 101, 1000.4)  # one bin +0.8%
        e.evaluate(1000.5)
        self.assertEqual((e.stats["shocks"], e.stats["bin_steps"]), (0, 1))
        e.on_account(DLMM_POOL, b64acct(dlmm_bytes(TOKEN, W.WSOL, 6, bin_step=80)), 102, 1000.8)  # five bins +4%
        with redirect_stdout(io.StringIO()):
            e.evaluate(1000.9)
        self.assertEqual((e.stats["shocks"], e.stats["big_shocks"]), (1, 1))
        self.assertIn("big_shock", [k for k, _ in self.events])

    def test_latency_is_slots_behind_tip(self):
        e = self.engine()
        e.note_tip(110)
        for s in (110, 109, 108, 110):
            e.note_arrival(s)
        st = e.latency_stats()
        self.assertEqual((st["samples"], st["median_slots"], st["same_slot_pct"]), (4, 1, 50.0))

    def test_replay_keeps_depth(self):
        e = self.engine()
        row = self.feed_gap(e, 10 ** 16)
        e.rec.raw_fh.flush()
        e2 = W.replay(mixed_config(), self.dir / "raw_updates.jsonl", self.dir / "replay")
        again = self.rows("dislocations.csv", self.dir / "replay")[-1]
        self.assertEqual((again["tradable"], again["depth_net_sol_1"]), (row["tradable"], row["depth_net_sol_1"]))

    def test_report_mentions_depth_and_latency(self):
        e = self.engine()
        self.feed_gap(e, 10 ** 16)
        e.note_tip(200)
        e.note_arrival(199)
        e.rec.write_json("latency.json", e.latency_stats())
        e.rec.raw_fh.flush()
        text = W.report(self.dir)
        self.assertIn("unverified", text)
        self.assertIn("behind the chain tip", text)


class NoTradingCode(unittest.TestCase):
    def test_no_signing_or_sending(self):
        src = Path(W.__file__).read_text()
        for word in ("sendTransaction", "sendBundle", "private_key", "secret_key", "Keypair", "signTransaction"):
            self.assertNotIn(word, src)


if __name__ == "__main__":
    unittest.main()
