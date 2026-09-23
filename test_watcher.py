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
        self.assertIn("Profitable gaps seen and closed: 1", text)
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
            e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, raw)), slot, 1000.0 + slot / 10)
            e.evaluate(1000.0 + slot / 10)
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

    def test_deep_gap_is_tradable_and_alerts(self):
        row = self.feed_gap(self.engine(), 10 ** 16)
        self.assertEqual(row["tradable"], "yes")
        self.assertGreater(float(row["depth_net_sol_1"]), 0.01)
        self.assertEqual([k for k, _ in self.events], ["big_shock", "gap_closed"])   # +3% is also a big shock

    def test_thin_gap_is_not_tradable(self):
        row = self.feed_gap(self.engine(), 10 ** 9)                             # almost no liquidity at the price
        self.assertEqual(row["tradable"], "no")
        self.assertLess(float(row["depth_net_sol_0_25"]), 0)

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
        self.assertIn("tradable after depth check", text)
        self.assertIn("behind the chain tip", text)


def oracle_bytes(pool, control=4000, group=16, va=50000):
    b = bytearray(254)
    b[8:40] = W.b58decode(pool)
    b[54:58], b[62:64], b[106:110] = control.to_bytes(4, "little"), group.to_bytes(2, "little"), va.to_bytes(4, "little")
    return bytes(b)


class OrcaAdaptiveFee(TempDirCase):
    def test_curve_check_and_pda(self):
        base_point = bytes([0x58] + [0x66] * 31)                       # ed25519 base point: on the curve
        self.assertTrue(W.on_curve(base_point))
        for program in (W.WHIRLPOOL_PROGRAM, "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"):
            self.assertTrue(W.on_curve(W.b58decode(program)))          # real keypairs are on the curve
        pda = W.whirlpool_oracle_address(WHIRL)
        self.assertFalse(W.on_curve(W.b58decode(pda)))                 # program addresses are never on it
        self.assertEqual(pda, W.whirlpool_oracle_address(WHIRL))        # deterministic
        self.assertNotEqual(pda, W.whirlpool_oracle_address(POOL_A))

    def test_adaptive_fee_formula(self):
        # crossed = 50000 * 16 = 800000; 4000 * 800000^2 / (100000 * 10000^2) = 256 millionths -> 0.0256%
        self.assertAlmostEqual(W.adaptive_fee_rate(oracle_bytes(WHIRL)), 0.000256)
        self.assertAlmostEqual(W.adaptive_fee_rate(oracle_bytes(WHIRL, va=500000)), 0.0256)
        self.assertEqual(W.adaptive_fee_rate(oracle_bytes(WHIRL, va=0)), 0.0)
        self.assertEqual(W.adaptive_fee_rate(oracle_bytes(WHIRL, va=10 ** 7)), 0.1)   # hard cap 10%

    def config_with_oracle(self):
        cfg = mixed_config()
        self.oracle = W.whirlpool_oracle_address(WHIRL)
        cfg["watches"][0]["pools"][1]["oracle"] = self.oracle
        return cfg

    def test_live_fee_is_base_plus_volatility(self):
        rec = W.Recorder(self.dir)
        self.addCleanup(rec.close)
        e = W.Engine(self.config_with_oracle(), rec)
        self.assertIn(self.oracle, e.vaults)
        orca = e.watches[0].pools[1]
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.0, 3000)), 100, 1000.0)
        self.assertAlmostEqual(orca.fee, 0.003)
        e.on_account(self.oracle, b64acct(oracle_bytes(WHIRL)), 101, 1000.1)
        self.assertAlmostEqual(orca.fee, 0.003 + 0.000256)
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.0, 1000)), 102, 1000.2)  # base fee changes
        self.assertAlmostEqual(orca.fee, 0.001 + 0.000256)
        e.on_account(self.oracle, b64acct(oracle_bytes(POOL_A, va=9999)), 103, 1000.3)   # wrong pool: ignored
        self.assertAlmostEqual(orca.adaptive_fee, 0.000256)

    def test_volatility_fee_closes_fake_gap_and_replays(self):
        rec = W.Recorder(self.dir)
        self.addCleanup(rec.close)
        e = W.Engine(self.config_with_oracle(), rec)
        e.on_vault(VA_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VA_Q, 10 ** 12, 100, 1000.0)
        e.on_account(self.oracle, b64acct(oracle_bytes(WHIRL, va=500000)), 100, 1000.0)  # 2.56% fee
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.02, 3000, 10 ** 16)), 100, 1000.0)    # +2% gap
        e.evaluate(1000.0)
        self.assertEqual(e.open_count(), 0)          # without the volatility fee this 2% gap would count
        e.rec.raw_fh.flush()
        again = W.replay(self.config_with_oracle(), self.dir / "raw_updates.jsonl", self.dir / "replay")
        self.assertAlmostEqual(again.watches[0].pools[1].fee, 0.003 + 0.0256)

    def test_upgrade_config_finds_oracle(self):
        cfg = mixed_config()
        oracle = W.whirlpool_oracle_address(WHIRL)
        rpc = FakeRpc({}, {oracle: b64acct(oracle_bytes(WHIRL), W.WHIRLPOOL_PROGRAM)})
        self.assertTrue(W.upgrade_config(cfg, rpc))
        self.assertEqual(cfg["watches"][0]["pools"][1]["oracle"], oracle)
        self.assertFalse(W.upgrade_config(cfg, rpc))                   # already done
        cfg2 = mixed_config()
        W.upgrade_config(cfg2, FakeRpc({}, {}))
        self.assertIsNone(cfg2["watches"][0]["pools"][1]["oracle"])     # fixed-fee pool


class NoTradingCode(unittest.TestCase):
    def test_no_signing_or_sending(self):
        src = Path(W.__file__).read_text()
        for word in ("sendTransaction", "sendBundle", "private_key", "secret_key", "Keypair", "signTransaction"):
            self.assertNotIn(word, src)


class PoolEarningsTests(unittest.TestCase):
    """Paper liquidity positions: fees from the on-chain fee counter, loss from price moves."""

    def pool(self, token_is_a=True):
        p = W.Pool({"name": "Orca X", "kind": "whirlpool", "address": WHIRL, "quote_mint": W.WSOL,
                    "quote_decimals": 9, "token_is_a": token_is_a, "fee": 0.003}, 6)
        self.move(p, 1.0)
        p.fg = (0, 0)
        return p

    @staticmethod
    def move(p, sq):
        p.sq = sq                                   # raw sqrt price; human price follows from decimals
        raw = sq * sq
        p.state_price = raw * 1e-3 if p.token_is_a else 1e-3 / raw

    def test_fee_growth_is_read_from_the_pool_account(self):
        b = bytearray(whirl_bytes(TOKEN, W.WSOL, 1.0))
        b[165:181], b[245:261] = (7 * 2 ** 64).to_bytes(16, "little"), (9 * 2 ** 64).to_bytes(16, "little")
        x = W.state_extra("whirlpool", bytes(b))
        self.assertEqual((x["fa"], x["fb"]), (7 * 2 ** 64, 9 * 2 ** 64))
        c = bytearray(clmm_bytes(TOKEN, TOKEN, W.WSOL, 1.0))
        c[277:293] = (5).to_bytes(16, "little")
        self.assertEqual(W.state_extra("clmm", bytes(c))["fa"], 5)

    def test_starts_at_exactly_the_paper_size_with_nothing_earned(self):
        for side in (True, False):
            sim = W.LpSim(self.pool(side), 0.05, 0.0)
            r = sim.result()
            self.assertAlmostEqual(sim._value(1.0), W.LP_START_VALUE, places=9)
            self.assertAlmostEqual(r["fees_pct"], 0.0)
            self.assertAlmostEqual(r["net_vs_hold_pct"], 0.0, places=9)

    @staticmethod
    def per_unit(sim, sol):
        """Fee-counter step (Q64.64) that pays this position `sol` of quote fees."""
        return int(sol * 1e9 / sim.liq * 2 ** 64)

    def test_fees_follow_the_fee_counter_while_in_range(self):
        p = self.pool()
        sim = W.LpSim(p, 0.05, 0.0)
        p.fg = (0, self.per_unit(sim, 0.4))          # 0.4 SOL of fees on a 100 SOL position
        sim.observe(3600.0)
        self.assertAlmostEqual(sim.fee_quote, 0.4, places=6)
        self.assertAlmostEqual(sim.result()["fees_pct"], 0.4, places=3)
        self.assertEqual(sim.result()["in_range_pct"], 100.0)
        self.assertEqual(sim.result()["skipped_updates"], 0)

    def test_counter_going_backwards_is_ignored(self):
        """A stale read (snapshot behind the live stream) must not look like a giant payout."""
        p = self.pool()
        p.fg = (0, 10 ** 12)
        sim = W.LpSim(p, 0.05, 0.0)
        p.fg = (0, 10 ** 12 - 5)                     # counter appears to go back: stale data
        sim.observe(10.0)
        self.assertEqual((sim.fee_tok, sim.fee_quote), (0.0, 0.0))
        self.assertEqual(sim.result()["skipped_updates"], 1)
        self.assertTrue(sim.sane())

    def test_absurd_single_step_is_skipped(self):
        p = self.pool()
        sim = W.LpSim(p, 0.05, 0.0)
        p.fg = (0, self.per_unit(sim, 50))           # half the position in one update: impossible
        sim.observe(10.0)
        self.assertEqual(sim.fee_quote, 0.0)
        self.assertEqual(sim.result()["skipped_updates"], 1)

    def test_same_slot_is_not_counted_twice(self):
        p = self.pool()
        sim = W.LpSim(p, 0.05, 0.0)
        p.fg = (0, self.per_unit(sim, 0.2))
        sim.observe(10.0, slot=100)
        sim.observe(11.0, slot=100)                  # snapshot replays the same slot
        sim.observe(12.0, slot=99)                   # and an older one
        self.assertAlmostEqual(sim.fee_quote, 0.2, places=6)

    def test_broken_position_restarts_itself(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "lp_state.json"
            p = self.pool()
            book = W.LpBook(path)
            book.observe(p, 0.0)
            book.save()
            bad = json.loads(path.read_text(encoding="utf-8"))
            for v in bad.values():
                v["sim"]["fee_quote"] = 1e12         # nonsense left by an earlier bug
            path.write_text(json.dumps(bad), encoding="utf-8")
            book2 = W.LpBook(path)
            book2.observe(self.pool(), 100.0)
            self.assertEqual(book2.restarted, len(W.LP_RANGES))
            for r in book2.summary():
                self.assertLess(abs(r["fees_pct"]), 1)
                self.assertEqual(r["hours"], 0.0)

    def test_sol_figures_costs_and_break_even(self):
        p = self.pool()
        p.base_fee = 0.003
        sim = W.LpSim(p, 0.05, 0.0)
        p.fg = (0, self.per_unit(sim, 0.5))          # 0.5% of the position in fees over an hour
        sim.observe(3600.0)
        r = sim.result()
        self.assertAlmostEqual(r["capital_sol"], W.LP_CAPITAL_SOL)
        self.assertAlmostEqual(r["cost_sol"], W.LP_TX_COST_SOL + W.LP_CAPITAL_SOL * 0.003, places=6)
        self.assertAlmostEqual(r["net_sol_per_day"], 0.12 * W.LP_CAPITAL_SOL, places=3)   # 0.5%/h = 12%/day
        self.assertAlmostEqual(r["days_to_break_even"], round(r["cost_sol"] / r["net_sol_per_day"], 2), places=6)
        self.assertGreater(r["days_to_break_even"], 0)          # a fast payback is never reported as "never"

    def test_losing_position_never_breaks_even(self):
        p = self.pool()
        sim = W.LpSim(p, 0.20, 0.0)
        self.move(p, 1.05)                           # price moves, no fees earned
        sim.observe(3600.0)
        r = sim.result()
        self.assertLess(r["net_sol_per_day"], 0)
        self.assertIsNone(r["days_to_break_even"])

    def test_worst_stretch_is_remembered(self):
        p = self.pool()
        sim = W.LpSim(p, 0.20, 0.0)
        self.move(p, 1.08)                           # a bad stretch
        sim.observe(60.0)
        low = sim.result()["net_vs_hold_pct"]
        self.assertLess(low, 0)
        self.move(p, 1.0)                            # price comes back
        p.fg = (0, self.per_unit(sim, 0.3))
        sim.observe(120.0)
        r = sim.result()
        self.assertGreater(r["net_vs_hold_pct"], 0)
        self.assertAlmostEqual(r["worst_net_pct"], low, places=4)   # the bad stretch stays on record

    def test_reset_clears_every_position(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "lp_state.json"
            book = W.LpBook(path)
            book.observe(self.pool(), 0.0)
            book.save()
            self.assertTrue(path.exists())
            book.reset()
            self.assertEqual(book.summary(), [])
            self.assertFalse(path.exists())

    def test_counter_wraparound_is_handled(self):
        p = self.pool()
        p.fg = (0, W.U128 - 10)
        sim = W.LpSim(p, 0.05, 0.0)
        p.fg = (0, 5)                                # a true u128 wrap is a tiny step, not a payout
        sim.observe(10.0)
        self.assertLess(sim.fee_quote, 1e-6)
        self.assertTrue(sim.sane())

    def test_price_moves_cost_money_versus_holding(self):
        for side in (True, False):
            p = self.pool(side)
            sim = W.LpSim(p, 0.20, 0.0)
            self.move(p, 1.05)                       # price about +10%, still inside +/-20%
            sim.observe(60.0)
            r = sim.result()
            self.assertLess(r["price_move_pct"], 0)
            self.assertGreater(r["price_move_pct"], -2)
            self.assertAlmostEqual(r["net_vs_hold_pct"], r["price_move_pct"] + r["fees_pct"], places=3)

    def test_no_fees_while_out_of_range(self):
        p = self.pool()
        sim = W.LpSim(p, 0.05, 0.0)
        self.move(p, 1.2)                            # jumps out of the +/-5% range
        sim.observe(10.0)
        p.fg = (2 ** 64 * 50, 2 ** 64 * 50)
        sim.observe(20.0)                            # was out of range for this whole interval
        self.assertEqual((sim.fee_tok, sim.fee_quote), (0.0, 0.0))
        self.assertFalse(sim.result()["in_range_now"])
        self.assertAlmostEqual(sim.result()["in_range_pct"], 50.0)

    def test_positions_survive_a_restart(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "lp_state.json"
            p = self.pool()
            book = W.LpBook(path)
            book.observe(p, 0.0)
            p.fg = (0, 2 ** 64 // 10 ** 7)
            book.observe(p, 100.0)
            book.save()
            before = {k: s.fee_quote for k, s in book.sims.items()}
            p2 = self.pool()
            p2.fg = (0, 2 ** 64 // 10 ** 7 * 3 // 2)  # fees kept accruing while the watcher was down
            book2 = W.LpBook(path)
            book2.observe(p2, 200.0)
            for k, s in book2.sims.items():
                self.assertEqual(s.t0, 0.0)
                self.assertAlmostEqual(s.fee_quote, before[k] * 1.5, places=12)
            self.assertEqual(len(book2.summary()), len(W.LP_RANGES))

    def test_engine_feeds_positions_and_report_lists_them(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = {"watches": [{"label": "X", "token_mint": TOKEN, "token_decimals": 6, "shock_pct": 0.5,
                                "pools": [{"name": "Orca X", "kind": "whirlpool", "address": WHIRL,
                                           "quote_mint": W.WSOL, "quote_decimals": 9, "token_is_a": True,
                                           "fee": 0.003},
                                          {"name": "Orca Y", "kind": "whirlpool", "address": POOL_A,
                                           "quote_mint": W.WSOL, "quote_decimals": 9, "token_is_a": True,
                                           "fee": 0.003}]}]}
            rec = W.Recorder(Path(d))
            e = W.Engine(cfg, rec)
            for i, fb in enumerate((0, 2 ** 64 // 10 ** 6)):
                b = bytearray(whirl_bytes(TOKEN, W.WSOL, 1.0, 3000, 10 ** 12))
                b[245:261] = fb.to_bytes(16, "little")
                e.on_account(WHIRL, {"data": [base64.b64encode(bytes(b)).decode(), "base64"]}, 10 + i, 3600.0 * i)
            rows = e.lp.summary()
            self.assertEqual({r["range_pct"] for r in rows}, {5, 20})
            self.assertTrue(all(r["fees_pct"] > 0 for r in rows))
            e.lp.save()
            rec.close()
            text = W.report(Path(d))
            self.assertIn("POOL EARNINGS", text)
            self.assertIn("Orca X", text)


def damm2_bytes(mint_a, mint_b, price_raw, liquidity_orca, fee_num=2_500_000, fa=0, fb=0, status=0,
                vault_a=None, vault_b=None):
    """A Meteora DAMM v2 pool account laid out as in Meteora's cp-amm source."""
    b = bytearray(1112)
    b[8:16] = fee_num.to_bytes(8, "little")
    b[168:200], b[200:232] = W.b58decode(mint_a), W.b58decode(mint_b)
    if vault_a:
        b[232:264], b[264:296] = W.b58decode(vault_a), W.b58decode(vault_b)
    b[360:376] = (int(liquidity_orca) << 64).to_bytes(16, "little")
    b[456:472] = int(price_raw ** 0.5 * 2 ** 64).to_bytes(16, "little")
    b[481] = status
    b[488:520], b[520:552] = int(fa).to_bytes(32, "little"), int(fb).to_bytes(32, "little")
    return bytes(b)


def fake_tx(slot, signer, deltas, tip=0, cu_price=0, fee=5000, extra_programs=(), err=None):
    """A jsonParsed transaction: `deltas` maps token account -> (mint, owner, pre, post)."""
    keys = [signer] + list(deltas)
    pre, post = [], []
    for i, (acct, (mint, owner, a, b)) in enumerate(deltas.items(), start=1):
        pre.append({"accountIndex": i, "mint": mint, "owner": owner, "uiTokenAmount": {"amount": str(a)}})
        post.append({"accountIndex": i, "mint": mint, "owner": owner, "uiTokenAmount": {"amount": str(b)}})
    top = [{"programId": p, "accounts": [], "data": ""} for p in extra_programs]
    if cu_price:
        top.append({"programId": W.COMPUTE_BUDGET, "data": W.b58encode(bytes([3]) + cu_price.to_bytes(8, "little"))})
    inner = []
    if tip:
        inner.append({"index": 0, "instructions": [{"program": "system", "programId": "11111111111111111111111111111111",
                      "parsed": {"type": "transfer", "info": {"source": signer, "lamports": tip,
                                 "destination": sorted(W.JITO_TIP_ACCOUNTS)[0]}}}]})
    return {"slot": slot, "blockTime": 1_790_000_000,
            "transaction": {"signatures": ["sig%d" % slot], "message": {
                "accountKeys": [{"pubkey": k, "signer": i == 0} for i, k in enumerate(keys)], "instructions": top}},
            "meta": {"err": err, "fee": fee, "computeUnitsConsumed": 120_000, "innerInstructions": inner,
                     "preTokenBalances": pre, "postTokenBalances": post}}


class FakeAuditRpc:
    def __init__(self, sigs: dict, txs: dict):
        self.sigs, self.txs, self.calls = sigs, txs, []

    def call(self, method, params):
        self.calls.append(method)
        if method == "getSignaturesForAddress":
            return self.sigs.get(params[0], [])
        if method == "getTransaction":
            return self.txs.get(params[0])
        raise RuntimeError(method)


class NewFeatureTests(TempDirCase):
    """DAMM v2 pools, triangle loops, and reading real transactions (who closed a gap, reality check)."""

    def engine(self, cfg):
        rec = W.Recorder(self.dir)
        self.addCleanup(rec.close)
        return W.Engine(cfg, rec)

    # -- DAMM v2 ---------------------------------------------------------------------------------
    def test_damm2_prices_both_orientations_and_reads_counters(self):
        data = damm2_bytes(TOKEN, W.WSOL, 1.0, 10 ** 15, fa=7 << 128, fb=9 << 128)
        p = W.Pool({"name": "D", "kind": "damm2", "address": "x", "quote_mint": W.WSOL, "quote_decimals": 9,
                    "token_is_a": True, "fee": 0.0}, 6)
        price, fee = p.decode_state(data)
        self.assertAlmostEqual(price, 0.001)
        self.assertAlmostEqual(fee, 0.0025)
        x = W.state_extra("damm2", data)
        self.assertEqual(x["L"], 10 ** 15)                            # rescaled to Orca units
        self.assertEqual((x["fa"], x["fb"]), (7 << 128, 9 << 128))
        q = W.Pool({"name": "D2", "kind": "damm2", "address": "x", "quote_mint": W.WSOL, "quote_decimals": 9,
                    "token_is_a": False, "fee": 0.0}, 6)
        price2, _ = q.decode_state(damm2_bytes(W.WSOL, TOKEN, 1.0, 10 ** 15))
        self.assertAlmostEqual(price2, 0.001)
        self.assertEqual(W.layout_mints("damm2", data), (TOKEN, W.WSOL))

    def test_damm2_swap_maths_matches_orca_for_same_state(self):
        a = W.Pool({"name": "D", "kind": "damm2", "address": "x", "quote_mint": W.WSOL, "quote_decimals": 9,
                    "token_is_a": True, "fee": 0.0025}, 6)
        o = W.Pool({"name": "O", "kind": "whirlpool", "address": "y", "quote_mint": W.WSOL, "quote_decimals": 9,
                    "token_is_a": True, "fee": 0.0025}, 6)
        for p in (a, o):
            p.state_price, p.L, p.sq = 0.001, 10 ** 15, 1.0
        self.assertTrue(a.can_quote_depth())
        self.assertAlmostEqual(a.buy_token(0.5), o.buy_token(0.5), places=9)
        self.assertAlmostEqual(a.sell_token(100.0), o.sell_token(100.0), places=12)

    def test_damm2_pool_feeds_earnings(self):
        cfg = {"watches": [{"label": "D", "token_mint": TOKEN, "token_decimals": 6, "shock_pct": 0.5,
                            "pools": [{"name": "Meteora DAMM v2 Dx", "kind": "damm2", "address": POOL_A,
                                       "quote_mint": W.WSOL, "quote_decimals": 9, "token_is_a": True, "fee": 0.0025},
                                      {"name": "A", "kind": "cp", "quote_mint": W.WSOL, "quote_decimals": 9,
                                       "base_vault": VA_T, "quote_vault": VA_Q, "fee": 0.0025}]}]}
        e = self.engine(cfg)
        e.on_account(POOL_A, b64acct(damm2_bytes(TOKEN, W.WSOL, 1.0, 10 ** 12)), 10, 0.0)
        liq = e.lp.sims[f"{POOL_A}|0.05"].liq
        fb = int(0.3e9 / liq * 2 ** 64)                        # pays the ±5% position 0.3 SOL (0.3%)
        e.on_account(POOL_A, b64acct(damm2_bytes(TOKEN, W.WSOL, 1.0, 10 ** 12, fb=fb)), 11, 3600.0)
        rows = {r["range_pct"]: r for r in e.lp.summary()}
        self.assertEqual(len(rows), len(W.LP_RANGES))
        self.assertAlmostEqual(rows[5]["fees_pct"], 0.3, places=3)
        self.assertTrue(0 < rows[20]["fees_pct"] < 0.3)          # wider range, thinner liquidity, less fee

    def test_solfi_and_vertigo_are_reported_not_guessed(self):
        self.assertIn("SolFi", W.KNOWN_UNSUPPORTED["SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe"])
        self.assertIn("Vertigo", W.KNOWN_UNSUPPORTED["vrTGoBuy5rYSxAfV3jaRJWHH6nN9WK4NRExGxsk1bCJ"])

    # -- triangle loops ----------------------------------------------------------------------------
    def loop_engine(self):
        e = self.engine(mixed_config([USDC_POOL], ref=True))
        for v, a in ((VA_T, 10 ** 12), (VA_Q, 10 ** 12), (REF_S, 10 ** 12), (REF_U, 150 * 10 ** 9),
                     (VU_T, 10 ** 12), (VU_Q, 150_000 * 10 ** 6)):
            e.on_vault(v, a, 100, 1000.0)                   # token = 0.001 SOL = 0.15 USDC everywhere
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.0)), 100, 1000.0)
        return e

    def test_triangle_loop_opens_and_closes(self):
        e = self.loop_engine()
        e.evaluate(1000.0)
        w = e.watches[0]
        self.assertEqual(w.loops, {})
        self.assertLess(w.loop_best, 0)                     # fees on three swaps: a small loss at rest
        e.on_vault(VU_Q, 156_000 * 10 ** 6, 101, 1000.4)    # USDC pool 4% richer
        e.evaluate(1000.5)
        self.assertIn(("SOL>token>USDC", "A", "U"), w.loops)
        self.assertGreater(w.loop_best, 0.01)
        e.on_vault(VU_Q, 150_000 * 10 ** 6, 103, 1001.2)
        e.evaluate(1001.3)
        self.assertEqual(w.loops, {})
        self.assertEqual(e.stats["loops"], 1)
        e.rec.loops.fh.flush()
        row = self.rows("loops.csv")[0]
        self.assertEqual(row["route"], "SOL>token>USDC")
        self.assertEqual(int(row["slots_open"]), 2)
        self.assertGreater(float(row["best_net_sol"]), float(row["net_sol_0_25"]))

    def test_loop_maths_is_three_real_swaps(self):
        e = self.loop_engine()
        e.evaluate(1000.0)
        w = e.watches[0]
        s, u = w.pools[0], w.pools[2]
        size = 1.0
        manual = W.swap_out(W.swap_out(W.swap_out(size, 1000, 10 ** 6, 0.0025) * 1, 10 ** 6, 150_000, 0.0025),
                            150_000, 1000, 0.0025) - size - w.cost_sol
        self.assertAlmostEqual(W.Engine.loop_net(w, s, u, "SOL>token>USDC", size), manual, places=9)

    # -- reading transactions -------------------------------------------------------------------------
    def test_parse_tx_reads_costs_tip_and_token_changes(self):
        tx = fake_tx(500, "Signer1111", {VA_T: (TOKEN, "amm", 100, 150), VA_Q: (W.WSOL, "amm", 1000, 940)},
                     tip=100_000, cu_price=50_000, fee=25_000, extra_programs=("ProgX",))
        t = W.parse_tx(tx)
        self.assertEqual(t["signer"], "Signer1111")
        self.assertEqual(t["tip"], 100_000)
        self.assertEqual(t["priority_fee"], 20_000)
        self.assertEqual(t["cu_price"], 50_000)
        self.assertEqual(t["deltas"][VA_T]["delta"], 50)
        self.assertEqual(t["deltas"][VA_Q]["delta"], -60)
        self.assertIn("ProgX", t["programs"])
        self.assertTrue(t["success"])

    def test_pool_state_before_a_slot(self):
        p = W.Pool({"name": "A", "kind": "cp", "quote_mint": W.WSOL, "quote_decimals": 9,
                    "base_vault": VA_T, "quote_vault": VA_Q, "fee": 0.0025}, 6)
        for slot, q in ((10, 100), (12, 200), (15, 300)):
            p.base, p.quote = 10 ** 9, q * 10 ** 9
            p.remember(slot)
        self.assertEqual(p.state_before(15).quote, 200 * 10 ** 9)
        self.assertEqual(p.state_before(13).quote, 200 * 10 ** 9)
        self.assertIsNone(p.state_before(10))                  # nothing older than the first snapshot
        self.assertEqual(p.quote, 300 * 10 ** 9)                # the live pool is untouched

    def test_reality_check_on_a_constant_product_swap(self):
        e = self.engine(mixed_config())
        e.on_vault(VA_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VA_Q, 10 ** 12, 100, 1000.0)
        w = e.watches[0]
        p = w.pools[0]
        sold = 5 * 10 ** 6                                     # someone sells 5 tokens
        out = int(W.swap_out(5.0, 10 ** 6, 1000.0, 0.0025) * 1e9)
        tx = fake_tx(105, "Trader", {VA_T: (TOKEN, "amm", 10 ** 12, 10 ** 12 + sold),
                                      VA_Q: (W.WSOL, "amm", 10 ** 12, 10 ** 12 - out)})
        rpc = FakeAuditRpc({p.address or VA_T: []}, {"sig105": tx})
        a = W.Auditor(e, rpc, self.dir, pause=0)
        self.addCleanup(a.close)
        flow = W.pool_flow(p, W.parse_tx(tx))
        res = W.check_swap(p, flow, p.state_before(105))
        self.assertEqual(res["direction"], "sell token")
        self.assertLess(abs(res["error_pct"]), 1e-4)
        self.assertIsNone(W.check_swap(p, {"token": 5, "quote": 5}, p.state_before(105)))   # a deposit, not a swap

    def test_auditor_finds_who_closed_a_gap(self):
        e = self.engine(mixed_config())
        w = e.watches[0]
        a_pool, orca = w.pools[0], w.pools[1]
        e.on_vault(VA_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VA_Q, 10 ** 12, 100, 1000.0)
        e.on_account(WHIRL, b64acct(whirl_bytes(TOKEN, W.WSOL, 1.0)), 100, 1000.0)
        orca.vault_side = {"OV_T": "token", "OV_Q": "quote"}
        arb = fake_tx(101, "ArbBot111", {VA_T: (TOKEN, "amm", 100, 90), VA_Q: (W.WSOL, "amm", 100, 110),
                                          "OV_T": (TOKEN, WHIRL, 50, 60), "OV_Q": (W.WSOL, WHIRL, 60, 49)},
                      tip=250_000, fee=10_000)
        rpc = FakeAuditRpc({WHIRL: [{"signature": "sig101", "slot": 101}]}, {"sig101": arb})
        aud = W.Auditor(e, rpc, self.dir, pause=0)
        self.addCleanup(aud.close)
        aud.find_closer({"watch": "TEST", "buy_pool": "A", "sell_pool": "Orca", "open_slot": 100,
                         "close_slot": 101, "opened_utc": "2026-09-23T00:00:00+00:00"})
        s = aud.summary()
        self.assertEqual(s["closers_found"], 1)
        self.assertEqual(s["by_arbitrage_bots"], 1)
        self.assertEqual(s["leaders"][0]["signer"], "ArbBot111")
        self.assertAlmostEqual(s["median_tip_sol"], 0.00025)
        aud.closers.fh.flush()
        self.assertEqual(self.rows("closers.csv")[0]["touched_both_pools"], "True")

    def test_auditor_reality_check_job_end_to_end(self):
        e = self.engine(mixed_config())
        e.on_vault(VA_T, 10 ** 12, 100, 1000.0)
        e.on_vault(VA_Q, 10 ** 12, 100, 1000.0)
        for s_ in range(101, 108):                              # a few more states on record
            e.on_vault(VA_Q, 10 ** 12, s_, 1000.0 + s_)
        p = e.watches[0].pools[0]
        p.address = "POOLA"
        paid = 2 * 10 ** 9                                      # someone pays 2 SOL for tokens
        got = int(W.swap_out(2.0, 1000.0, 10 ** 6, 0.0025) * 1e6)
        tx = fake_tx(110, "Buyer", {VA_T: (TOKEN, "amm", 10 ** 12, 10 ** 12 - got),
                                     VA_Q: (W.WSOL, "amm", 10 ** 12, 10 ** 12 + paid)})
        rpc = FakeAuditRpc({"POOLA": [{"signature": "sig110", "slot": 110}]}, {"sig110": tx})
        aud = W.Auditor(e, rpc, self.dir, pause=0)
        self.addCleanup(aud.close)
        e.watches[0].pools[1].history.clear()                   # only the cp pool is eligible
        aud.check_random_pool()
        s = aud.summary()
        self.assertEqual(s["checks"], 1)
        self.assertLess(s["median_error_pct"], 0.001)
        self.assertEqual(s["recent_checks"][0]["direction"], "buy token")

    def test_report_lists_the_new_sections(self):
        e = self.engine(mixed_config())
        e.rec.close()
        text = W.report(self.dir)
        for head in ("TRIANGLE LOOPS", "WHO CLOSED THE GAPS", "PRICE-MATHS REALITY CHECK"):
            self.assertIn(head, text)


if __name__ == "__main__":
    unittest.main()
