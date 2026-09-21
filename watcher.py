#!/usr/bin/env python3
"""
Orbit Dislocation Watcher — READ-ONLY research tool.

What it does
    Streams live price changes of a token's pools on Solana (Raydium AMM v4,
    Raydium CPMM, PumpSwap, Orca Whirlpool, Raydium CLMM, Meteora DLMM; quoted
    in SOL or USDC) and measures how often a price gap between two pools
    appears, how big it is after fees, and how quickly someone closes it.

What it never does
    It never signs or sends a transaction, never connects a wallet, and never
    needs a private key or seed phrase. There is no code path for trading.

Standard library only: no pip installs are needed. Python 3.10 or newer.

Commands (run from this folder):
    python watcher.py discover <TOKEN_MINT> --write   find pools, save config
    python watcher.py watch                           start recording
    python watcher.py report                          summarise what was seen
    python watcher.py replay                          re-analyse saved data
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import collections
import csv
import hashlib
import json
import math
import os
import ssl
import statistics
import struct
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

VERSION = "0.3"
ROOT = Path(__file__).resolve().parent
USER_AGENT = f"orbit-dislocation-watcher/{VERSION}"

# ---------------------------------------------------------------------------
# Known addresses
# ---------------------------------------------------------------------------
WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
QUOTE_SYMBOL = {WSOL: "SOL", USDC: "USDC"}

# Screening assumptions (edit per watch in config.json or the dashboard):
DEFAULT_COST_SOL = 0.0005        # cost per attempt: network fee + priority fee + tip
DEFAULT_MIN_PROFIT_SOL = 0.0005  # exact pairs: minimum net profit after cost
DEFAULT_MIN_NET_GAP_PCT = 0.2    # other pairs: minimum price gap after all fees
DEFAULT_BIG_SHOCK_PCT = 3.0      # a single-update move this large is an ANB-type event
DEPTH_SIZES_SOL = (0.25, 1.0, 5.0)  # trade sizes simulated for the depth check

# Constant-product pools: price = quote reserve / token reserve (read from vaults).
# Value: (display name, default fee). Check the real fee of each pool.
SUPPORTED_PROGRAMS = {
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": ("Raydium AMM v4", 0.0025),
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C": ("Raydium CPMM", 0.0025),
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": ("PumpSwap", 0.0030),
}
# Concentrated-liquidity pools: price decoded from the pool account itself.
# Byte offsets come from each program's published source code.
CL_PROGRAMS = {
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": ("Orca Whirlpool", "whirlpool"),
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": ("Raydium CLMM", "clmm"),
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": ("Meteora DLMM", "dlmm"),
}
KINDS = {"cp", "whirlpool", "clmm", "dlmm"}
# Still unsupported (shared vaults / other pricing).
KNOWN_UNSUPPORTED = {
    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG": "Meteora DAMM v2",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB": "Meteora DAMM v1",
}
# Fallback SOL/USDC reference pool (Raydium AMM v4), used for SOL<->USDC comparisons.
SOL_USDC_FALLBACK = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2"


# ---------------------------------------------------------------------------
# Pool-account decoding (little-endian fields; offsets include the 8-byte
# Anchor discriminator)
# ---------------------------------------------------------------------------
def _u(data: bytes, off: int, size: int, signed: bool = False) -> int:
    if len(data) < off + size:
        raise ValueError("pool account too short")
    return int.from_bytes(data[off:off + size], "little", signed=signed)


# (mint A offset, mint B offset) per kind; A/B = Whirlpool a/b, CLMM 0/1, DLMM x/y
MINT_OFFSETS = {"whirlpool": (101, 181), "clmm": (73, 105), "dlmm": (88, 120)}


def layout_mints(kind: str, data: bytes) -> tuple[str, str]:
    a, b = MINT_OFFSETS[kind]
    if len(data) < b + 32:
        raise ValueError("pool account too short")
    return b58encode(data[a:a + 32]), b58encode(data[b:b + 32])


def dlmm_fee(data: bytes) -> float:
    """Meteora DLMM total fee rate (base + variable), as a fraction."""
    base_factor, bin_step = _u(data, 8, 2), _u(data, 80, 2)
    vfc, power, va = _u(data, 16, 4), data[34], _u(data, 40, 4)
    base = base_factor * bin_step * 10 * 10 ** power
    var = 0
    if vfc:
        sq = (va * bin_step) ** 2
        var = -(-(vfc * sq) // 100_000_000_000)  # ceil division
    return min(base + var, 100_000_000) / 1_000_000_000


def raw_price_ab(kind: str, data: bytes) -> float:
    """Price of 1 raw unit of token A in raw units of token B."""
    if kind == "whirlpool":
        return (_u(data, 65, 16) / 2 ** 64) ** 2
    if kind == "clmm":
        return (_u(data, 253, 16) / 2 ** 64) ** 2
    if kind == "dlmm":
        return (1 + _u(data, 80, 2) / 10_000) ** _u(data, 76, 4, signed=True)
    raise ValueError(kind)


def live_fee(kind: str, data: bytes) -> float | None:
    if kind == "whirlpool":
        return _u(data, 45, 2) / 1_000_000   # hundredths of a basis point
    if kind == "dlmm":
        # Swap fee plus one bin width: DLMM prices move in bins, and the active bin may have
        # run out of the token you want to buy, so the real price can be one bin away.
        return min(dlmm_fee(data) + _u(data, 80, 2) / 10_000, 0.2)
    return None  # CLMM fee lives in a separate config account


def state_extra(kind: str, data: bytes) -> dict:
    """Liquidity and exact sqrt price (Whirlpool/CLMM) or bin width (DLMM), for depth checks."""
    if kind == "whirlpool":
        return {"L": _u(data, 49, 16), "sq": _u(data, 65, 16) / 2 ** 64}
    if kind == "clmm":
        return {"L": _u(data, 237, 16), "sq": _u(data, 253, 16) / 2 ** 64}
    if kind == "dlmm":
        return {"bin": _u(data, 80, 2) / 10_000}
    return {}


def now_iso(t: float | None = None) -> str:
    return datetime.fromtimestamp(time.time() if t is None else t, timezone.utc).isoformat(timespec="milliseconds")


# ---------------------------------------------------------------------------
# Base58 (Solana addresses are 32 bytes written in base58)
# ---------------------------------------------------------------------------
B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58encode(data: bytes) -> str:
    n = int.from_bytes(data, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = B58[r] + out
    return "1" * (len(data) - len(data.lstrip(b"\0"))) + out


def b58decode(text: str) -> bytes:
    n = 0
    for ch in text:
        n = n * 58 + B58.index(ch)
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\0" * (len(text) - len(text.lstrip("1"))) + body


# ---------------------------------------------------------------------------
# Pool maths (constant product: x * y = k)
# ---------------------------------------------------------------------------
def swap_out(amount_in: float, reserve_in: float, reserve_out: float, fee: float) -> float:
    """Tokens received for `amount_in`, after the pool fee."""
    if amount_in <= 0:
        return 0.0
    a = amount_in * (1 - fee)
    return reserve_out * a / (reserve_in + a)


def best_round_trip(cheap: tuple, rich: tuple) -> tuple[float, float]:
    """
    Buy the token on the cheap pool with quote currency, sell it on the rich pool.
    Each pool is (token_reserve, quote_reserve, fee) in normal units.
    Returns (best trade size in quote, gross profit in quote), or (0, 0).
    The profit curve has one peak, so a simple ternary search finds it.
    """
    cb, cq, cf = cheap
    rb, rq, rf = rich

    def profit(q: float) -> float:
        return swap_out(swap_out(q, cq, cb, cf), rb, rq, rf) - q

    lo, hi = 0.0, cq
    for _ in range(200):
        m1, m2 = lo + (hi - lo) / 3, hi - (hi - lo) / 3
        if profit(m1) < profit(m2):
            lo = m1
        else:
            hi = m2
    q = (lo + hi) / 2
    p = profit(q)
    return (q, p) if p > 0 else (0.0, 0.0)


# ---------------------------------------------------------------------------
# Output files
# ---------------------------------------------------------------------------
DISLOCATION_FIELDS = ["opened_utc", "watch", "buy_pool", "sell_pool", "method", "open_slot", "close_slot",
                      "slots_open", "seconds_open", "peak_gap_pct", "peak_net_gap_pct", "peak_net_quote",
                      "best_size_quote", "quote", "depth_net_sol_0_25", "depth_net_sol_1",
                      "depth_net_sol_5", "tradable"]
SHOCK_FIELDS = ["time_utc", "slot", "watch", "pool", "move_pct", "best_net_gap_pct_visible",
                "best_net_quote_visible", "profitable_gap_visible", "quote"]


# Column layouts written by earlier versions, so mixed files can be repaired.
_D1 = ["opened_utc", "watch", "buy_pool", "sell_pool", "open_slot", "close_slot", "slots_open", "seconds_open",
       "peak_gap_pct", "peak_net_quote", "best_size_quote", "quote"]
_D2 = _D1[:4] + ["method"] + _D1[4:8] + ["peak_gap_pct", "peak_net_gap_pct", "peak_net_quote", "best_size_quote",
                                        "quote"]
_S1 = ["time_utc", "slot", "watch", "pool", "move_pct", "best_net_quote_visible", "profitable_gap_visible", "quote"]
CSV_HISTORY = {"dislocations.csv": [_D1, _D2], "shocks.csv": [_S1]}


def migrate_csv(path: Path, fields: list[str]) -> None:
    """Rewrite a CSV whose header differs from `fields` (older version), keeping every row.

    Rows matching the old header are mapped by column name; rows already written in the
    new layout (appended under an old header) are mapped by position."""
    if not path.exists() or path.stat().st_size == 0:
        return
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    if not rows or rows[0] == fields:
        return
    old = rows[0]
    layouts = [old, fields] + [h for h in CSV_HISTORY.get(path.name, []) if h != old]
    out = []
    for r in rows[1:]:
        layout = next((h for h in layouts if len(h) == len(r)), None)
        if layout:
            out.append({k: v for k, v in zip(layout, r) if k in fields})
    tmp = path.with_suffix(".migrating")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, restval="")
        w.writeheader()
        w.writerows(out)
    tmp.replace(path)


class CsvFile:
    def __init__(self, path: Path, fields: list[str]):
        migrate_csv(path, fields)
        new = not path.exists() or path.stat().st_size == 0
        self.fh = open(path, "a", newline="", encoding="utf-8")
        self.w = csv.DictWriter(self.fh, fieldnames=fields, extrasaction="ignore")
        if new:
            self.w.writeheader()
            self.fh.flush()

    def write(self, row: dict) -> None:
        self.w.writerow(row)
        self.fh.flush()

    def close(self) -> None:
        self.fh.close()


class Recorder:
    """Writes findings to the output folder."""

    def __init__(self, folder: Path, keep_raw: bool = True):
        folder.mkdir(parents=True, exist_ok=True)
        self.folder = folder
        self.dislocations = CsvFile(folder / "dislocations.csv", DISLOCATION_FIELDS)
        self.shocks = CsvFile(folder / "shocks.csv", SHOCK_FIELDS)
        self.raw_fh = open(folder / "raw_updates.jsonl", "a", encoding="utf-8") if keep_raw else None
        self._raw_count = 0
        self._last_flush = 0.0

    def raw(self, t: float, account: str, slot: int, amount: int | None = None,
            price: float | None = None, fee: float | None = None, extra: dict | None = None) -> None:
        """One line per account update: vault balances ('a') or decoded pool state ('p', 'f')."""
        if not self.raw_fh:
            return
        row = {"t": round(t, 3), "v": account, "s": slot}
        if amount is not None:
            row["a"] = amount
        else:
            row["p"] = price
            if fee is not None:
                row["f"] = fee
            if extra:
                row["x"] = extra
        self.raw_fh.write(json.dumps(row) + "\n")
        self._raw_count += 1
        if self._raw_count % 50 == 0 or time.monotonic() - self._last_flush > 2:
            self.raw_fh.flush()
            self._last_flush = time.monotonic()

    def write_json(self, name: str, data: dict) -> None:
        tmp = self.folder / (name + ".tmp")
        tmp.write_text(json.dumps(data), encoding="utf-8")
        tmp.replace(self.folder / name)

    def close(self) -> None:
        self.dislocations.close()
        self.shocks.close()
        if self.raw_fh:
            self.raw_fh.close()


# ---------------------------------------------------------------------------
# The engine: keeps pool state, spots shocks and gaps
# ---------------------------------------------------------------------------
class Pool:
    """One pool of a token against SOL or USDC."""

    def __init__(self, d: dict, token_decimals: int, default_quote_mint: str | None = None,
                 default_quote_decimals: int | None = None):
        self.name = d["name"]
        self.address = d.get("address", "")
        self.kind = d.get("kind", "cp")
        if self.kind not in KINDS:
            raise ValueError(f"Unknown pool kind {self.kind!r}")
        self.quote_mint = d.get("quote_mint", default_quote_mint)
        qdec = d.get("quote_decimals", default_quote_decimals)
        if self.quote_mint is None or qdec is None:
            raise ValueError(f"Pool {self.name!r} needs quote_mint and quote_decimals")
        self.qdec, self.tdec = int(qdec), int(token_decimals)
        self.quote_sym = QUOTE_SYMBOL.get(self.quote_mint, "quote")
        self.fee = float(d["fee"])
        if self.kind == "cp":
            self.base_vault, self.quote_vault = d["base_vault"], d["quote_vault"]
        else:
            if not self.address:
                raise ValueError(f"Pool {self.name!r} needs its address")
            self.token_is_a = bool(d["token_is_a"])
        self.base: int | None = None
        self.quote: int | None = None
        self.state_price: float | None = None
        self.slots: dict[str, int] = {}
        self.last_price: float | None = None
        self.suspect = False
        self.L: int | None = None        # Whirlpool/CLMM active liquidity (raw)
        self.sq: float | None = None     # sqrt(raw price of A in B)
        self.bin_step: float | None = None  # DLMM bin width as a fraction

    def accounts(self) -> list[tuple[str, str, str]]:
        """(address, role, RPC encoding) for every account to subscribe to."""
        if self.kind == "cp":
            return [(self.base_vault, "base", "jsonParsed"), (self.quote_vault, "quote", "jsonParsed")]
        return [(self.address, "state", "base64")]

    def ready(self) -> bool:
        if self.kind == "cp":
            return bool(self.base) and bool(self.quote)
        return bool(self.state_price) and self.state_price > 0

    def reserves(self) -> tuple[float, float, float]:
        return self.base / 10 ** self.tdec, self.quote / 10 ** self.qdec, self.fee

    def price(self) -> float:
        """Token price in this pool's own quote (SOL or USDC)."""
        if self.kind == "cp":
            b, q, _ = self.reserves()
            return q / b
        return self.state_price

    def can_quote_depth(self) -> bool:
        if self.kind == "cp":
            return self.ready()
        return self.kind in ("whirlpool", "clmm") and bool(self.L) and bool(self.sq)

    def buy_token(self, q: float) -> float:
        """Tokens received for q (human units of quote), including the fee."""
        if self.kind == "cp":
            b, qq, f = self.reserves()
            return swap_out(q, qq, b, f)
        L, s = self.L, self.sq
        amt = q * (1 - self.fee) * 10 ** self.qdec
        if self.token_is_a:                       # pay B, receive A
            s1 = s + amt / L
            return L * (1 / s - 1 / s1) / 10 ** self.tdec
        s1 = 1 / (1 / s + amt / L)                # pay A, receive B
        return L * (s - s1) / 10 ** self.tdec

    def sell_token(self, t: float) -> float:
        """Quote received for t tokens (human units), including the fee."""
        if self.kind == "cp":
            b, qq, f = self.reserves()
            return swap_out(t, b, qq, f)
        L, s = self.L, self.sq
        amt = t * (1 - self.fee) * 10 ** self.tdec
        if self.token_is_a:                       # pay A, receive B
            s1 = 1 / (1 / s + amt / L)
            return L * (s - s1) / 10 ** self.qdec
        s1 = s + amt / L                          # pay B, receive A
        return L * (1 / s - 1 / s1) / 10 ** self.qdec

    def decode_state(self, data: bytes) -> tuple[float, float | None]:
        """(token price in quote, live fee or None) from a concentrated-liquidity pool account."""
        a_dec, b_dec = (self.tdec, self.qdec) if self.token_is_a else (self.qdec, self.tdec)
        human_ab = raw_price_ab(self.kind, data) * 10 ** (a_dec - b_dec)
        price = human_ab if self.token_is_a else 1 / human_ab
        if not (price > 0 and math.isfinite(price)):
            raise ValueError("bad price")
        return price, live_fee(self.kind, data)


class Watch:
    def __init__(self, d: dict):
        self.label = d["label"]
        tdec = int(d["token_decimals"])
        wq, wqd = d.get("quote_mint"), d.get("quote_decimals")  # older single-quote configs
        self.cost_sol = float(d.get("cost_sol", d.get("cost_quote", DEFAULT_COST_SOL)))
        self.min_profit_sol = float(d.get("min_profit_sol", d.get("min_profit_quote", DEFAULT_MIN_PROFIT_SOL)))
        self.min_net_gap = float(d.get("min_net_gap_pct", DEFAULT_MIN_NET_GAP_PCT)) / 100
        self.shock = float(d["shock_pct"]) / 100
        self.big_shock = float(d.get("big_shock_pct", DEFAULT_BIG_SHOCK_PCT)) / 100
        self.auto_until = d.get("auto_until")
        self.pools = [Pool(p, tdec, wq, wqd) for p in d["pools"]]
        if len(self.pools) < 2:
            raise ValueError(f"Watch {self.label!r} needs at least two pools.")
        ref = d.get("sol_usdc")
        self.ref = Pool(ref, 9) if ref else None
        if any(p.quote_mint not in QUOTE_SYMBOL for p in self.pools):
            raise ValueError("Pools must be quoted in SOL or USDC.")
        if self.ref is None and len({p.quote_mint for p in self.pools}) > 1:
            raise ValueError(f"Watch {self.label!r} mixes SOL and USDC pools but has no sol_usdc reference.")
        self.open: dict[tuple, dict] = {}

    def settings(self) -> dict:
        return {"cost_sol": self.cost_sol, "min_profit_sol": self.min_profit_sol,
                "min_net_gap_pct": round(self.min_net_gap * 100, 6), "shock_pct": round(self.shock * 100, 6),
                "big_shock_pct": round(self.big_shock * 100, 6)}


class Engine:
    def __init__(self, config: dict, recorder: Recorder):
        self.watches = [Watch(w) for w in config["watches"]]
        self.rec = recorder
        self.acct: dict[str, list] = {}       # address -> [(watch, pool, role)]
        self.encoding: dict[str, str] = {}
        for w in self.watches:
            for p in w.pools + ([w.ref] if w.ref else []):
                for addr, role, enc in p.accounts():
                    self.acct.setdefault(addr, []).append((w, p, role))
                    self.encoding[addr] = enc
        self.last_slot = 0
        self.updates = 0
        self.dirty: set[int] = set()
        self.stats = {"shocks": 0, "shock_gaps": 0, "dislocations": 0, "interrupted": 0,
                      "tradable": 0, "big_shocks": 0, "bin_steps": 0}
        self.warned: set[str] = set()
        self.on_event = None                   # callback(kind, data) for alerts
        self.tip_slot = 0                      # newest slot the RPC has announced
        self.lag_slots: collections.deque = collections.deque(maxlen=5000)

    def _emit(self, kind: str, data: dict) -> None:
        if self.on_event:
            try:
                self.on_event(kind, data)
            except Exception as e:  # alerts must never break measuring
                print(f"Alert handler error: {e}", flush=True)

    # -- latency --------------------------------------------------------------
    def note_tip(self, slot: int) -> None:
        self.tip_slot = max(self.tip_slot, slot)

    def note_arrival(self, slot: int) -> None:
        """How many slots behind the chain tip a live update arrived."""
        if self.tip_slot:
            self.lag_slots.append(max(0, self.tip_slot - slot))

    def latency_stats(self) -> dict | None:
        lags = list(self.lag_slots)
        if not lags:
            return None
        lags.sort()
        return {"samples": len(lags), "median_slots": lags[len(lags) // 2],
                "p90_slots": lags[min(len(lags) - 1, int(len(lags) * 0.9))],
                "same_slot_pct": round(100 * sum(x == 0 for x in lags) / len(lags), 1)}

    @property
    def vaults(self) -> list[str]:
        """Every account address the engine needs (vaults and pool accounts)."""
        return list(self.acct)

    # -- input --------------------------------------------------------------
    def _apply(self, addr: str, slot: int, setter) -> bool:
        entries = self.acct.get(addr)
        if not entries or any(slot < p.slots.get(role, 0) for _, p, role in entries):
            return False  # unknown account, or older than what we already have
        for w, p, role in entries:
            setter(p, role)
            p.slots[role] = slot
            self.dirty.add(id(w))
        self.last_slot = max(self.last_slot, slot)
        self.updates += 1
        return True

    def on_vault(self, vault: str, amount: int, slot: int, t: float, log: bool = True) -> None:
        """New token balance of a constant-product pool vault."""
        if self._apply(vault, slot, lambda p, role: setattr(p, role, int(amount))) and log:
            self.rec.raw(t, vault, slot, amount=int(amount))

    def on_state(self, addr: str, price: float, fee: float | None, slot: int, t: float, log: bool = True,
                 extra: dict | None = None) -> None:
        """New decoded price (and live fee, liquidity) of a concentrated-liquidity pool."""
        extra = extra or {}

        def setter(p, role):
            p.state_price = price
            if fee is not None:
                p.fee = fee
            if "L" in extra:
                p.L, p.sq = int(extra["L"]), float(extra["sq"])
            if "bin" in extra:
                p.bin_step = float(extra["bin"])
        if self._apply(addr, slot, setter) and log:
            self.rec.raw(t, addr, slot, price=price, fee=fee, extra=extra)

    def on_account(self, addr: str, value, slot: int, t: float) -> None:
        """Raw RPC account value (snapshot or live notification)."""
        if not value:
            return
        if self.encoding.get(addr) == "jsonParsed":
            info = token_info(value)
            if info:
                self.on_vault(addr, info[1], slot, t)
            return
        entries = self.acct.get(addr)
        if not entries:
            return
        try:
            data = base64.b64decode(value["data"][0])
            pool = entries[0][1]
            price, fee = pool.decode_state(data)
            extra = state_extra(pool.kind, data)
        except (KeyError, TypeError, ValueError, IndexError, OverflowError, ZeroDivisionError):
            return
        self.on_state(addr, price, fee, slot, t, extra=extra)

    # -- analysis -----------------------------------------------------------
    def to_sol(self, w: Watch, p: Pool) -> float | None:
        if not p.ready():
            return None
        if p.quote_mint == WSOL:
            return p.price()
        if w.ref and w.ref.ready():
            return p.price() / w.ref.price()
        return None

    def evaluate(self, t: float) -> None:
        """Check every watch that changed since the last check."""
        for w in self.watches:
            if id(w) in self.dirty:
                self._evaluate_watch(w, t)
        self.dirty.clear()

    def interrupt(self) -> None:
        """Connection lost: open gaps can no longer be timed honestly, so drop them."""
        for w in self.watches:
            self.stats["interrupted"] += len(w.open)
            w.open.clear()

    def _check_suspects(self, w: Watch, pools: list, sol: dict) -> None:
        """A concentrated pool >50% away from the rest almost certainly means a decoding problem."""
        cp = [sol[id(p)] for p in pools if p.kind == "cp"]
        if not cp and len(pools) < 3:
            return
        anchor = statistics.median(cp or [sol[id(p)] for p in pools])
        for p in pools:
            if p.kind != "cp" and not p.suspect and abs(sol[id(p)] / anchor - 1) > 0.5:
                p.suspect = True
                print(f"{w.label}: {p.name} price {sol[id(p)]:.10g} SOL is far from {anchor:.10g} SOL; "
                      "excluding it. Please report this.", flush=True)

    def _depth(self, w: Watch, cheap: Pool, rich: Pool, cross: bool) -> dict | None:
        """Net SOL after fees and cost for a real round trip at each test size, or None if unknown."""
        if not (cheap.can_quote_depth() and rich.can_quote_depth()):
            return None
        conv = {}
        for p in (cheap, rich):
            if p.quote_mint == WSOL:
                conv[id(p)] = 1.0
            elif w.ref and w.ref.ready():
                conv[id(p)] = w.ref.price()      # USDC per SOL
            else:
                return None
        ref_fee = w.ref.fee if cross and w.ref else 0.0
        out = {}
        try:
            for size in DEPTH_SIZES_SOL:
                tokens = cheap.buy_token(size * conv[id(cheap)])
                sol_back = rich.sell_token(tokens) / conv[id(rich)] * (1 - ref_fee)
                out[size] = sol_back - size - w.cost_sol
        except (ZeroDivisionError, OverflowError, ValueError):
            return None
        return out

    def _evaluate_watch(self, w: Watch, t: float) -> None:
        sol = {id(p): self.to_sol(w, p) for p in w.pools}
        ready = [p for p in w.pools if sol[id(p)]]
        self._check_suspects(w, ready, sol)
        ready = [p for p in ready if not p.suspect]
        vis_gap: dict[int, float] = {}
        vis_net: dict[int, float] = {}
        vis_ok: dict[int, bool] = {}
        for i in range(len(ready)):
            for j in range(i + 1, len(ready)):
                a, b = ready[i], ready[j]
                cheap, rich = (a, b) if sol[id(a)] <= sol[id(b)] else (b, a)
                gap = sol[id(rich)] / sol[id(cheap)] - 1
                cross = cheap.quote_mint != rich.quote_mint
                ref_fee = w.ref.fee if cross and w.ref else 0.0
                net_gap = (1 + gap) * (1 - cheap.fee) * (1 - rich.fee) * (1 - ref_fee) - 1
                method, size, net = "gap", 0.0, None
                if cheap.kind == "cp" and rich.kind == "cp" and not cross:
                    conv = 1.0 if cheap.quote_mint == WSOL else (w.ref.price() if w.ref and w.ref.ready() else None)
                    if conv:
                        method = "exact"
                        size, gross = best_round_trip(cheap.reserves(), rich.reserves())
                        net = (gross if gross > 0 else 0.0) - w.cost_sol * conv
                        ok = net >= w.min_profit_sol * conv
                if method == "gap":
                    ok = net_gap >= w.min_net_gap
                quote = cheap.quote_sym if not cross else "SOL"
                for p in (a, b):
                    vis_gap[id(p)] = max(vis_gap.get(id(p), float("-inf")), net_gap)
                    vis_ok[id(p)] = vis_ok.get(id(p), False) or ok
                    if net is not None:
                        vis_net[id(p)] = max(vis_net.get(id(p), float("-inf")), net)
                depth = self._depth(w, cheap, rich, cross) if ok else None
                self._track(w, (a.name, b.name), cheap, rich, gap, net_gap, method, size, net, ok, quote, t, depth)

        for p in ready:
            price = p.price()  # own quote, so SOL/USD moves don't count as shocks
            if p.last_price:
                move = price / p.last_price - 1
                if p.kind == "dlmm" and p.bin_step and abs(move) <= p.bin_step * 1.5 + 1e-12:
                    self.stats["bin_steps"] += 1        # Meteora moved one bin: normal, not a shock
                elif abs(move) >= w.shock:
                    visible = vis_ok.get(id(p), False)
                    if abs(move) >= w.big_shock:
                        self.stats["big_shocks"] += 1
                        self._emit("big_shock", {"watch": w.label, "pool": p.name, "move_pct": round(move * 100, 3),
                                                 "slot": self.last_slot, "gap_visible": visible})
                    self.stats["shocks"] += 1
                    self.stats["shock_gaps"] += int(visible)
                    g, n = vis_gap.get(id(p)), vis_net.get(id(p))
                    self.rec.shocks.write({
                        "time_utc": now_iso(t), "slot": self.last_slot, "watch": w.label, "pool": p.name,
                        "move_pct": round(move * 100, 4),
                        "best_net_gap_pct_visible": round(g * 100, 4) if g is not None else "",
                        "best_net_quote_visible": round(n, 9) if n is not None else "",
                        "profitable_gap_visible": "yes" if visible else "no", "quote": p.quote_sym})
            p.last_price = price

    def _track(self, w, key, cheap, rich, gap, net_gap, method, size, net, ok, quote, t, depth=None) -> None:
        cur = w.open.get(key)
        best = max(depth.values()) if depth else None
        if ok:
            if cur is None:
                w.open[key] = dict(start=t, start_slot=self.last_slot, buy=cheap.name, sell=rich.name,
                                   method=method, peak_gap=gap, peak_net_gap=net_gap, peak_net=net,
                                   size=size, quote=quote, depth=depth, depth_best=best)
            else:
                if best is not None and (cur["depth_best"] is None or best > cur["depth_best"]):
                    cur.update(depth=depth, depth_best=best)
                cur["peak_gap"] = max(cur["peak_gap"], gap)
                if net_gap > cur["peak_net_gap"]:
                    cur.update(peak_net_gap=net_gap, buy=cheap.name, sell=rich.name)
                if net is not None and (cur["peak_net"] is None or net > cur["peak_net"]):
                    cur.update(peak_net=net, size=size)
        elif cur is not None:
            self.stats["dislocations"] += 1
            d = cur["depth"]
            tradable = "unknown" if d is None else ("yes" if cur["depth_best"] >= w.min_profit_sol else "no")
            self.stats["tradable"] += tradable == "yes"
            row = {
                "opened_utc": now_iso(cur["start"]), "watch": w.label, "buy_pool": cur["buy"],
                "sell_pool": cur["sell"], "method": cur["method"], "open_slot": cur["start_slot"],
                "close_slot": self.last_slot, "slots_open": self.last_slot - cur["start_slot"],
                "seconds_open": round(t - cur["start"], 3),
                "peak_gap_pct": round(cur["peak_gap"] * 100, 4),
                "peak_net_gap_pct": round(cur["peak_net_gap"] * 100, 4),
                "peak_net_quote": round(cur["peak_net"], 9) if cur["peak_net"] is not None else "",
                "best_size_quote": round(cur["size"], 6) if cur["peak_net"] is not None else "",
                "quote": cur["quote"],
                "depth_net_sol_0_25": round(d[0.25], 6) if d else "",
                "depth_net_sol_1": round(d[1.0], 6) if d else "",
                "depth_net_sol_5": round(d[5.0], 6) if d else "",
                "tradable": tradable}
            self.rec.dislocations.write(row)
            self._emit("gap_closed", row)
            del w.open[key]

    def open_count(self) -> int:
        return sum(len(w.open) for w in self.watches)

    def best_net_gap_pct(self, w: Watch) -> float | None:
        """Largest price gap between any two pools right now, after both fees (and the SOL/USDC fee)."""
        pools = [(p, px) for p in w.pools if not p.suspect and (px := self.to_sol(w, p))]
        best = None
        for i in range(len(pools)):
            for j in range(i + 1, len(pools)):
                (a, pa), (b, pb) = pools[i], pools[j]
                ref_fee = w.ref.fee if a.quote_mint != b.quote_mint and w.ref else 0.0
                net = (max(pa, pb) / min(pa, pb)) * (1 - a.fee) * (1 - b.fee) * (1 - ref_fee) - 1
                best = net if best is None else max(best, net)
        return None if best is None else best * 100

    def max_gap_pct(self, w: Watch) -> float | None:
        prices = [s for p in w.pools if not p.suspect and (s := self.to_sol(w, p))]
        if len(prices) < 2:
            return None
        return (max(prices) / min(prices) - 1) * 100


# ---------------------------------------------------------------------------
# Solana JSON-RPC over HTTPS (read-only calls only)
# ---------------------------------------------------------------------------
class Rpc:
    def __init__(self, url: str, pause: float = 0.15):
        self.url = url
        self.pause = pause

    def call(self, method: str, params: list):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
        for attempt in range(5):
            req = urllib.request.Request(self.url, body, {"Content-Type": "application/json",
                                                          "User-Agent": USER_AGENT})
            try:
                with urllib.request.urlopen(req, timeout=20) as r:
                    out = json.load(r)
                time.sleep(self.pause)
                if "error" in out:
                    raise RuntimeError(f"RPC error in {method}: {out['error'].get('message', out['error'])}")
                return out["result"]
            except urllib.error.HTTPError as e:
                if e.code == 429 or e.code >= 500:
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError(f"RPC HTTP {e.code} for {method}. Check the RPC address or key.") from None
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                time.sleep(2 ** attempt)
        raise RuntimeError(f"RPC unreachable for {method} after several tries.")


def get_multiple(rpc, keys: list[str], encoding: str = "jsonParsed") -> tuple[int, list]:
    """getMultipleAccounts in batches of 100. Returns (latest slot, values)."""
    slot, values = 0, []
    for i in range(0, len(keys), 100):
        res = rpc.call("getMultipleAccounts", [keys[i:i + 100], {"encoding": encoding, "commitment": "processed"}])
        slot = max(slot, res["context"]["slot"])
        values.extend(res["value"])
    return slot, values


def token_info(value) -> tuple[str, int, int] | None:
    """(mint, raw amount, decimals) if the account is an SPL token account."""
    try:
        parsed = value["data"]["parsed"]
        if parsed.get("type") != "account":
            return None
        info = parsed["info"]
        return info["mint"], int(info["tokenAmount"]["amount"]), int(info["tokenAmount"]["decimals"])
    except (TypeError, KeyError, ValueError, AttributeError):
        return None


def find_vaults(rpc, pool_data: bytes, mints: set[str]) -> dict[str, tuple[str, int, int]]:
    """
    Find the pool's two reserve vaults without knowing each DEX's data layout:
    every 32-byte window of the pool account could be an address, so we ask the
    RPC which of them are token accounts holding one of our two mints, and keep
    the largest per mint. Returns {mint: (vault, raw amount, decimals)}.
    """
    offsets = sorted(range(0, len(pool_data) - 31), key=lambda o: (o % 8 not in (0, 3), o))
    seen, candidates = set(), []
    for o in offsets:
        chunk = pool_data[o:o + 32]
        if chunk.count(0) > 16:
            continue
        key = b58encode(chunk)
        if key not in seen:
            seen.add(key)
            candidates.append(key)
    found: dict[str, tuple[str, int, int]] = {}
    for i in range(0, len(candidates), 100):
        batch = candidates[i:i + 100]
        _, values = get_multiple(rpc, batch)
        for key, value in zip(batch, values):
            info = token_info(value)
            if info and info[0] in mints and (info[0] not in found or info[1] > found[info[0]][1]):
                found[info[0]] = (key, info[1], info[2])
        if set(found) == mints:
            break
    return found


# ---------------------------------------------------------------------------
# Minimal WebSocket client (standard library only, RFC 6455)
# ---------------------------------------------------------------------------
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def ws_accept_key(key: str) -> str:
    return base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()


def _xor(data: bytes, mask: bytes) -> bytes:
    if not data:
        return data
    m = (mask * (len(data) // 4 + 1))[:len(data)]
    return (int.from_bytes(data, "big") ^ int.from_bytes(m, "big")).to_bytes(len(data), "big")


def ws_encode_frame(opcode: int, payload: bytes, mask: bool) -> bytes:
    head = bytes([0x80 | opcode])
    n = len(payload)
    bit = 0x80 if mask else 0
    if n < 126:
        head += bytes([bit | n])
    elif n < 65536:
        head += bytes([bit | 126]) + struct.pack(">H", n)
    else:
        head += bytes([bit | 127]) + struct.pack(">Q", n)
    if mask:
        key = os.urandom(4)
        return head + key + _xor(payload, key)
    return head + payload


async def ws_read_frame(reader: asyncio.StreamReader) -> tuple[bool, int, bytes]:
    b1, b2 = await reader.readexactly(2)
    fin, opcode = bool(b1 & 0x80), b1 & 0x0F
    n = b2 & 0x7F
    if n == 126:
        n = struct.unpack(">H", await reader.readexactly(2))[0]
    elif n == 127:
        n = struct.unpack(">Q", await reader.readexactly(8))[0]
    key = await reader.readexactly(4) if b2 & 0x80 else None
    payload = await reader.readexactly(n)
    return fin, opcode, _xor(payload, key) if key else payload


class WebSocket:
    """Connects, reads messages into a queue in the background, answers pings."""

    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer
        self.queue: asyncio.Queue = asyncio.Queue()
        self.last_rx = time.monotonic()
        self.closed = False
        self._task = asyncio.create_task(self._read_loop())

    @classmethod
    async def connect(cls, url: str, timeout: float = 15) -> "WebSocket":
        u = urlparse(url)
        secure = u.scheme == "wss"
        port = u.port or (443 if secure else 80)
        path = (u.path or "/") + (f"?{u.query}" if u.query else "")
        ctx = ssl.create_default_context() if secure else None
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(u.hostname, port, ssl=ctx, server_hostname=u.hostname if secure else None,
                                    limit=2 ** 24), timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        request = (f"GET {path} HTTP/1.1\r\nHost: {u.netloc}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                   f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: {USER_AGENT}\r\n\r\n")
        writer.write(request.encode())
        await writer.drain()
        head = (await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout)).decode("latin-1")
        lines = head.split("\r\n")
        if len(lines[0].split()) < 2 or lines[0].split()[1] != "101":
            writer.close()
            raise ConnectionError(f"WebSocket upgrade refused: {lines[0]}")
        headers = {k.strip().lower(): v.strip() for k, v in (l.split(":", 1) for l in lines[1:] if ":" in l)}
        if headers.get("sec-websocket-accept") != ws_accept_key(key):
            writer.close()
            raise ConnectionError("WebSocket handshake check failed.")
        return cls(reader, writer)

    async def _read_loop(self) -> None:
        parts: list[bytes] = []
        try:
            while True:
                fin, op, payload = await ws_read_frame(self.reader)
                self.last_rx = time.monotonic()
                if op == 0x9:      # ping -> pong
                    self.writer.write(ws_encode_frame(0xA, payload, True))
                    await self.writer.drain()
                elif op == 0xA:    # pong
                    pass
                elif op == 0x8:    # close
                    raise ConnectionError("Server closed the stream.")
                elif op in (0x1, 0x2, 0x0):
                    parts.append(payload)
                    if fin:
                        await self.queue.put(b"".join(parts).decode("utf-8", "replace"))
                        parts = []
        except asyncio.CancelledError:
            raise
        except (asyncio.IncompleteReadError, ConnectionError, OSError) as e:
            self.closed = True
            await self.queue.put(ConnectionError(str(e) or "Stream ended."))

    async def send(self, text: str) -> None:
        self.writer.write(ws_encode_frame(0x1, text.encode(), True))
        await self.writer.drain()

    async def ping(self) -> None:
        self.writer.write(ws_encode_frame(0x9, b"orbit", True))
        await self.writer.drain()

    async def close(self) -> None:
        self._task.cancel()
        try:
            if not self.closed:
                self.writer.write(ws_encode_frame(0x8, b"", True))
            self.writer.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Live watching
# ---------------------------------------------------------------------------
def snapshot(engine: Engine, rpc) -> None:
    """Load the current state of every vault and pool account before streaming changes."""
    t = time.time()
    for enc in ("jsonParsed", "base64"):
        keys = [a for a in engine.vaults if engine.encoding[a] == enc]
        if keys:
            slot, values = get_multiple(rpc, keys, encoding=enc)
            for addr, value in zip(keys, values):
                engine.on_account(addr, value, slot, t)
    engine.evaluate(t)


def print_status(engine: Engine, started: float) -> None:
    mins = (time.time() - started) / 60
    gaps = "  ".join(f"{w.label} gap {g:.3f}%" for w in engine.watches
                     if (g := engine.max_gap_pct(w)) is not None)
    s = engine.stats
    lat = engine.latency_stats()
    lag = f"behind tip {lat['median_slots']} slots (p90 {lat['p90_slots']}) | " if lat else ""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {mins:5.1f} min | updates {engine.updates} | "
          f"slot {engine.last_slot} | {lag}shocks {s['shocks']} | gaps closed {s['dislocations']} "
          f"(tradable {s['tradable']}) | open now {engine.open_count()} | {gaps}", flush=True)
    if lat:
        try:
            engine.rec.write_json("latency.json", lat)
        except OSError:
            pass


async def run_watch(config: dict, rpc, recorder: Recorder, ws_url: str, stop_after: float | None = None,
                    status_every: float = 60, first_backoff: float = 2, eval_delay: float | None = None,
                    connect=WebSocket.connect, on_engine=None) -> Engine:
    engine = Engine(config, recorder)
    if on_engine:
        on_engine(engine)
    delay = (config.get("evaluate_delay_ms", 150) if eval_delay is None else eval_delay * 1000) / 1000
    commitment = config.get("commitment", "processed")
    loop = asyncio.get_running_loop()
    started = time.time()
    backoff = first_backoff
    print(f"Watching {len(engine.watches)} watch(es), {len(engine.vaults)} accounts. Read-only. Ctrl+C to stop.",
          flush=True)

    def stop_now() -> bool:
        return stop_after is not None and time.time() - started >= stop_after

    while not stop_now():
        ws = None
        timer = None
        try:
            await asyncio.to_thread(snapshot, engine, rpc)
            ws = await connect(ws_url)
            pending, subs = {}, {}
            for i, vault in enumerate(engine.vaults, 1):
                pending[i] = vault
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": i, "method": "accountSubscribe",
                                          "params": [vault, {"encoding": engine.encoding[vault],
                                                             "commitment": commitment}]}))
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": len(engine.vaults) + 1, "method": "slotSubscribe"}))
            backoff = first_backoff
            last_ping = last_status = time.monotonic()

            def fire():
                nonlocal timer
                timer = None
                engine.evaluate(time.time())

            while not stop_now():
                try:
                    item = await asyncio.wait_for(ws.queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    item = None
                mono = time.monotonic()
                if mono - last_ping > 20:
                    await ws.ping()
                    last_ping = mono
                if mono - ws.last_rx > 75:
                    raise ConnectionError("No data for 75 seconds.")
                if mono - last_status >= status_every:
                    print_status(engine, started)
                    last_status = mono
                if item is None:
                    continue
                if isinstance(item, Exception):
                    raise item
                msg = json.loads(item)
                if "id" in msg and msg["id"] in pending:
                    vault = pending.pop(msg["id"])
                    if "error" in msg:
                        raise RuntimeError(f"Subscription refused for {vault}: {msg['error'].get('message')}")
                    subs[msg["result"]] = vault
                    continue
                if msg.get("method") == "slotNotification":
                    engine.note_tip(int(msg["params"]["result"]["slot"]))
                    continue
                if msg.get("method") == "accountNotification":
                    p = msg["params"]
                    vault = subs.get(p["subscription"])
                    if vault:
                        slot = p["result"]["context"]["slot"]
                        engine.note_arrival(slot)
                        engine.on_account(vault, p["result"]["value"], slot, time.time())
                        if timer is None:
                            timer = loop.call_later(delay, fire)
        except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError, json.JSONDecodeError) as e:
            engine.interrupt()
            if stop_now():
                break
            print(f"Connection problem: {e}. Reconnecting in {backoff:.0f}s...", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)
        finally:
            if timer is not None:
                timer.cancel()
            if ws is not None:
                await ws.close()
    engine.evaluate(time.time())
    return engine


# ---------------------------------------------------------------------------
# Discovery: find supported pools for a token
# ---------------------------------------------------------------------------
def fetch_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def dexscreener_pairs(mint: str, fetch=fetch_json) -> list[dict]:
    try:
        data = fetch(f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}")
        pairs = data if isinstance(data, list) else data.get("pairs") or []
    except Exception:
        data = fetch(f"https://api.dexscreener.com/latest/dex/tokens/{mint}")
        pairs = data.get("pairs") or []
    return [p for p in pairs if p.get("chainId", "solana") == "solana"]


MIN_LIQUIDITY_USD = 10_000  # smaller pools are usually abandoned: their "gaps" never close


def mint_decimals(rpc, mints: list[str]) -> dict[str, int]:
    _, values = get_multiple(rpc, mints)
    out = {}
    for m, v in zip(mints, values):
        try:
            out[m] = int(v["data"]["parsed"]["info"]["decimals"])
        except (TypeError, KeyError, ValueError):
            pass
    return out


def find_pools(mint: str, rpc, pairs: list[dict], quotes: set[str], max_pools: int,
               min_liquidity_usd: float) -> tuple[list[dict], list[str], str | None, int | None]:
    """Inspect DexScreener pairs; return (pool configs, notes, token symbol, token decimals)."""
    notes, pools, symbol, decimals = [], [], None, {}
    for pair in sorted(pairs, key=lambda p: -((p.get("liquidity") or {}).get("usd") or 0)):
        base, quote = pair["baseToken"]["address"], pair["quoteToken"]["address"]
        other = quote if base == mint else base if quote == mint else None
        if other is None:
            continue
        symbol = symbol or (pair["baseToken"] if base == mint else pair["quoteToken"]).get("symbol")
        addr = pair["pairAddress"]
        liq = (pair.get("liquidity") or {}).get("usd", 0) or 0
        if other not in quotes:
            notes.append(f"skip {addr[:8]}…: paired with {QUOTE_SYMBOL.get(other, other[:8] + '…')}, "
                         f"not {' or '.join(QUOTE_SYMBOL[q] for q in sorted(quotes))}")
            continue
        if liq < min_liquidity_usd:
            notes.append(f"skip {addr[:8]}…: liquidity ${liq:,.0f} is below ${min_liquidity_usd:,.0f} "
                         "(dust pools show gaps nobody can trade)")
            continue
        if len(pools) >= max_pools:
            notes.append(f"skip {addr[:8]}…: already have {max_pools} bigger pools")
            continue
        acct = rpc.call("getAccountInfo", [addr, {"encoding": "base64"}])["value"]
        if not acct:
            notes.append(f"skip {addr[:8]}…: account not found")
            continue
        owner, data = acct["owner"], base64.b64decode(acct["data"][0])
        qsym = QUOTE_SYMBOL[other]
        if owner in SUPPORTED_PROGRAMS:
            dex, fee = SUPPORTED_PROGRAMS[owner]
            vaults = find_vaults(rpc, data, {mint, other})
            if set(vaults) != {mint, other}:
                notes.append(f"skip {addr[:8]}… ({dex}): could not locate both vaults")
                continue
            decimals.setdefault(mint, vaults[mint][2])
            pools.append(dict(name=f"{dex} {addr[:4]}", kind="cp", address=addr, quote_mint=other,
                              quote_decimals=vaults[other][2], base_vault=vaults[mint][0],
                              quote_vault=vaults[other][0], fee=fee, liquidity_usd=round(liq)))
        elif owner in CL_PROGRAMS:
            dex, kind = CL_PROGRAMS[owner]
            try:
                ma, mb = layout_mints(kind, data)
            except ValueError:
                ma = mb = None
            if {ma, mb} != {mint, other}:
                notes.append(f"skip {addr[:8]}… ({dex}): pool data did not match the expected layout")
                continue
            missing = [m for m in (mint, other) if m not in decimals]
            if missing:
                decimals.update(mint_decimals(rpc, missing))
            if mint not in decimals or other not in decimals:
                notes.append(f"skip {addr[:8]}… ({dex}): could not read token decimals")
                continue
            if kind == "clmm":
                cfg = rpc.call("getAccountInfo", [b58encode(data[9:41]), {"encoding": "base64"}])["value"]
                fee = _u(base64.b64decode(cfg["data"][0]), 47, 4) / 1_000_000 if cfg else 0.0
                if not 0 < fee < 0.1:
                    fee = 0.0025
                    notes.append(f"note {addr[:8]}… ({dex}): fee not readable, assuming 0.25%")
            else:
                fee = live_fee(kind, data)
            pools.append(dict(name=f"{dex} {addr[:4]}", kind=kind, address=addr, quote_mint=other,
                              quote_decimals=decimals[other], token_is_a=(ma == mint), fee=round(fee, 8),
                              liquidity_usd=round(liq)))
        else:
            name = KNOWN_UNSUPPORTED.get(owner, f"program {owner[:8]}…")
            notes.append(f"skip {addr[:8]}… ({name}): pool type not supported")
            continue
        notes.append(f"ok   {addr[:8]}… {pools[-1]['name'].rsplit(' ', 1)[0]} vs {qsym}, liquidity ${liq:,.0f}")
    return pools, notes, symbol, decimals.get(mint)


def reference_pool(rpc, fetch=fetch_json) -> tuple[dict | None, str]:
    """Most liquid supported SOL/USDC pool, used to compare SOL- and USDC-quoted pools."""
    try:
        pairs = [p for p in dexscreener_pairs(WSOL, fetch)
                 if {p["baseToken"]["address"], p["quoteToken"]["address"]} == {WSOL, USDC}]
    except Exception:
        pairs = []
    pairs.append({"pairAddress": SOL_USDC_FALLBACK, "liquidity": {"usd": 1},
                  "baseToken": {"address": WSOL, "symbol": "SOL"}, "quoteToken": {"address": USDC}})
    for pair in sorted(pairs, key=lambda p: -((p.get("liquidity") or {}).get("usd") or 0)):
        pools, _, _, _ = find_pools(WSOL, rpc, [pair], {USDC}, 1, 0)
        if pools:
            ref = pools[0]
            ref["name"] = "SOL/USDC ref " + ref["name"]
            return ref, f"SOL/USDC reference: {ref['name']}"
    return None, "No SOL/USDC reference pool found; USDC pools can't be compared with SOL pools."


def discover(mint: str, rpc, fetch=fetch_json, max_pools: int = 6,
             min_liquidity_usd: float = MIN_LIQUIDITY_USD) -> tuple[list[dict], list[str]]:
    """Returns (watch configs, notes for the user). One watch per token, SOL and USDC pools together."""
    pairs = dexscreener_pairs(mint, fetch)
    if not pairs:
        return [], ["No pools listed for this token on DexScreener."]
    pools, notes, symbol, tdec = find_pools(mint, rpc, pairs, set(QUOTE_SYMBOL), max_pools, min_liquidity_usd)
    ref = None
    if len({p["quote_mint"] for p in pools}) > 1:
        ref, note = reference_pool(rpc, fetch)
        notes.append(note)
        if ref is None:
            by_quote = {}
            for p in pools:
                by_quote.setdefault(p["quote_mint"], []).append(p)
            pools = max(by_quote.values(), key=len)
    if len(pools) < 2 or tdec is None:
        notes.append(f"{symbol or mint[:6]}: only {len(pools)} usable pool(s); need 2+ to compare")
        return [], notes
    watch = dict(label=symbol or mint[:6], token_mint=mint, token_decimals=tdec, cost_sol=DEFAULT_COST_SOL,
                 min_profit_sol=DEFAULT_MIN_PROFIT_SOL, min_net_gap_pct=DEFAULT_MIN_NET_GAP_PCT,
                 shock_pct=0.5, pools=pools)
    if ref:
        watch["sol_usdc"] = ref
    return [watch], notes


# ---------------------------------------------------------------------------
# Report and replay
# ---------------------------------------------------------------------------
def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def raw_span(path: Path) -> tuple[int, float]:
    """(number of updates, hours covered) from the raw log."""
    count, first, last = 0, None, None
    if path.exists():
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    t = json.loads(line)["t"]
                except (ValueError, KeyError):
                    continue
                count += 1
                first = t if first is None else first
                last = t
    return count, ((last - first) / 3600 if count > 1 else 0.0)


def pct(part: int, whole: int) -> str:
    return f"{100 * part / whole:.0f}%" if whole else "n/a"


def report(folder: Path, raw_path: Path | None = None) -> str:
    dis = read_csv(folder / "dislocations.csv")
    shocks = read_csv(folder / "shocks.csv")
    updates, hours = raw_span(raw_path or folder / "raw_updates.jsonl")
    out = ["ORBIT DISLOCATION WATCHER — REPORT", "=" * 34,
           f"Observed: {hours:.2f} hours, {updates} pool updates.", ""]
    vis = sum(1 for s in shocks if s["profitable_gap_visible"] == "yes")
    out.append(f"Shocks (one pool's price jumped in a single update): {len(shocks)}")
    out.append(f"  ...with a profitable gap visible to this watcher: {vis} ({pct(vis, len(shocks))})")
    out.append("")
    lat_file = folder / "latency.json"
    if lat_file.exists():
        try:
            lat = json.loads(lat_file.read_text(encoding="utf-8"))
            out.insert(3, f"Speed: updates arrived a median of {lat['median_slots']} slots behind the chain tip "
                          f"(p90 {lat['p90_slots']}; {lat['same_slot_pct']}% in the same slot). "
                          "1 slot is about 0.4 s.")
        except (ValueError, KeyError):
            pass
    out.append(f"Profitable gaps seen and closed: {len(dis)}"
               + (f"  (~{len(dis) / hours:.1f} per hour)" if hours > 0.05 else ""))
    trad = [d for d in dis if d.get("tradable") == "yes"]
    checked = [d for d in dis if d.get("tradable") in ("yes", "no")]
    if checked or trad:
        out.append(f"  tradable after depth check (0.25 / 1 / 5 SOL round trips): {len(trad)} of {len(checked)} "
                   f"checked; {len(dis) - len(checked)} could not be depth-checked (Meteora or older rows)")
    if dis:
        slots = [int(d["slots_open"]) for d in dis]
        secs = [float(d["seconds_open"]) for d in dis]
        out.append(f"  closed within 1 slot (~0.4 s): {pct(sum(s <= 1 for s in slots), len(slots))}")
        out.append(f"  closed within 2 slots (~0.8 s): {pct(sum(s <= 2 for s in slots), len(slots))}")
        out.append(f"  median time open: {statistics.median(slots):.0f} slots / {statistics.median(secs):.2f} s")
        if len(slots) >= 10:
            q = statistics.quantiles(slots, n=10)
            out.append(f"  90% closed within: {q[-1]:.0f} slots")
        for label in sorted({d["watch"] for d in dis}):
            rows = [d for d in dis if d["watch"] == label]
            exact = [d for d in rows if d.get("peak_net_quote") not in (None, "")]
            gaps = [float(d["peak_net_gap_pct"]) for d in rows if d.get("peak_net_gap_pct") not in (None, "")]
            line = f"  {label}: {len(rows)} gaps"
            if gaps:
                line += f", median net gap after fees {statistics.median(gaps):.3f}%, largest {max(gaps):.3f}%"
            out.append(line)
            for unit in sorted({d["quote"] for d in exact}):
                nets = [float(d["peak_net_quote"]) for d in exact if d["quote"] == unit]
                out.append(f"    exact pairs: median best net {statistics.median(nets):.6f} {unit}, "
                           f"largest {max(nets):.6f} {unit}, sum of peaks {sum(nets):.6f} {unit}")
    out += ["", "WHAT THIS MEANS"]
    if hours < 1:
        out.append("- Less than an hour of data. Let it run for a day or more before drawing conclusions.")
    if checked and not trad:
        out.append("- None of the depth-checked gaps survived a real round trip at 0.25, 1 or 5 SOL: the price "
                   "difference existed, but the pools could not absorb even a small trade profitably.")
    if trad:
        out.append(f"- {len(trad)} gap(s) looked tradable after the depth check. Look at them one by one in "
                   "dislocations.csv (columns depth_net_sol_*): how long they stayed open matters most.")
    if not dis:
        out.append("- No gap bigger than your cost assumption was visible. Either none happened, or they opened "
                   "and closed inside one slot, before a home connection could even see them. Either way there "
                   "was nothing a bot like Orbit could have captured on these pools.")
    else:
        fast = sum(int(d["slots_open"]) <= 2 for d in dis) / len(dis)
        if fast >= 0.5:
            out.append(f"- {fast:.0%} of gaps closed within about 0.8 seconds. To win one, a bot must see it, "
                       "decide, sign and land a transaction inside that window, against searchers who run "
                       "servers next to validators and pay Jito tips.")
        else:
            out.append("- Some gaps lasted several slots. Before getting excited, check them one by one: thin "
                       "liquidity, token transfer taxes, frozen tokens or wrong fee settings often make a gap "
                       "impossible to trade. Lasting gaps are usually untradable, not free money.")
    out.append("- 'Net gap' is the price difference after both pools' fees (and the SOL/USDC swap fee for "
               "cross-quote pairs). It says nothing about how much size the pools could take.")
    out.append("- 'Best net' (exact Raydium/PumpSwap pairs only) is a ceiling from pool maths minus your cost "
               "assumption, not a fill.")
    return "\n".join(out)


def replay(config: dict, raw_path: Path, out_folder: Path) -> Engine:
    """Re-run the analysis over saved raw updates, e.g. with different thresholds."""
    rec = Recorder(out_folder, keep_raw=False)
    engine = Engine(config, rec)
    current_slot, last_t = None, 0.0
    with open(raw_path, encoding="utf-8") as fh:
        for line in fh:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if current_slot is not None and r["s"] != current_slot:
                engine.evaluate(last_t)
            if "a" in r:
                engine.on_vault(r["v"], r["a"], r["s"], r["t"], log=False)
            elif "p" in r:
                engine.on_state(r["v"], r["p"], r.get("f"), r["s"], r["t"], log=False, extra=r.get("x"))
            current_slot, last_t = r["s"], r["t"]
    engine.evaluate(last_t)
    rec.close()
    return engine


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------
def load_config(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"No {path.name} yet. Run:  python watcher.py discover <TOKEN_MINT> --write")
    cfg = json.loads(path.read_text(encoding="utf-8"))
    cfg["rpc_http"] = os.environ.get("SOLANA_RPC_HTTP") or cfg.get("rpc_http", "https://api.mainnet-beta.solana.com")
    cfg["rpc_ws"] = os.environ.get("SOLANA_RPC_WS") or cfg.get("rpc_ws", "wss://api.mainnet-beta.solana.com")
    if not cfg.get("watches"):
        raise SystemExit("config.json has no watches. Run discover with --write first.")
    for w in cfg["watches"]:
        Watch(w)  # validates
    return cfg


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="Read-only Solana pool dislocation watcher.")
    ap.add_argument("--config", type=Path, default=ROOT / "config.json")
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("discover", help="find supported pools for a token")
    d.add_argument("mint")
    d.add_argument("--write", action="store_true", help="save the result into config.json")
    w = sub.add_parser("watch", help="stream and record")
    w.add_argument("--minutes", type=float, help="stop automatically after N minutes")
    w.add_argument("--out", type=Path, default=ROOT / "data")
    r = sub.add_parser("report", help="summarise recorded data")
    r.add_argument("--folder", type=Path, default=ROOT / "data")
    rp = sub.add_parser("replay", help="re-analyse raw data with the current config thresholds")
    rp.add_argument("--input", type=Path, default=ROOT / "data" / "raw_updates.jsonl")
    rp.add_argument("--out", type=Path, default=ROOT / "data_replay")
    args = ap.parse_args(argv)

    if args.cmd == "discover":
        cfg = json.loads(args.config.read_text(encoding="utf-8")) if args.config.exists() else {}
        rpc = Rpc(os.environ.get("SOLANA_RPC_HTTP") or cfg.get("rpc_http", "https://api.mainnet-beta.solana.com"))
        print(f"Looking up pools for {args.mint} ...")
        try:
            watches, notes = discover(args.mint, rpc)
        except (urllib.error.URLError, RuntimeError, OSError, ValueError) as e:
            raise SystemExit(f"Could not reach DexScreener or the Solana RPC ({e}). "
                             "Check your internet connection, or set SOLANA_RPC_HTTP to another RPC.") from None
        print("\n".join(notes))
        if not watches:
            print("\nNo watch could be built for this token. Try another token with 2+ supported pools above $10k liquidity.")
            return
        print("\n" + json.dumps(watches, indent=2))
        print("\nFees are defaults per DEX. Check each pool's real fee and edit config.json if different.")
        if args.write:
            cfg.setdefault("rpc_http", "https://api.mainnet-beta.solana.com")
            cfg.setdefault("rpc_ws", "wss://api.mainnet-beta.solana.com")
            cfg.setdefault("commitment", "processed")
            cfg.setdefault("evaluate_delay_ms", 150)
            labels = {x["label"] for x in watches}
            cfg["watches"] = [x for x in cfg.get("watches", []) if x["label"] not in labels] + watches
            args.config.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
            print(f"Saved to {args.config.name}.")
    elif args.cmd == "watch":
        cfg = load_config(args.config)
        rec = Recorder(args.out)
        engine = None
        try:
            engine = asyncio.run(run_watch(cfg, Rpc(cfg["rpc_http"]), rec, cfg["rpc_ws"],
                                           stop_after=args.minutes * 60 if args.minutes else None))
        except KeyboardInterrupt:
            print("\nStopped.")
        finally:
            rec.close()
        print("\n" + report(args.out))
    elif args.cmd == "report":
        print(report(args.folder))
    elif args.cmd == "replay":
        cfg = load_config(args.config)
        replay(cfg, args.input, args.out)
        print(report(args.out, raw_path=args.input))


if __name__ == "__main__":
    main()
