#!/usr/bin/env python3
"""
Orbit Watcher — private hosted dashboard.

Runs the read-only dislocation watcher around the clock and serves a
password-protected web dashboard. Standard library only.

Environment variables:
    ADMIN_PASSWORD   required, 12+ characters: the dashboard login
    DATA_DIR         where config and data are kept (default ./storage;
                     on Railway point it at the volume, e.g. /data)
    PORT             web port (Railway sets this automatically)
    SOLANA_RPC_HTTP  optional private RPC (https://...)
    SOLANA_RPC_WS    optional private RPC (wss://...)
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID   optional: phone alerts
"""
from __future__ import annotations

import asyncio
import collections
import contextlib
import csv
import hashlib
import hmac
import html
import io
import json
import os
import secrets
import sys
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import watcher as W

ROOT = Path(__file__).resolve().parent
SESSION_HOURS = 12
RAW_ROTATE_BYTES = 200 * 1024 * 1024
DOWNLOADS = {"dislocations.csv": "text/csv", "shocks.csv": "text/csv",
             "raw_updates.jsonl": "application/x-ndjson"}
GECKO_NEW_POOLS = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1"
HUNTER_DEFAULTS = {"enabled": False, "min_liquidity_usd": 10_000, "max_temp": 3, "minutes": 30, "every_s": 300}


# ---------------------------------------------------------------------------
# Telegram alerts (optional)
# ---------------------------------------------------------------------------
class Notifier:
    def __init__(self, token: str = "", chat_id: str = "", sender=None):
        self.token, self.chat_id = token, chat_id
        self.enabled = bool(token and chat_id)
        self.sender = sender or self._post
        self.sent = 0
        self.errors = 0
        self._last = 0.0

    def send(self, text: str, force: bool = False) -> bool:
        if not self.enabled:
            return False
        if not force and time.time() - self._last < 5:   # at most one alert every 5 s
            return False
        self._last = time.time()
        threading.Thread(target=self._deliver, args=(text[:3500],), daemon=True).start()
        return True

    def _deliver(self, text: str) -> None:
        try:
            self.sender(text)
            self.sent += 1
        except Exception:
            self.errors += 1

    def _post(self, text: str) -> None:
        body = urllib.parse.urlencode({"chat_id": self.chat_id, "text": text}).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{self.token}/sendMessage", body)
        with urllib.request.urlopen(req, timeout=15):
            pass


def gecko_new_pools(fetch) -> list[dict]:
    """Newest Solana pools from GeckoTerminal (public, no key)."""
    data = fetch(GECKO_NEW_POOLS)
    out = []
    for item in (data or {}).get("data", []):
        a, rel = item.get("attributes") or {}, item.get("relationships") or {}
        mint = lambda k: ((rel.get(k) or {}).get("data") or {}).get("id", "").split("_", 1)[-1]
        try:
            liq = float(a.get("reserve_in_usd") or 0)
        except (TypeError, ValueError):
            liq = 0.0
        out.append({"pool": a.get("address", ""), "name": html.unescape(a.get("name", "?")), "base": mint("base_token"),
                    "quote": mint("quote_token"), "liquidity_usd": liq})
    return out


# ---------------------------------------------------------------------------
# Log capture: everything printed also goes to the dashboard's activity log
# ---------------------------------------------------------------------------
class LogTee(io.TextIOBase):
    def __init__(self, original, buffer: collections.deque):
        self.original, self.buffer = original, buffer

    def write(self, s: str) -> int:
        self.original.write(s)
        for line in s.splitlines():
            if line.strip():
                self.buffer.append(f"{time.strftime('%H:%M:%S')}  {line.strip()}")
        return len(s)

    def flush(self) -> None:
        self.original.flush()


def tail_csv(path: Path, n: int) -> list[dict]:
    """Last n rows of a CSV without reading the whole file."""
    if not path.exists():
        return []
    with open(path, "rb") as fh:
        header = fh.readline().decode()
        size = fh.seek(0, 2)
        fh.seek(max(len(header.encode()), size - 256 * 1024))
        chunk = fh.read().decode("utf-8", "replace")
    lines = chunk.splitlines()
    if size > 256 * 1024:
        lines = lines[1:]  # first line may be cut
    rows = list(csv.DictReader([header] + lines[-n:]))
    return rows[::-1]


# ---------------------------------------------------------------------------
# Application state
# ---------------------------------------------------------------------------
class App:
    def __init__(self, data_dir: Path, password: str, fetch=W.fetch_json, rpc_factory=W.Rpc,
                 connect=W.WebSocket.connect, first_backoff: float = 2, eval_delay: float | None = None,
                 notifier: Notifier | None = None):
        self.dir = Path(data_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.cfg_path = self.dir / "config.json"
        self.out = self.dir / "data"
        self.pw_hash = hashlib.sha256(password.encode()).digest()
        secret_file = self.dir / ".session_secret"
        if not secret_file.exists():
            secret_file.write_text(secrets.token_hex(32))
        self.secret = secret_file.read_text().strip().encode()
        self.fetch, self.rpc_factory, self.connect = fetch, rpc_factory, connect
        self.first_backoff, self.eval_delay = first_backoff, eval_delay
        self.logs: collections.deque = collections.deque(maxlen=300)
        self.engine: W.Engine | None = None
        self.task: asyncio.Task | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.recorder: W.Recorder | None = None
        self.state = "starting"
        self.started = time.time()
        self.cfg_lock = threading.Lock()
        self.failed_logins: dict[str, list[float]] = {}
        self.discovered: dict[str, list[dict]] = {}
        self.notifier = notifier or Notifier(os.environ.get("TELEGRAM_BOT_TOKEN", ""),
                                             os.environ.get("TELEGRAM_CHAT_ID", ""))
        self.alerts: collections.deque = collections.deque(maxlen=30)
        self.hunter = {"last_check": None, "checked": 0, "recent": collections.deque(maxlen=12), "error": ""}
        self.hunt_seen: set[str] = set()
        self.last_daily = time.time()
        self.last_hunt = 0.0
        self.history: collections.deque = collections.deque(maxlen=720)   # 1 hour at 5-second samples
        self._rates = {"t": 0.0}

    # -- config -------------------------------------------------------------
    def config(self) -> dict:
        with self.cfg_lock:
            if self.cfg_path.exists():
                return json.loads(self.cfg_path.read_text(encoding="utf-8"))
        return {"rpc_http": "https://api.mainnet-beta.solana.com", "rpc_ws": "wss://api.mainnet-beta.solana.com",
                "commitment": "processed", "evaluate_delay_ms": 150, "watches": []}

    def save_config(self, cfg: dict) -> None:
        for w in cfg["watches"]:
            W.Watch(w)  # validate before saving
        with self.cfg_lock:
            tmp = self.cfg_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
            tmp.replace(self.cfg_path)

    def rpc_urls(self, cfg: dict) -> tuple[str, str]:
        return (os.environ.get("SOLANA_RPC_HTTP") or cfg.get("rpc_http", "https://api.mainnet-beta.solana.com"),
                os.environ.get("SOLANA_RPC_WS") or cfg.get("rpc_ws", "wss://api.mainnet-beta.solana.com"))

    # -- watcher supervision (runs on the asyncio loop) -----------------------
    async def supervise(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.recorder = W.Recorder(self.out)
        await self.restart()
        asyncio.get_running_loop().create_task(self._sample_loop())
        while True:
            await asyncio.sleep(30)
            self.maintain()
            h = self.hunter_cfg()
            if h["enabled"] and time.time() - self.last_hunt >= h["every_s"]:
                self.last_hunt = time.time()
                try:
                    await asyncio.to_thread(self.hunt_once)
                except Exception as e:
                    self.hunter["error"] = f"{type(e).__name__}: {e}"
            elif any(w.get("auto_until") and w["auto_until"] < time.time() for w in self.config().get("watches", [])):
                await asyncio.to_thread(self.expire_auto)

    async def restart(self) -> None:
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.task
        self.engine, self.task = None, None
        cfg = self.config()
        if not cfg.get("watches"):
            self.state = "idle — add a token to start watching"
            return
        http_url, ws_url = self.rpc_urls(cfg)
        self.started = time.time()
        self.state = "running"
        self.task = asyncio.create_task(self._run(cfg, http_url, ws_url))

    async def _run(self, cfg, http_url, ws_url) -> None:
        try:
            try:
                if await asyncio.to_thread(W.upgrade_config, cfg, self.rpc_factory(http_url)):
                    self.save_config(cfg)
                    print("Orca pools: adaptive-fee oracles located and saved.", flush=True)
            except Exception as e:
                print(f"Orca oracle check skipped ({e}).", flush=True)
            await W.run_watch(cfg, self.rpc_factory(http_url), self.recorder, ws_url,
                              first_backoff=self.first_backoff, eval_delay=self.eval_delay,
                              connect=self.connect, on_engine=self._attach)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # bad config etc.
            self.state = f"stopped: {e}"
            print(f"Watcher stopped: {e}")

    async def _sample_loop(self, every: float = 5.0) -> None:
        while True:
            self.sample()
            await asyncio.sleep(every)

    def sample(self) -> None:
        """One point for the live charts: gap per watch, total updates, latency."""
        e = self.engine
        if not e:
            return
        lat = e.latency_stats()
        self.history.append({"t": round(time.time(), 1), "updates": e.updates,
                             "lag": lat["median_slots"] if lat else None,
                             "gaps": {w.label: round(g, 4) for w in e.watches
                                      if (g := e.max_gap_pct(w)) is not None},
                             "net": {w.label: round(g, 4) for w in e.watches
                                     if (g := e.best_net_gap_pct(w)) is not None},
                             "lp": {f"{r['pool']} ±{r['range_pct']}%": r["net_vs_hold_pct"]
                                    for r in e.lp.summary()}})

    def rates(self) -> dict:
        """Depth-check pass rate and speed of closing, from the full CSV (cached 30 s)."""
        if time.time() - self._rates["t"] < 30:
            return self._rates
        rows = W.read_csv(self.out / "dislocations.csv")
        checked = [r for r in rows if r.get("tradable") in ("yes", "no")]
        by_watch = []
        for label in sorted({r.get("watch", "").strip() for r in rows} - {""}):
            mine = [r for r in rows if r.get("watch", "").strip() == label]
            slots = sorted(int(r["slots_open"]) for r in mine if r.get("slots_open", "").isdigit())
            depth = [float(r["depth_net_sol_2_5"]) for r in mine if r.get("depth_net_sol_2_5") not in (None, "")]
            by_watch.append({"watch": label, "gaps": len(mine),
                             "tradable": sum(r.get("tradable") == "yes" for r in mine),
                             "median_slots": slots[len(slots) // 2] if slots else None,
                             "within_1_slot": sum(x <= 1 for x in slots),
                             "best_depth_sol": max(depth) if depth else None})
        self._rates = {"t": time.time(), "gaps": len(rows), "checked": len(checked), "by_watch": by_watch,
                       "tradable": sum(r["tradable"] == "yes" for r in checked),
                       "within_1_slot": sum(1 for r in rows if r.get("slots_open", "").isdigit()
                                            and int(r["slots_open"]) <= 1)}
        return self._rates

    def reset_earnings(self) -> bool:
        """Forget all paper positions and start them again from current prices."""
        e = self.engine
        if not e:
            return False
        e.lp.reset()
        self.history.clear()
        print("Pool earnings tracking reset: all paper positions start again from now.", flush=True)
        return True

    def lp_rows(self) -> list[dict]:
        """Pool-earnings rows, each tagged with the watch (token) it belongs to."""
        e = self.engine
        if not e:
            return []
        owner = {}
        for w in e.watches:
            for p in w.pools:
                owner.setdefault(p.address, w.label)
            if w.ref:
                owner.setdefault(w.ref.address, "SOL/USDC")
        return [{**r, "watch": owner.get(r["address"], "?")} for r in e.lp.summary()]

    def open_gaps(self) -> list[dict]:
        e, out = self.engine, []
        if not e:
            return out
        for w in e.watches:
            try:
                items = list(w.open.values())
            except RuntimeError:        # changed while reading; next poll catches it
                continue
            for g in items:
                out.append({"watch": w.label, "buy": g["buy"], "sell": g["sell"], "since": g["start"],
                            "net_gap_pct": round(g["peak_net_gap"] * 100, 4), "method": g["method"],
                            "depth_best": g.get("depth_best"), "min_profit": w.min_profit_sol})
        return out

    def _attach(self, engine) -> None:
        engine.on_event = self.handle_event
        self.engine = engine

    def handle_event(self, kind: str, data: dict) -> None:
        """Called by the engine: a gap closed or a big shock happened."""
        if kind == "gap_closed" and data.get("tradable") == "yes":
            best = max(float(data[k]) for k in ("depth_net_sol_0_25", "depth_net_sol_1", "depth_net_sol_2_5"))
            text = (f"Orbit: tradable gap on {data['watch']}\n{data['buy_pool']} -> {data['sell_pool']}\n"
                    f"net after fees {data['peak_net_gap_pct']}%, best depth-checked net {best:.5f} SOL, "
                    f"open {data['slots_open']} slots ({data['seconds_open']} s). Read-only: no trade was made.")
        elif kind == "big_shock":
            text = (f"Orbit: big shock on {data['watch']}: {data['pool']} moved {data['move_pct']}% in one update "
                    f"(slot {data['slot']}). Profitable gap visible: {'yes' if data['gap_visible'] else 'no'}.")
        else:
            return
        self.alerts.appendleft(f"{time.strftime('%H:%M:%S')}  {text.splitlines()[0]} " +
                               " ".join(text.splitlines()[1:]))
        print(text.replace("\n", " | "), flush=True)
        self.notifier.send(text)

    # -- new-pool hunter ------------------------------------------------------
    def hunter_cfg(self) -> dict:
        return {**HUNTER_DEFAULTS, **(self.config().get("hunter") or {})}

    def set_hunter(self, enabled: bool) -> None:
        cfg = self.config()
        cfg["hunter"] = {**HUNTER_DEFAULTS, **(cfg.get("hunter") or {}), "enabled": bool(enabled)}
        self.save_config(cfg)
        self.last_hunt = 0.0

    def expire_auto(self) -> list[str]:
        cfg = self.config()
        now = time.time()
        gone = [w["label"] for w in cfg["watches"] if w.get("auto_until") and w["auto_until"] < now]
        if gone:
            cfg["watches"] = [w for w in cfg["watches"] if w["label"] not in gone]
            self.save_config(cfg)
            print(f"Hunter: finished watching {', '.join(gone)}", flush=True)
            self.request_restart()
        return gone

    def hunt_once(self) -> list[str]:
        """Look at the newest pools; auto-watch tokens that now have 2+ usable pools."""
        h = self.hunter_cfg()
        self.expire_auto()
        cfg = self.config()
        watched = {w["token_mint"] for w in cfg["watches"]}
        temp = [w for w in cfg["watches"] if w.get("auto_until")]
        self.hunter.update(last_check=time.time(), error="")
        added = []
        http_url, _ = self.rpc_urls(cfg)
        for p in gecko_new_pools(self.fetch):
            if len(temp) + len(added) >= h["max_temp"]:
                break
            mint = p["base"] if p["quote"] in W.QUOTE_SYMBOL else p["quote"] if p["base"] in W.QUOTE_SYMBOL else None
            if not mint or mint in watched or mint in self.hunt_seen or p["liquidity_usd"] < h["min_liquidity_usd"]:
                continue
            self.hunt_seen.add(mint)
            self.hunter["checked"] += 1
            watches, notes = W.discover(mint, self.rpc_factory(http_url), self.fetch,
                                        min_liquidity_usd=h["min_liquidity_usd"])
            if not watches:
                self.hunter["recent"].appendleft({"time": time.strftime("%H:%M"), "name": p["name"],
                                                  "result": "only 1 usable pool"})
                continue
            w = watches[0]
            w["label"] = "NEW " + w["label"]
            w["auto_until"] = time.time() + h["minutes"] * 60
            added.append(w)
            self.hunter["recent"].appendleft({"time": time.strftime("%H:%M"), "name": p["name"],
                                              "result": f"watching {len(w['pools'])} pools for {h['minutes']} min"})
        if added:
            cfg = self.config()
            labels = {w["label"] for w in added}
            cfg["watches"] = [w for w in cfg["watches"] if w["label"] not in labels] + added
            self.save_config(cfg)
            print(f"Hunter: now watching {', '.join(sorted(labels))}", flush=True)
            self.request_restart()
        return [w["label"] for w in added]

    def request_restart(self) -> None:
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.restart(), self.loop).result(timeout=30)

    def maintain(self) -> None:
        """Rotate the raw log at 200 MB (keep one old file); send the daily summary."""
        if self.notifier.enabled and time.time() - self.last_daily >= 86400:
            self.last_daily = time.time()
            self.notifier.send("Orbit daily summary\n" + "\n".join(W.report(self.out).splitlines()[2:16]), force=True)
        raw = self.out / "raw_updates.jsonl"
        if self.recorder and self.recorder.raw_fh and raw.exists() and raw.stat().st_size > RAW_ROTATE_BYTES:
            self.recorder.raw_fh.close()
            raw.replace(self.out / "raw_updates.1.jsonl")
            self.recorder.raw_fh = open(raw, "a", encoding="utf-8")
            print("Raw log rotated (kept one previous file).")

    # -- auth -----------------------------------------------------------------
    def check_password(self, pw: str) -> bool:
        return hmac.compare_digest(hashlib.sha256(pw.encode()).digest(), self.pw_hash)

    def make_session(self) -> str:
        exp = str(int(time.time()) + SESSION_HOURS * 3600)
        return exp + "." + hmac.new(self.secret, exp.encode(), hashlib.sha256).hexdigest()

    def valid_session(self, token: str | None) -> bool:
        if not token or "." not in token:
            return False
        exp, sig = token.split(".", 1)
        good = hmac.new(self.secret, exp.encode(), hashlib.sha256).hexdigest()
        return exp.isdigit() and int(exp) > time.time() and hmac.compare_digest(sig, good)

    def login_blocked(self, ip: str) -> bool:
        now = time.time()
        recent = [t for t in self.failed_logins.get(ip, []) if now - t < 900]
        self.failed_logins[ip] = recent
        return len(recent) >= 5

    # -- dashboard data -------------------------------------------------------
    def snapshot(self) -> dict:
        e, cfg = self.engine, self.config()
        live = {w.label: w for w in e.watches} if e else {}
        watches = []
        for wc in cfg.get("watches", []):
            lw = live.get(wc["label"])
            try:
                settings = W.Watch(wc).settings()
            except (ValueError, KeyError):
                settings = {}
            pools = []
            for i, pc in enumerate(wc["pools"]):
                lp = lw.pools[i] if lw and i < len(lw.pools) else None
                kind = pc.get("kind", "cp")
                pools.append({"name": pc["name"], "address": pc.get("address", ""), "kind": kind,
                              "quote": W.QUOTE_SYMBOL.get(pc.get("quote_mint", wc.get("quote_mint")), "?"),
                              "fee": lp.fee if lp else pc["fee"],
                              "fee_editable": kind in ("cp", "clmm"), "liquidity_usd": pc.get("liquidity_usd"),
                              "adaptive_fee": lp.adaptive_fee if lp and lp.oracle else None,
                              "price": lp.price() if lp and lp.ready() else None,
                              "suspect": bool(lp and lp.suspect)})
            ref = wc.get("sol_usdc")
            watches.append({"label": wc["label"], **settings, "pools": pools, "auto_until": wc.get("auto_until"),
                            "ref": ref["name"] if ref else None,
                            "gap_pct": e.max_gap_pct(lw) if e and lw else None,
                            "net_gap_pct": e.best_net_gap_pct(lw) if e and lw else None,
                            "open": len(lw.open) if lw else 0})
        return {
            "state": self.state, "uptime_s": round(time.time() - self.started),
            "updates": e.updates if e else 0, "slot": e.last_slot if e else 0,
            "stats": dict(e.stats) if e else {}, "watches": watches,
            "rpc": "private" if os.environ.get("SOLANA_RPC_HTTP") else "public",
            "latency": e.latency_stats() if e else None,
            "lp": self.lp_rows(),
            "history": list(self.history)[-720::2], "open_gaps": self.open_gaps(),
            "rates": {k: v for k, v in self.rates().items() if k != "t"},
            "min_net_gap_pct": min((w.min_net_gap * 100 for w in e.watches), default=0.2) if e else 0.2,
            "alerts_enabled": self.notifier.enabled, "alerts": list(self.alerts)[:10],
            "hunter": {**self.hunter_cfg(), "last_check": self.hunter["last_check"], "checked": self.hunter["checked"],
                       "recent": list(self.hunter["recent"]), "error": self.hunter["error"]},
            "dislocations": tail_csv(self.out / "dislocations.csv", 50),
            "shocks": tail_csv(self.out / "shocks.csv", 50),
            "logs": list(self.logs)[-60:][::-1],
        }

    # -- watch management (called from web threads) --------------------------
    def discover(self, mint: str) -> dict:
        mint = mint.strip()
        if not (32 <= len(mint) <= 44) or any(c not in W.B58 for c in mint):
            raise ValueError("That doesn't look like a Solana mint address.")
        http_url, _ = self.rpc_urls(self.config())
        watches, notes = W.discover(mint, self.rpc_factory(http_url), self.fetch)
        self.discovered[mint] = watches
        return {"watches": watches, "notes": notes}

    def add_watches(self, mint: str) -> list[str]:
        found = self.discovered.get(mint.strip())
        if not found:
            raise ValueError("Run discovery for this token first.")
        cfg = self.config()
        labels = {w["label"] for w in found}
        cfg["watches"] = [w for w in cfg.get("watches", []) if w["label"] not in labels] + found
        self.save_config(cfg)
        self.request_restart()
        return sorted(labels)

    def remove_watch(self, label: str) -> None:
        cfg = self.config()
        before = len(cfg["watches"])
        cfg["watches"] = [w for w in cfg["watches"] if w["label"] != label]
        if len(cfg["watches"]) == before:
            raise ValueError("No such watch.")
        self.save_config(cfg)
        self.request_restart()

    def update_watch(self, body: dict) -> None:
        cfg = self.config()
        target = next((w for w in cfg["watches"] if w["label"] == body.get("label")), None)
        if not target:
            raise ValueError("No such watch.")
        for key, lo, hi in (("cost_sol", 0, 1e3), ("min_profit_sol", 0, 1e3), ("min_net_gap_pct", 0, 100),
                            ("shock_pct", 0.01, 100), ("big_shock_pct", 0.1, 100)):
            if key in body:
                v = float(body[key])
                if not lo <= v <= hi:
                    raise ValueError(f"{key} out of range")
                target[key] = v
        for old_key in ("cost_quote", "min_profit_quote"):   # older configs
            if old_key in target and old_key.replace("quote", "sol") in target:
                target.pop(old_key)
        for name, fee in (body.get("fees") or {}).items():
            pool = next((p for p in target["pools"] if p["name"] == name), None)
            fee = float(fee)
            if not pool or not 0 <= fee < 0.2:
                raise ValueError(f"Bad fee for {name}")
            if pool.get("kind", "cp") not in ("cp", "clmm"):
                continue  # Whirlpool and DLMM fees are read live from the pool
            pool["fee"] = fee
        drop = set(body.get("remove_pools") or [])
        if drop:
            if not drop <= {p["name"] for p in target["pools"]}:
                raise ValueError("Unknown pool name.")
            if len(target["pools"]) - len(drop) < 2:
                raise ValueError("A watch needs at least 2 pools. Remove the whole watch instead.")
            target["pools"] = [p for p in target["pools"] if p["name"] not in drop]
        self.save_config(cfg)
        self.request_restart()


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------
def make_handler(app: App):
    page = (ROOT / "dashboard.html").read_text(encoding="utf-8")

    class Handler(BaseHTTPRequestHandler):
        server_version = "Orbit"
        sys_version = ""

        def log_message(self, *args):
            pass

        # helpers
        def ip(self) -> str:
            fwd = self.headers.get("X-Forwarded-For")
            return fwd.split(",")[0].strip() if fwd else self.client_address[0]

        def https(self) -> bool:
            return self.headers.get("X-Forwarded-Proto", "") == "https"

        def cookie(self) -> str | None:
            for part in (self.headers.get("Cookie") or "").split(";"):
                k, _, v = part.strip().partition("=")
                if k == "orbit_session":
                    return v
            return None

        def authed(self) -> bool:
            return app.valid_session(self.cookie())

        def send(self, code: int, body: bytes, ctype: str, extra: dict | None = None):
            nonce = secrets.token_urlsafe(12)
            if ctype.startswith("text/html"):
                body = body.replace(b"{{NONCE}}", nonce.encode())
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("Content-Security-Policy",
                             f"default-src 'self'; script-src 'nonce-{nonce}'; style-src 'self' 'nonce-{nonce}'; "
                             "img-src 'self' data:; frame-ancestors 'none'; form-action 'self'; base-uri 'none'")
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def json(self, obj, code=200):
            self.send(code, json.dumps(obj).encode(), "application/json")

        def redirect(self, where: str, extra: dict | None = None):
            self.send(303, b"", "text/plain", {"Location": where, **(extra or {})})

        def body(self) -> bytes:
            n = int(self.headers.get("Content-Length") or 0)
            if n > 65536:
                raise ValueError("Request too large")
            return self.rfile.read(n)

        def same_origin_api(self) -> bool:
            if self.headers.get("X-Orbit") != "1":
                return False
            origin = self.headers.get("Origin")
            return origin is None or urlparse(origin).netloc == self.headers.get("Host")

        # routes
        def do_HEAD(self):
            self.do_GET()

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/healthz":
                return self.send(200, b"ok", "text/plain")
            if path == "/login":
                return self.send(200, login_page(parse_qs(urlparse(self.path).query).get("e") is not None),
                                 "text/html; charset=utf-8")
            if not self.authed():
                if path.startswith("/api/") or path.startswith("/download/"):
                    return self.json({"error": "login required"}, 401)
                return self.redirect("/login")
            if path == "/":
                return self.send(200, page.encode(), "text/html; charset=utf-8")
            if path == "/api/state":
                return self.json(app.snapshot())
            if path == "/api/report":
                if app.recorder and app.recorder.raw_fh and not app.recorder.raw_fh.closed:
                    app.recorder.raw_fh.flush()
                return self.send(200, W.report(app.out).encode(), "text/plain; charset=utf-8")
            if path.startswith("/download/"):
                name = path.rsplit("/", 1)[-1]
                f = app.out / name
                if name not in DOWNLOADS or not f.exists():
                    return self.json({"error": "not found"}, 404)
                return self.send(200, f.read_bytes(), DOWNLOADS[name],
                                 {"Content-Disposition": f'attachment; filename="{name}"'})
            self.json({"error": "not found"}, 404)

        def do_POST(self):
            path = urlparse(self.path).path
            if path == "/login":
                ip = self.ip()
                if app.login_blocked(ip):
                    return self.send(429, b"Too many attempts. Wait 15 minutes.", "text/plain")
                pw = parse_qs(self.body().decode()).get("password", [""])[0]
                if not app.check_password(pw):
                    app.failed_logins.setdefault(ip, []).append(time.time())
                    time.sleep(0.5)
                    return self.redirect("/login?e=1")
                flags = "; Secure" if self.https() else ""
                return self.redirect("/", {"Set-Cookie": f"orbit_session={app.make_session()}; HttpOnly; "
                                                         f"SameSite=Strict; Path=/; Max-Age={SESSION_HOURS * 3600}{flags}"})
            if not self.authed():
                return self.json({"error": "login required"}, 401)
            if not self.same_origin_api():
                return self.json({"error": "blocked"}, 403)
            try:
                data = json.loads(self.body() or b"{}")
                if path == "/api/logout":
                    return self.send(200, b"{}", "application/json",
                                     {"Set-Cookie": "orbit_session=; Max-Age=0; Path=/; HttpOnly; SameSite=Strict"})
                if path == "/api/discover":
                    return self.json(app.discover(str(data.get("mint", ""))))
                if path == "/api/watches/add":
                    return self.json({"added": app.add_watches(str(data.get("mint", "")))})
                if path == "/api/watches/remove":
                    app.remove_watch(str(data.get("label", "")))
                    return self.json({"ok": True})
                if path == "/api/watches/update":
                    app.update_watch(data)
                    return self.json({"ok": True})
                if path == "/api/hunter":
                    app.set_hunter(bool(data.get("enabled")))
                    return self.json({"ok": True})
                if path == "/api/test-alert":
                    ok = app.notifier.send("Orbit: test alert. Alerts are working.", force=True)
                    return self.json({"ok": ok, "enabled": app.notifier.enabled})
                if path == "/api/lp/reset":
                    return self.json({"ok": app.reset_earnings()})
                if path == "/api/restart":
                    app.request_restart()
                    return self.json({"ok": True})
                return self.json({"error": "not found"}, 404)
            except (ValueError, KeyError, TypeError) as e:
                return self.json({"error": str(e)}, 400)
            except Exception as e:  # network trouble during discovery, etc.
                return self.json({"error": f"{type(e).__name__}: {e}"}, 502)

    return Handler


def login_page(error: bool) -> bytes:
    msg = '<p class="err">Wrong password.</p>' if error else ""
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Orbit · Sign in</title>
<style nonce="{{{{NONCE}}}}">
body{{margin:0;min-height:100vh;display:grid;place-items:center;background:#0b1016;color:#e6edf3;
font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}}
form{{background:#121a23;border:1px solid #223041;border-radius:14px;padding:32px;width:min(360px,90vw)}}
h1{{font-size:20px;margin:0 0 4px;letter-spacing:.08em}} p{{color:#8b9bb0;margin:0 0 20px}}
input{{width:100%;box-sizing:border-box;padding:12px;border-radius:8px;border:1px solid #2b3b4f;
background:#0b1016;color:#e6edf3;font-size:15px}} button{{margin-top:14px;width:100%;padding:12px;border:0;
border-radius:8px;background:#3ddc97;color:#06281a;font-weight:600;font-size:15px;cursor:pointer}}
.err{{color:#ff7b72;margin:12px 0 0}}</style></head><body>
<form method="post" action="/login"><h1>ORBIT</h1><p>Private dislocation watcher</p>
<input type="password" name="password" placeholder="Password" autofocus required autocomplete="current-password">
<button>Sign in</button>{msg}</form></body></html>""".encode()


def main() -> None:
    password = os.environ.get("ADMIN_PASSWORD", "")
    if len(password) < 12:
        raise SystemExit("Set ADMIN_PASSWORD to at least 12 characters before starting.")
    app = App(Path(os.environ.get("DATA_DIR") or ROOT / "storage"), password)
    sys.stdout = LogTee(sys.stdout, app.logs)
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), make_handler(app))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"Orbit dashboard on port {port}. Read-only watcher; no trading code.")
    try:
        asyncio.run(app.supervise())
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        if app.recorder:
            app.recorder.close()


if __name__ == "__main__":
    main()
