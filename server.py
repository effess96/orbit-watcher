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
import io
import re
import shutil
import sys
import threading
import zipfile
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import regime as R
import watcher as W

ROOT = Path(__file__).resolve().parent
SESSION_HOURS = 12
RAW_ROTATE_BYTES = 64 * 1024 * 1024                 # move the raw log aside at this size...
RAW_ROTATE_HOURS = 6                                # ...or this age, then gzip it (about 7x smaller)
RAW_KEEP_MB = float(os.environ.get("ORBIT_RAW_KEEP_MB", 1000))   # oldest compressed raw files go past this
DOWNLOADS = {"dislocations.csv": "text/csv", "shocks.csv": "text/csv", "loops.csv": "text/csv",
             "closers.csv": "text/csv", "quote_checks.csv": "text/csv",
             "raw_updates.jsonl": "application/x-ndjson"}
BUNDLE_FILES = ("dislocations.csv", "shocks.csv", "loops.csv", "closers.csv", "quote_checks.csv",
                "lp_state.json", "latency.json", "regime.json", "regime_log.jsonl")
DISK_WARN_MB, DISK_FAIL_MB = 1000, 300   # free space left on the data volume
MEMORY_WARN_MB = 700
RECONNECT_WARN, RECONNECT_FAIL = 3, 10   # stream drops in the last hour
STALL_ALERT_S = 600              # Telegram alert when no pool update arrives for this long
MOOD_FAILS_ALERT = 3             # ...or when the market-mood job fails this many times in a row
MOOD_EVERY_S = 6 * 3600          # market mood refresh (heavy fetches happen once a day; the rest hit the cache)
ARCHIVE_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
SECRET_IN_TEXT = re.compile(r"(api[-_]?key=)[^&\s\"']+|(bot)\d+:[A-Za-z0-9_-]+", re.I)


def memory_mb() -> float | None:
    """Resident memory of this process in MB (Linux), None elsewhere."""
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    except OSError:
        return None
    return None


def redact(text: str) -> str:
    """Hide API keys and bot tokens before anything leaves the server in a bundle."""
    return SECRET_IN_TEXT.sub(lambda m: (m.group(1) or m.group(2)) + "***", text)
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
        self.auditor: W.Auditor | None = None
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
        self.last_archive = time.time()
        self.last_hunt = 0.0
        self.history: collections.deque = collections.deque(maxlen=720)   # 1 hour at 5-second samples
        self._rates = {"t": 0.0}
        self._audit_cache: tuple = (0.0, [], [])
        self.archive = self.out / "archive"
        self.raw_started = time.time()
        self.raw_lock = threading.Lock()
        self.mood: dict | None = None
        self.mood_status = {"state": "waiting", "error": "", "last_try": None, "fails": 0}
        self.watchdog = {"updates": -1, "since": time.time(), "stalled": False, "mood_alerted": False}
        try:
            self.mood = json.loads((self.out / "regime.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            pass

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
        old = self.out / "raw_updates.1.jsonl"          # left over from the old one-file rotation
        if old.exists():
            (self.archive / "raw").mkdir(parents=True, exist_ok=True)
            moved = self.archive / "raw" / f"raw_{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-old.jsonl"
            old.replace(moved)
            threading.Thread(target=self._compress_raw, args=(moved,), daemon=True).start()
        await self.restart()
        asyncio.get_running_loop().create_task(self._sample_loop())
        if os.environ.get("ORBIT_MOOD", "1") != "0":
            threading.Thread(target=self._mood_loop, daemon=True).start()
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
            self._http_url = http_url
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
        rows = e.lp.summary()
        self.history.append({"t": round(time.time(), 1), "updates": e.updates,
                             "lag": lat["median_slots"] if lat else None,
                             "gaps": {w.label: round(g, 4) for w in e.watches
                                      if (g := e.max_gap_pct(w, active_only=True)) is not None},
                             "net": {w.label: round(g, 4) for w in e.watches
                                     if (g := e.best_net_gap_pct(w, active_only=True)) is not None},
                             "gaps_seen": e.stats.get("dislocations", 0) + e.open_count(),
                             "tradable": e.stats.get("tradable", 0),
                             "shocks": e.stats.get("shocks", 0),
                             "lp": {f"{r['pool']} ±{r['range_pct']}%" + (" auto" if r.get("strategy") == "rebalance" else ""): r["net_vs_hold_pct"] for r in rows},
                             "lp_sol": {f"{r['pool']} ±{r['range_pct']}%" + (" auto" if r.get("strategy") == "rebalance" else ""): r["net_sol_per_day"] for r in rows
                                        if not r.get("early")},
                             "loop": {w.label: round(w.loop_best, 6) for w in e.watches if w.loop_best is not None}})

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
        try:
            if e.lp.path:
                e.lp.save()                           # freshest numbers into the archive
        except Exception:
            pass
        try:
            name = self.save_archive("before-reset")
        except Exception as err:                      # a failed copy must not block the reset
            name = f"not saved ({type(err).__name__}: {err})"
        e.lp.reset()
        self.history.clear()
        print(f"Pool earnings tracking reset: all paper positions start again from now "
              f"(old results kept in archive {name}).", flush=True)
        return True

    # -- market mood (crypto regime + LP weather) ---------------------------------
    def run_mood_once(self, data: "R.RegimeData | None" = None) -> dict:
        self.mood_status.update(state="fetching", last_try=time.time())
        data = data or R.RegimeData(self.out / "regime_cache", log=lambda m: print(m, flush=True))
        watches = R.watches_for_mood(self.config())
        result = R.analyse(data, watches)
        result["safety"] = self.check_safety(watches)
        for t in result["tokens"]:
            t["safety"] = next((x for x in result["safety"] if x.get("mint") == t["mint"]), None)
        self.out.mkdir(parents=True, exist_ok=True)
        old = (self.mood or {}).get("composite", {}).get("zone")
        self.mood = result
        tmp = self.out / "regime.tmp"
        tmp.write_text(json.dumps(result, indent=1), encoding="utf-8")
        tmp.replace(self.out / "regime.json")
        c = result["composite"]
        with open(self.out / "regime_log.jsonl", "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"t": result["t"], "score": c.get("score"), "zone": c.get("zone"),
                                 "sol": result["sol"].get("price"), "sol_vol30": result["sol"].get("vol30"),
                                 "weather": {t["watch"]: [t["weather_5"], t["weather_20"]] for t in result["tokens"]}}) + "\n")
        self.mood_status.update(state="ok", error="", fails=0)
        self.watchdog["mood_alerted"] = False
        print(f"Market mood: {c.get('zone')} (score {c.get('score')}/100); "
              f"{result['requests']} requests in {result['seconds']} s", flush=True)
        if old and c.get("zone") != old:
            self.notifier.send(f"Orbit market mood changed: {old} -> {c.get('zone')} (score {c.get('score')}/100). "
                               "Descriptive only, not a trade signal.", force=True)
        return result

    def check_safety(self, watches: list[dict]) -> list[dict]:
        """On-chain token checks (mint/freeze authority, Token-2022 features, holder concentration). Read-only."""
        url = getattr(self, "_http_url", None)
        if not url:
            return []
        rpc = self.rpc_factory(url)
        vaults: set[str] = set()
        for w in (self.engine.watches if self.engine else []):
            for p in w.pools:
                vaults |= set(p.vault_side) | {v for v in (getattr(p, "base_vault", None), getattr(p, "quote_vault", None)) if v}
        out = []
        for w in watches:
            try:
                out.append({**W.token_safety(rpc, w["mint"], vaults), "watch": w["label"].strip()})
            except Exception as e:
                out.append({"mint": w["mint"], "watch": w["label"].strip(), "error": f"{type(e).__name__}"})
        return out

    def _mood_loop(self, first_wait: float = 60) -> None:
        time.sleep(first_wait)
        while True:
            try:
                self.run_mood_once()
                wait = MOOD_EVERY_S
            except Exception as e:
                self.mood_status.update(state="error", error=f"{type(e).__name__}: {e}"[:200],
                                        fails=self.mood_status.get("fails", 0) + 1)
                print(f"Market mood: failed ({type(e).__name__}: {e}); retrying in 30 min", flush=True)
                wait = 1800
            time.sleep(wait)

    # -- archives ---------------------------------------------------------------
    def save_archive(self, reason: str = "manual") -> str:
        """Copy the report, earnings and small CSVs into archive/reports/<time>-<reason>/ (raw data excluded)."""
        if self.recorder and self.recorder.raw_fh and not self.recorder.raw_fh.closed:
            self.recorder.raw_fh.flush()
        name = f"{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{re.sub(r'[^a-z0-9-]', '', reason.lower())[:20]}"
        folder = self.archive / "reports" / name
        folder.mkdir(parents=True, exist_ok=True)
        for f in BUNDLE_FILES:
            if (self.out / f).exists():
                shutil.copy2(self.out / f, folder / f)
        try:
            text = W.report(self.out)
        except Exception as err:
            text = f"Report could not be built: {type(err).__name__}: {err}"
        (folder / "report.txt").write_text(text, encoding="utf-8")
        try:
            rows = self.lp_rows()
        except Exception:
            rows = []
        (folder / "earnings.json").write_text(json.dumps(rows, indent=1), encoding="utf-8")
        print(f"Archive saved: {name}", flush=True)
        return name

    def archives(self) -> dict:
        reports, raw = [], []
        base = self.archive / "reports"
        if base.exists():
            for d in sorted(base.iterdir(), reverse=True):
                if d.is_dir():
                    size = sum(f.stat().st_size for f in d.iterdir() if f.is_file())
                    reports.append({"name": d.name, "bytes": size, "t": d.stat().st_mtime})
        for x in reversed(self.raw_index()):
            f = self.archive / "raw" / x["file"]
            if f.exists():
                raw.append({**x, "bytes": f.stat().st_size})
        live = self.out / "raw_updates.jsonl"
        return {"reports": reports, "raw": raw, "raw_live_bytes": live.stat().st_size if live.exists() else 0,
                "raw_keep_mb": RAW_KEEP_MB, "rotate_hours": RAW_ROTATE_HOURS}

    def report_archive_zip(self, name: str) -> bytes | None:
        folder = self.archive / "reports" / name
        if not ARCHIVE_NAME.match(name) or not folder.is_dir():
            return None
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for f in sorted(folder.iterdir()):
                z.write(f, f"{name}/{f.name}")
        return buf.getvalue()

    def bundle(self) -> bytes:
        """Everything Claude needs to review the run, without the heavy raw data (usually well under 5 MB)."""
        if self.recorder and self.recorder.raw_fh and not self.recorder.raw_fh.closed:
            self.recorder.raw_fh.flush()
        stamp = f"{datetime.now(timezone.utc):%Y%m%d-%H%M}"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            try:
                text = W.report(self.out)
            except Exception as err:
                text = f"Report could not be built: {type(err).__name__}: {err}"
            z.writestr(f"orbit-{stamp}/report.txt", text)
            for f in BUNDLE_FILES:
                if (self.out / f).exists():
                    z.write(self.out / f, f"orbit-{stamp}/{f}")
            try:
                snap = self.snapshot()
            except Exception as err:
                snap = {"error": f"{type(err).__name__}: {err}"}
            snap.pop("logs", None)
            z.writestr(f"orbit-{stamp}/state.json", redact(json.dumps(snap, indent=1, default=str)))
            z.writestr(f"orbit-{stamp}/logs.txt", redact("\n".join(str(x) for x in list(self.logs))))
            base = self.archive / "reports"
            if base.exists():
                for d in sorted(base.iterdir()):
                    if (d / "report.txt").exists():
                        z.write(d / "report.txt", f"orbit-{stamp}/previous_reports/{d.name}.txt")
        return buf.getvalue()

    # -- raw data: rotate, compress, cap ------------------------------------
    def raw_index(self) -> list[dict]:
        try:
            return json.loads((self.archive / "raw" / "index.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return []

    def rotate_raw(self, wait: bool = False) -> None:
        """Move the raw log aside and gzip it in the background, so the live file stays small."""
        if not self.recorder:
            return
        moved = self.recorder.rotate_raw(self.archive / "raw")
        self.raw_started = time.time()
        if moved:
            th = threading.Thread(target=self._compress_raw, args=(moved,), daemon=True)
            th.start()
            if wait:
                th.join()

    def _compress_raw(self, src: Path) -> None:
        count, first, last = 0, None, None
        gz = src.with_name(src.name + ".gz")
        try:
            with open(src, encoding="utf-8") as fin, W.gzip.open(gz, "wt", encoding="utf-8", compresslevel=6) as fout:
                for line in fin:
                    fout.write(line)
                    try:
                        t = json.loads(line)["t"]
                    except (ValueError, KeyError):
                        continue
                    count += 1
                    first = t if first is None else first
                    last = t
            src.unlink()
        except OSError as e:
            print(f"Raw archive: could not compress {src.name}: {e}", flush=True)
            return
        with self.raw_lock:
            idx = self.raw_index()
            idx.append({"file": gz.name, "updates": count, "from": first, "to": last,
                        "hours": round((last - first) / 3600, 4) if count > 1 else 0.0})
            total = sum((self.archive / "raw" / x["file"]).stat().st_size
                        for x in idx if (self.archive / "raw" / x["file"]).exists())
            while len(idx) > 1 and total > RAW_KEEP_MB * 1024 * 1024:
                old = idx.pop(0)
                f = self.archive / "raw" / old["file"]
                if f.exists():
                    total -= f.stat().st_size
                    f.unlink()
                print(f"Raw archive: removed oldest file {old['file']} to stay under {RAW_KEEP_MB:g} MB", flush=True)
            tmp = self.archive / "raw" / "index.tmp"
            tmp.write_text(json.dumps(idx), encoding="utf-8")
            tmp.replace(self.archive / "raw" / "index.json")
        print(f"Raw archive: {gz.name} saved ({gz.stat().st_size / 1e6:.1f} MB compressed)", flush=True)

    def audit_summary(self) -> dict:
        """Who closed the gaps and how accurate the price maths is (live worker, or the saved CSVs)."""
        # always from the saved files, so a restart doesn't wipe the counts; live worker adds its counters
        if time.time() - self._audit_cache[0] > 30:
            self._audit_cache = (time.time(), W.read_csv(self.out / "closers.csv"),
                                 W.read_csv(self.out / "quote_checks.csv"))
        _, closers, checks = self._audit_cache
        return W.summarise_audit(closers, checks, dict(self.auditor.stats) if self.auditor else None)

    def loop_state(self) -> dict:
        e = self.engine
        live = []
        if e:
            for w in e.watches:
                for (route, sp, up), cur in list(w.loops.items()):
                    live.append({"watch": w.label, "route": route, "sol_pool": sp, "usdc_pool": up,
                                 "since": cur["start"], "best_net_sol": round(cur["best"], 6),
                                 "best_size_sol": round(cur["size"], 4)})
        return {"open": live, "closed": tail_csv(self.out / "loops.csv", 30),
                "best_now": {w.label: w.loop_best for w in e.watches if w.loop_best is not None} if e else {},
                "count": e.stats.get("loops", 0) if e else 0}

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
        # positions whose pool is no longer watched (the hunter's expired tokens) are dropped from the table
        return [{**r, "watch": owner[r["address"]]} for r in e.lp.summary() if r["address"] in owner]

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
        if self.auditor:
            self.auditor.close()
            self.auditor = None
        if os.environ.get("ORBIT_AUDIT", "1") != "0" and getattr(self, "_http_url", None):
            self.auditor = W.Auditor(engine, self.rpc_factory(self._http_url), self.out)
            self.auditor.start()

    def handle_event(self, kind: str, data: dict) -> None:
        """Called by the engine: a gap closed or a big shock happened."""
        if self.auditor:
            self.auditor.on_event(kind, data)
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
        """Compress the raw log every few hours (or at 64 MB) into archive/raw; send the daily summary."""
        if self.notifier.enabled and time.time() - self.last_daily >= 86400:
            self.last_daily = time.time()
            self.notifier.send("Orbit daily summary\n" + "\n".join(W.report(self.out).splitlines()[2:16]), force=True)
        self.check_health()
        if time.time() - self.last_archive >= 86400:          # one automatic copy a day, no button needed
            self.last_archive = time.time()
            try:
                self.save_archive("daily")
            except Exception as err:
                print(f"Daily archive failed: {err}", flush=True)
        raw = self.out / "raw_updates.jsonl"
        if self.recorder and self.recorder.raw_fh and raw.exists() and raw.stat().st_size > 0 and (
                raw.stat().st_size > RAW_ROTATE_BYTES or time.time() - self.raw_started > RAW_ROTATE_HOURS * 3600):
            self.rotate_raw()

    def health_checks(self, now: float | None = None) -> list[dict]:
        """Every health check in one place: name, status (ok / warn / fail / wait) and a plain-words detail.
        The dashboard shows all of them; Telegram hears about any that turn to 'fail' (and when they recover)."""
        now = now or time.time()
        e, out = self.engine, []
        add = lambda key, name, status, detail: out.append({"key": key, "name": name, "status": status, "detail": detail})
        up = now - self.started
        # 1. watcher running
        run = self.state == "running"
        add("watcher", "Watcher", "ok" if run else "wait" if up < 120 else "fail",
            f"{self.state}, up {int(up // 3600)} h {int(up % 3600 // 60)} min")
        # 2. data flowing (updates keep arriving)
        quiet = now - self.watchdog["since"]
        add("data", "Data stream", "ok" if quiet < 120 else "warn" if quiet < STALL_ALERT_S else "fail",
            f"{e.updates:,} pool updates; last new data {int(quiet)} s ago" if e else "not started")
        # 3. speed
        lat = e.latency_stats() if e else None
        if lat:
            m = lat["median_slots"]
            add("lag", "Speed", "ok" if m <= 1 else "warn" if m <= 3 else "fail", f"median {m} slot(s) behind the chain tip")
        else:
            add("lag", "Speed", "wait", "measuring")
        # 4. stream drops
        drops = sum(1 for t in (e.reconnects if e else []) if now - t < 3600)
        add("reconnects", "Connection", "ok" if drops < RECONNECT_WARN else "warn" if drops < RECONNECT_FAIL else "fail",
            f"{drops} reconnect(s) in the last hour")
        # 5. market mood fresh
        age = (now - self.mood["t"]) / 3600 if self.mood else None
        if age is None:
            add("mood", "Market mood", "wait" if up < 1800 else "fail", self.mood_status.get("error") or "no reading yet")
        else:
            st = "ok" if age < 7 else "warn" if age < 13 else "fail"
            if self.mood_status.get("fails", 0) >= MOOD_FAILS_ALERT:
                st = "fail"
            add("mood", "Market mood", st, f"updated {age:.1f} h ago" + (f"; last error: {self.mood_status['error']}"
                                                                       if self.mood_status.get("error") else ""))
        # 6. paper positions being saved
        lp_file = self.out / "lp_state.json"
        if e and e.lp.sims:
            saved = now - lp_file.stat().st_mtime if lp_file.exists() else 1e9
            add("positions", "Earnings tracking", "ok" if saved < 300 else "warn" if saved < 1800 else "fail",
                f"{len(e.lp.sims)} paper positions, saved {int(saved)} s ago"
                + (f"; {e.lp.restarted} restarted after odd readings" if e.lp.restarted else ""))
        else:
            add("positions", "Earnings tracking", "wait" if up < 600 else "warn", "no positions yet")
        # 7. price maths still matches real swaps (catches a DEX changing its data layout)
        _, _, checks = self._audit_cache if self._audit_cache[0] else (0, [], W.read_csv(self.out / "quote_checks.csv"))
        errs = [abs(float(c["error_pct"])) for c in checks[-100:] if c.get("error_pct") not in (None, "")]
        if len(errs) >= 20:
            good = sum(x <= 0.1 for x in errs) / len(errs)
            add("accuracy", "Price maths", "ok" if good >= 0.9 else "warn" if good >= 0.75 else "fail",
                f"{good:.0%} of the last {len(errs)} real swaps re-priced within 0.1%")
        else:
            add("accuracy", "Price maths", "wait", f"{len(errs)} of 20 swaps needed for a reading")
        # 8. background lookups alive
        if self.auditor:
            last = now - self.auditor._last_check if self.auditor._last_check else None
            errs_n = self.auditor.stats.get("errors", 0)
            st = "ok" if last is not None and last < 300 else "wait" if up < 300 else "warn"
            add("auditor", "Transaction lookups", st,
                (f"last check {int(last)} s ago" if last is not None else "not started") + f"; {errs_n} error(s) since start")
        # 9. idle pools
        if e:
            pools = [p for w in e.watches for p in w.pools]
            idle = [p for p in pools if now - p.last_update > 7200]
            add("pools", "Pools", "ok" if len(idle) <= len(pools) // 3 else "warn",
                f"{len(pools) - len(idle)} of {len(pools)} pools updated in the last 2 h"
                + (f"; quiet: {', '.join(p.name for p in idle[:4])}" if idle else ""))
        # 10. disk space on the volume
        try:
            free = shutil.disk_usage(self.out if self.out.exists() else self.dir).free / 1e6
            add("disk", "Disk space", "ok" if free >= DISK_WARN_MB else "warn" if free >= DISK_FAIL_MB else "fail",
                f"{free:,.0f} MB free")
        except OSError:
            pass
        # 11. raw data compression keeping up
        raw = self.out / "raw_updates.jsonl"
        size = raw.stat().st_size / 1e6 if raw.exists() else 0
        add("raw", "Raw data", "ok" if size < 2 * RAW_ROTATE_BYTES / 1e6 else "fail",
            f"current file {size:.0f} MB; compressed every {RAW_ROTATE_HOURS} h")
        # 12. memory
        rss = memory_mb()
        if rss is not None:
            add("memory", "Memory", "ok" if rss < MEMORY_WARN_MB else "warn", f"{rss:.0f} MB in use")
        # 13. alerts
        add("alerts", "Telegram alerts", ("ok" if not self.notifier.errors else "warn") if self.notifier.enabled else "warn",
            ("on" if self.notifier.enabled else "off: you won't hear about trouble")
            + (f"; {self.notifier.errors} failed send(s)" if self.notifier.errors else ""))
        return out

    def safe_health(self) -> list[dict]:
        try:
            return self.health_checks()
        except Exception as err:
            return [{"key": "health", "name": "Health checks", "status": "warn", "detail": f"could not run: {type(err).__name__}"}]

    def check_health(self, now: float | None = None) -> None:
        """Telegram 'trouble' alerts: data stream stalled, or the market-mood job keeps failing. Once each, plus
        a recovery message."""
        now = now or time.time()
        wd, e = self.watchdog, self.engine
        updates = e.updates if e else -1
        if updates != wd["updates"]:
            if wd["stalled"]:
                self.notifier.send(f"Orbit recovered: pool updates are flowing again ({updates:,} so far).", force=True)
                print("Watchdog: data flowing again.", flush=True)
            wd.update(updates=updates, since=now, stalled=False)
        elif not wd["stalled"] and now - wd["since"] >= STALL_ALERT_S:
            wd["stalled"] = True
            msg = (f"Orbit trouble: no pool updates for {int((now - wd['since']) / 60)} minutes (state: {self.state}). "
                   "Check the dashboard; Railway may need a restart.")
            self.notifier.send(msg, force=True)
            print("Watchdog: " + msg, flush=True)
        if self.mood_status.get("fails", 0) >= MOOD_FAILS_ALERT and not wd["mood_alerted"]:
            wd["mood_alerted"] = True
            self.notifier.send(f"Orbit trouble: market mood failed {self.mood_status['fails']} times in a row "
                               f"({self.mood_status.get('error', '')}).", force=True)
        # every other check: one message when it turns to 'fail', one when it is fine again
        failing = wd.setdefault("failing", set())
        try:
            checks = self.health_checks(now)
        except Exception as err:                         # a broken check must never stop the watchdog
            print(f"Health checks failed to run: {type(err).__name__}: {err}", flush=True)
            checks = []
        for c in checks:
            if c["key"] in ("data", "mood"):
                continue                                   # handled above with their own wording
            if c["status"] == "fail" and c["key"] not in failing:
                failing.add(c["key"])
                self.notifier.send(f"Orbit trouble: {c['name']}: {c['detail']}", force=True)
                print(f"Health: {c['name']} failing: {c['detail']}", flush=True)
            elif c["status"] in ("ok", "warn") and c["key"] in failing:
                failing.discard(c["key"])
                self.notifier.send(f"Orbit recovered: {c['name']}: {c['detail']}", force=True)

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
                              "suspect": bool(lp and lp.suspect),
                              "idle_min": round((time.time() - lp.last_update) / 60) if lp else None})
            ref = wc.get("sol_usdc")
            watches.append({"label": wc["label"], **settings, "pools": pools, "auto_until": wc.get("auto_until"),
                            "ref": ref["name"] if ref else None,
                            "gap_pct": e.max_gap_pct(lw, active_only=True) if e and lw else None,
                            "net_gap_pct": e.best_net_gap_pct(lw, active_only=True) if e and lw else None,
                            "open": len(lw.open) if lw else 0})
        return {
            "state": self.state, "uptime_s": round(time.time() - self.started),
            "updates": e.updates if e else 0, "slot": e.last_slot if e else 0,
            "stats": dict(e.stats) if e else {}, "watches": watches,
            "rpc": "private" if os.environ.get("SOLANA_RPC_HTTP") else "public",
            "latency": e.latency_stats() if e else None,
            "lp": (lp := self.lp_rows()),
            "lp_spread": W.lp_spread(lp),
            "capital_sol": W.LP_CAPITAL_SOL,
            "health": self.safe_health(),
            "verdict": W.lp_verdict(lp)[1:],
            "mood": self.mood, "mood_status": self.mood_status,
            "audit": self.audit_summary(),
            "loops": self.loop_state(),
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
            if path == "/api/archives":
                return self.json(app.archives())
            if path == "/download/bundle.zip":
                data = app.bundle()
                return self.send(200, data, "application/zip", {
                    "Content-Disposition": f'attachment; filename="orbit-bundle-{time.strftime("%Y%m%d-%H%M", time.gmtime())}.zip"'})
            if path.startswith("/download/archive/"):
                parts = path.split("/")
                kind, name = (parts[3], parts[4]) if len(parts) == 5 else ("", "")
                if kind == "report" and ARCHIVE_NAME.match(name):
                    data = app.report_archive_zip(name)
                    if data:
                        return self.send(200, data, "application/zip",
                                         {"Content-Disposition": f'attachment; filename="orbit-{name}.zip"'})
                if kind == "raw" and ARCHIVE_NAME.match(name) and name.endswith(".jsonl.gz"):
                    f = app.archive / "raw" / name
                    if f.is_file():
                        return self.send(200, f.read_bytes(), "application/gzip",
                                         {"Content-Disposition": f'attachment; filename="{name}"'})
                return self.json({"error": "not found"}, 404)
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
                if path == "/api/archive/save":
                    return self.json({"ok": True, "name": app.save_archive("manual")})
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
.err{{color:#ff7b72;margin:12px 0 0}}
@media (prefers-color-scheme: light){{body{{background:#f4f5f7;color:#131820}}form{{background:#fff;border-color:#e4e7ec;
box-shadow:0 1px 3px rgba(16,24,40,.08)}}p{{color:#5b6472}}input{{background:#f8f9fb;border-color:#d3d8e0;color:#131820}}
button{{background:#11875a;color:#fff}}.err{{color:#c42b2b}}}}</style></head><body>
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
