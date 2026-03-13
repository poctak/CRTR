#!/usr/bin/env python3
# progressive30s_entry_service.py
# ------------------------------------------------------------
# 🟢 BTC 30s momentum / acceleration -> create MARKET BUY intents
#
# NO 30s candle DB table required.
#
# Data source:
#   - Binance websocket aggTrade stream
#   - service builds 30s candles in memory
#
# Main idea:
#   - aggregate 30s candles for BTC + configured alt symbols
#   - on every newly CLOSED BTC 30s candle:
#       move_recent = current BTC closed candle change
#       move_prev   = previous BTC closed candle change
#       acceleration = move_recent - move_prev
#   - if BTC trigger passes:
#       scan alts on the SAME 30s closed timestamp
#       buy only "laggards" (small move, not too much)
#       skip overextended / illiquid targets
#   - insert MARKET intents idempotently into trade_intents
#
# Important changes:
#   - cooldown is applied ONLY when at least one intent was created
#   - expired candles are force-closed even without a new trade
#   - insert is compatible with trade_intents schema containing:
#       side, support_price, entry_mode, status, ...
#   - pending NEW/SENT intent on symbol is checked before insert
# ------------------------------------------------------------

import os
import json
import time
import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone

import aiohttp
import asyncpg


# ==========================================================
# ENV helpers
# ==========================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default


def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return float(v) if v else default


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def parse_symbols(env_name: str, allow_empty: bool = False) -> List[str]:
    raw = os.getenv(env_name, "").strip()
    if not raw:
        if allow_empty:
            return []
        raise RuntimeError(f"{env_name} is empty")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


# ==========================================================
# CONFIG
# ==========================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# DB
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
TRADE_INTENTS_TABLE = os.getenv("TRADE_INTENTS_TABLE", "public.trade_intents").strip()

# Symbols
BTC_SYMBOL = os.getenv("BTC_SYMBOL", "BTCUSDC").strip().upper()
SYMBOLS = parse_symbols("SYMBOLS")
SYMBOLS = [s for s in SYMBOLS if s != BTC_SYMBOL]
ALL_SYMBOLS = [BTC_SYMBOL] + SYMBOLS

# Websocket
BINANCE_WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443/stream").strip()
WS_PING_INTERVAL = env_float("WS_PING_INTERVAL", 20.0)
WS_RECONNECT_SEC = env_float("WS_RECONNECT_SEC", 5.0)

# Candle aggregation
CANDLE_SEC = env_int("CANDLE_SEC", 30)
MAX_CANDLES_PER_SYMBOL = env_int("MAX_CANDLES_PER_SYMBOL", 20)

# Trigger loop
SCAN_INTERVAL_SEC = env_float("SCAN_INTERVAL_SEC", 0.5)
COOLDOWN_CANDLES = env_int("COOLDOWN_CANDLES", 4)

# Allow tiny grace after bucket end before evaluating just-closed candle
POST_CLOSE_GRACE_MS = env_int("POST_CLOSE_GRACE_MS", 900)

# BTC trigger
BTC_REQUIRE_GREEN = env_bool("BTC_REQUIRE_GREEN", True)
BTC_MIN_VQ = env_float("BTC_MIN_VQ", 500000.0)
BTC_MIN_TRADES = env_int("BTC_MIN_TRADES", 1500)
BTC_MOVE_RECENT_TH = env_float("BTC_MOVE_RECENT_TH", 0.0018)   # 0.18%
BTC_ACCEL_TH = env_float("BTC_ACCEL_TH", 0.0008)               # 0.08%
BTC_MAX_RANGE_PCT = env_float("BTC_MAX_RANGE_PCT", 0.0035)     # 0.35%
BTC_USE_PREV_CONFIRM = env_bool("BTC_USE_PREV_CONFIRM", True)
BTC_PREV_MIN_MOVE = env_float("BTC_PREV_MIN_MOVE", 0.0003)     # 0.03%

# Target filters (laggards)
TARGET_REQUIRE_GREEN = env_bool("TARGET_REQUIRE_GREEN", False)
TARGET_MIN_MOVE_PCT = env_float("TARGET_MIN_MOVE_PCT", 0.0005)    # 0.05%
TARGET_MAX_MOVE_PCT = env_float("TARGET_MAX_MOVE_PCT", 0.0025)    # 0.25%
TARGET_MAX_RANGE_PCT = env_float("TARGET_MAX_RANGE_PCT", 0.0060)  # 0.60%
TARGET_MIN_VQ = env_float("TARGET_MIN_VQ", 80000.0)
TARGET_MIN_TRADES = env_int("TARGET_MIN_TRADES", 80)

# Relative filter vs BTC
USE_RELATIVE_FILTER = env_bool("USE_RELATIVE_FILTER", True)
RELATIVE_MIN = env_float("RELATIVE_MIN", -0.0012)
RELATIVE_MAX = env_float("RELATIVE_MAX", 0.0008)

# Intent settings
QUOTE_AMOUNT = env_float("QUOTE_AMOUNT", 15.0)
INTENT_STATUS = os.getenv("INTENT_STATUS", "NEW").strip().upper()
INTENT_SOURCE = os.getenv("INTENT_SOURCE", "PROGRESSIVE30S").strip()
INTENT_SIDE = os.getenv("INTENT_SIDE", "BUY").strip().upper()
ENTRY_MODE = os.getenv("ENTRY_MODE", "MARKET").strip().upper()

# Limits / debug
MAX_TARGETS_PER_TRIGGER = env_int("MAX_TARGETS_PER_TRIGGER", 15)
DEBUG = env_bool("DEBUG", False)


# ==========================================================
# SQL
# ==========================================================
SQL_HAS_PENDING = f"""
SELECT 1
FROM {TRADE_INTENTS_TABLE}
WHERE symbol = $1
  AND status IN ('NEW', 'SENT')
LIMIT 1;
"""

SQL_INSERT_INTENT = f"""
INSERT INTO {TRADE_INTENTS_TABLE}(
  symbol,
  ts,
  source,
  side,
  quote_amount,
  limit_price,
  support_price,
  meta,
  status,
  created_at,
  updated_at,
  entry_mode
)
VALUES(
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8::jsonb,
  $9,
  NOW(),
  NOW(),
  $10
)
ON CONFLICT (symbol, ts) DO NOTHING
RETURNING id;
"""


# ==========================================================
# Helpers
# ==========================================================
def pct_change(o: float, c: float) -> float:
    if o <= 0:
        return 0.0
    return (c - o) / o


def pct_range(o: float, h: float, l: float) -> float:
    if o <= 0:
        return 0.0
    return (h - l) / o


def floor_ts_ms(ts_ms: int, bucket_sec: int) -> int:
    bucket_ms = bucket_sec * 1000
    return (ts_ms // bucket_ms) * bucket_ms


def ms_to_dt_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def utc_ms_now() -> int:
    return int(time.time() * 1000)


def pick_targets() -> List[str]:
    return SYMBOLS[: max(0, MAX_TARGETS_PER_TRIGGER)]


# ==========================================================
# Candle model
# ==========================================================
@dataclass
class Candle:
    symbol: str
    start_ms: int
    end_ms: int
    o: float
    h: float
    l: float
    c: float
    v_base: float
    v_quote: float
    trades_count: int

    def to_meta(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "o": float(self.o),
            "h": float(self.h),
            "l": float(self.l),
            "c": float(self.c),
            "v_base": float(self.v_base),
            "v_quote": float(self.v_quote),
            "trades_count": int(self.trades_count),
        }


class InMemoryCandleStore:
    def __init__(self, symbols: List[str], candle_sec: int, max_candles: int):
        self.symbols = symbols
        self.candle_sec = candle_sec
        self.max_candles = max_candles
        self.current: Dict[str, Optional[Candle]] = {s: None for s in symbols}
        self.closed: Dict[str, List[Candle]] = {s: [] for s in symbols}
        self.last_trade_ms: Dict[str, int] = {s: 0 for s in symbols}
        self._lock = asyncio.Lock()

    def _append_closed_unlocked(self, symbol: str, candle: Candle) -> None:
        arr = self.closed[symbol]
        if arr and arr[-1].end_ms == candle.end_ms:
            return
        arr.append(candle)
        if len(arr) > self.max_candles:
            self.closed[symbol] = arr[-self.max_candles:]

    async def on_trade(self, symbol: str, price: float, qty: float, trade_ms: int) -> None:
        bucket_start = floor_ts_ms(trade_ms, self.candle_sec)
        bucket_end = bucket_start + self.candle_sec * 1000

        async with self._lock:
            self.last_trade_ms[symbol] = trade_ms
            cur = self.current.get(symbol)

            if cur is None:
                self.current[symbol] = Candle(
                    symbol=symbol,
                    start_ms=bucket_start,
                    end_ms=bucket_end,
                    o=price,
                    h=price,
                    l=price,
                    c=price,
                    v_base=qty,
                    v_quote=qty * price,
                    trades_count=1,
                )
                return

            if bucket_start == cur.start_ms:
                cur.h = max(cur.h, price)
                cur.l = min(cur.l, price)
                cur.c = price
                cur.v_base += qty
                cur.v_quote += qty * price
                cur.trades_count += 1
                return

            if bucket_start > cur.start_ms:
                self._append_closed_unlocked(symbol, cur)
                self.current[symbol] = Candle(
                    symbol=symbol,
                    start_ms=bucket_start,
                    end_ms=bucket_end,
                    o=price,
                    h=price,
                    l=price,
                    c=price,
                    v_base=qty,
                    v_quote=qty * price,
                    trades_count=1,
                )
                return

            # old / out-of-order trade -> ignore

    async def force_close_expired(self, now_ms: int) -> int:
        """
        Close candles whose end_ms is already in the past,
        even if no next trade arrived yet.
        """
        closed_count = 0
        async with self._lock:
            for symbol in self.symbols:
                cur = self.current.get(symbol)
                if cur is None:
                    continue

                if cur.end_ms <= now_ms:
                    self._append_closed_unlocked(symbol, cur)
                    self.current[symbol] = None
                    closed_count += 1

        return closed_count

    async def get_last_closed(self, symbol: str, n: int) -> List[Candle]:
        async with self._lock:
            arr = self.closed.get(symbol, [])
            return arr[-n:] if len(arr) >= n else arr[:]

    async def get_closed_at(self, symbol: str, end_ms: int) -> Optional[Candle]:
        async with self._lock:
            arr = self.closed.get(symbol, [])
            for c in reversed(arr):
                if c.end_ms == end_ms:
                    return c
            return None

    async def get_snapshot_info(self) -> Dict[str, Any]:
        async with self._lock:
            ready = {s: len(self.closed[s]) for s in self.symbols}
            current = {
                s: (self.current[s].end_ms if self.current[s] else None)
                for s in self.symbols
            }
            return {
                "closed_counts": ready,
                "last_trade_ms": dict(self.last_trade_ms),
                "current_end_ms": current,
            }


# ==========================================================
# Intent insert
# ==========================================================
async def create_market_intent(
    pool: asyncpg.Pool,
    symbol: str,
    ts_dt: datetime,
    ref_price: float,
    meta_extra: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[int], str]:
    meta = {
        "reason": "BTC_30S_ACCEL",
        "ref_price": float(ref_price),
        "version": "2026-03-13",
    }
    if meta_extra:
        meta.update(meta_extra)

    async with pool.acquire() as conn:
        pending = await conn.fetchrow(SQL_HAS_PENDING, symbol)
        if pending:
            return None, "pending_exists"

        row = await conn.fetchrow(
            SQL_INSERT_INTENT,
            symbol,
            ts_dt,
            INTENT_SOURCE,
            INTENT_SIDE,
            float(QUOTE_AMOUNT),
            float(ref_price),    # placeholder for MARKET
            None,                # support_price nullable in your schema
            json.dumps(meta),
            INTENT_STATUS,
            ENTRY_MODE,
        )

    if not row:
        return None, "duplicate_ts"

    return int(row["id"]), "inserted"


# ==========================================================
# Websocket consumer
# ==========================================================
async def ws_consumer(store: InMemoryCandleStore) -> None:
    stream_names = [f"{s.lower()}@aggTrade" for s in ALL_SYMBOLS]
    ws_url = f"{BINANCE_WS_BASE}?streams={'/'.join(stream_names)}"

    while True:
        try:
            logging.info("WS_CONNECT %s", ws_url)
            timeout = aiohttp.ClientTimeout(total=None, sock_read=None, sock_connect=30)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(ws_url, heartbeat=WS_PING_INTERVAL) as ws:
                    logging.info("WS_CONNECTED streams=%d", len(stream_names))

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                                data = payload.get("data", {})

                                if not data:
                                    continue

                                symbol = data.get("s")
                                price = float(data.get("p"))
                                qty = float(data.get("q"))
                                trade_ms = int(data.get("T"))

                                if symbol not in ALL_SYMBOLS:
                                    continue

                                await store.on_trade(symbol, price, qty, trade_ms)

                            except Exception as e:
                                logging.exception("WS_MESSAGE_ERROR: %s", e)

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError(f"websocket error: {ws.exception()}")

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                            raise RuntimeError("websocket closed")

        except Exception as e:
            logging.exception("WS_LOOP_ERROR: %s | reconnecting in %.1fs", e, WS_RECONNECT_SEC)
            await asyncio.sleep(WS_RECONNECT_SEC)


# ==========================================================
# Strategy loop
# ==========================================================
async def strategy_loop(store: InMemoryCandleStore, pool: asyncpg.Pool) -> None:
    last_btc_end_ms: Optional[int] = None
    cooldown_left = 0

    while True:
        try:
            # Force-close expired candles so we are not waiting for "next trade"
            # to mark previous bucket as closed.
            now_ms = utc_ms_now()
            await store.force_close_expired(now_ms - POST_CLOSE_GRACE_MS)

            btc_last2 = await store.get_last_closed(BTC_SYMBOL, 2)

            if len(btc_last2) < 2:
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            prev = btc_last2[-2]
            latest = btc_last2[-1]
            btc_end_ms = latest.end_ms

            if last_btc_end_ms is None:
                last_btc_end_ms = btc_end_ms
                logging.info("Bootstrap last_btc_end_ms=%s (waiting for next 30s close)", btc_end_ms)
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            if btc_end_ms == last_btc_end_ms:
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            last_btc_end_ms = btc_end_ms

            move_recent = pct_change(latest.o, latest.c)
            move_prev = pct_change(prev.o, prev.c)
            acceleration = move_recent - move_prev
            btc_range = pct_range(latest.o, latest.h, latest.l)
            btc_vq = latest.v_quote
            btc_trades = latest.trades_count
            btc_ts_dt = ms_to_dt_utc(latest.end_ms)

            if DEBUG:
                logging.info(
                    "BTC_30S_CLOSE ts=%s recent=%.3f%% prev=%.3f%% accel=%.3f%% range=%.3f%% vq=%.2f trades=%d",
                    btc_ts_dt.isoformat(),
                    move_recent * 100.0,
                    move_prev * 100.0,
                    acceleration * 100.0,
                    btc_range * 100.0,
                    btc_vq,
                    btc_trades,
                )

            if cooldown_left > 0:
                cooldown_left -= 1
                logging.info("COOLDOWN active (%d candles left)", cooldown_left)
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            # BTC trigger
            if BTC_REQUIRE_GREEN and latest.c <= latest.o:
                logging.info("NO_TRIGGER btc_not_green | ts=%s", btc_ts_dt.isoformat())
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            if btc_vq < BTC_MIN_VQ:
                logging.info("NO_TRIGGER btc_vq_low | ts=%s vq=%.2f < %.2f", btc_ts_dt.isoformat(), btc_vq, BTC_MIN_VQ)
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            if btc_trades < BTC_MIN_TRADES:
                logging.info(
                    "NO_TRIGGER btc_trades_low | ts=%s trades=%d < %d",
                    btc_ts_dt.isoformat(), btc_trades, BTC_MIN_TRADES
                )
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            if move_recent < BTC_MOVE_RECENT_TH:
                logging.info(
                    "NO_TRIGGER btc_recent_move_small | ts=%s recent=%.3f%% < %.3f%%",
                    btc_ts_dt.isoformat(),
                    move_recent * 100.0,
                    BTC_MOVE_RECENT_TH * 100.0,
                )
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            if acceleration < BTC_ACCEL_TH:
                logging.info(
                    "NO_TRIGGER btc_accel_small | ts=%s accel=%.3f%% < %.3f%%",
                    btc_ts_dt.isoformat(),
                    acceleration * 100.0,
                    BTC_ACCEL_TH * 100.0,
                )
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            if BTC_USE_PREV_CONFIRM and move_prev < BTC_PREV_MIN_MOVE:
                logging.info(
                    "NO_TRIGGER btc_prev_move_small | ts=%s prev=%.3f%% < %.3f%%",
                    btc_ts_dt.isoformat(),
                    move_prev * 100.0,
                    BTC_PREV_MIN_MOVE * 100.0,
                )
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            if btc_range > BTC_MAX_RANGE_PCT:
                logging.info(
                    "NO_TRIGGER btc_range_too_big | ts=%s range=%.3f%% > %.3f%%",
                    btc_ts_dt.isoformat(),
                    btc_range * 100.0,
                    BTC_MAX_RANGE_PCT * 100.0,
                )
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            targets = pick_targets()
            if not targets:
                logging.warning("TRIGGERED but no targets")
                await asyncio.sleep(SCAN_INTERVAL_SEC)
                continue

            logging.warning(
                "TRIGGER BTC_30S_ACCEL | ts=%s recent=%.3f%% prev=%.3f%% accel=%.3f%% vq=%.2f trades=%d -> targets=%d",
                btc_ts_dt.isoformat(),
                move_recent * 100.0,
                move_prev * 100.0,
                acceleration * 100.0,
                btc_vq,
                btc_trades,
                len(targets),
            )

            created = 0
            existed = 0
            pending = 0
            missing = 0
            skipped = 0
            errors = 0

            for sym in targets:
                try:
                    cc = await store.get_closed_at(sym, latest.end_ms)

                    if cc is None:
                        missing += 1
                        if DEBUG:
                            logging.info("TARGET_SKIP %s | no closed candle at ts=%s", sym, btc_ts_dt.isoformat())
                        continue

                    tmove = pct_change(cc.o, cc.c)
                    trange = pct_range(cc.o, cc.h, cc.l)
                    rel = tmove - move_recent

                    if TARGET_REQUIRE_GREEN and cc.c <= cc.o:
                        skipped += 1
                        logging.info("TARGET_SKIP %s | not_green | ts=%s", sym, btc_ts_dt.isoformat())
                        continue

                    if cc.v_quote < TARGET_MIN_VQ:
                        skipped += 1
                        if DEBUG:
                            logging.info(
                                "TARGET_SKIP %s | vq_low=%.2f < %.2f | ts=%s",
                                sym, cc.v_quote, TARGET_MIN_VQ, btc_ts_dt.isoformat()
                            )
                        continue

                    if cc.trades_count < TARGET_MIN_TRADES:
                        skipped += 1
                        if DEBUG:
                            logging.info(
                                "TARGET_SKIP %s | trades_low=%d < %d | ts=%s",
                                sym, cc.trades_count, TARGET_MIN_TRADES, btc_ts_dt.isoformat()
                            )
                        continue

                    if tmove < TARGET_MIN_MOVE_PCT:
                        skipped += 1
                        if DEBUG:
                            logging.info(
                                "TARGET_SKIP %s | move_too_small=%.3f%% < %.3f%% | ts=%s",
                                sym, tmove * 100.0, TARGET_MIN_MOVE_PCT * 100.0, btc_ts_dt.isoformat()
                            )
                        continue

                    if tmove > TARGET_MAX_MOVE_PCT:
                        skipped += 1
                        logging.info(
                            "TARGET_SKIP %s | move_too_big=%.3f%% > %.3f%% | ts=%s",
                            sym, tmove * 100.0, TARGET_MAX_MOVE_PCT * 100.0, btc_ts_dt.isoformat()
                        )
                        continue

                    if trange > TARGET_MAX_RANGE_PCT:
                        skipped += 1
                        logging.info(
                            "TARGET_SKIP %s | range_too_big=%.3f%% > %.3f%% | ts=%s",
                            sym, trange * 100.0, TARGET_MAX_RANGE_PCT * 100.0, btc_ts_dt.isoformat()
                        )
                        continue

                    if USE_RELATIVE_FILTER and (rel < RELATIVE_MIN or rel > RELATIVE_MAX):
                        skipped += 1
                        if DEBUG:
                            logging.info(
                                "TARGET_SKIP %s | rel_out_of_band=%.3f%% not in [%.3f%%, %.3f%%] | ts=%s",
                                sym,
                                rel * 100.0,
                                RELATIVE_MIN * 100.0,
                                RELATIVE_MAX * 100.0,
                                btc_ts_dt.isoformat(),
                            )
                        continue

                    ref_price = cc.c

                    intent_id, result = await create_market_intent(
                        pool,
                        symbol=sym,
                        ts_dt=btc_ts_dt,
                        ref_price=ref_price,
                        meta_extra={
                            "btc_ts": btc_ts_dt.isoformat(),
                            "btc_recent_move_pct": float(move_recent),
                            "btc_prev_move_pct": float(move_prev),
                            "btc_acceleration_pct": float(acceleration),
                            "btc_vq": float(btc_vq),
                            "btc_trades": int(btc_trades),
                            "btc_range_pct": float(btc_range),
                            "btc_candle": latest.to_meta(),
                            "btc_prev_candle": prev.to_meta(),
                            "tgt_candle": cc.to_meta(),
                            "tgt_move_pct": float(tmove),
                            "tgt_range_pct": float(trange),
                            "tgt_relative_vs_btc": float(rel),
                        },
                    )

                    if intent_id is not None:
                        created += 1
                        logging.warning(
                            "INTENT_CREATED id=%d %s | ts=%s MARKET ref=%.6f | tgt_move=%.3f%% rel=%.3f%%",
                            intent_id,
                            sym,
                            btc_ts_dt.isoformat(),
                            ref_price,
                            tmove * 100.0,
                            rel * 100.0,
                        )
                    else:
                        if result == "pending_exists":
                            pending += 1
                            logging.info(
                                "INTENT_SKIP %s | pending NEW/SENT already exists | ts=%s",
                                sym,
                                btc_ts_dt.isoformat(),
                            )
                        else:
                            existed += 1
                            logging.info(
                                "INTENT_EXISTS %s | ts=%s ref=%.6f",
                                sym,
                                btc_ts_dt.isoformat(),
                                ref_price,
                            )

                except Exception as e:
                    errors += 1
                    logging.exception("INTENT_ERROR %s | ts=%s | %s", sym, btc_ts_dt.isoformat(), e)

            logging.warning(
                "TRIGGER_SUMMARY | ts=%s created=%d existed=%d pending=%d skipped=%d missing_candle=%d errors=%d",
                btc_ts_dt.isoformat(),
                created,
                existed,
                pending,
                skipped,
                missing,
                errors,
            )

            # IMPORTANT CHANGE:
            # cooldown only when at least one trade intent was actually created
            if created > 0:
                cooldown_left = COOLDOWN_CANDLES
                logging.info(
                    "COOLDOWN_SET | candles=%d | reason=created_intents=%d | ts=%s",
                    COOLDOWN_CANDLES,
                    created,
                    btc_ts_dt.isoformat(),
                )
            else:
                logging.info(
                    "COOLDOWN_NOT_SET | reason=no_intents_created | ts=%s",
                    btc_ts_dt.isoformat(),
                )

        except Exception as e:
            logging.exception("STRATEGY_LOOP_ERROR: %s", e)

        await asyncio.sleep(SCAN_INTERVAL_SEC)


# ==========================================================
# Main
# ==========================================================
async def main() -> None:
    if ENTRY_MODE != "MARKET":
        raise RuntimeError("This service is intended for ENTRY_MODE=MARKET")

    if INTENT_SIDE != "BUY":
        raise RuntimeError("INTENT_SIDE must be BUY")

    logging.info(
        "Starting progressive30s_entry_service | btc=%s | targets=%d | candle_sec=%d | "
        "btc_recent_th=%.3f%% | btc_accel_th=%.3f%% | tgt_move=[%.3f%%..%.3f%%] | "
        "tgt_max_range=%.3f%% | rel_filter=%s | cooldown=%d | grace_ms=%d",
        BTC_SYMBOL,
        len(SYMBOLS),
        CANDLE_SEC,
        BTC_MOVE_RECENT_TH * 100.0,
        BTC_ACCEL_TH * 100.0,
        TARGET_MIN_MOVE_PCT * 100.0,
        TARGET_MAX_MOVE_PCT * 100.0,
        TARGET_MAX_RANGE_PCT * 100.0,
        USE_RELATIVE_FILTER,
        COOLDOWN_CANDLES,
        POST_CLOSE_GRACE_MS,
    )

    pool = await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        min_size=1,
        max_size=5,
    )
    logging.info("DB pool created | host=%s db=%s user=%s", DB_HOST, DB_NAME, DB_USER)

    store = InMemoryCandleStore(
        symbols=ALL_SYMBOLS,
        candle_sec=CANDLE_SEC,
        max_candles=MAX_CANDLES_PER_SYMBOL,
    )

    await asyncio.gather(
        ws_consumer(store),
        strategy_loop(store, pool),
    )


if __name__ == "__main__":
    asyncio.run(main())
