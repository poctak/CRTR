#!/usr/bin/env python3
# short_signal_service.py
# ------------------------------------------------------------
# Reads candles from public.candles_5m_short and creates SHORT trade intents
# into public.trade_intents_short.
#
# Core idea:
#   1) BTC regime gate:
#      - BTC 5m candle must be red enough
#      - BTC quote volume must be large enough
#      - optional BTC buy_ratio cap
#
#   2) Local symbol setup:
#      - breakdown below recent support low
#      - sufficient local range
#      - red candle
#      - volume expansion vs recent average
#      - weak buyer pressure (buy_ratio cap)
#
# Output:
#   - writes rows into public.trade_intents_short
#
# Important:
#   - DB-only service, no Binance WS/API here
#   - Works on CLOSED candles already stored in DB
#   - Keeps short model fully isolated from long model
# ------------------------------------------------------------

import os
import json
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

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


def env_str(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def parse_symbols_csv(raw: str) -> List[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


# ==========================================================
# CONFIG
# ==========================================================
LOG_LEVEL = env_str("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

DB_HOST = env_str("DB_HOST", "db")
DB_NAME = env_str("DB_NAME", "pumpdb")
DB_USER = env_str("DB_USER", "pumpuser")
DB_PASSWORD = env_str("DB_PASSWORD", "")

CANDLES_TABLE = env_str("CANDLES_SHORT_TABLE", "public.candles_5m_short")
INTENTS_TABLE = env_str("TRADE_INTENTS_SHORT_TABLE", "public.trade_intents_short")
POSITIONS_TABLE = env_str("POSITIONS_OPEN_SHORT_TABLE", "public.positions_open_short")

POLL_EVERY_SEC = env_int("SHORT_SIG_POLL_EVERY_SEC", 10)
MAX_SYMBOLS_PER_RUN = env_int("SHORT_SIG_MAX_SYMBOLS_PER_RUN", 200)
DEBUG_EVERY = env_int("SHORT_SIG_DEBUG_EVERY", 20)

# Universe
QUOTE_CCY = env_str("QUOTE_CCY", "USDT").upper()
BTC_SYMBOL = env_str("BTC_SYMBOL", f"BTC{QUOTE_CCY}").upper()

# Signal output
SIGNAL_SOURCE = env_str("SHORT_SIGNAL_SOURCE", "SHORT_BREAKDOWN")
ENTRY_MODE = env_str("SHORT_ENTRY_MODE", "MARKET").upper()   # LIMIT | MARKET
QUOTE_AMOUNT = env_float("SHORT_QUOTE_AMOUNT", 10.0)
LIMIT_OFFSET_PCT = env_float("SHORT_LIMIT_OFFSET_PCT", 0.0005)  # if LIMIT: slightly below close

# BTC regime gate
BTC_DUMP_MIN_PCT = env_float("BTC_DUMP_MIN_PCT", 0.0035)              # 0.35%
BTC_MIN_VQUOTE = env_float("BTC_MIN_VQUOTE", 2500000.0)
BTC_MAX_BUY_RATIO = env_float("BTC_MAX_BUY_RATIO", 0.52)
USE_BTC_BUY_RATIO_GATE = env_bool("USE_BTC_BUY_RATIO_GATE", True)

# Local setup
LOOKBACK_BARS = env_int("SHORT_LOOKBACK_BARS", 24)                    # 24x5m = 2h
SUPPORT_LOOKBACK_BARS = env_int("SHORT_SUPPORT_LOOKBACK_BARS", 12)    # recent support
MIN_RANGE_PCT = env_float("SHORT_MIN_RANGE_PCT", 0.010)               # 1.0%
BREAKDOWN_BUFFER_PCT = env_float("SHORT_BREAKDOWN_BUFFER_PCT", 0.0015) # 0.15%
MIN_RED_BODY_PCT = env_float("SHORT_MIN_RED_BODY_PCT", 0.0020)        # 0.20%
MIN_VOLUME_MULT = env_float("SHORT_MIN_VOLUME_MULT", 1.20)
MAX_LOCAL_BUY_RATIO = env_float("SHORT_MAX_LOCAL_BUY_RATIO", 0.48)

# Optional relative weakness gate
USE_RELATIVE_WEAKNESS_GATE = env_bool("USE_RELATIVE_WEAKNESS_GATE", True)
RELATIVE_WEAKNESS_MIN = env_float("RELATIVE_WEAKNESS_MIN", 0.0020)    # coin drop exceeds BTC drop by >= 0.2%

# General safeguards
MIN_PRICE = env_float("SHORT_MIN_PRICE", 0.000001)
REQUIRE_NO_OPEN_POSITION = env_bool("SHORT_REQUIRE_NO_OPEN_POSITION", True)
REQUIRE_NO_PENDING_INTENT = env_bool("SHORT_REQUIRE_NO_PENDING_INTENT", True)


# ==========================================================
# Dataclass
# ==========================================================
@dataclass
class Candle:
    symbol: str
    ts: datetime
    o: float
    h: float
    l: float
    c: float
    v_base: float
    v_quote: float
    trades_count: int
    taker_buy_base: float
    taker_buy_quote: float

    @property
    def buy_ratio(self) -> float:
        return (self.taker_buy_quote / self.v_quote) if self.v_quote > 0 else 0.0

    @property
    def red_body_pct(self) -> float:
        return ((self.o - self.c) / self.o) if self.o > 0 else 0.0

    @property
    def close_change_pct(self) -> float:
        return ((self.c - self.o) / self.o) if self.o > 0 else 0.0


# ==========================================================
# SQL
# ==========================================================
LIST_SYMBOLS_SQL = f"""
SELECT DISTINCT symbol
FROM {CANDLES_TABLE}
WHERE symbol <> $1
  AND symbol LIKE '%' || $2
ORDER BY symbol
LIMIT $3
"""

FETCH_LAST_N_SQL = f"""
SELECT
    symbol, ts, o, h, l, c,
    v_base, v_quote, trades_count,
    taker_buy_base, taker_buy_quote
FROM {CANDLES_TABLE}
WHERE symbol = $1
ORDER BY ts DESC
LIMIT $2
"""

HAS_OPEN_POSITION_SQL = f"""
SELECT 1
FROM {POSITIONS_TABLE}
WHERE symbol = $1
  AND status = 'OPEN'
LIMIT 1
"""

HAS_PENDING_INTENT_SQL = f"""
SELECT 1
FROM {INTENTS_TABLE}
WHERE symbol = $1
  AND status IN ('NEW', 'SENT')
LIMIT 1
"""

INSERT_INTENT_SQL = f"""
INSERT INTO {INTENTS_TABLE}(
    symbol,
    ts,
    source,
    side,
    quote_amount,
    limit_price,
    support_price,
    meta,
    status,
    entry_mode
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,'NEW',$9)
ON CONFLICT(symbol, ts) DO NOTHING
"""

# Optional helpful index creation for performance; safe to keep here
CREATE_INDEXES_SQL = f"""
CREATE INDEX IF NOT EXISTS idx_candles_5m_short_symbol_ts_desc
ON {CANDLES_TABLE}(symbol, ts DESC);

CREATE INDEX IF NOT EXISTS idx_positions_open_short_symbol_status
ON {POSITIONS_TABLE}(symbol, status);

CREATE INDEX IF NOT EXISTS idx_trade_intents_short_symbol_status
ON {INTENTS_TABLE}(symbol, status);
"""


# ==========================================================
# Helpers
# ==========================================================
def row_to_candle(r: asyncpg.Record) -> Candle:
    return Candle(
        symbol=str(r["symbol"]).upper(),
        ts=r["ts"] if r["ts"].tzinfo else r["ts"].replace(tzinfo=timezone.utc),
        o=float(r["o"]),
        h=float(r["h"]),
        l=float(r["l"]),
        c=float(r["c"]),
        v_base=float(r["v_base"]),
        v_quote=float(r["v_quote"]),
        trades_count=int(r["trades_count"] or 0),
        taker_buy_base=float(r["taker_buy_base"] or 0.0),
        taker_buy_quote=float(r["taker_buy_quote"] or 0.0),
    )


def avg(nums: List[float]) -> float:
    return sum(nums) / len(nums) if nums else 0.0


def pct_change(a: float, b: float) -> float:
    # from a to b
    return ((b - a) / a) if a > 0 else 0.0


async def fetch_candles(conn: asyncpg.Connection, symbol: str, n: int) -> List[Candle]:
    rows = await conn.fetch(FETCH_LAST_N_SQL, symbol, n)
    candles = [row_to_candle(r) for r in rows]
    candles.reverse()  # oldest -> newest
    return candles


async def has_open_position(conn: asyncpg.Connection, symbol: str) -> bool:
    row = await conn.fetchrow(HAS_OPEN_POSITION_SQL, symbol)
    return row is not None


async def has_pending_intent(conn: asyncpg.Connection, symbol: str) -> bool:
    row = await conn.fetchrow(HAS_PENDING_INTENT_SQL, symbol)
    return row is not None


def build_limit_price(last_close: float) -> float:
    if ENTRY_MODE == "LIMIT":
        px = last_close * (1.0 - LIMIT_OFFSET_PCT)
        return max(px, MIN_PRICE)
    return max(last_close, MIN_PRICE)


# ==========================================================
# Signal logic
# ==========================================================
def btc_gate(btc: Candle) -> Dict[str, Any]:
    btc_dump_pct = max(0.0, (btc.o - btc.c) / btc.o) if btc.o > 0 else 0.0
    ok = True
    reasons = []

    if btc_dump_pct < BTC_DUMP_MIN_PCT:
        ok = False
        reasons.append(f"btc_dump_pct<{BTC_DUMP_MIN_PCT:.4f}")

    if btc.v_quote < BTC_MIN_VQUOTE:
        ok = False
        reasons.append(f"btc_vq<{BTC_MIN_VQUOTE:.0f}")

    if USE_BTC_BUY_RATIO_GATE and btc.buy_ratio > BTC_MAX_BUY_RATIO:
        ok = False
        reasons.append(f"btc_buy_ratio>{BTC_MAX_BUY_RATIO:.3f}")

    return {
        "ok": ok,
        "btc_dump_pct": btc_dump_pct,
        "btc_v_quote": btc.v_quote,
        "btc_buy_ratio": btc.buy_ratio,
        "reasons": reasons,
    }


def local_short_setup(symbol: str, candles: List[Candle], btc_dump_pct: float) -> Dict[str, Any]:
    need = max(LOOKBACK_BARS, SUPPORT_LOOKBACK_BARS + 1)
    if len(candles) < need:
        return {"ok": False, "reason": f"not_enough_bars({len(candles)}<{need})"}

    last = candles[-1]
    prev = candles[-2]

    window_all = candles[-LOOKBACK_BARS:]
    support_window = candles[-(SUPPORT_LOOKBACK_BARS + 1):-1]  # excludes current bar

    recent_high = max(x.h for x in window_all)
    recent_low = min(x.l for x in window_all)
    range_pct = ((recent_high - recent_low) / recent_low) if recent_low > 0 else 0.0

    support_low = min(x.l for x in support_window)
    avg_v_quote = avg([x.v_quote for x in candles[-(SUPPORT_LOOKBACK_BARS + 1):-1]])
    vol_mult = (last.v_quote / avg_v_quote) if avg_v_quote > 0 else 0.0

    red_body_pct = last.red_body_pct
    breakdown_pct = ((support_low - last.c) / support_low) if support_low > 0 else 0.0
    coin_dump_pct = max(0.0, (last.o - last.c) / last.o) if last.o > 0 else 0.0
    relative_weakness = coin_dump_pct - btc_dump_pct

    # conditions
    cond_range = range_pct >= MIN_RANGE_PCT
    cond_break = last.c < support_low * (1.0 - BREAKDOWN_BUFFER_PCT)
    cond_red = last.c < last.o and red_body_pct >= MIN_RED_BODY_PCT
    cond_vol = vol_mult >= MIN_VOLUME_MULT
    cond_buy_ratio = last.buy_ratio <= MAX_LOCAL_BUY_RATIO
    cond_prev_not_broken = prev.c >= support_low * (1.0 - BREAKDOWN_BUFFER_PCT)

    cond_rel_weak = True
    if USE_RELATIVE_WEAKNESS_GATE:
        cond_rel_weak = relative_weakness >= RELATIVE_WEAKNESS_MIN

    ok = all([
        cond_range,
        cond_break,
        cond_red,
        cond_vol,
        cond_buy_ratio,
        cond_prev_not_broken,
        cond_rel_weak,
    ])

    return {
        "ok": ok,
        "symbol": symbol,
        "ts": last.ts,
        "last_close": last.c,
        "support_low": support_low,
        "range_pct": range_pct,
        "breakdown_pct": breakdown_pct,
        "red_body_pct": red_body_pct,
        "v_quote": last.v_quote,
        "avg_v_quote": avg_v_quote,
        "vol_mult": vol_mult,
        "buy_ratio": last.buy_ratio,
        "coin_dump_pct": coin_dump_pct,
        "relative_weakness": relative_weakness,
        "conditions": {
            "range_ok": cond_range,
            "break_ok": cond_break,
            "red_ok": cond_red,
            "vol_ok": cond_vol,
            "buy_ratio_ok": cond_buy_ratio,
            "prev_not_broken_ok": cond_prev_not_broken,
            "relative_weak_ok": cond_rel_weak,
        }
    }


# ==========================================================
# DB init
# ==========================================================
async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREATE_INDEXES_SQL)

    logging.info(
        "DB init OK | candles=%s | intents=%s | positions=%s",
        CANDLES_TABLE, INTENTS_TABLE, POSITIONS_TABLE
    )


# ==========================================================
# Main evaluation
# ==========================================================
async def evaluate_once(pool: asyncpg.Pool, run_no: int) -> None:
    async with pool.acquire() as conn:
        # 1) BTC gate
        btc_candles = await fetch_candles(conn, BTC_SYMBOL, 2)
        if len(btc_candles) < 1:
            logging.warning("BTC data missing for %s", BTC_SYMBOL)
            return

        btc = btc_candles[-1]
        btc_info = btc_gate(btc)
        if not btc_info["ok"]:
            logging.info(
                "BTC_GATE_OFF %s | dump=%.4f vq=%.2f buy_ratio=%.3f | %s",
                BTC_SYMBOL,
                btc_info["btc_dump_pct"],
                btc_info["btc_v_quote"],
                btc_info["btc_buy_ratio"],
                ",".join(btc_info["reasons"]) if btc_info["reasons"] else "-"
            )
            return

        # 2) Symbol universe
        symbols = await conn.fetch(LIST_SYMBOLS_SQL, BTC_SYMBOL, QUOTE_CCY, MAX_SYMBOLS_PER_RUN)
        if not symbols:
            logging.warning("No symbols found in %s", CANDLES_TABLE)
            return

        created = 0
        checked = 0

        for row in symbols:
            symbol = str(row["symbol"]).upper()
            checked += 1

            # safety gates
            if REQUIRE_NO_OPEN_POSITION and await has_open_position(conn, symbol):
                continue

            if REQUIRE_NO_PENDING_INTENT and await has_pending_intent(conn, symbol):
                continue

            candles = await fetch_candles(conn, symbol, max(LOOKBACK_BARS, SUPPORT_LOOKBACK_BARS + 1) + 2)
            if not candles:
                continue

            # Need last candle to be aligned with BTC last candle
            if candles[-1].ts != btc.ts:
                logging.debug(
                    "TS_MISMATCH %s | symbol_ts=%s btc_ts=%s",
                    symbol, candles[-1].ts.isoformat(), btc.ts.isoformat()
                )
                continue

            sig = local_short_setup(symbol, candles, btc_info["btc_dump_pct"])

            if (checked % DEBUG_EVERY) == 0:
                logging.info(
                    "SHORT_DEBUG %s | ts=%s | ok=%s | break=%.4f range=%.4f red=%.4f vol_mult=%.2f buy_ratio=%.3f rel_weak=%.4f",
                    symbol,
                    candles[-1].ts.isoformat(),
                    sig.get("ok"),
                    sig.get("breakdown_pct", 0.0),
                    sig.get("range_pct", 0.0),
                    sig.get("red_body_pct", 0.0),
                    sig.get("vol_mult", 0.0),
                    sig.get("buy_ratio", 0.0),
                    sig.get("relative_weakness", 0.0),
                )

            if not sig["ok"]:
                continue

            limit_price = build_limit_price(sig["last_close"])
            support_price = sig["support_low"]

            meta = {
                "model": "SHORT_BREAKDOWN_V1",
                "btc_symbol": BTC_SYMBOL,
                "btc_ts": btc.ts.isoformat(),
                "btc_dump_pct": btc_info["btc_dump_pct"],
                "btc_v_quote": btc_info["btc_v_quote"],
                "btc_buy_ratio": btc_info["btc_buy_ratio"],
                "range_pct": sig["range_pct"],
                "breakdown_pct": sig["breakdown_pct"],
                "red_body_pct": sig["red_body_pct"],
                "v_quote": sig["v_quote"],
                "avg_v_quote": sig["avg_v_quote"],
                "vol_mult": sig["vol_mult"],
                "buy_ratio": sig["buy_ratio"],
                "support_low": sig["support_low"],
                "relative_weakness": sig["relative_weakness"],
                "conditions": sig["conditions"],
            }

            result = await conn.execute(
                INSERT_INTENT_SQL,
                symbol,
                sig["ts"],
                SIGNAL_SOURCE,
                "SELL",
                QUOTE_AMOUNT,
                limit_price,
                support_price,
                json.dumps(meta),
                ENTRY_MODE,
            )

            # asyncpg returns strings like "INSERT 0 1" or "INSERT 0 0"
            inserted = result.endswith("1")
            if inserted:
                created += 1
                logging.warning(
                    "SHORT_INTENT_CREATED %s | ts=%s | mode=%s side=SELL | quote=%.2f | limit=%.8f support=%.8f | break=%.4f vol_mult=%.2f buy_ratio=%.3f",
                    symbol,
                    sig["ts"].isoformat(),
                    ENTRY_MODE,
                    QUOTE_AMOUNT,
                    limit_price,
                    support_price,
                    sig["breakdown_pct"],
                    sig["vol_mult"],
                    sig["buy_ratio"],
                )

        logging.info(
            "SHORT_RUN_OK #%d | btc_ts=%s | checked=%d | created=%d",
            run_no,
            btc.ts.isoformat(),
            checked,
            created,
        )


# ==========================================================
# Loop
# ==========================================================
async def run_loop(pool: asyncpg.Pool) -> None:
    run_no = 0
    while True:
        try:
            run_no += 1
            await evaluate_once(pool, run_no)
        except Exception as e:
            logging.exception("SHORT_SIGNAL loop error: %s", e)

        await asyncio.sleep(POLL_EVERY_SEC)


# ==========================================================
# main
# ==========================================================
async def main() -> None:
    logging.info(
        "Starting short_signal_service | candles=%s | intents=%s | positions=%s | btc=%s | entry_mode=%s | quote=%.2f",
        CANDLES_TABLE,
        INTENTS_TABLE,
        POSITIONS_TABLE,
        BTC_SYMBOL,
        ENTRY_MODE,
        QUOTE_AMOUNT,
    )

    pool = await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        min_size=1,
        max_size=8,
    )
    logging.info("DB pool created | host=%s db=%s user=%s", DB_HOST, DB_NAME, DB_USER)

    await init_db(pool)
    await run_loop(pool)


if __name__ == "__main__":
    asyncio.run(main())
