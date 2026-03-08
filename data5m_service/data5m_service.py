#!/usr/bin/env python3
# data5m_service.py
# ------------------------------------------------------------
# Single source of market data ingestion (5m klines by default).
# - Connects to Binance WS kline stream(s)
# - On CLOSED kline, upserts candle into Timescale/Postgres table
#
# This service is intended to be the ONLY one that connects to Binance WS.
# Other services (models) read candles from DB.
#
# Table schema:
#   symbol TEXT NOT NULL
#   ts TIMESTAMPTZ NOT NULL          -- candle CLOSE_TIME
#   o,h,l,c DOUBLE PRECISION NOT NULL
#   v_base DOUBLE PRECISION NOT NULL
#   v_quote DOUBLE PRECISION NOT NULL
#   trades_count INTEGER
#   taker_buy_base DOUBLE PRECISION NOT NULL DEFAULT 0
#   taker_buy_quote DOUBLE PRECISION NOT NULL DEFAULT 0
#   PRIMARY KEY(symbol, ts)
#
# Notes:
# - Stores only CLOSED candles (k["x"] == True)
# - Uses Binance kline fields:
#     o,h,l,c
#     v = base asset volume
#     q = quote asset volume
#     n = trades count
#     V = taker buy base asset volume
#     Q = taker buy quote asset volume
# ------------------------------------------------------------

import os
import json
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

import asyncpg
import websockets


# ==========================================================
# ENV helpers
# ==========================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default


def parse_symbols() -> List[str]:
    raw = os.getenv("SYMBOLS", "").strip()
    if not raw:
        raise RuntimeError("SYMBOLS is empty")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def tf_to_minutes(tf: str) -> int:
    tf = tf.lower().strip()
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    raise ValueError(f"Unsupported TF={tf}")


# ==========================================================
# CONFIG
# ==========================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TF = os.getenv("TF", "5m").strip()
SYMBOLS = parse_symbols()

TF_MIN = tf_to_minutes(TF)
if TF_MIN <= 0 or (60 % TF_MIN) != 0:
    raise RuntimeError(f"TF must divide an hour cleanly. Got TF={TF} ({TF_MIN}min)")

HEARTBEAT_EVERY = env_int("HEARTBEAT_EVERY", 200)
DEBUG_EVERY = env_int("DEBUG_EVERY", 20)
WS_SYMBOLS_PER_CONN = env_int("WS_SYMBOLS_PER_CONN", 40)

# DB
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

CANDLES_TABLE = os.getenv("CANDLES_TABLE", "public.candles_5m").strip()


# ==========================================================
# LOGGING
# ==========================================================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


# ==========================================================
# SQL
# ==========================================================
CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {CANDLES_TABLE} (
  symbol TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  o DOUBLE PRECISION NOT NULL,
  h DOUBLE PRECISION NOT NULL,
  l DOUBLE PRECISION NOT NULL,
  c DOUBLE PRECISION NOT NULL,
  v_base DOUBLE PRECISION NOT NULL,
  v_quote DOUBLE PRECISION NOT NULL,
  trades_count INTEGER,
  taker_buy_base DOUBLE PRECISION NOT NULL DEFAULT 0,
  taker_buy_quote DOUBLE PRECISION NOT NULL DEFAULT 0,
  PRIMARY KEY(symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_candles_5m_symbol_ts
  ON {CANDLES_TABLE}(symbol, ts DESC);
"""

ALTER_SQL = f"""
ALTER TABLE {CANDLES_TABLE}
ADD COLUMN IF NOT EXISTS taker_buy_base DOUBLE PRECISION NOT NULL DEFAULT 0;

ALTER TABLE {CANDLES_TABLE}
ADD COLUMN IF NOT EXISTS taker_buy_quote DOUBLE PRECISION NOT NULL DEFAULT 0;
"""

INSERT_CANDLE_SQL = f"""
INSERT INTO {CANDLES_TABLE}(
  symbol, ts,
  o, h, l, c,
  v_base, v_quote,
  trades_count,
  taker_buy_base, taker_buy_quote
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT(symbol, ts) DO UPDATE SET
  o=EXCLUDED.o,
  h=EXCLUDED.h,
  l=EXCLUDED.l,
  c=EXCLUDED.c,
  v_base=EXCLUDED.v_base,
  v_quote=EXCLUDED.v_quote,
  trades_count=EXCLUDED.trades_count,
  taker_buy_base=EXCLUDED.taker_buy_base,
  taker_buy_quote=EXCLUDED.taker_buy_quote
"""


# ==========================================================
# WS helpers
# ==========================================================
def ws_url(symbols: List[str], tf: str) -> str:
    streams = "/".join([f"{s.lower()}@kline_{tf}" for s in symbols])
    return f"wss://stream.binance.com:9443/stream?streams={streams}"


def chunk_symbols(symbols: List[str], chunk_size: int) -> List[List[str]]:
    if chunk_size <= 0:
        raise ValueError("WS_SYMBOLS_PER_CONN must be > 0")
    return [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]


# ==========================================================
# Runtime counters
# ==========================================================
CANDLE_COUNTER = 0
LAST_TS: Optional[datetime] = None
DB_LAT_MS_SUM = 0.0
DB_LAT_MS_N = 0


# ==========================================================
# DB init
# ==========================================================
async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREATE_SQL)
        await conn.execute(ALTER_SQL)

    logging.info(
        "DB init OK | table=%s | ensured columns: taker_buy_base, taker_buy_quote",
        CANDLES_TABLE
    )


# ==========================================================
# Per-candle handler
# ==========================================================
async def on_closed_kline(pool: asyncpg.Pool, symbol: str, k: dict) -> None:
    global CANDLE_COUNTER, LAST_TS, DB_LAT_MS_SUM, DB_LAT_MS_N

    # Binance kline fields:
    # t=open time (ms)
    # T=close time (ms)
    # o,h,l,c
    # v=base volume
    # q=quote volume
    # n=trade count
    # V=taker buy base volume
    # Q=taker buy quote volume
    # x=is final candle
    close_time = datetime.fromtimestamp(int(k["T"]) / 1000.0, tz=timezone.utc)

    o = float(k["o"])
    h = float(k["h"])
    l = float(k["l"])
    c = float(k["c"])

    v_base = float(k["v"])
    v_quote = float(k["q"])
    trades_count = int(k["n"])

    taker_buy_base = float(k.get("V", 0.0))
    taker_buy_quote = float(k.get("Q", 0.0))

    t0 = time.perf_counter()
    async with pool.acquire() as conn:
        await conn.execute(
            INSERT_CANDLE_SQL,
            symbol,
            close_time,
            o, h, l, c,
            v_base, v_quote,
            trades_count,
            taker_buy_base, taker_buy_quote
        )
    db_ms = (time.perf_counter() - t0) * 1000.0

    DB_LAT_MS_SUM += db_ms
    DB_LAT_MS_N += 1
    CANDLE_COUNTER += 1
    LAST_TS = close_time

    if CANDLE_COUNTER % DEBUG_EVERY == 0:
        sell_quote_est = max(0.0, v_quote - taker_buy_quote)
        buy_ratio = (taker_buy_quote / v_quote) if v_quote > 0 else 0.0

        logging.info(
            "DATA_DEBUG %s | ts=%s | o=%.6f h=%.6f l=%.6f c=%.6f | "
            "vq=%.2f trades=%d | taker_buy_q=%.2f sell_q_est=%.2f buy_ratio=%.3f | db=%.1fms",
            symbol,
            close_time.isoformat(),
            o, h, l, c,
            v_quote,
            trades_count,
            taker_buy_quote,
            sell_quote_est,
            buy_ratio,
            db_ms
        )

    if CANDLE_COUNTER % HEARTBEAT_EVERY == 0:
        avg_db = (DB_LAT_MS_SUM / DB_LAT_MS_N) if DB_LAT_MS_N else 0.0
        logging.info(
            "HEARTBEAT | candles=%d | last_ts=%s | symbols=%d | tf=%s | table=%s | avg_db=%.1fms",
            CANDLE_COUNTER,
            (LAST_TS.isoformat() if LAST_TS else "n/a"),
            len(SYMBOLS),
            TF,
            CANDLES_TABLE,
            avg_db
        )


# ==========================================================
# WS loops
# ==========================================================
async def ws_loop(pool: asyncpg.Pool) -> None:
    groups = chunk_symbols(SYMBOLS, WS_SYMBOLS_PER_CONN)
    logging.info("WS groups=%d (chunk_size=%d)", len(groups), WS_SYMBOLS_PER_CONN)

    while True:
        try:
            tasks = []
            for idx, group in enumerate(groups, start=1):
                url = ws_url(group, TF)
                tasks.append(asyncio.create_task(ws_loop_one(pool, url, idx, len(group))))
            await asyncio.gather(*tasks)
        except Exception as e:
            logging.exception("Top-level WS supervisor error: %s | restarting in 3s", e)
            await asyncio.sleep(3)


async def ws_loop_one(pool: asyncpg.Pool, url: str, group_idx: int, n_symbols: int) -> None:
    while True:
        try:
            logging.info("Connecting WS group=%d symbols=%d | %s", group_idx, n_symbols, url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logging.info("Connected WS group=%d symbols=%d", group_idx, n_symbols)

                async for message in ws:
                    payload = json.loads(message)
                    k = payload.get("data", {}).get("k", {})

                    if not k:
                        continue

                    # Only closed candles
                    if not bool(k.get("x")):
                        continue

                    symbol = (k.get("s") or "").upper()
                    if not symbol:
                        continue

                    await on_closed_kline(pool, symbol, k)

        except Exception as e:
            logging.exception("WS group=%d error: %s | reconnecting in 3s", group_idx, e)
            await asyncio.sleep(3)


# ==========================================================
# main
# ==========================================================
async def main() -> None:
    logging.info(
        "Starting data5m_service | tf=%s (%dmin) | symbols=%d | table=%s",
        TF, TF_MIN, len(SYMBOLS), CANDLES_TABLE
    )

    pool = await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        min_size=1,
        max_size=5
    )
    logging.info("DB pool created | host=%s db=%s user=%s", DB_HOST, DB_NAME, DB_USER)

    await init_db(pool)
    await ws_loop(pool)


if __name__ == "__main__":
    asyncio.run(main())
