# order_executor_service.py
# ------------------------------------------------------------
# Binance Spot Order Executor (WS API version, no listenKey)
# ------------------------------------------------------------

import os
import json
import hmac
import time
import math
import asyncio
import logging
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import aiohttp
import asyncpg
import websockets

# ==========================================================
# ENV
# ==========================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return float(v) if v else default

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name, "").strip()
    return v if v else default

LOG_LEVEL = env_str("LOG_LEVEL", "INFO").upper()

DB_HOST = env_str("DB_HOST", "db")
DB_NAME = env_str("DB_NAME", "pumpdb")
DB_USER = env_str("DB_USER", "pumpuser")
DB_PASSWORD = env_str("DB_PASSWORD", "")

BINANCE_API_KEY = env_str("BINANCE_API_KEY")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET")

BINANCE_REST = env_str("BINANCE_REST", "https://api.binance.com").rstrip("/")
BINANCE_WS_API = env_str("BINANCE_WS_API", "wss://ws-api.binance.com:443/ws-api/v3").rstrip("/")

POLL_EVERY_SEC = env_int("EXEC_POLL_EVERY_SEC", 2)
MAX_OPEN_ORDERS_TOTAL = env_int("MAX_OPEN_ORDERS_TOTAL", 5)
MAX_OPEN_ORDERS_PER_SYMBOL = env_int("MAX_OPEN_ORDERS_PER_SYMBOL", 1)

DEFAULT_SL_PCT = env_float("SPOT_DEFAULT_SL_PCT", 0.01)
DEFAULT_TP_PCT = env_float("SPOT_DEFAULT_TP_PCT", 0.01)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# ==========================================================
# SQL
# ==========================================================

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS trade_intents (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  side TEXT NOT NULL DEFAULT 'BUY',
  quote_amount DOUBLE PRECISION NOT NULL,
  limit_price DOUBLE PRECISION NOT NULL,
  status TEXT NOT NULL DEFAULT 'NEW',
  order_id BIGINT,
  client_order_id TEXT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS spot_orders (
  symbol TEXT,
  order_id BIGINT,
  side TEXT,
  status TEXT,
  executed_qty DOUBLE PRECISION,
  cumm_quote_qty DOUBLE PRECISION,
  PRIMARY KEY(symbol, order_id)
);

CREATE TABLE IF NOT EXISTS spot_fills (
  symbol TEXT,
  order_id BIGINT,
  trade_id BIGINT,
  price DOUBLE PRECISION,
  qty DOUBLE PRECISION,
  PRIMARY KEY(symbol, trade_id)
);

ALTER TABLE positions_open
ADD COLUMN IF NOT EXISTS qty DOUBLE PRECISION;

ALTER TABLE positions_open
ADD COLUMN IF NOT EXISTS entry_order_id BIGINT;
"""

# ==========================================================
# REST helpers
# ==========================================================

def _ts_ms() -> int:
    return int(time.time() * 1000)

def _sign(secret: str, query: str) -> str:
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()

async def _rest(session, method, path, params=None, signed=False):
    if params is None:
        params = {}

    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    if signed:
        params["timestamp"] = _ts_ms()
        query = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
        params["signature"] = _sign(BINANCE_API_SECRET, query)

    url = f"{BINANCE_REST}{path}"
    async with session.request(method, url, params=params, headers=headers) as resp:
        txt = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"REST error {resp.status}: {txt}")
        return json.loads(txt)

# ==========================================================
# ORDER PLACEMENT
# ==========================================================

async def place_limit_buy(session, symbol, quote_amount, limit_price, client_id):
    qty = quote_amount / limit_price

    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": f"{qty:.6f}",
        "price": f"{limit_price:.6f}",
        "newClientOrderId": client_id,
    }

    res = await _rest(session, "POST", "/api/v3/order", params, signed=True)
    return int(res["orderId"])

# ==========================================================
# USER STREAM (WS API)
# ==========================================================

async def ws_subscribe(ws):
    ts = _ts_ms()
    params = {"apiKey": BINANCE_API_KEY, "timestamp": ts}
    sig = _sign(BINANCE_API_SECRET, "&".join([f"{k}={params[k]}" for k in sorted(params.keys())]))

    req = {
        "id": "sub_1",
        "method": "userDataStream.subscribe.signature",
        "params": {
            "apiKey": BINANCE_API_KEY,
            "timestamp": ts,
            "signature": sig
        }
    }

    await ws.send(json.dumps(req))
    resp = json.loads(await ws.recv())
    if resp.get("status") != 200:
        raise RuntimeError(f"Subscribe failed: {resp}")
    return resp["result"]["subscriptionId"]

async def handle_execution(pool, ev):
    symbol = ev["s"]
    order_id = int(ev["i"])
    status = ev["X"]
    side = ev["S"]

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO spot_orders(symbol, order_id, side, status, executed_qty, cumm_quote_qty) "
            "VALUES($1,$2,$3,$4,$5,$6) "
            "ON CONFLICT(symbol,order_id) DO UPDATE SET status=$4, executed_qty=$5, cumm_quote_qty=$6",
            symbol,
            order_id,
            side,
            status,
            float(ev["z"]),
            float(ev["Z"])
        )

async def user_stream_loop(pool):
    while True:
        try:
            async with websockets.connect(BINANCE_WS_API, ping_interval=20) as ws:
                sub_id = await ws_subscribe(ws)
                logging.info("Subscribed user stream id=%s", sub_id)

                async for msg in ws:
                    payload = json.loads(msg)
                    ev = payload.get("event")
                    if not ev:
                        continue

                    if ev.get("e") == "executionReport":
                        logging.info("EXEC REPORT %s", ev)
                        await handle_execution(pool, ev)

        except Exception as e:
            logging.exception("WS error: %s", e)
            await asyncio.sleep(3)

# ==========================================================
# INTENT POLLER
# ==========================================================

async def poll_and_place(pool, session):
    while True:
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT id,symbol,quote_amount,limit_price FROM trade_intents WHERE status='NEW' LIMIT 10"
                )

            for r in rows:
                client_id = f"AC_{r['symbol']}_{int(time.time())}"
                try:
                    order_id = await place_limit_buy(
                        session, r["symbol"], r["quote_amount"], r["limit_price"], client_id
                    )
                except Exception as e:
                    logging.error("Order failed: %s", e)
                    continue

                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE trade_intents SET status='SENT', order_id=$2 WHERE id=$1",
                        r["id"], order_id
                    )

            await asyncio.sleep(POLL_EVERY_SEC)

        except Exception as e:
            logging.exception("Poll error: %s", e)
            await asyncio.sleep(2)

# ==========================================================
# MAIN
# ==========================================================

async def main():
    pool = await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    async with pool.acquire() as conn:
        await conn.execute(CREATE_SQL)

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            user_stream_loop(pool),
            poll_and_place(pool, session),
        )

if __name__ == "__main__":
    asyncio.run(main())
