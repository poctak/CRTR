# order_executor_service.py
# Binance Spot Order Executor (WS API compliant, 2026+)

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

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name, "").strip()
    return v if v else default

def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return float(v) if v else default


LOG_LEVEL = env_str("LOG_LEVEL", "INFO").upper()

DB_HOST = env_str("DB_HOST", "db")
DB_NAME = env_str("DB_NAME", "pumpdb")
DB_USER = env_str("DB_USER", "pumpuser")
DB_PASSWORD = env_str("DB_PASSWORD", "")

BINANCE_API_KEY = env_str("BINANCE_API_KEY")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET")

BINANCE_REST = env_str("BINANCE_REST", "https://api.binance.com").rstrip("/")
BINANCE_WS_API = env_str("BINANCE_WS_API", "wss://ws-api.binance.com:443/ws-api/v3")

POLL_EVERY_SEC = env_int("EXEC_POLL_EVERY_SEC", 2)

DEFAULT_SL_PCT = env_float("SPOT_DEFAULT_SL_PCT", 0.01)
DEFAULT_TP_PCT = env_float("SPOT_DEFAULT_TP_PCT", 0.01)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


# ==========================================================
# SQL
# ==========================================================

UPSERT_SPOT_ORDER_SQL = """
INSERT INTO spot_orders(
  symbol, side, type, time_in_force, client_order_id, order_id,
  price, orig_qty, executed_qty, cumm_quote_qty, status
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT(symbol, order_id) DO UPDATE SET
  executed_qty=EXCLUDED.executed_qty,
  cumm_quote_qty=EXCLUDED.cumm_quote_qty,
  status=EXCLUDED.status,
  updated_at=NOW()
"""

INSERT_FILL_SQL = """
INSERT INTO spot_fills(
  symbol, order_id, trade_id, side, price, qty, quote_qty,
  commission, commission_asset, trade_time
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT(symbol, trade_id) DO NOTHING
"""

UPSERT_POSITION_OPEN_SQL = """
INSERT INTO positions_open(
  symbol, entry_time, entry_price, sl, tp, status, opened_at, updated_at, qty, entry_order_id
)
VALUES($1,$2,$3,$4,$5,'OPEN',NOW(),NOW(),$6,$7)
ON CONFLICT(symbol) DO UPDATE SET
  entry_time=EXCLUDED.entry_time,
  entry_price=EXCLUDED.entry_price,
  sl=EXCLUDED.sl,
  tp=EXCLUDED.tp,
  status='OPEN',
  qty=EXCLUDED.qty,
  entry_order_id=EXCLUDED.entry_order_id,
  updated_at=NOW()
"""


# ==========================================================
# Helpers
# ==========================================================

def _ts_ms() -> int:
    return int(time.time() * 1000)

def _sign(secret: str, payload: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()

def _parse_time(ms: Any) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)


# ==========================================================
# REST
# ==========================================================

async def rest(session, method, path, params=None, signed=False):
    if params is None:
        params = {}

    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    if signed:
        params["timestamp"] = _ts_ms()
        query = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
        params["signature"] = _sign(BINANCE_API_SECRET, query)

    async with session.request(method, BINANCE_REST + path, params=params, headers=headers) as resp:
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

    res = await rest(session, "POST", "/api/v3/order", params, signed=True)
    return int(res["orderId"])


# ==========================================================
# USER STREAM
# ==========================================================

async def ws_subscribe(ws):
    ts = _ts_ms()
    params = {"apiKey": BINANCE_API_KEY, "timestamp": ts}
    payload = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
    signature = _sign(BINANCE_API_SECRET, payload)

    req = {
        "id": "sub_1",
        "method": "userDataStream.subscribe.signature",
        "params": {
            "apiKey": BINANCE_API_KEY,
            "timestamp": ts,
            "signature": signature,
        }
    }

    await ws.send(json.dumps(req))
    resp = json.loads(await ws.recv())
    if resp.get("status") != 200:
        raise RuntimeError(f"Subscribe failed: {resp}")

    return resp["result"]["subscriptionId"]


async def handle_execution(pool, ev):
    symbol = ev["s"]
    side = ev["S"]
    order_id = int(ev["i"])
    client_id = ev.get("c") or ""
    status = ev["X"]

    otype = ev.get("o") or "UNKNOWN"
    tif = ev.get("f")

    price = float(ev.get("p", 0))
    orig_qty = float(ev.get("q", 0))
    executed_qty = float(ev.get("z", 0))
    cumm_quote = float(ev.get("Z", 0))

    async with pool.acquire() as conn:
        await conn.execute(
            UPSERT_SPOT_ORDER_SQL,
            symbol, side, otype, tif, client_id, order_id,
            price, orig_qty, executed_qty, cumm_quote, status
        )

        if ev.get("x") == "TRADE":
            trade_id = int(ev.get("t", 0))
            last_px = float(ev.get("L", 0))
            last_qty = float(ev.get("l", 0))
            quote_qty = last_px * last_qty
            commission = float(ev.get("n", 0))
            commission_asset = ev.get("N") or ""
            trade_time = _parse_time(ev.get("T", _ts_ms()))

            await conn.execute(
                INSERT_FILL_SQL,
                symbol, order_id, trade_id, side,
                last_px, last_qty, quote_qty,
                commission, commission_asset, trade_time
            )

        if side == "BUY" and status == "FILLED" and executed_qty > 0:
            entry_price = cumm_quote / executed_qty
            entry_time = _parse_time(ev.get("T", _ts_ms()))
            sl = entry_price * (1 - DEFAULT_SL_PCT)
            tp = entry_price * (1 + DEFAULT_TP_PCT)

            await conn.execute(
                UPSERT_POSITION_OPEN_SQL,
                symbol, entry_time, entry_price,
                sl, tp, executed_qty, order_id
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
# MAIN
# ==========================================================

async def main():
    pool = await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            user_stream_loop(pool),
        )

if __name__ == "__main__":
    asyncio.run(main())
