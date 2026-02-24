# executor_service.py
# ------------------------------------------------------------
# Reads trade_intents (source=ACCUM) from Timescale/Postgres
# and places REAL Binance LIMIT BUY orders for USDC pairs.
#
# FIXES:
#   - Rounds quantity to LOT_SIZE stepSize (floors) -> fixes -1111 precision
#   - Rounds price to PRICE_FILTER tickSize (floors)
#   - Signature generation is built from the EXACT query string we send
#     (we send signed requests as url?query_string, not via params=)
#     -> fixes -1022 signature invalid caused by param re-ordering/encoding
#   - Checks minQty + minNotional
#   - Marks intent ERROR on any failure to avoid "pending intent" deadlocks
#
# Notes:
#   - trade_intents.quote_amount is treated as QUOTE currency amount (USDC).
#   - This module does NOT place OCO/SL/TP on Binance; it writes SL/TP to DB only
#     for your exit_service logic.
# ------------------------------------------------------------

import os
import json
import time
import hmac
import hashlib
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import aiohttp
import asyncpg
from decimal import Decimal, ROUND_DOWN


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

# Binance
BINANCE_BASE_URL = env_str("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/")
BINANCE_API_KEY = env_str("BINANCE_API_KEY", "")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET", "")

# Execution loop
POLL_EVERY_SEC = env_int("EXEC_POLL_EVERY_SEC", 2)
MAX_BATCH = env_int("EXEC_MAX_BATCH", 25)

# Quote currency (we're using USDC now)
QUOTE_CCY = env_str("QUOTE_CCY", "USDC").upper()

# exchangeInfo cache
EXINFO_TTL_SEC = env_int("EXINFO_TTL_SEC", 900)  # 15 minutes

# clientOrderId prefix
CLIENT_OID_PREFIX = env_str("CLIENT_OID_PREFIX", "ACC")

# DB defaults for positions_open (used by exit_service)
EXEC_DEFAULT_SL_PCT = env_float("EXEC_DEFAULT_SL_PCT", 0.003)  # 0.3%
EXEC_DEFAULT_TP_PCT = env_float("EXEC_DEFAULT_TP_PCT", 0.003)  # 0.3%

# Binance timing
BINANCE_RECV_WINDOW = env_int("BINANCE_RECV_WINDOW", 5000)

# Debug signature inputs (set true temporarily if needed)
DEBUG_SIGN = env_str("DEBUG_SIGN", "false").lower() in ("1", "true", "yes", "y", "on")


# ==========================================================
# SQL
# ==========================================================
PICK_INTENTS_SQL = """
WITH picked AS (
  SELECT id
  FROM trade_intents
  WHERE status='NEW'
    AND side='BUY'
    AND source='ACCUM'
  ORDER BY created_at ASC
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE trade_intents ti
SET status='SENT', updated_at=NOW()
FROM picked
WHERE ti.id = picked.id
RETURNING ti.*;
"""

MARK_INTENT_ERROR_SQL = """
UPDATE trade_intents
SET status='ERROR', error=$2, updated_at=NOW()
WHERE id=$1
"""

MARK_INTENT_SENT_SQL = """
UPDATE trade_intents
SET order_id=$2, client_order_id=$3, updated_at=NOW()
WHERE id=$1
"""

HAS_OPEN_POSITION_SQL = """
SELECT 1 FROM positions_open WHERE symbol=$1 AND status='OPEN' LIMIT 1
"""

UPSERT_POSITION_OPEN_SQL = """
INSERT INTO positions_open(symbol, entry_time, entry_price, sl, tp, status, qty, entry_order_id, opened_at, updated_at)
VALUES($1,$2,$3,$4,$5,'OPEN',$6,$7,NOW(),NOW())
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
# Decimal helpers
# ==========================================================
def _d(x: Any) -> Decimal:
    return Decimal(str(x))

def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

def decimal_places(step: Decimal) -> int:
    s = format(step.normalize(), "f")
    if "." not in s:
        return 0
    return len(s.split(".")[1].rstrip("0"))

def fmt_decimal(value: Decimal, step: Decimal) -> str:
    dp = decimal_places(step)
    q = value.quantize(Decimal(10) ** -dp, rounding=ROUND_DOWN)
    return format(q, "f")


# ==========================================================
# Binance signing (robust)
# ==========================================================
def build_query_string(params: Dict[str, Any]) -> str:
    """
    Build query string in a deterministic order and with EXACT string values.
    Binance signature verification is over the exact query string.
    """
    # Deterministic: sort keys, and stringify values as-is
    items = []
    for k in sorted(params.keys()):
        v = params[k]
        items.append(f"{k}={v}")
    return "&".join(items)

def sign_query(query_string: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()

async def binance_public(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    params: Dict[str, Any],
) -> Tuple[int, str]:
    url = f"{BINANCE_BASE_URL}{path}"
    async with session.request(method.upper(), url, params=params) as resp:
        return resp.status, await resp.text()

async def binance_signed(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    params: Dict[str, Any],
) -> Tuple[int, str]:
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        return 401, "Missing BINANCE_API_KEY/SECRET"

    url = f"{BINANCE_BASE_URL}{path}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    qp = dict(params)
    qp["timestamp"] = int(time.time() * 1000)
    qp["recvWindow"] = BINANCE_RECV_WINDOW

    qs = build_query_string(qp)
    sig = sign_query(qs, BINANCE_API_SECRET)
    full_qs = f"{qs}&signature={sig}"

    if DEBUG_SIGN:
        # DO NOT log secrets. Logging query string is okay (it doesn't contain secret).
        logging.info("SIGN_DEBUG | %s %s?%s", method.upper(), path, full_qs)

    # IMPORTANT: send signed query as raw URL string to avoid any re-encoding/re-ordering
    async with session.request(method.upper(), url + "?" + full_qs, headers=headers) as resp:
        return resp.status, await resp.text()


# ==========================================================
# exchangeInfo cache
# ==========================================================
@dataclass
class SymbolFilters:
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal

_EX_CACHE: Dict[str, Tuple[float, SymbolFilters]] = {}

def _parse_filters(symbol: str, exinfo_json: Dict[str, Any]) -> SymbolFilters:
    sym = None
    for s in exinfo_json.get("symbols", []):
        if (s.get("symbol") or "").upper() == symbol.upper():
            sym = s
            break
    if not sym:
        raise RuntimeError(f"exchangeInfo missing symbol={symbol}")

    tick = Decimal("0")
    step = Decimal("0")
    min_qty = Decimal("0")
    min_notional = Decimal("0")

    for f in sym.get("filters", []):
        ft = f.get("filterType")
        if ft == "PRICE_FILTER":
            tick = _d(f.get("tickSize", "0"))
        elif ft == "LOT_SIZE":
            step = _d(f.get("stepSize", "0"))
            min_qty = _d(f.get("minQty", "0"))
        elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
            mn = f.get("minNotional")
            if mn is not None:
                min_notional = _d(mn)

    if tick <= 0 or step <= 0:
        raise RuntimeError(f"Bad filters for {symbol}: tick={tick} step={step}")

    return SymbolFilters(tick_size=tick, step_size=step, min_qty=min_qty, min_notional=min_notional)

async def get_symbol_filters(session: aiohttp.ClientSession, symbol: str) -> SymbolFilters:
    now = time.time()
    cached = _EX_CACHE.get(symbol)
    if cached and (now - cached[0]) < EXINFO_TTL_SEC:
        return cached[1]

    status, text = await binance_public(session, "GET", "/api/v3/exchangeInfo", {"symbol": symbol})
    if status != 200:
        raise RuntimeError(f"Binance exchangeInfo failed {status}: {text}")

    exj = json.loads(text)
    filt = _parse_filters(symbol, exj)
    _EX_CACHE[symbol] = (now, filt)
    return filt


# ==========================================================
# Order placement
# ==========================================================
def build_client_order_id(intent_id: int, symbol: str) -> str:
    ts = int(time.time())
    # keep reasonably short
    return f"{CLIENT_OID_PREFIX}-{symbol}-{intent_id}-{ts}"

def compute_qty_price(quote_amount: Decimal, limit_price: Decimal, f: SymbolFilters) -> Tuple[str, str, Decimal, Decimal]:
    # Floor price to tickSize
    px = floor_to_step(limit_price, f.tick_size)
    if px <= 0:
        raise ValueError("price<=0 after tick rounding")

    # qty = quote / price, floor to stepSize
    raw_qty = quote_amount / px
    qty = floor_to_step(raw_qty, f.step_size)

    if f.min_qty > 0 and qty < f.min_qty:
        raise ValueError(f"qty<{f.min_qty} after rounding (qty={qty})")

    if f.min_notional > 0:
        notional = qty * px
        if notional < f.min_notional:
            raise ValueError(f"notional<{f.min_notional} (notional={notional}, qty={qty}, px={px})")

    qty_s = fmt_decimal(qty, f.step_size)
    px_s = fmt_decimal(px, f.tick_size)
    return qty_s, px_s, qty, px

async def place_limit_buy(
    session: aiohttp.ClientSession,
    intent_id: int,
    symbol: str,
    quote_amount: float,
    limit_price: float,
) -> Tuple[int, str]:
    f = await get_symbol_filters(session, symbol)

    qa = _d(quote_amount)
    lp = _d(limit_price)

    qty_s, px_s, _, _ = compute_qty_price(qa, lp, f)
    client_oid = build_client_order_id(intent_id, symbol)

    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": qty_s,
        "price": px_s,
        "newClientOrderId": client_oid,
    }

    status, text = await binance_signed(session, "POST", "/api/v3/order", params)
    if status != 200:
        raise RuntimeError(f"Binance REST POST /api/v3/order failed {status}: {text}")

    resp = json.loads(text)
    order_id = int(resp.get("orderId"))
    return order_id, client_oid


# ==========================================================
# Processing
# ==========================================================
async def process_intent(conn: asyncpg.Connection, session: aiohttp.ClientSession, intent: asyncpg.Record) -> None:
    intent_id = int(intent["id"])
    symbol = str(intent["symbol"]).upper()
    quote_amount = float(intent["quote_amount"])
    limit_price = float(intent["limit_price"])

    if not symbol.endswith(QUOTE_CCY):
        raise RuntimeError(f"Symbol {symbol} does not end with {QUOTE_CCY} (QUOTE_CCY={QUOTE_CCY})")

    has_pos = await conn.fetchrow(HAS_OPEN_POSITION_SQL, symbol)
    if has_pos:
        await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, "OPEN_POSITION_EXISTS")
        logging.warning("INTENT_SKIP id=%d %s | open position exists", intent_id, symbol)
        return

    # Place order on Binance
    order_id, client_oid = await place_limit_buy(session, intent_id, symbol, quote_amount, limit_price)
    await conn.execute(MARK_INTENT_SENT_SQL, intent_id, order_id, client_oid)

    # Write OPEN position (logical) for exit_service
    entry_time = datetime.now(timezone.utc)
    entry_price = float(limit_price)
    sl = entry_price * (1.0 - EXEC_DEFAULT_SL_PCT)
    tp = entry_price * (1.0 + EXEC_DEFAULT_TP_PCT)

    # Store rounded qty for DB
    f = await get_symbol_filters(session, symbol)
    qty_s, px_s, qty_d, px_d = compute_qty_price(_d(quote_amount), _d(limit_price), f)
    qty = float(qty_s)

    await conn.execute(
        UPSERT_POSITION_OPEN_SQL,
        symbol, entry_time, entry_price, sl, tp, qty, order_id
    )

    logging.warning(
        "INTENT_SENT id=%d %s | quote=%.2f %s | limit=%s qty=%s | order_id=%d client_oid=%s",
        intent_id, symbol, quote_amount, QUOTE_CCY, px_s, qty_s, order_id, client_oid
    )


# ==========================================================
# Worker loop
# ==========================================================
async def worker_loop(pool: asyncpg.Pool) -> None:
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        intents = await conn.fetch(PICK_INTENTS_SQL, MAX_BATCH)

                        if not intents:
                            await asyncio.sleep(POLL_EVERY_SEC)
                            continue

                        for intent in intents:
                            try:
                                await process_intent(conn, session, intent)
                            except Exception as e:
                                err = str(e)
                                await conn.execute(MARK_INTENT_ERROR_SQL, int(intent["id"]), err)
                                logging.error("INTENT_ERROR id=%s %s | %s", intent["id"], intent["symbol"], err)

            except Exception as e:
                logging.exception("Worker loop error: %s | sleeping %ds", e, POLL_EVERY_SEC)
                await asyncio.sleep(POLL_EVERY_SEC)


# ==========================================================
# main
# ==========================================================
async def main() -> None:
    logging.info("Starting executor_service | quote=%s | poll=%ds | batch=%d", QUOTE_CCY, POLL_EVERY_SEC, MAX_BATCH)
    pool = await asyncpg.create_pool(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        min_size=1, max_size=5
    )
    logging.info("DB pool created | host=%s db=%s user=%s", DB_HOST, DB_NAME, DB_USER)
    await worker_loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
