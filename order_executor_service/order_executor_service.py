# executor_service.py
# ------------------------------------------------------------
# Reads trade_intents (source=ACCUM) from Timescale/Postgres
# and places REAL Binance LIMIT BUY orders for USDC pairs.
#
# FIX:
#   - Rounds quantity to LOT_SIZE stepSize (floors) -> fixes -1111 precision
#   - Rounds price to PRICE_FILTER tickSize (floors)
#   - Checks minQty + minNotional
#
# NOTE:
#   - quote_amount in trade_intents is treated as QUOTE currency amount (USDC)
# ------------------------------------------------------------

import os
import json
import time
import hmac
import hashlib
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
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
BINANCE_BASE_URL = env_str("BINANCE_BASE_URL", "https://api.binance.com")
BINANCE_API_KEY = env_str("BINANCE_API_KEY", "")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET", "")
if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    logging.warning("BINANCE_API_KEY/SECRET missing. Orders will fail.")

# Trade intent processing
POLL_EVERY_SEC = env_int("EXEC_POLL_EVERY_SEC", 2)
LOCK_TIMEOUT_SEC = env_int("EXEC_LOCK_TIMEOUT_SEC", 10)
MAX_BATCH = env_int("EXEC_MAX_BATCH", 25)

# We are going USDC now (but keep it configurable)
QUOTE_CCY = env_str("QUOTE_CCY", "USDC").upper()

# exchangeInfo cache
EXINFO_TTL_SEC = env_int("EXINFO_TTL_SEC", 900)  # 15 min

# Optional: client order id prefix
CLIENT_OID_PREFIX = env_str("CLIENT_OID_PREFIX", "ACC")

# ==========================================================
# SQL
# ==========================================================
# Pick pending intents safely (Postgres)
PICK_INTENTS_SQL = """
WITH picked AS (
  SELECT id
  FROM trade_intents
  WHERE status='NEW'
    AND side='BUY'
    AND (source='ACCUM')
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

# positions_open: assumes table exists (you already have it)
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

HAS_OPEN_POSITION_SQL = """
SELECT 1 FROM positions_open WHERE symbol=$1 AND status='OPEN' LIMIT 1
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
    # step like 0.001 -> 3 dp, 1 -> 0 dp
    s = format(step.normalize(), "f")
    if "." not in s:
        return 0
    return len(s.split(".")[1].rstrip("0"))

def fmt_decimal(value: Decimal, step: Decimal) -> str:
    # Format with correct decimals (no scientific notation)
    dp = decimal_places(step)
    q = value.quantize(Decimal(10) ** -dp, rounding=ROUND_DOWN)
    # remove trailing zeros while keeping dp if needed
    return format(q, "f")

# ==========================================================
# Binance client (signed)
# ==========================================================
def _sign(query_string: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()

async def binance_request(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    params: Dict[str, Any],
    signed: bool = False,
) -> Tuple[int, str]:
    url = BINANCE_BASE_URL.rstrip("/") + path

    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY
    } if BINANCE_API_KEY else {}

    qp = dict(params)
    if signed:
        qp["timestamp"] = int(time.time() * 1000)
        # Optional recvWindow
        qp["recvWindow"] = env_int("BINANCE_RECV_WINDOW", 5000)
        # Build query string
        qs = "&".join([f"{k}={qp[k]}" for k in sorted(qp.keys())])
        qp["signature"] = _sign(qs, BINANCE_API_SECRET)

    async with session.request(method.upper(), url, params=qp, headers=headers) as resp:
        text = await resp.text()
        return resp.status, text

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
            # Binance has both variants depending on market
            mn = f.get("minNotional")
            if mn is not None:
                min_notional = _d(mn)

    if tick <= 0 or step <= 0:
        raise RuntimeError(f"Bad filters for {symbol}: tick={tick} step={step}")

    return SymbolFilters(
        tick_size=tick,
        step_size=step,
        min_qty=min_qty,
        min_notional=min_notional,
    )

async def get_symbol_filters(session: aiohttp.ClientSession, symbol: str) -> SymbolFilters:
    now = time.time()
    cached = _EX_CACHE.get(symbol)
    if cached and (now - cached[0]) < EXINFO_TTL_SEC:
        return cached[1]

    status, text = await binance_request(
        session, "GET", "/api/v3/exchangeInfo", {"symbol": symbol}, signed=False
    )
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
    # max 36 chars on Binance for newClientOrderId (usually ok)
    ts = int(time.time())
    return f"{CLIENT_OID_PREFIX}-{symbol}-{intent_id}-{ts}"

def compute_qty_and_price(quote_amount: Decimal, limit_price: Decimal, f: SymbolFilters) -> Tuple[str, str]:
    # Floor price to tick
    px = floor_to_step(limit_price, f.tick_size)
    if px <= 0:
        raise ValueError("price<=0 after tick rounding")

    # Compute qty = quote / price, floor to step
    raw_qty = quote_amount / px
    qty = floor_to_step(raw_qty, f.step_size)

    # minQty check
    if f.min_qty > 0 and qty < f.min_qty:
        raise ValueError(f"qty<{f.min_qty} after rounding (qty={qty})")

    # minNotional check (if provided)
    if f.min_notional > 0:
        notional = qty * px
        if notional < f.min_notional:
            raise ValueError(f"notional<{f.min_notional} (notional={notional}, qty={qty}, px={px})")

    qty_s = fmt_decimal(qty, f.step_size)
    px_s = fmt_decimal(px, f.tick_size)
    return qty_s, px_s

async def place_limit_buy(
    session: aiohttp.ClientSession,
    symbol: str,
    quote_amount: float,
    limit_price: float,
    intent_id: int,
) -> Tuple[int, str, str]:
    """
    Returns: (order_id, client_order_id, raw_response_text)
    """
    f = await get_symbol_filters(session, symbol)

    qa = _d(quote_amount)
    lp = _d(limit_price)

    qty_s, px_s = compute_qty_and_price(qa, lp, f)
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

    status, text = await binance_request(session, "POST", "/api/v3/order", params, signed=True)
    if status != 200:
        raise RuntimeError(f"Binance REST POST /api/v3/order failed {status}: {text}")

    resp = json.loads(text)
    order_id = int(resp.get("orderId"))
    return order_id, client_oid, text

# ==========================================================
# Main loop
# ==========================================================
async def process_intent(conn: asyncpg.Connection, session: aiohttp.ClientSession, intent: asyncpg.Record) -> None:
    intent_id = int(intent["id"])
    symbol = str(intent["symbol"]).upper()
    quote_amount = float(intent["quote_amount"])
    limit_price = float(intent["limit_price"])

    # Sanity: enforce USDC symbols (optional)
    if not symbol.endswith(QUOTE_CCY):
        raise RuntimeError(f"Symbol {symbol} does not end with {QUOTE_CCY} (QUOTE_CCY={QUOTE_CCY})")

    # If already in position, mark intent ERROR (or CANCELLED)
    has_pos = await conn.fetchrow(HAS_OPEN_POSITION_SQL, symbol)
    if has_pos:
        await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, "OPEN_POSITION_EXISTS")
        logging.warning("INTENT_SKIP id=%d %s | open position exists", intent_id, symbol)
        return

    # Place order
    order_id, client_oid, raw = await place_limit_buy(session, symbol, quote_amount, limit_price, intent_id)

    await conn.execute(MARK_INTENT_SENT_SQL, intent_id, order_id, client_oid)

    # Optional: write positions_open right away as OPEN (common pattern in your project)
    # Here SL/TP are placeholders (executor can compute proper later).
    entry_time = datetime.now(timezone.utc)
    entry_price = float(limit_price)
    sl = entry_price * (1.0 - env_float("EXEC_DEFAULT_SL_PCT", 0.003))  # default 0.3%
    tp = entry_price * (1.0 + env_float("EXEC_DEFAULT_TP_PCT", 0.003))  # default 0.3%

    # qty is derived from quote/price; compute again with rounding to keep consistent
    f = await get_symbol_filters(session, symbol)
    qty_s, px_s = compute_qty_and_price(_d(quote_amount), _d(limit_price), f)
    qty = float(qty_s)

    await conn.execute(
        UPSERT_POSITION_OPEN_SQL,
        symbol, entry_time, entry_price, sl, tp, qty, order_id
    )

    logging.warning(
        "INTENT_SENT id=%d %s | quote=%.2f %s | limit=%s qty=%s | order_id=%d client_oid=%s",
        intent_id, symbol, quote_amount, QUOTE_CCY, px_s, qty_s, order_id, client_oid
    )

async def worker_loop(pool: asyncpg.Pool) -> None:
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        intents = await conn.fetch(PICK_INTENTS_SQL, MAX_BATCH)

                        if not intents:
                            # nothing to do
                            await asyncio.sleep(POLL_EVERY_SEC)
                            continue

                        for intent in intents:
                            try:
                                await process_intent(conn, session, intent)
                            except Exception as e:
                                err = str(e)
                                await conn.execute(MARK_INTENT_ERROR_SQL, int(intent["id"]), err)
                                logging.error(
                                    "INTENT_ERROR id=%s %s | %s",
                                    intent["id"], intent["symbol"], err
                                )
            except Exception as e:
                logging.exception("Worker loop error: %s | sleeping %ds", e, POLL_EVERY_SEC)
                await asyncio.sleep(POLL_EVERY_SEC)

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
