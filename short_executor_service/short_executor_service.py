#!/usr/bin/env python3
# short_executor.py
# ------------------------------------------------------------
# Reads trade_intents_short from Postgres and opens REAL Binance Futures SHORTs.
#
# Supports:
#   - LIMIT SELL  (entry_mode='LIMIT')
#   - MARKET SELL (entry_mode='MARKET') using quantity
#
# Flow:
#   - pick NEW intents and mark SENT
#   - place futures order
#   - store order_id + client_order_id
#   - optional fast wait_for_fill
#   - reconcile SENT intents continuously
#   - once FILLED:
#       * write positions_open_short
#       * write trade_log_short ENTRY row
#
# Important:
#   - Uses Binance USDⓈ-M Futures API (/fapi/v1/*)
#   - Assumes one-way mode
#   - This service is isolated from long/spot model
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
from typing import Any, Dict, Tuple, Optional, List, Set

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


def parse_sources(csv: str) -> List[str]:
    if not csv:
        return []
    return [x.strip() for x in csv.split(",") if x.strip()]


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

# Futures API
BINANCE_FUTURES_BASE_URL = env_str("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com").rstrip("/")
BINANCE_API_KEY = env_str("BINANCE_API_KEY", "")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET", "")

# DB tables
INTENTS_TABLE = env_str("TRADE_INTENTS_SHORT_TABLE", "public.trade_intents_short")
POSITIONS_TABLE = env_str("POSITIONS_OPEN_SHORT_TABLE", "public.positions_open_short")
TRADE_LOG_TABLE = env_str("TRADE_LOG_SHORT_TABLE", "public.trade_log_short")

# Execution loop
POLL_EVERY_SEC = env_int("SHORT_EXEC_POLL_EVERY_SEC", 2)
MAX_BATCH = env_int("SHORT_EXEC_MAX_BATCH", 25)
EXEC_MAX_INFLIGHT = env_int("SHORT_EXEC_MAX_INFLIGHT", 20)

# Optional source filter
EXEC_SOURCES_CSV = env_str("SHORT_EXEC_SOURCES", "").strip()
EXEC_SOURCES: List[str] = parse_sources(EXEC_SOURCES_CSV)

# Quote currency filter
QUOTE_CCY = env_str("QUOTE_CCY", "USDT").upper()

# exchangeInfo cache
EXINFO_TTL_SEC = env_int("SHORT_EXINFO_TTL_SEC", 900)

# clientOrderId prefix
CLIENT_OID_PREFIX = env_str("SHORT_CLIENT_OID_PREFIX", "SHRT")

# Defaults stored in positions_open_short
SHORT_DEFAULT_SL_PCT = env_float("SHORT_DEFAULT_SL_PCT", 0.004)   # +0.4% above entry
SHORT_DEFAULT_TP_PCT = env_float("SHORT_DEFAULT_TP_PCT", 0.006)   # -0.6% below entry

# Binance timing
BINANCE_RECV_WINDOW = env_int("BINANCE_RECV_WINDOW", 5000)

# Fill polling
FILL_POLL_EVERY_SEC = env_float("SHORT_FILL_POLL_EVERY_SEC", 1.0)
FILL_WAIT_MAX_SEC = env_int("SHORT_FILL_WAIT_MAX_SEC", 30)

# Reconciler
RECONCILE_EVERY_SEC = env_int("SHORT_RECONCILE_EVERY_SEC", 5)
RECONCILE_BATCH = env_int("SHORT_RECONCILE_BATCH", 50)

# Cancel stale LIMIT orders after N seconds
EXEC_CANCEL_AFTER_SEC = env_int("SHORT_EXEC_CANCEL_AFTER_SEC", 3600)

# Leverage
FUTURES_LEVERAGE = env_int("SHORT_FUTURES_LEVERAGE", 1)
SET_LEVERAGE_ON_EACH_ORDER = env_str("SET_LEVERAGE_ON_EACH_ORDER", "true").lower() in ("1", "true", "yes", "y", "on")

DEBUG_SIGN = env_str("DEBUG_SIGN", "false").lower() in ("1", "true", "yes", "y", "on")


# ==========================================================
# SQL
# ==========================================================
PICK_NEW_INTENTS_SQL = f"""
WITH picked AS (
  SELECT id
  FROM {INTENTS_TABLE}
  WHERE status='NEW'
    AND side='SELL'
    AND (cardinality($2::text[]) = 0 OR source = ANY($2::text[]))
  ORDER BY created_at ASC
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE {INTENTS_TABLE} ti
SET status='SENT', updated_at=NOW()
FROM picked
WHERE ti.id = picked.id
RETURNING ti.*;
"""

PICK_SENT_INTENTS_SQL = f"""
WITH picked AS (
  SELECT id
  FROM {INTENTS_TABLE}
  WHERE status='SENT'
    AND side='SELL'
    AND order_id IS NOT NULL
    AND (cardinality($2::text[]) = 0 OR source = ANY($2::text[]))
  ORDER BY updated_at ASC
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
SELECT ti.*
FROM {INTENTS_TABLE} ti
JOIN picked p ON p.id = ti.id;
"""

MARK_INTENT_ERROR_SQL = f"""
UPDATE {INTENTS_TABLE}
SET status='ERROR', error=$2, updated_at=NOW()
WHERE id=$1
"""

MARK_INTENT_SENT_SQL = f"""
UPDATE {INTENTS_TABLE}
SET order_id=$2, client_order_id=$3
WHERE id=$1
"""

MARK_INTENT_FILLED_SQL = f"""
UPDATE {INTENTS_TABLE}
SET status='FILLED', updated_at=NOW()
WHERE id=$1
"""

HAS_OPEN_POSITION_SQL = f"""
SELECT 1
FROM {POSITIONS_TABLE}
WHERE symbol=$1 AND status='OPEN'
LIMIT 1
"""

UPSERT_POSITION_OPEN_SQL = f"""
INSERT INTO {POSITIONS_TABLE}(
    symbol, entry_time, entry_price, sl, tp, status, qty, entry_order_id, opened_at, updated_at
)
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

INSERT_TRADE_LOG_ENTRY_SQL = f"""
INSERT INTO {TRADE_LOG_TABLE}(
    symbol, entry_time, entry_price, reason,
    entry_source, entry_mode, intent_id, entry_order_id
)
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (intent_id, reason) DO UPDATE SET
    entry_order_id = COALESCE({TRADE_LOG_TABLE}.entry_order_id, EXCLUDED.entry_order_id),
    entry_time     = COALESCE({TRADE_LOG_TABLE}.entry_time,     EXCLUDED.entry_time),
    entry_price    = COALESCE({TRADE_LOG_TABLE}.entry_price,    EXCLUDED.entry_price),
    entry_source   = COALESCE({TRADE_LOG_TABLE}.entry_source,   EXCLUDED.entry_source),
    entry_mode     = COALESCE({TRADE_LOG_TABLE}.entry_mode,     EXCLUDED.entry_mode)
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
# Binance signing
# ==========================================================
def build_query_string(params: Dict[str, Any]) -> str:
    return "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])


def sign_query(query_string: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()


async def binance_public(session: aiohttp.ClientSession, method: str, path: str, params: Dict[str, Any]) -> Tuple[int, str]:
    url = f"{BINANCE_FUTURES_BASE_URL}{path}"
    async with session.request(method.upper(), url, params=params) as resp:
        return resp.status, await resp.text()


async def binance_signed(session: aiohttp.ClientSession, method: str, path: str, params: Dict[str, Any]) -> Tuple[int, str]:
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        return 401, "Missing BINANCE_API_KEY/SECRET"

    url = f"{BINANCE_FUTURES_BASE_URL}{path}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    qp = dict(params)
    qp["timestamp"] = int(time.time() * 1000)
    qp["recvWindow"] = BINANCE_RECV_WINDOW

    qs = build_query_string(qp)
    sig = sign_query(qs, BINANCE_API_SECRET)
    full_qs = f"{qs}&signature={sig}"

    if DEBUG_SIGN:
        logging.info("SIGN_DEBUG | %s %s?%s", method.upper(), path, full_qs)

    async with session.request(method.upper(), url + "?" + full_qs, headers=headers) as resp:
        return resp.status, await resp.text()


# ==========================================================
# Futures exchangeInfo cache
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
        raise RuntimeError(f"futures exchangeInfo missing symbol={symbol}")

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
            mn = f.get("notional") or f.get("minNotional")
            if mn is not None:
                min_notional = _d(mn)

    if tick <= 0 or step <= 0:
        raise RuntimeError(f"Bad futures filters for {symbol}: tick={tick} step={step}")

    return SymbolFilters(
        tick_size=tick,
        step_size=step,
        min_qty=min_qty,
        min_notional=min_notional
    )


async def get_symbol_filters(session: aiohttp.ClientSession, symbol: str) -> SymbolFilters:
    now = time.time()
    cached = _EX_CACHE.get(symbol)
    if cached and (now - cached[0]) < EXINFO_TTL_SEC:
        return cached[1]

    status, text = await binance_public(session, "GET", "/fapi/v1/exchangeInfo", {"symbol": symbol})
    if status != 200:
        raise RuntimeError(f"Binance futures exchangeInfo failed {status}: {text}")

    exj = json.loads(text)
    filt = _parse_filters(symbol, exj)
    _EX_CACHE[symbol] = (now, filt)
    return filt


# ==========================================================
# Futures helpers
# ==========================================================
def build_client_order_id(intent_id: int, symbol: str) -> str:
    return f"{CLIENT_OID_PREFIX}-{symbol}-{intent_id}-{int(time.time())}"


def compute_qty_from_quote(quote_amount: Decimal, ref_price: Decimal, f: SymbolFilters) -> str:
    if ref_price <= 0:
        raise ValueError("ref_price<=0")

    qty = floor_to_step(quote_amount / ref_price, f.step_size)

    if f.min_qty > 0 and qty < f.min_qty:
        raise ValueError(f"qty<{f.min_qty} after rounding (qty={qty})")

    if f.min_notional > 0:
        notional = qty * ref_price
        if notional < f.min_notional:
            raise ValueError(f"notional<{f.min_notional} (notional={notional}, qty={qty}, px={ref_price})")

    return fmt_decimal(qty, f.step_size)


def compute_limit_price(limit_price: Decimal, f: SymbolFilters) -> str:
    px = floor_to_step(limit_price, f.tick_size)
    if px <= 0:
        raise ValueError("price<=0 after tick rounding")
    return fmt_decimal(px, f.tick_size)


async def set_symbol_leverage(session: aiohttp.ClientSession, symbol: str, leverage: int) -> None:
    status, text = await binance_signed(
        session,
        "POST",
        "/fapi/v1/leverage",
        {"symbol": symbol, "leverage": leverage}
    )
    if status != 200:
        raise RuntimeError(f"Binance futures set leverage failed {status}: {text}")


async def place_limit_sell(
    session: aiohttp.ClientSession,
    intent_id: int,
    symbol: str,
    quote_amount: float,
    limit_price: float
) -> Tuple[int, str, str, str]:
    f = await get_symbol_filters(session, symbol)
    ref_price = _d(limit_price)
    qty_s = compute_qty_from_quote(_d(quote_amount), ref_price, f)
    px_s = compute_limit_price(_d(limit_price), f)
    client_oid = build_client_order_id(intent_id, symbol)

    if SET_LEVERAGE_ON_EACH_ORDER:
        await set_symbol_leverage(session, symbol, FUTURES_LEVERAGE)

    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": qty_s,
        "price": px_s,
        "newClientOrderId": client_oid,
        "workingType": "CONTRACT_PRICE",
    }

    status, text = await binance_signed(session, "POST", "/fapi/v1/order", params)
    if status != 200:
        raise RuntimeError(f"Binance futures LIMIT SELL failed {status}: {text}")

    resp = json.loads(text)
    return int(resp["orderId"]), client_oid, qty_s, px_s


async def place_market_sell(
    session: aiohttp.ClientSession,
    intent_id: int,
    symbol: str,
    quote_amount: float,
    ref_price: float
) -> Tuple[int, str, str]:
    f = await get_symbol_filters(session, symbol)
    qty_s = compute_qty_from_quote(_d(quote_amount), _d(ref_price), f)
    client_oid = build_client_order_id(intent_id, symbol)

    if SET_LEVERAGE_ON_EACH_ORDER:
        await set_symbol_leverage(session, symbol, FUTURES_LEVERAGE)

    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": qty_s,
        "newClientOrderId": client_oid,
        "workingType": "CONTRACT_PRICE",
    }

    status, text = await binance_signed(session, "POST", "/fapi/v1/order", params)
    if status != 200:
        raise RuntimeError(f"Binance futures MARKET SELL failed {status}: {text}")

    resp = json.loads(text)
    return int(resp["orderId"]), client_oid, qty_s


async def fetch_order(session: aiohttp.ClientSession, symbol: str, order_id: int) -> Dict[str, Any]:
    status, text = await binance_signed(session, "GET", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
    if status != 200:
        raise RuntimeError(f"Binance futures GET /fapi/v1/order failed {status}: {text}")
    return json.loads(text)


async def cancel_order(session: aiohttp.ClientSession, symbol: str, order_id: int) -> Dict[str, Any]:
    status, text = await binance_signed(session, "DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
    if status != 200:
        raise RuntimeError(f"Binance futures DELETE /fapi/v1/order failed {status}: {text}")
    return json.loads(text)


def avg_fill_price(executed_qty: Decimal, cum_quote_qty: Decimal) -> Decimal:
    return (cum_quote_qty / executed_qty) if executed_qty > 0 else Decimal("0")


async def wait_for_fill(session: aiohttp.ClientSession, symbol: str, order_id: int, max_wait_sec: int) -> Tuple[bool, Dict[str, Any]]:
    deadline = time.time() + max_wait_sec
    last: Dict[str, Any] = {}
    while time.time() < deadline:
        last = await fetch_order(session, symbol, order_id)
        st = (last.get("status") or "").upper()
        if st == "FILLED":
            return True, last
        if st in ("CANCELED", "CANCELLED", "REJECTED", "EXPIRED"):
            return False, last
        await asyncio.sleep(FILL_POLL_EVERY_SEC)

    if not last:
        last = await fetch_order(session, symbol, order_id)
    return False, last


def order_time_utc(ordj: Dict[str, Any]) -> datetime:
    for k in ("updateTime", "time"):
        v = ordj.get(k)
        try:
            if v is not None:
                ms = int(v)
                return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc)


# ==========================================================
# Core actions
# ==========================================================
async def write_position_from_order(
    conn: asyncpg.Connection,
    intent: asyncpg.Record,
    order_id: int,
    ordj: Dict[str, Any]
) -> None:
    intent_id = int(intent["id"])
    symbol = str(intent["symbol"]).upper()
    intent_source = str(intent.get("source") or "")
    entry_mode = str(intent.get("entry_mode") or "LIMIT").upper()

    executed_qty = _d(ordj.get("executedQty", "0"))
    cum_quote = _d(ordj.get("cumQuote", ordj.get("cumQuoteQty", "0")))
    st = (ordj.get("status") or "").upper()

    if st != "FILLED":
        raise RuntimeError(f"write_position_from_order called with status={st}")

    px = avg_fill_price(executed_qty, cum_quote)
    if executed_qty <= 0 or px <= 0:
        raise RuntimeError(f"FILLED but invalid executedQty/avgPx: executedQty={executed_qty} cumQuote={cum_quote}")

    entry_time = order_time_utc(ordj)
    entry_price = float(px)
    qty = float(executed_qty)

    # SHORT position: SL above entry, TP below entry
    sl = entry_price * (1.0 + SHORT_DEFAULT_SL_PCT)
    tp = entry_price * (1.0 - SHORT_DEFAULT_TP_PCT)

    await conn.execute(MARK_INTENT_FILLED_SQL, intent_id)
    await conn.execute(
        UPSERT_POSITION_OPEN_SQL,
        symbol,
        entry_time,
        entry_price,
        sl,
        tp,
        qty,
        order_id
    )

    reason = f"ENTRY_{entry_mode}"
    await conn.execute(
        INSERT_TRADE_LOG_ENTRY_SQL,
        symbol,
        entry_time,
        entry_price,
        reason,
        intent_source,
        entry_mode,
        intent_id,
        order_id
    )

    logging.warning(
        "SHORT_POSITION_OPENED id=%d %s | FILLED order_id=%d | avg_fill=%.10f qty=%.10f | source=%s mode=%s | sl=%.10f tp=%.10f",
        intent_id, symbol, order_id, entry_price, qty, intent_source, entry_mode, sl, tp
    )


# ==========================================================
# NEW intents loop
# ==========================================================
async def _place_and_kickoff_fill_wait(pool: asyncpg.Pool, session: aiohttp.ClientSession, intent: asyncpg.Record) -> None:
    intent_id = int(intent["id"])
    symbol = str(intent["symbol"]).upper()
    quote_amount = float(intent["quote_amount"])
    entry_mode = str(intent.get("entry_mode") or "LIMIT").upper()
    limit_price = float(intent["limit_price"])

    if not symbol.endswith(QUOTE_CCY):
        async with pool.acquire() as conn:
            await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, f"BAD_SYMBOL_QUOTE({QUOTE_CCY})")
        raise RuntimeError(f"Symbol {symbol} does not end with {QUOTE_CCY}")

    async with pool.acquire() as conn:
        has_pos = await conn.fetchrow(HAS_OPEN_POSITION_SQL, symbol)
        if has_pos:
            await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, "OPEN_POSITION_EXISTS")
            logging.warning("SHORT_INTENT_SKIP id=%d %s | open short position exists", intent_id, symbol)
            return

    try:
        if entry_mode == "MARKET":
            order_id, client_oid, qty_s = await place_market_sell(session, intent_id, symbol, quote_amount, limit_price)
            px_s = "MKT"
        else:
            order_id, client_oid, qty_s, px_s = await place_limit_sell(session, intent_id, symbol, quote_amount, limit_price)
    except Exception as e:
        async with pool.acquire() as conn:
            await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, f"PLACE_FAILED: {str(e)}")
        logging.error("SHORT_INTENT_ERROR id=%d %s | place failed | %s", intent_id, symbol, str(e))
        return

    async with pool.acquire() as conn:
        await conn.execute(MARK_INTENT_SENT_SQL, intent_id, order_id, client_oid)

    if entry_mode == "MARKET":
        logging.warning(
            "SHORT_INTENT_SENT id=%d %s | mode=MARKET | quote=%.2f %s | qty=%s | order_id=%d client_oid=%s | ref_limit_price=%.8f leverage=%dx",
            intent_id, symbol, quote_amount, QUOTE_CCY, qty_s, order_id, client_oid, limit_price, FUTURES_LEVERAGE
        )
    else:
        logging.warning(
            "SHORT_INTENT_SENT id=%d %s | mode=LIMIT | quote=%.2f %s | limit=%s qty=%s | order_id=%d client_oid=%s leverage=%dx",
            intent_id, symbol, quote_amount, QUOTE_CCY, px_s, qty_s, order_id, client_oid, FUTURES_LEVERAGE
        )

    try:
        filled, ordj = await wait_for_fill(session, symbol, order_id, FILL_WAIT_MAX_SEC)
        if filled:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await write_position_from_order(conn, intent, order_id, ordj)
        else:
            st = (ordj.get("status") or "").upper()
            logging.warning(
                "SHORT_INTENT_NOT_FILLED id=%d %s | mode=%s order_id=%d status=%s | waited=%ds | will reconcile later%s",
                intent_id, symbol, entry_mode, order_id, st, FILL_WAIT_MAX_SEC,
                "" if entry_mode == "MARKET" else f" (timeout cancel after {EXEC_CANCEL_AFTER_SEC}s)"
            )
    except Exception as e:
        logging.error("SHORT_FILL_WAIT_ERROR id=%d %s | order_id=%d | %s", intent_id, symbol, order_id, str(e))


async def new_intents_loop(pool: asyncpg.Pool) -> None:
    timeout = aiohttp.ClientTimeout(total=30)
    inflight_sem = asyncio.Semaphore(EXEC_MAX_INFLIGHT)
    tasks: Set[asyncio.Task] = set()

    def _done_cb(t: asyncio.Task) -> None:
        tasks.discard(t)
        try:
            _ = t.result()
        except Exception as e:
            logging.error("SHORT_INTENT_TASK_ERROR | %s", str(e))
        finally:
            inflight_sem.release()

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        intents = await conn.fetch(PICK_NEW_INTENTS_SQL, MAX_BATCH, EXEC_SOURCES)

                if not intents:
                    await asyncio.sleep(POLL_EVERY_SEC)
                    continue

                for it in intents:
                    await inflight_sem.acquire()
                    t = asyncio.create_task(_place_and_kickoff_fill_wait(pool, session, it))
                    tasks.add(t)
                    t.add_done_callback(_done_cb)

                await asyncio.sleep(0)

            except Exception as e:
                logging.exception("SHORT NEW loop error: %s | sleeping %ds", e, POLL_EVERY_SEC)
                await asyncio.sleep(POLL_EVERY_SEC)


# ==========================================================
# Reconciler
# ==========================================================
async def reconcile_sent_once(conn: asyncpg.Connection, session: aiohttp.ClientSession) -> None:
    intents = await conn.fetch(PICK_SENT_INTENTS_SQL, RECONCILE_BATCH, EXEC_SOURCES)
    if not intents:
        return

    now = datetime.now(timezone.utc)

    for it in intents:
        intent_id = int(it["id"])
        symbol = str(it["symbol"]).upper()
        order_id = int(it["order_id"])
        entry_mode = str(it.get("entry_mode") or "LIMIT").upper()

        sent_at = it.get("updated_at")
        age_sec: Optional[float] = None
        if isinstance(sent_at, datetime):
            if sent_at.tzinfo is None:
                sent_at = sent_at.replace(tzinfo=timezone.utc)
            age_sec = (now - sent_at).total_seconds()

        try:
            ordj = await fetch_order(session, symbol, order_id)
            st = (ordj.get("status") or "").upper()

            if st == "FILLED":
                await write_position_from_order(conn, it, order_id, ordj)
                continue

            if st in ("CANCELED", "CANCELLED", "REJECTED", "EXPIRED"):
                await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, f"ORDER_{st}")
                logging.error("SHORT_INTENT_ERROR id=%d %s | order_id=%d status=%s", intent_id, symbol, order_id, st)
                continue

            if entry_mode == "LIMIT" and age_sec is not None and age_sec >= EXEC_CANCEL_AFTER_SEC:
                try:
                    _ = await cancel_order(session, symbol, order_id)
                    ordj2 = await fetch_order(session, symbol, order_id)
                    st2 = (ordj2.get("status") or "").upper()

                    await conn.execute(MARK_INTENT_ERROR_SQL, intent_id, f"TIMEOUT_CANCEL_{st2}")
                    logging.warning(
                        "SHORT_INTENT_TIMEOUT_CANCEL id=%d %s | order_id=%d age=%.0fs | final_status=%s | marked ERROR",
                        intent_id, symbol, order_id, age_sec, st2
                    )
                except Exception as ce:
                    logging.error(
                        "SHORT_CANCEL_ERROR id=%d %s | order_id=%d age=%.0fs | %s",
                        intent_id, symbol, order_id, age_sec, str(ce)
                    )
                continue

            logging.info(
                "SHORT_RECONCILE_PENDING id=%d %s | mode=%s | order_id=%d status=%s age=%ss",
                intent_id, symbol, entry_mode, order_id, st, f"{age_sec:.0f}" if age_sec is not None else "?"
            )

        except Exception as e:
            logging.error("SHORT_RECONCILE_ERROR id=%d %s | order_id=%d | %s", intent_id, symbol, order_id, str(e))


async def reconcile_loop(pool: asyncpg.Pool) -> None:
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await reconcile_sent_once(conn, session)
                await asyncio.sleep(RECONCILE_EVERY_SEC)
            except Exception as e:
                logging.exception("SHORT RECONCILE loop error: %s | sleeping %ds", e, RECONCILE_EVERY_SEC)
                await asyncio.sleep(RECONCILE_EVERY_SEC)


# ==========================================================
# main
# ==========================================================
async def main() -> None:
    logging.info(
        "Starting short_executor | intents=%s | positions=%s | trade_log=%s | quote=%s | sources=%s | new_poll=%ds batch=%d inflight=%d | fill_wait=%ds | reconcile=%ds batch=%d | cancel_after=%ds | leverage=%dx",
        INTENTS_TABLE,
        POSITIONS_TABLE,
        TRADE_LOG_TABLE,
        QUOTE_CCY,
        ",".join(EXEC_SOURCES) if EXEC_SOURCES else "<ALL>",
        POLL_EVERY_SEC,
        MAX_BATCH,
        EXEC_MAX_INFLIGHT,
        FILL_WAIT_MAX_SEC,
        RECONCILE_EVERY_SEC,
        RECONCILE_BATCH,
        EXEC_CANCEL_AFTER_SEC,
        FUTURES_LEVERAGE,
    )

    pool = await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        min_size=1,
        max_size=10
    )
    logging.info("DB pool created | host=%s db=%s user=%s", DB_HOST, DB_NAME, DB_USER)

    await asyncio.gather(
        new_intents_loop(pool),
        reconcile_loop(pool),
    )


if __name__ == "__main__":
    asyncio.run(main())
