#!/usr/bin/env python3
import os
import time
import hmac
import json
import hashlib
import asyncio
import logging
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from decimal import Decimal, ROUND_DOWN, getcontext

import aiohttp
import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# =========================
# DECIMAL
# =========================
getcontext().prec = 28

def d(x: Any) -> Decimal:
    return Decimal(str(x))

# =========================
# ENV
# =========================
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

POSITIONS_TABLE = os.getenv("POSITIONS_OPEN_SHORT_TABLE", "public.positions_open_short")
TRADE_LOG_TABLE = os.getenv("TRADE_LOG_SHORT_TABLE", "public.trade_log_short")
RUNNER_TABLE = os.getenv("EXIT_RUNNER_SHORT_TABLE", "public.exit_runner_state_short")

BINANCE_BASE_URL = os.getenv("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com").rstrip("/")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
BINANCE_RECV_WINDOW = int(os.getenv("BINANCE_RECV_WINDOW", "5000"))

EXECUTOR_POLL_SEC = int(os.getenv("SHORT_EXECUTOR_POLL_SEC", "2"))
ORDER_CHECK_SEC = int(os.getenv("SHORT_ORDER_CHECK_SEC", "3"))
LIMIT_TTL_SEC = int(os.getenv("SHORT_LIMIT_TTL_SEC", "30"))
MAX_RETRIES = int(os.getenv("SHORT_MAX_RETRIES", "3"))
FALLBACK_TO_MARKET = os.getenv("SHORT_FALLBACK_TO_MARKET", "1").strip().lower() in ("1", "true", "yes")
REPRICE_USING_BOOK_ASK = os.getenv("SHORT_REPRICE_USING_BOOK_ASK", "1").strip().lower() in ("1", "true", "yes")
PROCESSING_STALE_SEC = int(os.getenv("SHORT_PROCESSING_STALE_SEC", "30"))
IDLE_LOG_SEC = int(os.getenv("SHORT_IDLE_LOG_SEC", "20"))
FEE_ROUNDTRIP_PCT = float(os.getenv("SHORT_FEE_ROUNDTRIP_PCT", "0.08"))

if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    logging.warning("BINANCE_API_KEY/SECRET not set -> placing signed orders will fail.")

# =========================
# BINANCE SIGNED REQUEST
# =========================
def _ts_ms() -> int:
    return int(time.time() * 1000)

def _sign(query_string: str) -> str:
    return hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

def _normalize_param_value(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)

def _build_query(params: Dict[str, Any]) -> str:
    items = []
    for k in sorted(params.keys()):
        v = params[k]
        if v is None:
            continue
        items.append((k, _normalize_param_value(v)))
    return urllib.parse.urlencode(items, quote_via=urllib.parse.quote, safe="")

async def binance_request(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    signed: bool = False,
) -> Any:
    if params is None:
        params = {}

    headers = {"X-MBX-APIKEY": BINANCE_API_KEY} if BINANCE_API_KEY else {}

    if signed:
        if not BINANCE_API_SECRET:
            raise RuntimeError("BINANCE_API_SECRET missing for signed request")

        p = dict(params)
        p["timestamp"] = _ts_ms()
        p["recvWindow"] = BINANCE_RECV_WINDOW

        qs = _build_query(p)
        sig = _sign(qs)
        full_qs = f"{qs}&signature={sig}" if qs else f"signature={sig}"
        url = f"{BINANCE_BASE_URL}{path}?{full_qs}"

        async with session.request(method, url, headers=headers) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Binance HTTP {resp.status} {path} -> {text}")
            try:
                return json.loads(text)
            except Exception:
                return text

    url = f"{BINANCE_BASE_URL}{path}"
    async with session.request(method, url, params=params, headers=headers) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"Binance HTTP {resp.status} {path} -> {text}")
        try:
            return json.loads(text)
        except Exception:
            return text

# =========================
# exchangeInfo cache
# =========================
def _decimals_from_step_str(step: str) -> int:
    if "." not in step:
        return 0
    frac = step.split(".", 1)[1].rstrip("0")
    return len(frac)

def _quantize_down(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

def _fmt_decimal_fixed(x: Decimal, decimals: int) -> str:
    if decimals <= 0:
        return str(x.quantize(Decimal("1"), rounding=ROUND_DOWN))
    q = Decimal("1").scaleb(-decimals)
    return format(x.quantize(q, rounding=ROUND_DOWN), "f")

@dataclass
class SymbolFilters:
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal
    tick_decimals: int
    step_decimals: int

EXINFO_CACHE: Dict[str, SymbolFilters] = {}

async def load_exchange_info(session: aiohttp.ClientSession) -> None:
    data = await binance_request(session, "GET", "/fapi/v1/exchangeInfo", signed=False)
    symbols = data.get("symbols", [])
    for s in symbols:
        sym = (s.get("symbol") or "").upper()
        if not sym:
            continue

        tick_str = "0"
        step_str = "0"
        min_qty_str = "0"
        min_notional_str = "0"

        for f in s.get("filters", []):
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                tick_str = str(f.get("tickSize", "0"))
            elif t == "LOT_SIZE":
                step_str = str(f.get("stepSize", "0"))
                min_qty_str = str(f.get("minQty", "0"))
            elif t in ("MIN_NOTIONAL", "NOTIONAL"):
                mn = f.get("notional") or f.get("minNotional")
                if mn is not None:
                    min_notional_str = str(mn)

        tick_dec = _decimals_from_step_str(tick_str)
        step_dec = _decimals_from_step_str(step_str)

        EXINFO_CACHE[sym] = SymbolFilters(
            tick_size=d(tick_str),
            step_size=d(step_str),
            min_qty=d(min_qty_str),
            min_notional=d(min_notional_str),
            tick_decimals=tick_dec,
            step_decimals=step_dec,
        )

    logging.info("Loaded futures exchangeInfo for %d symbols", len(EXINFO_CACHE))

async def book_ask_price(session: aiohttp.ClientSession, symbol: str) -> Optional[Decimal]:
    data = await binance_request(session, "GET", "/fapi/v1/ticker/bookTicker", params={"symbol": symbol}, signed=False)
    ask = data.get("askPrice")
    try:
        a = d(ask)
        return a if a > 0 else None
    except Exception:
        return None

# =========================
# DB
# =========================
async def init_db_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        min_size=1,
        max_size=5,
        command_timeout=60,
    )

async def db_claim_one_closing(pool: asyncpg.Pool) -> Optional[asyncpg.Record]:
    sql = f"""
    WITH cte AS (
      SELECT symbol
      FROM {POSITIONS_TABLE}
      WHERE status='CLOSING'
        AND exit_filled_at IS NULL
        AND (
              exit_order_status IS DISTINCT FROM 'PROCESSING'
              OR updated_at < (NOW() - INTERVAL '{PROCESSING_STALE_SEC} seconds')
            )
      ORDER BY close_requested_at NULLS LAST, updated_at
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    UPDATE {POSITIONS_TABLE} p
    SET exit_order_status='PROCESSING', updated_at=NOW()
    FROM cte
    WHERE p.symbol = cte.symbol
    RETURNING p.*;
    """
    async with pool.acquire() as conn:
        return await conn.fetchrow(sql)

async def db_set_order_placed(pool: asyncpg.Pool, symbol: str, order_id: str, limit_price: float, status: str) -> None:
    sql = f"""
    UPDATE {POSITIONS_TABLE}
    SET
      exit_order_id=$2,
      exit_order_price=$3,
      exit_order_status=$4,
      exit_order_placed_at=NOW(),
      updated_at=NOW()
    WHERE symbol=$1 AND status='CLOSING'
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, order_id, limit_price, status)

async def db_set_order_status(pool: asyncpg.Pool, symbol: str, status: str) -> None:
    sql = f"""
    UPDATE {POSITIONS_TABLE}
    SET exit_order_status=$2, updated_at=NOW()
    WHERE symbol=$1 AND status='CLOSING'
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, status)

async def db_inc_retries(pool: asyncpg.Pool, symbol: str) -> int:
    sql = f"""
    UPDATE {POSITIONS_TABLE}
    SET exit_retries = COALESCE(exit_retries,0) + 1, updated_at=NOW()
    WHERE symbol=$1 AND status='CLOSING'
    RETURNING exit_retries
    """
    async with pool.acquire() as conn:
        r = await conn.fetchrow(sql, symbol)
    return int(r["exit_retries"]) if r and r["exit_retries"] is not None else 0

async def db_set_fill(pool: asyncpg.Pool, symbol: str, fill_price: float) -> None:
    sql = f"""
    UPDATE {POSITIONS_TABLE}
    SET
      exit_fill_price=$2,
      exit_filled_at=NOW(),
      exit_order_status='FILLED',
      updated_at=NOW()
    WHERE symbol=$1 AND status='CLOSING'
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, fill_price)

async def db_load_position(pool: asyncpg.Pool, symbol: str) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetchrow(f"SELECT * FROM {POSITIONS_TABLE} WHERE symbol=$1", symbol)

# =========================
# trade_log UPSERT (EXIT)
# =========================
async def upsert_trade_log_exit(pool: asyncpg.Pool, pos: asyncpg.Record) -> None:
    entry_price = float(pos["entry_price"])
    exit_price = float(pos["exit_fill_price"])

    gross_pct = (entry_price - exit_price) / entry_price * 100.0 if entry_price > 0 else 0.0
    net_pct = gross_pct - FEE_ROUNDTRIP_PCT

    reason = pos["close_reason"] if "close_reason" in pos and pos["close_reason"] else "UNKNOWN"
    entry_order_id = pos["entry_order_id"] if "entry_order_id" in pos else None

    if entry_order_id is None:
        sql_fallback = f"""
        INSERT INTO {TRADE_LOG_TABLE}(symbol, entry_time, exit_time, entry_price, exit_price, reason, pnl_pct)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        """
        async with pool.acquire() as conn:
            await conn.execute(
                sql_fallback,
                pos["symbol"],
                pos["entry_time"],
                pos["exit_filled_at"],
                entry_price,
                exit_price,
                reason,
                net_pct,
            )
        return

    sql = f"""
    INSERT INTO {TRADE_LOG_TABLE}(
      symbol, entry_time, exit_time, entry_price, exit_price, reason, pnl_pct,
      entry_order_id
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (entry_order_id) DO UPDATE SET
      exit_time  = COALESCE(EXCLUDED.exit_time,  {TRADE_LOG_TABLE}.exit_time),
      exit_price = COALESCE(EXCLUDED.exit_price, {TRADE_LOG_TABLE}.exit_price),
      reason     = COALESCE(EXCLUDED.reason,     {TRADE_LOG_TABLE}.reason),
      pnl_pct    = COALESCE(EXCLUDED.pnl_pct,    {TRADE_LOG_TABLE}.pnl_pct);
    """
    async with pool.acquire() as conn:
        await conn.execute(
            sql,
            pos["symbol"],
            pos["entry_time"],
            pos["exit_filled_at"],
            entry_price,
            exit_price,
            reason,
            net_pct,
            entry_order_id,
        )

async def delete_position_and_cleanup(pool: asyncpg.Pool, symbol: str) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute(f"DELETE FROM {RUNNER_TABLE} WHERE symbol=$1", symbol)
            except Exception as e:
                logging.debug("cleanup short runner state failed for %s: %s", symbol, e)
            await conn.execute(f"DELETE FROM {POSITIONS_TABLE} WHERE symbol=$1", symbol)

# =========================
# ORDER EXECUTION
# =========================
def compute_limit_price(close_ref: Decimal, ask: Optional[Decimal]) -> Decimal:
    if REPRICE_USING_BOOK_ASK and ask and ask > 0:
        return ask
    return close_ref

def normalize_qty_price(symbol: str, qty: Decimal, price: Decimal) -> Tuple[str, str, float, Decimal]:
    f = EXINFO_CACHE.get(symbol)
    if not f:
        qty_q = qty
        price_q = price
        qty_str = _fmt_decimal_fixed(qty_q, 8)
        price_str = _fmt_decimal_fixed(price_q, 8)
        return qty_str, price_str, float(price_q), qty_q

    qty_q = _quantize_down(qty, f.step_size)
    price_q = _quantize_down(price, f.tick_size)

    qty_str = _fmt_decimal_fixed(qty_q, f.step_decimals)
    price_str = _fmt_decimal_fixed(price_q, f.tick_decimals)
    return qty_str, price_str, float(price_q), qty_q

async def place_limit_buy_reduce_only(session: aiohttp.ClientSession, symbol: str, qty_str: str, price_str: str) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": qty_str,
        "price": price_str,
        "reduceOnly": True,
        "newOrderRespType": "RESULT",
        "workingType": "CONTRACT_PRICE",
    }
    return await binance_request(session, "POST", "/fapi/v1/order", params=params, signed=True)

async def place_market_buy_reduce_only(session: aiohttp.ClientSession, symbol: str, qty_str: str) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "BUY",
        "type": "MARKET",
        "quantity": qty_str,
        "reduceOnly": True,
        "newOrderRespType": "RESULT",
        "workingType": "CONTRACT_PRICE",
    }
    return await binance_request(session, "POST", "/fapi/v1/order", params=params, signed=True)

async def get_order(session: aiohttp.ClientSession, symbol: str, order_id: str) -> Dict[str, Any]:
    return await binance_request(session, "GET", "/fapi/v1/order", params={"symbol": symbol, "orderId": order_id}, signed=True)

async def cancel_order(session: aiohttp.ClientSession, symbol: str, order_id: str) -> Dict[str, Any]:
    return await binance_request(session, "DELETE", "/fapi/v1/order", params={"symbol": symbol, "orderId": order_id}, signed=True)

def extract_fill_price(order_resp: Dict[str, Any]) -> Optional[float]:
    try:
        executed = d(order_resp.get("executedQty", "0"))
        cum_quote = d(order_resp.get("cumQuote", order_resp.get("cumQuoteQty", "0")))
        if executed > 0 and cum_quote > 0:
            return float(cum_quote / executed)
    except Exception:
        pass
    return None

# =========================
# CORE PROCESSING
# =========================
async def process_position(pool: asyncpg.Pool, session: aiohttp.ClientSession, pos: asyncpg.Record) -> None:
    symbol = (pos["symbol"] or "").upper()
    f = EXINFO_CACHE.get(symbol)
    if not f:
        logging.error("No futures exchange info for %s", symbol)
        await db_set_order_status(pool, symbol, "ERROR_NO_EXINFO")
        return

    db_qty = d(pos["qty"] if "qty" in pos and pos["qty"] is not None else "0")
    if db_qty <= 0:
        logging.error("CLOSING SHORT %s but qty missing/invalid -> cannot execute BUY", symbol)
        await db_set_order_status(pool, symbol, "ERROR_NO_QTY")
        return

    qty = db_qty

    close_ref = d(pos["close_ref_price"] if "close_ref_price" in pos and pos["close_ref_price"] is not None else "0")
    if close_ref <= 0:
        logging.warning("CLOSING SHORT %s close_ref_price missing/invalid, will use ask if enabled", symbol)

    if not (pos["exit_order_id"] if "exit_order_id" in pos else None):
        ask = await book_ask_price(session, symbol) if REPRICE_USING_BOOK_ASK else None
        base_price = close_ref if close_ref > 0 else (ask or Decimal("0"))
        raw_price = compute_limit_price(base_price, ask)

        qty_str, price_str, price_for_db, qty_q = normalize_qty_price(symbol, qty, raw_price)

        if qty_q < f.min_qty or d(qty_str) <= 0:
            logging.error(
                "SHORT qty too small after round: %s db=%s minQty=%s",
                qty_str, str(db_qty), str(f.min_qty)
            )
            await db_set_order_status(pool, symbol, "ERROR_QTY_TOO_SMALL")
            return

        logging.warning(
            "PLACE LIMIT BUY reduceOnly %s qty=%s price=%s (db_qty=%s ref=%s ask=%s)",
            symbol, qty_str, price_str, str(db_qty), str(close_ref), str(ask) if ask else None
        )

        resp = await place_limit_buy_reduce_only(session, symbol, qty_str, price_str)
        order_id = str(resp.get("orderId"))
        status = str(resp.get("status") or "NEW")
        await db_set_order_placed(pool, symbol, order_id, price_for_db, status)
        return

    order_id = str(pos["exit_order_id"])
    placed_at = pos["exit_order_placed_at"] if "exit_order_placed_at" in pos else None
    retries = int(pos["exit_retries"] if "exit_retries" in pos and pos["exit_retries"] is not None else 0)

    o = await get_order(session, symbol, order_id)
    status = str(o.get("status") or "")
    await db_set_order_status(pool, symbol, status)

    if status == "FILLED":
        fp = extract_fill_price(o) or float(pos["exit_order_price"] if "exit_order_price" in pos and pos["exit_order_price"] else 0.0)
        if fp <= 0:
            fp = float(pos["exit_order_price"] if "exit_order_price" in pos and pos["exit_order_price"] else 0.0)

        await db_set_fill(pool, symbol, fp)

        pos2 = await db_load_position(pool, symbol)
        if pos2:
            await upsert_trade_log_exit(pool, pos2)
            await delete_position_and_cleanup(pool, symbol)

        logging.warning("✅ SHORT CLOSED %s FILLED order=%s fill=%s", symbol, order_id, fp)
        return

    if status in ("CANCELED", "CANCELLED", "REJECTED", "EXPIRED"):
        logging.warning("SHORT order %s status=%s -> retry", symbol, status)
        retries = await db_inc_retries(pool, symbol)

        if retries > MAX_RETRIES and FALLBACK_TO_MARKET:
            qty_str, _, _, qty_q = normalize_qty_price(symbol, qty, Decimal("1"))
            if qty_q < f.min_qty or d(qty_str) <= 0:
                await db_set_order_status(pool, symbol, "ERROR_QTY_TOO_SMALL")
                return

            logging.warning("FALLBACK MARKET BUY reduceOnly %s qty=%s (retries=%s)", symbol, qty_str, retries)
            m = await place_market_buy_reduce_only(session, symbol, qty_str)
            fp = extract_fill_price(m) or float(pos["exit_order_price"] if "exit_order_price" in pos and pos["exit_order_price"] else 0.0)

            await db_set_fill(pool, symbol, fp if fp > 0 else float(pos["exit_order_price"] if "exit_order_price" in pos and pos["exit_order_price"] else 0.0))
            pos2 = await db_load_position(pool, symbol)
            if pos2:
                await upsert_trade_log_exit(pool, pos2)
                await delete_position_and_cleanup(pool, symbol)

            logging.warning("✅ SHORT CLOSED %s MARKET fallback fill=%s", symbol, fp)
        return

    if placed_at:
        now = datetime.now(timezone.utc)
        age = (now - placed_at).total_seconds()
        if age >= LIMIT_TTL_SEC:
            if retries >= MAX_RETRIES and FALLBACK_TO_MARKET:
                logging.warning("SHORT TTL reached. Cancel + MARKET fallback %s order=%s", symbol, order_id)
                try:
                    await cancel_order(session, symbol, order_id)
                except Exception as e:
                    logging.warning("SHORT cancel failed %s: %s", symbol, e)

                await db_inc_retries(pool, symbol)

                qty_str, _, _, qty_q = normalize_qty_price(symbol, qty, Decimal("1"))
                if qty_q < f.min_qty or d(qty_str) <= 0:
                    await db_set_order_status(pool, symbol, "ERROR_QTY_TOO_SMALL")
                    return

                m = await place_market_buy_reduce_only(session, symbol, qty_str)
                fp = extract_fill_price(m) or float(pos["exit_order_price"] if "exit_order_price" in pos and pos["exit_order_price"] else 0.0)

                await db_set_fill(pool, symbol, fp if fp > 0 else float(pos["exit_order_price"] if "exit_order_price" in pos and pos["exit_order_price"] else 0.0))
                pos2 = await db_load_position(pool, symbol)
                if pos2:
                    await upsert_trade_log_exit(pool, pos2)
                    await delete_position_and_cleanup(pool, symbol)

                logging.warning("✅ SHORT CLOSED %s MARKET fallback fill=%s", symbol, fp)
                return

            logging.warning("SHORT TTL reached. Cancel + reprice LIMIT %s order=%s age=%.1fs retries=%s",
                            symbol, order_id, age, retries)
            try:
                await cancel_order(session, symbol, order_id)
            except Exception as e:
                logging.warning("SHORT cancel failed %s: %s", symbol, e)

            retries2 = await db_inc_retries(pool, symbol)

            ask = await book_ask_price(session, symbol) if REPRICE_USING_BOOK_ASK else None
            base_price = close_ref if close_ref > 0 else (ask or Decimal("0"))
            raw_price = compute_limit_price(base_price, ask)

            qty_str, price_str, price_for_db, qty_q = normalize_qty_price(symbol, qty, raw_price)
            if qty_q < f.min_qty or d(qty_str) <= 0:
                await db_set_order_status(pool, symbol, "ERROR_QTY_TOO_SMALL")
                return

            resp = await place_limit_buy_reduce_only(session, symbol, qty_str, price_str)
            new_id = str(resp.get("orderId"))
            new_status = str(resp.get("status") or "NEW")
            await db_set_order_placed(pool, symbol, new_id, price_for_db, new_status)

            logging.warning("SHORT REPLACED LIMIT %s new_order=%s price=%s retries=%s ask=%s",
                            symbol, new_id, price_str, retries2, str(ask) if ask else None)
            return

# =========================
# MAIN LOOP
# =========================
async def loop(pool: asyncpg.Pool) -> None:
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await load_exchange_info(session)

        last_idle = 0.0

        while True:
            try:
                pos = await db_claim_one_closing(pool)
                if not pos:
                    now = time.time()
                    if now - last_idle >= IDLE_LOG_SEC:
                        logging.info("Idle: no SHORT CLOSING positions")
                        last_idle = now
                    await asyncio.sleep(EXECUTOR_POLL_SEC)
                    continue

                await process_position(pool, session, pos)
                await asyncio.sleep(ORDER_CHECK_SEC)

            except Exception as e:
                logging.error("short exit executor error: %s", e)
                await asyncio.sleep(2)

async def main() -> None:
    pool = await init_db_pool()
    await loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
