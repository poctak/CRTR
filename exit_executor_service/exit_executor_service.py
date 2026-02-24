import os
import time
import hmac
import json
import math
import hashlib
import asyncio
import logging
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import aiohttp
import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# =========================
# ENV
# =========================
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "pumpdb")
DB_USER = os.getenv("DB_USER", "pumpuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
BINANCE_RECV_WINDOW = int(os.getenv("BINANCE_RECV_WINDOW", "5000"))

EXECUTOR_POLL_SEC = int(os.getenv("EXECUTOR_POLL_SEC", "2"))     # jak často hledat práci v DB
ORDER_CHECK_SEC = int(os.getenv("ORDER_CHECK_SEC", "3"))         # jak často checkovat stav orderu
LIMIT_TTL_SEC = int(os.getenv("LIMIT_TTL_SEC", "30"))            # po kolika sekundách limit rušit/reprice
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))                 # kolikrát max cancel/replace
FALLBACK_TO_MARKET = os.getenv("FALLBACK_TO_MARKET", "1").strip().lower() in ("1", "true", "yes")
REPRICE_USING_BOOK_BID = os.getenv("REPRICE_USING_BOOK_BID", "1").strip().lower() in ("1", "true", "yes")

FEE_ROUNDTRIP_PCT = float(os.getenv("FEE_ROUNDTRIP_PCT", "0.15"))

if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    logging.warning("BINANCE_API_KEY/SECRET not set -> placing signed orders will fail.")

# =========================
# BINANCE HELPERS (SIGNED REQUESTS FIX)
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
    # Binance expects strings; avoid scientific notation and keep stable formatting.
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)

def _build_query(params: Dict[str, Any]) -> str:
    """
    Build URL-encoded query string in a deterministic way.
    Signature must match EXACT bytes of the query string sent to Binance.
    """
    items = []
    for k in sorted(params.keys()):
        v = params[k]
        if v is None:
            continue
        items.append((k, _normalize_param_value(v)))

    # urlencode with quote_via=quote to match typical encoding, spaces -> %20 (not '+')
    return urllib.parse.urlencode(items, quote_via=urllib.parse.quote, safe="")

async def binance_request(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    signed: bool = False,
) -> Any:
    """
    IMPORTANT:
    - For signed endpoints we construct query string ourselves, sign it, and send as raw URL.
    - Do NOT pass params=dict to aiohttp for signed requests (can reorder/encode differently).
    """
    if params is None:
        params = {}

    headers = {"X-MBX-APIKEY": BINANCE_API_KEY} if BINANCE_API_KEY else {}

    if signed:
        if not BINANCE_API_SECRET:
            raise RuntimeError("BINANCE_API_SECRET missing for signed request")

        params = dict(params)  # copy
        params["timestamp"] = _ts_ms()
        params["recvWindow"] = BINANCE_RECV_WINDOW

        qs = _build_query(params)
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

    # unsigned request: safe to use params dict
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
# exchangeInfo cache (tick/step rounding)
# =========================

@dataclass
class SymbolFilters:
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float

EXINFO_CACHE: Dict[str, SymbolFilters] = {}

def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _floor_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step

def _round_price_to_tick(p: float, tick: float) -> float:
    if tick <= 0:
        return p
    return math.floor(p / tick) * tick

def _format_decimal(x: float) -> str:
    # Avoid scientific notation; keep enough precision.
    s = f"{x:.16f}"
    s = s.rstrip("0").rstrip(".")
    return s if s else "0"

async def load_exchange_info(session: aiohttp.ClientSession) -> None:
    data = await binance_request(session, "GET", "/api/v3/exchangeInfo", signed=False)
    symbols = data.get("symbols", [])
    for s in symbols:
        sym = (s.get("symbol") or "").upper()
        if not sym:
            continue
        tick = step = min_qty = min_notional = 0.0
        for f in s.get("filters", []):
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                tick = _to_float(f.get("tickSize"), 0.0)
            elif t == "LOT_SIZE":
                step = _to_float(f.get("stepSize"), 0.0)
                min_qty = _to_float(f.get("minQty"), 0.0)
            elif t == "MIN_NOTIONAL":
                min_notional = _to_float(f.get("minNotional"), 0.0)
        EXINFO_CACHE[sym] = SymbolFilters(tick_size=tick, step_size=step, min_qty=min_qty, min_notional=min_notional)

    logging.info("Loaded exchangeInfo for %d symbols", len(EXINFO_CACHE))

async def book_bid_price(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    data = await binance_request(session, "GET", "/api/v3/ticker/bookTicker", params={"symbol": symbol}, signed=False)
    bid = _to_float(data.get("bidPrice"), 0.0)
    return bid if bid > 0 else None

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
    """
    Atomicky si "claimne" 1 řádek CLOSING (FOR UPDATE SKIP LOCKED) a nastaví exit_order_status=PROCESSING.
    Tím zabráníme tomu, aby si dvě instance vzaly stejnou práci.
    """
    sql = """
    WITH cte AS (
      SELECT symbol
      FROM positions_open
      WHERE status='CLOSING'
        AND (exit_filled_at IS NULL)
        AND (exit_order_status IS DISTINCT FROM 'PROCESSING')
      ORDER BY close_requested_at NULLS LAST, updated_at
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    UPDATE positions_open p
    SET exit_order_status='PROCESSING', updated_at=NOW()
    FROM cte
    WHERE p.symbol = cte.symbol
    RETURNING p.*;
    """
    async with pool.acquire() as conn:
        return await conn.fetchrow(sql)

async def db_set_order_placed(
    pool: asyncpg.Pool,
    symbol: str,
    order_id: str,
    limit_price: float,
    status: str,
) -> None:
    sql = """
    UPDATE positions_open
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
    sql = """
    UPDATE positions_open
    SET exit_order_status=$2, updated_at=NOW()
    WHERE symbol=$1 AND status='CLOSING'
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, symbol, status)

async def db_inc_retries(pool: asyncpg.Pool, symbol: str) -> int:
    sql = """
    UPDATE positions_open
    SET exit_retries = COALESCE(exit_retries,0) + 1, updated_at=NOW()
    WHERE symbol=$1 AND status='CLOSING'
    RETURNING exit_retries
    """
    async with pool.acquire() as conn:
        r = await conn.fetchrow(sql, symbol)
    return int(r["exit_retries"]) if r and r["exit_retries"] is not None else 0

async def db_set_fill(pool: asyncpg.Pool, symbol: str, fill_price: float) -> None:
    sql = """
    UPDATE positions_open
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
        return await conn.fetchrow("SELECT * FROM positions_open WHERE symbol=$1", symbol)

async def insert_trade_log(pool: asyncpg.Pool, pos: asyncpg.Record) -> None:
    entry_price = float(pos["entry_price"])
    exit_price = float(pos["exit_fill_price"])
    gross_pct = (exit_price - entry_price) / entry_price * 100.0 if entry_price > 0 else 0.0
    net_pct = gross_pct - FEE_ROUNDTRIP_PCT

    sql = """
    INSERT INTO trade_log(symbol, entry_time, exit_time, entry_price, exit_price, reason, pnl_pct)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    """
    async with pool.acquire() as conn:
        await conn.execute(
            sql,
            pos["symbol"],
            pos["entry_time"],
            pos["exit_filled_at"],
            entry_price,
            exit_price,
            (pos.get("close_reason") or "UNKNOWN"),
            net_pct,
        )

async def delete_position(pool: asyncpg.Pool, symbol: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM positions_open WHERE symbol=$1", symbol)

# =========================
# ORDER EXECUTION
# =========================

def compute_limit_price(symbol: str, close_ref_price: float, bid_price: Optional[float]) -> float:
    # Pro rychlý SELL fill je nejlepší limit na BID (pokud zapnuto).
    if REPRICE_USING_BOOK_BID and bid_price and bid_price > 0:
        return bid_price
    return close_ref_price

def normalize_qty_price(symbol: str, qty: float, price: float) -> Tuple[float, float]:
    f = EXINFO_CACHE.get(symbol)
    if not f:
        return qty, price

    q = _floor_to_step(qty, f.step_size)
    p = _round_price_to_tick(price, f.tick_size)

    if q < f.min_qty:
        q = 0.0
    return q, p

async def place_limit_sell(session: aiohttp.ClientSession, symbol: str, qty: float, price: float) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": _format_decimal(qty),
        "price": _format_decimal(price),
        "newOrderRespType": "RESULT",
    }
    return await binance_request(session, "POST", "/api/v3/order", params=params, signed=True)

async def place_market_sell(session: aiohttp.ClientSession, symbol: str, qty: float) -> Dict[str, Any]:
    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": _format_decimal(qty),
        "newOrderRespType": "RESULT",
    }
    return await binance_request(session, "POST", "/api/v3/order", params=params, signed=True)

async def get_order(session: aiohttp.ClientSession, symbol: str, order_id: str) -> Dict[str, Any]:
    params = {"symbol": symbol, "orderId": order_id}
    return await binance_request(session, "GET", "/api/v3/order", params=params, signed=True)

async def cancel_order(session: aiohttp.ClientSession, symbol: str, order_id: str) -> Dict[str, Any]:
    params = {"symbol": symbol, "orderId": order_id}
    return await binance_request(session, "DELETE", "/api/v3/order", params=params, signed=True)

def extract_fill_price(order_resp: Dict[str, Any]) -> Optional[float]:
    executed = _to_float(order_resp.get("executedQty"), 0.0)
    cum_quote = _to_float(order_resp.get("cummulativeQuoteQty"), 0.0)
    if executed > 0 and cum_quote > 0:
        return cum_quote / executed
    return None

# =========================
# CORE PROCESSING
# =========================

async def process_position(pool: asyncpg.Pool, session: aiohttp.ClientSession, pos: asyncpg.Record) -> None:
    symbol = (pos["symbol"] or "").upper()
    qty = float(pos.get("qty") or 0.0)
    if qty <= 0:
        logging.error("CLOSING %s but qty missing/invalid -> cannot execute sell", symbol)
        await db_set_order_status(pool, symbol, "ERROR_NO_QTY")
        return

    close_ref = float(pos.get("close_ref_price") or 0.0)
    if close_ref <= 0:
        logging.warning("CLOSING %s close_ref_price missing/invalid, will use bid if enabled", symbol)

    # 1) pokud ještě nebyl order, založ LIMIT SELL
    if not pos.get("exit_order_id"):
        bid = await book_bid_price(session, symbol) if REPRICE_USING_BOOK_BID else None
        base_price = close_ref if close_ref > 0 else (bid or 0.0)
        raw_price = compute_limit_price(symbol, base_price, bid)

        qty2, price2 = normalize_qty_price(symbol, qty, raw_price)
        if qty2 <= 0 or price2 <= 0:
            logging.error("Cannot normalize qty/price for %s: qty=%s price=%s (raw=%s)", symbol, qty2, price2, raw_price)
            await db_set_order_status(pool, symbol, "ERROR_FILTERS")
            return

        logging.warning("PLACE LIMIT SELL %s qty=%s price=%s (ref=%s bid=%s)",
                        symbol, qty2, price2, close_ref, bid)

        resp = await place_limit_sell(session, symbol, qty2, price2)
        order_id = str(resp.get("orderId"))
        status = str(resp.get("status") or "NEW")
        await db_set_order_placed(pool, symbol, order_id, price2, status)
        return

    # 2) jinak kontroluj order
    order_id = str(pos.get("exit_order_id"))
    placed_at = pos.get("exit_order_placed_at")
    retries = int(pos.get("exit_retries") or 0)

    o = await get_order(session, symbol, order_id)
    status = str(o.get("status") or "")
    await db_set_order_status(pool, symbol, status)

    if status == "FILLED":
        fp = extract_fill_price(o) or float(pos.get("exit_order_price") or 0.0)
        if fp <= 0:
            fp = float(pos.get("exit_order_price") or 0.0)

        await db_set_fill(pool, symbol, fp)

        pos2 = await db_load_position(pool, symbol)
        if pos2:
            await insert_trade_log(pool, pos2)
            await delete_position(pool, symbol)

        logging.warning("✅ CLOSED %s FILLED order=%s fill=%s", symbol, order_id, fp)
        return

    if status in ("CANCELED", "REJECTED", "EXPIRED"):
        logging.warning("Order %s status=%s -> retry", symbol, status)
        retries = await db_inc_retries(pool, symbol)

        if retries > MAX_RETRIES and FALLBACK_TO_MARKET:
            qty2, _ = normalize_qty_price(symbol, qty, 1.0)
            logging.warning("FALLBACK MARKET SELL %s qty=%s (retries=%s)", symbol, qty2, retries)
            m = await place_market_sell(session, symbol, qty2)
            fp = extract_fill_price(m) or float(pos.get("exit_order_price") or 0.0)

            # uložíme fill + log
            await db_set_fill(pool, symbol, fp if fp > 0 else float(pos.get("exit_order_price") or 0.0))
            pos2 = await db_load_position(pool, symbol)
            if pos2:
                await insert_trade_log(pool, pos2)
                await delete_position(pool, symbol)

            logging.warning("✅ CLOSED %s MARKET fallback fill=%s", symbol, fp)
        return

    # TTL: když order dlouho nefilluje, zruš a reprice / market fallback
    if placed_at:
        now = datetime.now(timezone.utc)
        age = (now - placed_at).total_seconds()
        if age >= LIMIT_TTL_SEC:
            if retries >= MAX_RETRIES and FALLBACK_TO_MARKET:
                logging.warning("TTL reached. Cancel + MARKET fallback %s order=%s", symbol, order_id)
                try:
                    await cancel_order(session, symbol, order_id)
                except Exception as e:
                    logging.warning("Cancel failed %s: %s", symbol, e)

                await db_inc_retries(pool, symbol)

                qty2, _ = normalize_qty_price(symbol, qty, 1.0)
                m = await place_market_sell(session, symbol, qty2)
                fp = extract_fill_price(m) or float(pos.get("exit_order_price") or 0.0)

                await db_set_fill(pool, symbol, fp if fp > 0 else float(pos.get("exit_order_price") or 0.0))
                pos2 = await db_load_position(pool, symbol)
                if pos2:
                    await insert_trade_log(pool, pos2)
                    await delete_position(pool, symbol)

                logging.warning("✅ CLOSED %s MARKET fallback fill=%s", symbol, fp)
                return

            # cancel + reprice limit
            logging.warning("TTL reached. Cancel + reprice LIMIT %s order=%s age=%.1fs retries=%s",
                            symbol, order_id, age, retries)
            try:
                await cancel_order(session, symbol, order_id)
            except Exception as e:
                logging.warning("Cancel failed %s: %s", symbol, e)

            retries2 = await db_inc_retries(pool, symbol)
            bid = await book_bid_price(session, symbol) if REPRICE_USING_BOOK_BID else None
            base_price = close_ref if close_ref > 0 else (bid or 0.0)
            raw_price = compute_limit_price(symbol, base_price, bid)

            qty2, price2 = normalize_qty_price(symbol, qty, raw_price)
            if qty2 <= 0 or price2 <= 0:
                await db_set_order_status(pool, symbol, "ERROR_REPRICE_FILTERS")
                return

            resp = await place_limit_sell(session, symbol, qty2, price2)
            new_id = str(resp.get("orderId"))
            new_status = str(resp.get("status") or "NEW")
            await db_set_order_placed(pool, symbol, new_id, price2, new_status)

            logging.warning("REPLACED LIMIT %s new_order=%s price=%s retries=%s bid=%s",
                            symbol, new_id, price2, retries2, bid)
            return

# =========================
# MAIN LOOP
# =========================

async def loop(pool: asyncpg.Pool) -> None:
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await load_exchange_info(session)

        while True:
            try:
                pos = await db_claim_one_closing(pool)
                if not pos:
                    await asyncio.sleep(EXECUTOR_POLL_SEC)
                    continue

                await process_position(pool, session, pos)
                await asyncio.sleep(ORDER_CHECK_SEC)

            except Exception as e:
                logging.error("executor error: %s", e)
                await asyncio.sleep(2)

async def main():
    pool = await init_db_pool()
    await loop(pool)

if __name__ == "__main__":
    asyncio.run(main())
