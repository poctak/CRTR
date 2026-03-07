#!/usr/bin/env python3
# accumulation_entry_service.py
# ------------------------------------------------------------
# 🔵 ALT breadth risk-on -> create LIMIT BUY intents based on public.accum_signals
#
# Main logic:
#   - PRIMARY GATE: alt_green_ratio breadth regime with hysteresis
#   - SECONDARY GUARD: BTC kill-switch against extreme move
#   - Breadth universe is taken from SYMBOLS env variable
#
# public.accum_signals:
#   id, symbol, ts, signal, details(jsonb)
#
# public.trade_intents:
#   symbol (NOT NULL)
#   ts (NOT NULL)
#   source (NOT NULL)
#   side (NOT NULL)
#   quote_amount (NOT NULL)
#   limit_price (NOT NULL)
#   support_price (nullable)
#   meta (NOT NULL)
#   status (NOT NULL, default 'NEW')
#   entry_mode (NOT NULL, default 'LIMIT')
#
# Idempotency:
#   - accum_signals has no status -> only recent signals (SIGNAL_MAX_AGE_MIN)
#   - skip if there is already NEW/SENT intent for (symbol, source)
#
# DB connection style:
#   DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
# ------------------------------------------------------------

import os
import json
import time
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, Tuple, List

import asyncpg

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ==========================================================
# ENV helpers
# ==========================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v and v.strip() else default
    except Exception:
        return default


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v and v.strip() else default


def env_list(name: str, default: str = "") -> List[str]:
    v = os.getenv(name, default)
    items: List[str] = []
    for x in (v or "").split(","):
        x = x.strip().upper()
        if x:
            items.append(x)
    return items


# ==========================================================
# Config
# ==========================================================
@dataclass
class Config:
    # DB
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    # Market data
    candles_table: str

    # BTC guardrail
    btc_symbol: str
    btc_lookback_bars: int
    btc_kill_dump_pct: float
    btc_kill_pump_pct: float

    # Breadth
    anchor_symbol: str
    alt_green_on: float
    alt_green_off: float
    alt_green_min_alts: int
    alt_green_cache_sec: float
    symbols: List[str]
    alt_exclude_symbols: List[str]

    # Signals
    signal_name: str
    signal_max_age_min: int
    max_signals_per_cycle: int

    # Loop
    poll_sec: int

    # Intent params
    quote_amount: float
    limit_offset_pct: float
    source: str
    side: str


@dataclass
class BreadthState:
    risk_on: bool
    ratio: float
    green: int
    total: int
    anchor_ts: Optional[datetime]
    fetched_at: float


def load_config() -> Config:
    return Config(
        # DB
        db_host=env_str("DB_HOST", "db"),
        db_port=env_int("DB_PORT", 5432),
        db_name=env_str("DB_NAME", "pumpdb"),
        db_user=env_str("DB_USER", "pumpuser"),
        db_password=env_str("DB_PASSWORD", ""),

        # Market data
        candles_table=env_str("CANDLES_TABLE", "public.candles_5m"),

        # BTC guardrail
        btc_symbol=env_str("BTC_SYMBOL", "BTCUSDC"),
        btc_lookback_bars=env_int("BTC_LOOKBACK_BARS", 3),
        btc_kill_dump_pct=env_float("BTC_KILL_DUMP_PCT", -0.008),
        btc_kill_pump_pct=env_float("BTC_KILL_PUMP_PCT", 0.012),

        # Breadth
        anchor_symbol=env_str("ANCHOR_SYMBOL", "BTCUSDC"),
        alt_green_on=env_float("ALT_GREEN_ON", 0.62),
        alt_green_off=env_float("ALT_GREEN_OFF", 0.55),
        alt_green_min_alts=env_int("ALT_GREEN_MIN_ALTS", 30),
        alt_green_cache_sec=env_float("ALT_GREEN_CACHE_SEC", 15.0),
        symbols=env_list("SYMBOLS", ""),
        alt_exclude_symbols=env_list(
            "ALT_EXCLUDE_SYMBOLS",
            "BTCUSDC,USDCUSDT,FDUSDUSDC,TUSDUSDC,USDPUSDC"
        ),

        # Signals
        signal_name=env_str("ACCUM_SIGNAL_NAME", "ACCUM_PHASE"),
        signal_max_age_min=env_int("SIGNAL_MAX_AGE_MIN", 60),
        max_signals_per_cycle=env_int("MAX_SIGNALS_PER_CYCLE", 50),

        # Loop
        poll_sec=env_int("POLL_SEC", 2),

        # Intent params
        quote_amount=env_float("ENTRY_QUOTE_AMOUNT", 10.0),
        limit_offset_pct=env_float("LIMIT_OFFSET_PCT", 0.0),
        source=env_str("INTENT_SOURCE", "ACCUM_BREADTH"),
        side=env_str("INTENT_SIDE", "BUY"),
    )


# ==========================================================
# BTC guard helpers
# ==========================================================
async def fetch_btc_change(pool: asyncpg.Pool, cfg: Config) -> Optional[float]:
    q = f"""
        SELECT ts, c
        FROM {cfg.candles_table}
        WHERE symbol=$1
        ORDER BY ts DESC
        LIMIT $2
    """
    rows = await pool.fetch(q, cfg.btc_symbol, cfg.btc_lookback_bars + 1)
    if len(rows) < cfg.btc_lookback_bars + 1:
        return None

    c_now = float(rows[0]["c"])
    c_then = float(rows[-1]["c"])
    if c_then <= 0:
        return None

    return (c_now / c_then) - 1.0


def btc_kill_switch(delta: float, cfg: Config) -> bool:
    return delta <= cfg.btc_kill_dump_pct or delta >= cfg.btc_kill_pump_pct


# ==========================================================
# Breadth helpers
# ==========================================================
def apply_hysteresis(prev_risk_on: bool, ratio: float, cfg: Config) -> bool:
    if not prev_risk_on:
        return ratio >= cfg.alt_green_on
    return ratio > cfg.alt_green_off


async def db_get_anchor_ts(pool: asyncpg.Pool, cfg: Config) -> Optional[datetime]:
    q = f"""
        SELECT MAX(ts) AS ts
        FROM {cfg.candles_table}
        WHERE symbol=$1
    """
    row = await pool.fetchrow(q, cfg.anchor_symbol)
    return row["ts"] if row and row["ts"] else None


async def db_get_alt_green_ratio(pool: asyncpg.Pool, cfg: Config, ts: datetime) -> Tuple[float, int, int]:
    exclude = [s.upper() for s in cfg.alt_exclude_symbols]
    universe = [s for s in cfg.symbols if s and s not in exclude]

    q = f"""
        SELECT
          COUNT(*) FILTER (WHERE c > o) AS green,
          COUNT(*) AS total
        FROM {cfg.candles_table}
        WHERE ts=$1
          AND symbol = ANY($2::text[])
          AND symbol <> ALL($3::text[])
    """
    row = await pool.fetchrow(q, ts, universe, exclude)

    green = int(row["green"] or 0)
    total = int(row["total"] or 0)
    ratio = (green / total) if total > 0 else 0.0
    return ratio, green, total


# ==========================================================
# Signals
# ==========================================================
async def fetch_recent_accum_signals(pool: asyncpg.Pool, cfg: Config):
    q = """
        SELECT id, symbol, ts, signal, details
        FROM public.accum_signals
        WHERE signal = $1
          AND ts >= NOW() - ($2::int * INTERVAL '1 minute')
        ORDER BY ts ASC
        LIMIT $3
    """
    return await pool.fetch(q, cfg.signal_name, cfg.signal_max_age_min, cfg.max_signals_per_cycle)


def _first_number(d: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for k in keys:
        if k in d and d[k] is not None:
            try:
                return float(d[k])
            except Exception:
                pass
    return None


def extract_prices(details: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    support = _first_number(details, ["support", "support_price", "supportPrice"])
    low = _first_number(details, ["low", "l", "range_low", "rangeLow"])

    if support is None:
        win = details.get("win") or details.get("window")
        try:
            if isinstance(win, str) and ".." in win:
                a = float(win.split("..")[0].strip("[] ()"))
                support = a
            elif isinstance(win, (list, tuple)) and len(win) >= 1:
                support = float(win[0])
        except Exception:
            pass

    return support, low


# ==========================================================
# Intents
# ==========================================================
async def active_intent_exists(pool: asyncpg.Pool, symbol: str, source: str) -> bool:
    q = """
        SELECT 1
        FROM public.trade_intents
        WHERE symbol=$1
          AND source=$2
          AND status IN ('NEW','SENT')
        LIMIT 1
    """
    return (await pool.fetchrow(q, symbol, source)) is not None


async def create_limit_intent(
    pool: asyncpg.Pool,
    *,
    symbol: str,
    ts,
    source: str,
    side: str,
    quote_amount: float,
    limit_price: float,
    support_price: Optional[float],
    meta: Dict[str, Any],
) -> int:
    q = """
        INSERT INTO public.trade_intents
            (symbol, ts, source, side, quote_amount, limit_price, support_price, meta, status, entry_mode)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, 'NEW', 'LIMIT')
        RETURNING id
    """
    row = await pool.fetchrow(
        q,
        symbol,
        ts,
        source,
        side,
        quote_amount,
        limit_price,
        support_price,
        json.dumps(meta),
    )
    return int(row["id"])


# ==========================================================
# Main loop
# ==========================================================
async def run(cfg: Config):
    if not cfg.symbols:
        raise RuntimeError("SYMBOLS is empty; breadth universe cannot be built")

    pool = await asyncpg.create_pool(
        host=cfg.db_host,
        port=cfg.db_port,
        database=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
        min_size=1,
        max_size=5,
    )

    breadth = BreadthState(
        risk_on=False,
        ratio=0.0,
        green=0,
        total=0,
        anchor_ts=None,
        fetched_at=0.0,
    )

    logging.info(
        (
            "ACCUM_ENTRY started | db=%s@%s:%d/%s | source=%s | signal=%s age<=%dmin | "
            "breadth anchor=%s ON/OFF=%.2f/%.2f MIN=%d cache=%.1fs | "
            "BTC guard=%s lookback=%d kill=[<=%.3f%% or >=%.3f%%]"
        ),
        cfg.db_user, cfg.db_host, cfg.db_port, cfg.db_name,
        cfg.source, cfg.signal_name, cfg.signal_max_age_min,
        cfg.anchor_symbol, cfg.alt_green_on, cfg.alt_green_off, cfg.alt_green_min_alts, cfg.alt_green_cache_sec,
        cfg.btc_symbol, cfg.btc_lookback_bars,
        cfg.btc_kill_dump_pct * 100.0,
        cfg.btc_kill_pump_pct * 100.0,
    )

    logging.info(
        "Breadth universe: SYMBOLS (%d symbols), excludes=%d",
        len(cfg.symbols),
        len(cfg.alt_exclude_symbols)
    )

    try:
        while True:
            try:
                now_loop = time.time()

                # --------------------------------------------------
                # 1) Refresh breadth cache
                # --------------------------------------------------
                if (now_loop - breadth.fetched_at) >= cfg.alt_green_cache_sec:
                    anchor_ts = await db_get_anchor_ts(pool, cfg)

                    if anchor_ts is None:
                        logging.warning("BREADTH unavailable | missing anchor ts for %s", cfg.anchor_symbol)
                    else:
                        ratio, green, total = await db_get_alt_green_ratio(pool, cfg, anchor_ts)

                        if total >= cfg.alt_green_min_alts:
                            next_risk_on = apply_hysteresis(breadth.risk_on, ratio, cfg)
                        else:
                            # při malém univerzu držíme předchozí stav,
                            # ale gate níž stejně nové entry zablokuje
                            next_risk_on = breadth.risk_on

                        breadth = BreadthState(
                            risk_on=next_risk_on,
                            ratio=ratio,
                            green=green,
                            total=total,
                            anchor_ts=anchor_ts,
                            fetched_at=now_loop,
                        )

                        logging.info(
                            "BREADTH | anchor=%s risk_on=%s ratio=%.3f (%d/%d) ON=%.2f OFF=%.2f MIN=%d",
                            anchor_ts.isoformat(),
                            breadth.risk_on,
                            breadth.ratio,
                            breadth.green,
                            breadth.total,
                            cfg.alt_green_on,
                            cfg.alt_green_off,
                            cfg.alt_green_min_alts,
                        )

                # --------------------------------------------------
                # 2) Breadth gate = hlavní podmínka
                # --------------------------------------------------
                if breadth.total < cfg.alt_green_min_alts:
                    logging.info(
                        "ENTRY_BLOCKED | breadth insufficient universe | total=%d min=%d",
                        breadth.total, cfg.alt_green_min_alts
                    )
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if not breadth.risk_on:
                    logging.info(
                        "ENTRY_BLOCKED | breadth risk_off | ratio=%.3f (%d/%d)",
                        breadth.ratio, breadth.green, breadth.total
                    )
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                # --------------------------------------------------
                # 3) BTC kill-switch = sekundární guardrail
                # --------------------------------------------------
                btc_delta = await fetch_btc_change(pool, cfg)
                if btc_delta is None:
                    logging.warning("BTC_CHANGE unavailable (need more candles)")
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if btc_kill_switch(btc_delta, cfg):
                    logging.info(
                        "ENTRY_BLOCKED | BTC_KILL_SWITCH | Δ=%+.3f%% | limits=[<=%.3f%% or >=%.3f%%]",
                        btc_delta * 100.0,
                        cfg.btc_kill_dump_pct * 100.0,
                        cfg.btc_kill_pump_pct * 100.0,
                    )
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                # --------------------------------------------------
                # 4) Recent accum signals
                # --------------------------------------------------
                signals = await fetch_recent_accum_signals(pool, cfg)
                if not signals:
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                for s in signals:
                    sid = int(s["id"])
                    sym = str(s["symbol"]).upper()
                    sig_ts = s["ts"]
                    details = dict(s["details"]) if s["details"] is not None else {}

                    if await active_intent_exists(pool, sym, cfg.source):
                        continue

                    support, low = extract_prices(details)
                    base = low if low and low > 0 else (support if support and support > 0 else None)

                    if not base:
                        logging.warning("SKIP_NO_PRICE | %s accum_id=%d (no low/support in details)", sym, sid)
                        continue

                    limit_price = base * (1.0 + cfg.limit_offset_pct)

                    meta = {
                        "regime": "ALT_BREADTH_RISK_ON",
                        "breadth": {
                            "risk_on": breadth.risk_on,
                            "ratio": breadth.ratio,
                            "green": breadth.green,
                            "total": breadth.total,
                            "anchor_ts": breadth.anchor_ts.isoformat() if breadth.anchor_ts else None,
                            "anchor_symbol": cfg.anchor_symbol,
                            "alt_green_on": cfg.alt_green_on,
                            "alt_green_off": cfg.alt_green_off,
                            "alt_green_min_alts": cfg.alt_green_min_alts,
                            "universe_mode": "SYMBOLS",
                            "symbols_count": len(cfg.symbols),
                            "exclude_symbols": cfg.alt_exclude_symbols,
                        },
                        "btc_guard": {
                            "btc_symbol": cfg.btc_symbol,
                            "btc_lookback_bars": cfg.btc_lookback_bars,
                            "btc_change": btc_delta,
                            "kill_dump_pct": cfg.btc_kill_dump_pct,
                            "kill_pump_pct": cfg.btc_kill_pump_pct,
                        },
                        "accum_signal": {
                            "id": sid,
                            "ts": sig_ts.isoformat() if sig_ts else None,
                            "signal": s["signal"],
                            "details": details,
                        },
                        "parsed": {
                            "support": support,
                            "low": low,
                            "base": base,
                            "limit_offset_pct": cfg.limit_offset_pct,
                        },
                    }

                    intent_id = await create_limit_intent(
                        pool,
                        symbol=sym,
                        ts=sig_ts,
                        source=cfg.source,
                        side=cfg.side,
                        quote_amount=cfg.quote_amount,
                        limit_price=limit_price,
                        support_price=support,
                        meta=meta,
                    )

                    logging.warning(
                        "INTENT_NEW_LIMIT | id=%d %s | quote=%.2f | limit=%.8f | breadth=%.3f (%d/%d) | btcΔ=%+.3f%% | accum_id=%d",
                        intent_id,
                        sym,
                        cfg.quote_amount,
                        limit_price,
                        breadth.ratio,
                        breadth.green,
                        breadth.total,
                        btc_delta * 100.0,
                        sid,
                    )

                await asyncio.sleep(cfg.poll_sec)

            except Exception as e:
                logging.exception("LOOP_ERROR: %s", e)
                await asyncio.sleep(3)

    finally:
        await pool.close()


def main():
    cfg = load_config()
    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
