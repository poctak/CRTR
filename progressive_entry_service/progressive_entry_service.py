#!/usr/bin/env python3
# progressive_service.py
# ------------------------------------------------------------
# 🟢 BTC pump -> create MARKET BUY intents for best candidates (top N)
# 🔵 BTC neutral -> do nothing (handled by accumulation_entry_service)
# 🔴 BTC dump -> do nothing (skip)
#
# Requires trade_intents columns:
#   symbol, ts, source, side, quote_amount, limit_price, support_price, meta, status, entry_mode
#
# Candidates are read from RT_TABLE (default public.rt_signals) with columns:
#   symbol text
#   ts timestamptz
#   close double precision
#   low double precision
#   support double precision NULL
#   volume_rel double precision
#   buy_ratio double precision
#   change_5m double precision
#   runup_from_support double precision
#
# Selection:
#   - BTC pump: btc_change_15m >= BTC_PUMP_MIN
#   - Filter candidates by thresholds
#   - Score: (volume_rel * buy_ratio) / (1 + runup)
#   - Take TOP_N and create MARKET intents (limit_price NULL)
# ------------------------------------------------------------

import os
import json
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

import asyncpg

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v and v.strip() else default

def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v and v.strip() else default


@dataclass
class Config:
    db_dsn: str

    # BTC pump detection
    btc_symbol: str
    candles_table: str
    btc_lookback_bars: int
    btc_pump_min: float
    require_close_near_high: bool
    close_near_high_pct: float

    # Candidates
    rt_table: str
    rt_fresh_sec: int
    top_n: int

    # Candidate thresholds
    alt_vol_rel_min: float
    alt_buy_ratio_min: float
    alt_change_min: float
    alt_runup_max: float

    # Polling
    poll_sec: int

    # Intent fields
    source: str
    side: str
    quote_amount: float


def load_config() -> Config:
    return Config(
        db_dsn=env_str("DB_DSN", "postgresql://pumpuser:pumpsecret@db:5432/pumpdb"),

        btc_symbol=env_str("BTC_SYMBOL", "BTCUSDC"),
        candles_table=env_str("CANDLES_TABLE", "public.candles_5m"),
        btc_lookback_bars=env_int("BTC_LOOKBACK_BARS", 3),

        btc_pump_min=env_float("BTC_PUMP_MIN", 0.006),
        require_close_near_high=env_int("BTC_CLOSE_NEAR_HIGH", 1) == 1,
        close_near_high_pct=env_float("BTC_CLOSE_NEAR_HIGH_PCT", 0.20),

        rt_table=env_str("RT_TABLE", "public.rt_signals"),
        rt_fresh_sec=env_int("RT_FRESH_SEC", 180),
        top_n=env_int("TOP_N", 3),

        alt_vol_rel_min=env_float("ALT_VOL_REL_MIN", 1.3),
        alt_buy_ratio_min=env_float("ALT_BUY_RATIO_MIN", 0.60),
        alt_change_min=env_float("ALT_CHANGE_MIN", 0.0),
        alt_runup_max=env_float("ALT_RUNUP_MAX", 0.010),

        poll_sec=env_int("POLL_SEC", 2),

        source=env_str("INTENT_SOURCE", "PROGRESSIVE_PUMP"),
        side=env_str("INTENT_SIDE", "BUY"),
        quote_amount=env_float("ENTRY_QUOTE_AMOUNT", 10.0),
    )


async def fetch_btc_rows(pool: asyncpg.Pool, cfg: Config):
    q = f"""
        SELECT ts, o, h, l, c
        FROM {cfg.candles_table}
        WHERE symbol=$1
        ORDER BY ts DESC
        LIMIT $2
    """
    return await pool.fetch(q, cfg.btc_symbol, cfg.btc_lookback_bars + 1)


def calc_btc_change(rows) -> Optional[float]:
    if len(rows) < 2:
        return None
    c_now = float(rows[0]["c"])
    c_then = float(rows[-1]["c"])
    if c_then <= 0:
        return None
    return (c_now / c_then) - 1.0


def close_near_high(rows, pct: float) -> bool:
    if not rows:
        return False
    h = float(rows[0]["h"])
    l = float(rows[0]["l"])
    c = float(rows[0]["c"])
    rng = max(1e-12, h - l)
    return c >= (h - pct * rng)


async def pending_intent_exists(pool: asyncpg.Pool, symbol: str, source: str) -> bool:
    q = """
        SELECT 1
        FROM public.trade_intents
        WHERE symbol=$1 AND source=$2 AND status IN ('PENDING','SENT')
        LIMIT 1
    """
    return (await pool.fetchrow(q, symbol, source)) is not None


async def fetch_candidates(pool: asyncpg.Pool, cfg: Config):
    q = f"""
        SELECT symbol, ts, close, low, support, volume_rel, buy_ratio, change_5m, runup_from_support
        FROM {cfg.rt_table}
        WHERE ts >= NOW() - ($1::int * INTERVAL '1 second')
    """
    return await pool.fetch(q, cfg.rt_fresh_sec)


def score(volume_rel: float, buy_ratio: float, runup: float) -> float:
    return (volume_rel * buy_ratio) / (1.0 + max(0.0, runup))


async def create_market_intent(
    pool: asyncpg.Pool,
    *,
    symbol: str,
    ts,
    source: str,
    side: str,
    quote_amount: float,
    support_price: Optional[float],
    meta: Dict[str, Any],
) -> int:
    q = """
        INSERT INTO public.trade_intents
            (symbol, ts, source, side, quote_amount, limit_price, support_price, meta, status, entry_mode)
        VALUES
            ($1, $2, $3, $4, $5, NULL, $6, $7::jsonb, 'PENDING', 'MARKET')
        RETURNING id
    """
    row = await pool.fetchrow(
        q,
        symbol,
        ts,
        source,
        side,
        quote_amount,
        support_price,
        json.dumps(meta),
    )
    return int(row["id"])


async def run(cfg: Config):
    pool = await asyncpg.create_pool(dsn=cfg.db_dsn, min_size=1, max_size=5)
    logging.info(
        "PROGRESSIVE started | source=%s | btc=%s pump_min=%+.3f%% | rt_table=%s top_n=%d",
        cfg.source, cfg.btc_symbol, cfg.btc_pump_min * 100.0, cfg.rt_table, cfg.top_n
    )

    try:
        while True:
            try:
                btc_rows = await fetch_btc_rows(pool, cfg)
                btc_delta = calc_btc_change(btc_rows)
                if btc_delta is None:
                    logging.warning("BTC_CHANGE unavailable (need more candles)")
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if btc_delta < cfg.btc_pump_min:
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if cfg.require_close_near_high and not close_near_high(btc_rows, cfg.close_near_high_pct):
                    logging.info("BTC_PUMP_WEAK_CLOSE | Δ=%+.3f%% | skip", btc_delta * 100.0)
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                rows = await fetch_candidates(pool, cfg)
                if not rows:
                    logging.info("BTC_PUMP | Δ=%+.3f%% | no candidates", btc_delta * 100.0)
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                candidates: List[Dict[str, Any]] = []
                for r in rows:
                    sym = str(r["symbol"])
                    vol_rel = float(r["volume_rel"] or 0.0)
                    buy = float(r["buy_ratio"] or 0.0)
                    chg = float(r["change_5m"] or 0.0)
                    runup = float(r["runup_from_support"] if r["runup_from_support"] is not None else 999.0)
                    sup = float(r["support"]) if r["support"] is not None else None

                    if vol_rel < cfg.alt_vol_rel_min:
                        continue
                    if buy < cfg.alt_buy_ratio_min:
                        continue
                    if chg < cfg.alt_change_min:
                        continue
                    if runup > cfg.alt_runup_max:
                        continue

                    candidates.append({
                        "symbol": sym,
                        "ts": r["ts"],
                        "close": float(r["close"]) if r["close"] is not None else None,
                        "low": float(r["low"]) if r["low"] is not None else None,
                        "support": sup,
                        "volume_rel": vol_rel,
                        "buy_ratio": buy,
                        "change_5m": chg,
                        "runup_from_support": runup,
                        "score": score(vol_rel, buy, runup),
                    })

                if not candidates:
                    logging.info("BTC_PUMP | Δ=%+.3f%% | candidates filtered out", btc_delta * 100.0)
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                candidates.sort(key=lambda x: x["score"], reverse=True)
                top = candidates[: cfg.top_n]

                for c in top:
                    sym = c["symbol"]
                    if await pending_intent_exists(pool, sym, cfg.source):
                        continue

                    meta = {
                        "regime": "BTC_PUMP",
                        "btc_change": btc_delta,
                        "score": c["score"],
                        "candidate": {
                            "close": c["close"],
                            "low": c["low"],
                            "support": c["support"],
                            "volume_rel": c["volume_rel"],
                            "buy_ratio": c["buy_ratio"],
                            "change_5m": c["change_5m"],
                            "runup_from_support": c["runup_from_support"],
                        },
                        "thresholds": {
                            "ALT_VOL_REL_MIN": cfg.alt_vol_rel_min,
                            "ALT_BUY_RATIO_MIN": cfg.alt_buy_ratio_min,
                            "ALT_CHANGE_MIN": cfg.alt_change_min,
                            "ALT_RUNUP_MAX": cfg.alt_runup_max,
                            "BTC_PUMP_MIN": cfg.btc_pump_min,
                        },
                    }

                    intent_id = await create_market_intent(
                        pool,
                        symbol=sym,
                        ts=c["ts"],  # ts = candidate signal time
                        source=cfg.source,
                        side=cfg.side,
                        quote_amount=cfg.quote_amount,
                        support_price=c["support"],
                        meta=meta,
                    )

                    logging.warning(
                        "INTENT_NEW_MARKET | id=%d %s | quote=%.2f | btcΔ=%+.3f%% | score=%.3f | vol=%.2fx buy=%.2f runup=%.2f%%",
                        intent_id,
                        sym,
                        cfg.quote_amount,
                        btc_delta * 100.0,
                        c["score"],
                        c["volume_rel"],
                        c["buy_ratio"],
                        c["runup_from_support"] * 100.0,
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
