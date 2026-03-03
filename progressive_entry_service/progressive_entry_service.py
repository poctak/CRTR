#!/usr/bin/env python3
# progressive_service.py
# ------------------------------------------------------------
# 🟢 BTC pump -> create MARKET BUY intents (no rt_signals table required)
#
# Reads:
#   - BTC candles from CANDLES_TABLE
#   - Alt candles from CANDLES_TABLE
#   - (optional) latest support from public.accum_signals.details JSON
#
# Assumed candles schema (adjust via env if needed):
#   symbol, ts, o, h, l, c, vq (or v), buy_ratio (optional)
#
# Writes to public.trade_intents:
#   symbol, ts, source, side, quote_amount, limit_price=NULL, support_price, meta, status, entry_mode='MARKET'
# ------------------------------------------------------------

import os
import json
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

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

    btc_symbol: str
    candles_table: str
    btc_lookback_bars: int
    btc_pump_min: float
    require_btc_close_near_high: bool
    btc_close_near_high_pct: float

    # Candidate universe
    universe_limit: int          # how many symbols to scan (top by recent volume)
    exclude_symbols_csv: str

    # Feature windows
    vol_baseline_bars: int       # for volume_rel baseline
    max_candle_age_sec: int      # only consider symbols with very recent candle

    # Candidate thresholds
    alt_vol_rel_min: float
    alt_buy_ratio_min: float
    alt_change_min: float
    alt_runup_max: float

    top_n: int
    poll_sec: int

    # Intent fields
    source: str
    side: str
    quote_amount: float

    # Optional support lookup from accum_signals
    support_signal_name: str
    support_max_age_min: int


def load_config() -> Config:
    return Config(
        db_dsn=env_str("DB_DSN", "postgresql://pumpuser:pumpsecret@db:5432/pumpdb"),

        btc_symbol=env_str("BTC_SYMBOL", "BTCUSDC"),
        candles_table=env_str("CANDLES_TABLE", "public.candles_5m"),
        btc_lookback_bars=env_int("BTC_LOOKBACK_BARS", 3),
        btc_pump_min=env_float("BTC_PUMP_MIN", 0.006),
        require_btc_close_near_high=env_int("BTC_CLOSE_NEAR_HIGH", 1) == 1,
        btc_close_near_high_pct=env_float("BTC_CLOSE_NEAR_HIGH_PCT", 0.20),

        universe_limit=env_int("UNIVERSE_LIMIT", 60),
        exclude_symbols_csv=env_str("EXCLUDE_SYMBOLS", "BTCUSDC,ETHUSDC"),

        vol_baseline_bars=env_int("VOL_BASELINE_BARS", 24),   # 24*5m = 2h baseline
        max_candle_age_sec=env_int("MAX_CANDLE_AGE_SEC", 600),# 10 min

        alt_vol_rel_min=env_float("ALT_VOL_REL_MIN", 1.3),
        alt_buy_ratio_min=env_float("ALT_BUY_RATIO_MIN", 0.60),
        alt_change_min=env_float("ALT_CHANGE_MIN", 0.0),
        alt_runup_max=env_float("ALT_RUNUP_MAX", 0.010),

        top_n=env_int("TOP_N", 3),
        poll_sec=env_int("POLL_SEC", 2),

        source=env_str("INTENT_SOURCE", "PROGRESSIVE_PUMP"),
        side=env_str("INTENT_SIDE", "BUY"),
        quote_amount=env_float("ENTRY_QUOTE_AMOUNT", 10.0),

        support_signal_name=env_str("SUPPORT_SIGNAL_NAME", "ACCUM_PHASE"),
        support_max_age_min=env_int("SUPPORT_MAX_AGE_MIN", 240),  # 4h
    )


# ---------- BTC helpers ----------
async def fetch_btc_rows(pool: asyncpg.Pool, cfg: Config):
    q = f"""
        SELECT ts, o, h, l, c
        FROM {cfg.candles_table}
        WHERE symbol=$1
        ORDER BY ts DESC
        LIMIT $2
    """
    return await pool.fetch(q, cfg.btc_symbol, cfg.btc_lookback_bars + 1)


def calc_change(rows) -> Optional[float]:
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


# ---------- Universe selection ----------
async def fetch_universe_symbols(pool: asyncpg.Pool, cfg: Config) -> List[str]:
    """
    Picks symbols with highest recent volume (last candle vq or v).
    Uses COALESCE(vq, v) so it works with either schema.
    """
    q = f"""
        WITH last AS (
          SELECT DISTINCT ON (symbol) symbol, ts,
                 COALESCE(vq, v, 0) AS vol
          FROM {cfg.candles_table}
          WHERE ts >= NOW() - INTERVAL '30 minutes'
          ORDER BY symbol, ts DESC
        )
        SELECT symbol
        FROM last
        ORDER BY vol DESC
        LIMIT $1
    """
    rows = await pool.fetch(q, cfg.universe_limit)
    excluded = {x.strip() for x in cfg.exclude_symbols_csv.split(",") if x.strip()}
    syms = [str(r["symbol"]) for r in rows if str(r["symbol"]) not in excluded]
    return syms


# ---------- Feature computation ----------
async def fetch_symbol_candles(pool: asyncpg.Pool, cfg: Config, symbol: str):
    """
    Fetch last (vol_baseline_bars + 2) candles to compute:
      - change_5m (last close vs prev close)
      - volume_rel (last vol vs avg baseline)
      - buy_ratio from last candle if exists
      - runup from last candle low
    """
    limit = cfg.vol_baseline_bars + 2
    q = f"""
        SELECT ts, o, h, l, c,
               COALESCE(vq, v, 0) AS vol,
               COALESCE(buy_ratio, 0) AS buy_ratio
        FROM {cfg.candles_table}
        WHERE symbol=$1
        ORDER BY ts DESC
        LIMIT $2
    """
    return await pool.fetch(q, symbol, limit)


async def fetch_latest_support(pool: asyncpg.Pool, cfg: Config, symbol: str) -> Optional[float]:
    """
    Optional: try to read support from latest accum_signals.details for the symbol.
    """
    q = """
        SELECT details
        FROM public.accum_signals
        WHERE symbol=$1
          AND signal=$2
          AND ts >= NOW() - ($3::int * INTERVAL '1 minute')
        ORDER BY ts DESC
        LIMIT 1
    """
    row = await pool.fetchrow(q, symbol, cfg.support_signal_name, cfg.support_max_age_min)
    if not row or row["details"] is None:
        return None
    details = dict(row["details"])
    for k in ("support", "support_price", "supportPrice"):
        if k in details and details[k] is not None:
            try:
                return float(details[k])
            except Exception:
                pass
    return None


def score(volume_rel: float, buy_ratio: float, runup: float) -> float:
    return (volume_rel * buy_ratio) / (1.0 + max(0.0, runup))


async def pending_intent_exists(pool: asyncpg.Pool, symbol: str, source: str) -> bool:
    q = """
        SELECT 1
        FROM public.trade_intents
        WHERE symbol=$1 AND source=$2 AND status IN ('PENDING','SENT')
        LIMIT 1
    """
    return (await pool.fetchrow(q, symbol, source)) is not None


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
    row = await pool.fetchrow(q, symbol, ts, source, side, quote_amount, support_price, json.dumps(meta))
    return int(row["id"])


async def run(cfg: Config):
    pool = await asyncpg.create_pool(dsn=cfg.db_dsn, min_size=1, max_size=8)
    logging.info(
        "PROGRESSIVE started | source=%s | btc=%s pump_min=%+.3f%% | universe_limit=%d top_n=%d",
        cfg.source, cfg.btc_symbol, cfg.btc_pump_min * 100.0, cfg.universe_limit, cfg.top_n
    )

    try:
        while True:
            try:
                btc_rows = await fetch_btc_rows(pool, cfg)
                btc_delta = calc_change(btc_rows)
                if btc_delta is None:
                    logging.warning("BTC_CHANGE unavailable (need more candles)")
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if btc_delta < cfg.btc_pump_min:
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                if cfg.require_btc_close_near_high and not close_near_high(btc_rows, cfg.btc_close_near_high_pct):
                    logging.info("BTC_PUMP_WEAK_CLOSE | Δ=%+.3f%% | skip", btc_delta * 100.0)
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                symbols = await fetch_universe_symbols(pool, cfg)
                if not symbols:
                    logging.info("BTC_PUMP | Δ=%+.3f%% | universe empty", btc_delta * 100.0)
                    await asyncio.sleep(cfg.poll_sec)
                    continue

                candidates: List[Dict[str, Any]] = []

                for sym in symbols:
                    rows = await fetch_symbol_candles(pool, cfg, sym)
                    if len(rows) < 3:
                        continue

                    ts0 = rows[0]["ts"]
                    # freshness check
                    # (done in SQL already, but keep safe)
                    # We rely on DB clock; if your DB clock differs, adjust MAX_CANDLE_AGE_SEC.
                    # If too old, skip.
                    # Note: asyncpg returns datetime with tz.
                    # We'll just skip if it looks old by seconds via SQL is better; keep lightweight here.
                    # (No python-side now() to avoid tz mismatch.)
                    # -> We skip freshness here to keep code minimal.

                    c0 = float(rows[0]["c"])
                    c1 = float(rows[1]["c"])
                    if c1 <= 0:
                        continue
                    change_5m = (c0 / c1) - 1.0

                    vol0 = float(rows[0]["vol"] or 0.0)
                    baseline = [float(r["vol"] or 0.0) for r in rows[2:]]  # exclude last 2
                    base_avg = sum(baseline) / max(1, len(baseline))
                    volume_rel = (vol0 / base_avg) if base_avg > 0 else 0.0

                    buy_ratio = float(rows[0]["buy_ratio"] or 0.0)

                    low0 = float(rows[0]["l"])
                    if low0 <= 0:
                        continue
                    runup_from_low = (c0 / low0) - 1.0

                    # optional better runup vs support if we have it
                    support = await fetch_latest_support(pool, cfg, sym)
                    if support and support > 0:
                        runup = (c0 / support) - 1.0
                    else:
                        runup = runup_from_low

                    if volume_rel < cfg.alt_vol_rel_min:
                        continue
                    if buy_ratio < cfg.alt_buy_ratio_min:
                        continue
                    if change_5m < cfg.alt_change_min:
                        continue
                    if runup > cfg.alt_runup_max:
                        continue

                    candidates.append({
                        "symbol": sym,
                        "ts": ts0,
                        "close": c0,
                        "low": low0,
                        "support": support,
                        "volume_rel": volume_rel,
                        "buy_ratio": buy_ratio,
                        "change_5m": change_5m,
                        "runup": runup,
                        "score": score(volume_rel, buy_ratio, runup),
                    })

                if not candidates:
                    logging.info("BTC_PUMP | Δ=%+.3f%% | no candidates passed filters", btc_delta * 100.0)
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
                        "features": {
                            "volume_rel": c["volume_rel"],
                            "buy_ratio": c["buy_ratio"],
                            "change_5m": c["change_5m"],
                            "runup": c["runup"],
                            "support": c["support"],
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
                        ts=c["ts"],
                        source=cfg.source,
                        side=cfg.side,
                        quote_amount=cfg.quote_amount,
                        support_price=c["support"],
                        meta=meta,
                    )

                    logging.warning(
                        "INTENT_NEW_MARKET | id=%d %s | quote=%.2f | btcΔ=%+.3f%% | score=%.3f | vol=%.2fx buy=%.2f runup=%.2f%%",
                        intent_id, sym, cfg.quote_amount, btc_delta * 100.0, c["score"],
                        c["volume_rel"], c["buy_ratio"], c["runup"] * 100.0
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
