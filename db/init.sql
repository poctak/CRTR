-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ==========================================================
-- KLINES TABLE (1m candles)
-- ==========================================================

CREATE TABLE IF NOT EXISTS klines_1m (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume_quote DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (time, symbol)
);

-- Convert to hypertable (safe if already exists)
SELECT create_hypertable('klines_1m', 'time', if_not_exists => TRUE);

-- Optional index for symbol queries
CREATE INDEX IF NOT EXISTS idx_klines_symbol_time
ON klines_1m (symbol, time DESC);

-- ==========================================================
-- OPEN POSITIONS TABLE
-- ==========================================================

CREATE TABLE IF NOT EXISTS positions_open (
    symbol TEXT PRIMARY KEY,
    entry_time TIMESTAMPTZ NOT NULL,
    entry_price DOUBLE PRECISION NOT NULL,
    sl DOUBLE PRECISION NOT NULL,
    tp DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN', -- OPEN | CLOSING
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_positions_status
ON positions_open (status);

-- ==========================================================
-- TRADE LOG TABLE
-- ==========================================================

CREATE TABLE IF NOT EXISTS trade_log (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    entry_time TIMESTAMPTZ NOT NULL,
    exit_time TIMESTAMPTZ NOT NULL,
    entry_price DOUBLE PRECISION NOT NULL,
    exit_price DOUBLE PRECISION NOT NULL,
    reason TEXT NOT NULL,
    pnl_pct DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trade_log_symbol_time
ON trade_log (symbol, exit_time DESC);

CREATE INDEX IF NOT EXISTS idx_klines_symbol_time_inc_vol
ON klines_1m (symbol, time DESC)
INCLUDE (volume_quote, open, high, low, close);

