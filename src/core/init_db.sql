-- Enable TimescaleDB Extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 1. Real-Time Market Ticks Table
CREATE TABLE IF NOT EXISTS market_ticks (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    price NUMERIC(12, 4) NOT NULL,
    volume NUMERIC(12, 4) NOT NULL
);

-- Convert to TimescaleDB Hypertable partitioned by time
SELECT create_hypertable('market_ticks', 'timestamp', if_not_exists => TRUE);

-- Index for fast symbol + time range lookups
CREATE INDEX IF NOT EXISTS idx_market_ticks_symbol_time ON market_ticks (symbol, timestamp DESC);

-- 2. Quantitative & News Signals Table
CREATE TABLE IF NOT EXISTS agent_signals (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    agent_id VARCHAR(50) NOT NULL,
    commodity VARCHAR(20) NOT NULL,
    action VARCHAR(10) NOT NULL,
    confidence NUMERIC(5, 4) NOT NULL,
    reasoning TEXT NOT NULL
);

-- 3. Executed Orders Audit Table
CREATE TABLE IF NOT EXISTS execution_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    commodity VARCHAR(20) NOT NULL,
    action VARCHAR(10) NOT NULL,
    quantity NUMERIC(10, 2) NOT NULL,
    order_type VARCHAR(20) NOT NULL,
    stop_loss NUMERIC(12, 4) NOT NULL,
    take_profit NUMERIC(12, 4) NOT NULL,
    synthesized_confidence NUMERIC(5, 4) NOT NULL,
    reasoning TEXT NOT NULL
);