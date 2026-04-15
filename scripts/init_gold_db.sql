-- Gold layer schema: Star schema for stock price analytics
-- Runs automatically when the PostgreSQL container first starts

CREATE DATABASE gold;

\c gold;

-- ── Dimension: ticker ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_ticker (
    ticker_key      SERIAL PRIMARY KEY,
    ticker_symbol   VARCHAR(10)  NOT NULL UNIQUE,
    company_name    VARCHAR(255),
    sector          VARCHAR(100),
    industry        VARCHAR(100),
    exchange        VARCHAR(50),
    currency        VARCHAR(10),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Dimension: date ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT PRIMARY KEY,   -- YYYYMMDD integer
    full_date       DATE NOT NULL,
    year            SMALLINT,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(10),
    week_of_year    SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    is_weekend      BOOLEAN,
    is_month_end    BOOLEAN,
    is_quarter_end  BOOLEAN
);

-- ── Fact: daily stock prices ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_stock_prices (
    price_key       BIGSERIAL PRIMARY KEY,
    date_key        INT          NOT NULL REFERENCES dim_date(date_key),
    ticker_key      INT          NOT NULL REFERENCES dim_ticker(ticker_key),
    open_price      NUMERIC(12,4),
    high_price      NUMERIC(12,4),
    low_price       NUMERIC(12,4),
    close_price     NUMERIC(12,4),
    adj_close_price NUMERIC(12,4),
    volume          BIGINT,
    daily_return    NUMERIC(10,6),   -- pct change from previous close
    price_range     NUMERIC(12,4),   -- high - low
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (date_key, ticker_key)
);

-- ── Indexes ───────────────────────────────────────────────────────────────
CREATE INDEX idx_fact_date_key    ON fact_stock_prices(date_key);
CREATE INDEX idx_fact_ticker_key  ON fact_stock_prices(ticker_key);
CREATE INDEX idx_fact_close_price ON fact_stock_prices(close_price);

-- ── Data quality audit log ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_audit_log (
    run_id          VARCHAR(50),
    layer           VARCHAR(20),   -- bronze / silver / gold
    ticker          VARCHAR(10),
    run_date        DATE,
    records_read    INT,
    records_written INT,
    records_rejected INT,
    status          VARCHAR(20),   -- success / failed / partial
    error_message   TEXT,
    duration_seconds NUMERIC(10,2),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
