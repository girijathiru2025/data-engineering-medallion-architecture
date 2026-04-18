-- Runs automatically when the PostgreSQL container first starts.
-- Creates the gold database (used by dbt) and the audit log table.
-- Note: dim_date, dim_ticker, and fact_stock_prices are managed by dbt in the gold schema.

CREATE DATABASE gold;

\c gold;

-- ── Data quality audit log ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_audit_log (
    run_id          VARCHAR(50),
    layer           VARCHAR(20),
    run_date        DATE,
    status          VARCHAR(20),   -- success / failed / partial
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
