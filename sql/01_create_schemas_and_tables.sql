-- =====================================================================================
-- DataTrust Medallion POC - one PostgreSQL database, layer schemas BRONZE / SILVER / GOLD
-- plus AUDIT for run logs. Run once:  python run_pipeline.py --setup
--
--   BRONZE : tables only. Created automatically by bronze_layer.py from the CSV header
--            (all columns TEXT = data exactly as delivered + batch_id, source_file,
--            ingested_at, load_mode). First load FULL, then APPEND.
--   SILVER : tables only (below). Cleaned, typed, de-duplicated. SCD1 = latest version,
--            SCD2 = full history (valid_from / valid_to / is_current).
--   GOLD   : views only, over SILVER (sql/02_gold_views.sql).
-- =====================================================================================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

-- ------------------------------------------------------------------ AUDIT
CREATE TABLE IF NOT EXISTS audit.batch_control (
    batch_id       TEXT,
    business_date  DATE,
    layer          TEXT,          -- BRONZE / SILVER / GOLD
    status         TEXT,          -- SUCCESS / FAILED / SKIPPED
    load_mode      TEXT,          -- bronze: FULL (first load) or APPEND
    rows_read      BIGINT,
    rows_written   BIGINT,
    rows_rejected  BIGINT,
    message        TEXT,
    started_at     TIMESTAMP,
    ended_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit.error_log (
    error_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id       TEXT,          -- NULL when the error happened before a batch existed (setup, upload)
    business_date  DATE,
    layer          TEXT,          -- RUN / SETUP / ADLS / BRONZE / SILVER / GOLD
    pipeline_id    TEXT,          -- which pipeline / step: PL_BRONZE_PAYMENTS, PL_SILVER_REFUNDS, PL_GOLD_VIEWS ...
    status         TEXT,          -- FAILED, CANCELLED (Ctrl+C) or WARNING (data quality issue, run continued)
    error_type     TEXT,          -- Python exception class, e.g. UndefinedColumn, AnalysisException
    sql_state      TEXT,          -- PostgreSQL error code when it is a database error, e.g. 42703
    error_message  TEXT,
    stack_trace    TEXT,          -- full Python traceback: file and line where it failed
    logged_at      TIMESTAMP DEFAULT now()
);
-- upgrade an error_log created by an earlier version of this kit
ALTER TABLE audit.error_log ADD COLUMN IF NOT EXISTS pipeline_id TEXT;
ALTER TABLE audit.error_log ADD COLUMN IF NOT EXISTS status      TEXT;
ALTER TABLE audit.error_log ADD COLUMN IF NOT EXISTS sql_state   TEXT;
ALTER TABLE audit.error_log ADD COLUMN IF NOT EXISTS stack_trace TEXT;

-- One row per pipeline STEP of every run: SUCCESS and FAILURE alike (each file, each entity, each check).
-- Failed steps carry the exact error message here; the full stack trace is in audit.error_log.
CREATE TABLE IF NOT EXISTS audit.step_log (
    step_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id       TEXT,
    business_date  DATE,          -- NULL = full load (all dates)
    layer          TEXT,          -- BRONZE / SILVER / GOLD
    pipeline_id    TEXT,          -- PL_BRONZE_PAYMENTS, PL_SILVER_CUSTOMERS, PL_GOLD_VALIDATE ...
    status         TEXT,          -- SUCCESS / FAILED / CANCELLED / SKIPPED
    rows_in        BIGINT,
    rows_out       BIGINT,
    rows_rejected  BIGINT,
    message        TEXT,          -- what the step did (SUCCESS) or why it was skipped
    error_type     TEXT,          -- FAILED / CANCELLED: exception class
    error_message  TEXT,          -- FAILED / CANCELLED: exact error message
    started_at     TIMESTAMP,
    ended_at       TIMESTAMP,
    duration_sec   NUMERIC(10,2)
);

-- ------------------------------------------------------------------ SILVER (SCD1)
CREATE TABLE IF NOT EXISTS silver.payment_orders (
    order_id        TEXT PRIMARY KEY,
    receipt         TEXT,
    customer_id     TEXT,
    business_date   DATE,
    created_at      TIMESTAMP,
    amount          NUMERIC(14,2),
    amount_paid     NUMERIC(14,2),
    amount_due      NUMERIC(14,2),
    currency        TEXT,
    payment_status  TEXT,         -- PAID / CREATED
    attempts        INTEGER,
    channel         TEXT,
    order_status    TEXT,         -- COMPLETED / CANCELLED
    batch_id        TEXT,
    loaded_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.payments (
    payment_id      TEXT PRIMARY KEY,
    order_id        TEXT,
    customer_id     TEXT,
    business_date   DATE,
    created_at      TIMESTAMP,
    captured_at     TIMESTAMP,
    amount          NUMERIC(14,2),
    currency        TEXT,
    status          TEXT,         -- CAPTURED / FAILED
    method          TEXT,         -- UPI / CARD / WALLET / NETBANKING
    card_network    TEXT,
    bank            TEXT,
    wallet          TEXT,
    email           TEXT,
    contact_masked  TEXT,         -- phone number masked in silver (PII)
    fee             NUMERIC(14,2),
    tax             NUMERIC(14,2),
    amount_refunded NUMERIC(14,2),
    error_code      TEXT,
    error_reason    TEXT,
    batch_id        TEXT,
    loaded_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.refunds (
    refund_id       TEXT PRIMARY KEY,
    payment_id      TEXT,
    order_id        TEXT,
    business_date   DATE,
    created_at      TIMESTAMP,
    processed_at    TIMESTAMP,
    amount          NUMERIC(14,2),
    currency        TEXT,
    status          TEXT,
    speed           TEXT,
    reason          TEXT,
    batch_id        TEXT,
    loaded_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.settlements (
    settlement_id       TEXT PRIMARY KEY,
    business_date       DATE,
    settled_at          TIMESTAMP,
    payments_count      INTEGER,
    gross_amount        NUMERIC(14,2),
    refunds             NUMERIC(14,2),
    fees                NUMERIC(14,2),
    tax                 NUMERIC(14,2),
    net_settled_amount  NUMERIC(14,2),
    currency            TEXT,
    status              TEXT,
    utr                 TEXT,
    batch_id            TEXT,
    loaded_at           TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.categories (
    category_id     INTEGER PRIMARY KEY,
    category_name   TEXT,
    slug            TEXT,
    image_url       TEXT,
    updated_at      TIMESTAMP,
    batch_id        TEXT,
    loaded_at       TIMESTAMP
);

-- ------------------------------------------------------------------ SILVER (SCD2: full history)
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_sk     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,   -- surrogate key
    customer_id     TEXT,                                              -- business key
    customer_name   TEXT,
    email           TEXT,
    city            TEXT,
    state           TEXT,
    region          TEXT,
    segment         TEXT,
    record_hash     TEXT,          -- hash of the tracked columns: changes -> new version
    valid_from      TIMESTAMP,
    valid_to        TIMESTAMP,     -- NULL for the current version
    is_current      BOOLEAN,
    batch_id        TEXT,
    loaded_at       TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_customers_current ON silver.customers (customer_id) WHERE is_current;

CREATE TABLE IF NOT EXISTS silver.products (
    product_sk      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id      INTEGER,
    title           TEXT,
    slug            TEXT,
    price           NUMERIC(12,2),
    category_id     INTEGER,
    image_url       TEXT,
    record_hash     TEXT,
    valid_from      TIMESTAMP,
    valid_to        TIMESTAMP,
    is_current      BOOLEAN,
    batch_id        TEXT,
    loaded_at       TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_products_current ON silver.products (product_id) WHERE is_current;

-- ------------------------------------------------------------------ SILVER (quarantine)
CREATE TABLE IF NOT EXISTS silver.rejected_records (
    entity          TEXT,          -- payments, payment_orders, ...
    record_key      TEXT,
    business_date   DATE,
    rule_name       TEXT,          -- which data quality rule failed
    raw_record      TEXT,          -- the bronze row as JSON
    batch_id        TEXT,
    rejected_at     TIMESTAMP
);