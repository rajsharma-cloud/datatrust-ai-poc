-- =====================================================================================
-- GOLD: simple business views over SILVER (no data is copied).
-- Re-created by gold_layer.py after every successful silver run.
-- =====================================================================================

-- 1. Daily revenue KPI: orders, payments, refunds, net revenue
CREATE OR REPLACE VIEW gold.daily_revenue AS
WITH o AS (
    SELECT business_date,
           COUNT(*)                                            AS total_orders,
           COUNT(*) FILTER (WHERE order_status = 'COMPLETED')  AS completed_orders,
           COUNT(*) FILTER (WHERE order_status = 'CANCELLED')  AS cancelled_orders
    FROM   silver.payment_orders
    GROUP  BY business_date),
p AS (
    SELECT business_date,
           COUNT(*) FILTER (WHERE status = 'CAPTURED')         AS captured_payments,
           COUNT(*) FILTER (WHERE status = 'FAILED')           AS failed_payments,
           SUM(amount) FILTER (WHERE status = 'CAPTURED')      AS gross_revenue
    FROM   silver.payments
    GROUP  BY business_date),
r AS (
    SELECT business_date, COUNT(*) AS refunds, SUM(amount) AS refund_amount
    FROM   silver.refunds
    GROUP  BY business_date)
SELECT o.business_date,
       o.total_orders,
       o.completed_orders,
       o.cancelled_orders,
       COALESCE(p.captured_payments, 0)                               AS captured_payments,
       COALESCE(p.failed_payments, 0)                                 AS failed_payments,
       COALESCE(p.gross_revenue, 0)                                   AS gross_revenue,
       COALESCE(r.refund_amount, 0)                                   AS refund_amount,
       COALESCE(p.gross_revenue, 0) - COALESCE(r.refund_amount, 0)    AS net_revenue,
       ROUND(p.gross_revenue / NULLIF(p.captured_payments, 0), 2)     AS avg_payment_value
FROM   o
LEFT   JOIN p ON p.business_date = o.business_date
LEFT   JOIN r ON r.business_date = o.business_date;

-- 2. Payment method performance (success rate per method)
CREATE OR REPLACE VIEW gold.payment_method_summary AS
SELECT business_date,
       method,
       COUNT(*)                                                       AS attempts,
       COUNT(*) FILTER (WHERE status = 'CAPTURED')                    AS captured,
       COUNT(*) FILTER (WHERE status = 'FAILED')                      AS failed,
       ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'CAPTURED') / COUNT(*), 2) AS success_rate_pct,
       COALESCE(SUM(amount) FILTER (WHERE status = 'CAPTURED'), 0)    AS captured_amount
FROM   silver.payments
GROUP  BY business_date, method;

-- 3. Reconciliation: what the payment gateway settled vs what reached SILVER
CREATE OR REPLACE VIEW gold.settlement_reconciliation AS
WITH p AS (
    SELECT business_date, COUNT(*) AS silver_payments, SUM(amount) AS silver_amount
    FROM   silver.payments
    WHERE  status = 'CAPTURED'
    GROUP  BY business_date),
rej AS (
    SELECT business_date, COUNT(*) AS rejected_payments
    FROM   silver.rejected_records
    WHERE  entity = 'payments'
    GROUP  BY business_date)
SELECT s.business_date,
       s.payments_count                                     AS settled_payments,
       COALESCE(p.silver_payments, 0)                       AS silver_payments,
       s.payments_count - COALESCE(p.silver_payments, 0)    AS payment_difference,
       s.gross_amount                                       AS settled_amount,
       COALESCE(p.silver_amount, 0)                         AS silver_amount,
       s.gross_amount - COALESCE(p.silver_amount, 0)        AS amount_difference,
       COALESCE(rej.rejected_payments, 0)                   AS rejected_payments,
       CASE WHEN s.payments_count = COALESCE(p.silver_payments, 0)
             AND s.gross_amount = COALESCE(p.silver_amount, 0) THEN 'MATCH' ELSE 'MISMATCH' END AS recon_status
FROM   silver.settlements s
LEFT   JOIN p   ON p.business_date = s.business_date
LEFT   JOIN rej ON rej.business_date = s.business_date;

-- 4. Customers: current version only (from the SCD2 table)
CREATE OR REPLACE VIEW gold.customer_current AS
SELECT customer_id, customer_name, email, city, state, region, segment, valid_from AS current_since
FROM   silver.customers
WHERE  is_current;

-- 5. Revenue by region and customer segment (payments joined to the current customer)
CREATE OR REPLACE VIEW gold.revenue_by_segment AS
SELECT p.business_date,
       COALESCE(c.region, 'UNKNOWN')   AS region,
       COALESCE(c.segment, 'UNKNOWN')  AS segment,
       COUNT(*)                        AS captured_payments,
       SUM(p.amount)                   AS captured_amount
FROM   silver.payments p
LEFT   JOIN silver.customers c ON c.customer_id = p.customer_id AND c.is_current
WHERE  p.status = 'CAPTURED'
GROUP  BY p.business_date, COALESCE(c.region, 'UNKNOWN'), COALESCE(c.segment, 'UNKNOWN');

-- 6. Product catalog: current product version with its category
CREATE OR REPLACE VIEW gold.product_catalog AS
SELECT p.product_id, p.title, p.price, p.category_id, c.category_name, p.valid_from AS price_since
FROM   silver.products p
LEFT   JOIN silver.categories c ON c.category_id = p.category_id
WHERE  p.is_current;

-- 7. Pipeline status per business date (latest batch): for the operations team
CREATE OR REPLACE VIEW gold.pipeline_status AS
WITH latest AS (
    SELECT DISTINCT ON (business_date) business_date, batch_id
    FROM   audit.batch_control
    WHERE  layer = 'BRONZE'
    ORDER  BY business_date, started_at DESC)
SELECT l.business_date,
       l.batch_id,
       MAX(b.load_mode)                                           AS bronze_load_mode,
       MAX(CASE WHEN b.layer = 'BRONZE' THEN b.status END)        AS bronze_status,
       MAX(CASE WHEN b.layer = 'SILVER' THEN b.status END)        AS silver_status,
       MAX(CASE WHEN b.layer = 'GOLD'   THEN b.status END)        AS gold_status,
       MAX(CASE WHEN b.layer = 'SILVER' THEN b.rows_rejected END) AS rows_rejected,
       MAX(b.ended_at)                                            AS finished_at
FROM   latest l
JOIN   audit.batch_control b ON b.batch_id = l.batch_id
GROUP  BY l.business_date, l.batch_id;

-- 8. Pipeline errors: every failure with batch, pipeline step and SQL error code (newest first)
CREATE OR REPLACE VIEW gold.pipeline_errors AS
SELECT error_id, logged_at, batch_id, business_date, layer, pipeline_id, status,
       error_type, sql_state, error_message
FROM   audit.error_log;

-- Every step of every run (SUCCESS and FAILED) with the exact error message; newest first
CREATE OR REPLACE VIEW gold.pipeline_steps AS
SELECT step_id, batch_id, COALESCE(business_date::text, 'ALL DATES') AS business_date, layer, pipeline_id,
       status, rows_in, rows_out, rows_rejected, duration_sec, message, error_type, error_message,
       started_at, ended_at
FROM   audit.step_log;