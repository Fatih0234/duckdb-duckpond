-- File Purpose:
-- - What this file is: a practical SQL playbook for profiling, cleaning checks,
--   mart-building, and dashboard queries on the DuckPond demo dataset.
-- - Why it is relevant: it lets you quickly run repeatable analyses in DuckDB CLI
--   or Metabase SQL editor against parquet files in LocalStack S3.
-- - Role in data flow: read staged parquet -> validate quality -> build marts ->
--   power dashboard cards.

-- =============================================================================
-- 0) DUCKDB + LOCALSTACK S3 SETUP
-- =============================================================================
-- Run this setup first in each fresh DuckDB session.
INSTALL httpfs;
LOAD httpfs;

SET s3_region='us-east-1';
SET s3_access_key_id='test';
SET s3_secret_access_key='test';
SET s3_endpoint='localhost:4566';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- Convenience views to shorten later queries.
CREATE OR REPLACE VIEW stg_customers AS
SELECT * FROM read_parquet('s3://datalake/stg_customers.parquet');

CREATE OR REPLACE VIEW stg_orders AS
SELECT * FROM read_parquet('s3://datalake/stg_orders.parquet');

CREATE OR REPLACE VIEW stg_payments AS
SELECT * FROM read_parquet('s3://datalake/stg_payments.parquet');

-- =============================================================================
-- 1) QUICK PROFILE ("WHAT DO WE HAVE?")
-- =============================================================================

-- Row counts for top KPI cards.
SELECT 'customers' AS metric, COUNT(*)::BIGINT AS value FROM stg_customers
UNION ALL
SELECT 'orders' AS metric, COUNT(*)::BIGINT AS value FROM stg_orders
UNION ALL
SELECT 'payments' AS metric, COUNT(*)::BIGINT AS value FROM stg_payments;

-- Distinct customer coverage in orders.
SELECT
    COUNT(DISTINCT customer_id) AS customers_with_orders,
    COUNT(*) AS total_orders
FROM stg_orders;

-- Order status distribution (great pie chart input).
SELECT
    status,
    COUNT(*) AS order_count
FROM stg_orders
GROUP BY status
ORDER BY order_count DESC;

-- Payment method totals (great bar chart input).
SELECT
    payment_method,
    COUNT(*) AS payment_events,
    ROUND(SUM(amount), 2) AS total_amount
FROM stg_payments
GROUP BY payment_method
ORDER BY total_amount DESC;

-- =============================================================================
-- 2) DATA QUALITY CHECKS ("CAN WE TRUST THIS?")
-- =============================================================================

-- 2.1 Null key checks.
SELECT
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids
FROM stg_customers;

SELECT
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids
FROM stg_orders;

SELECT
    SUM(CASE WHEN payment_id IS NULL THEN 1 ELSE 0 END) AS null_payment_ids,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids
FROM stg_payments;

-- 2.2 Duplicate key checks.
SELECT customer_id, COUNT(*) AS cnt
FROM stg_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

SELECT order_id, COUNT(*) AS cnt
FROM stg_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

SELECT payment_id, COUNT(*) AS cnt
FROM stg_payments
GROUP BY payment_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- 2.3 Referential integrity checks.
-- Payments with missing parent order.
SELECT COUNT(*) AS payments_without_matching_order
FROM stg_payments p
LEFT JOIN stg_orders o ON p.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Orders with missing parent customer.
SELECT COUNT(*) AS orders_without_matching_customer
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- 2.4 Amount sanity checks.
SELECT
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) AS negative_amount_rows,
    SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) AS zero_amount_rows,
    ROUND(MIN(amount), 2) AS min_amount,
    ROUND(MAX(amount), 2) AS max_amount
FROM stg_payments;

-- 2.5 Duplicate order_id in payments with mixed amounts (your specific caveat).
SELECT
    order_id,
    COUNT(*) AS payment_rows,
    ROUND(MIN(amount), 2) AS min_amount,
    ROUND(MAX(amount), 2) AS max_amount,
    ROUND(SUM(amount), 2) AS total_amount
FROM stg_payments
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY payment_rows DESC, order_id;

-- =============================================================================
-- 3) MART/GOLD TABLES ("STRUCTURED FOR ANALYTICS")
-- =============================================================================
-- Best-practice pattern used here:
-- - one clean mart for orders grain (`mart_orders_enriched`)
-- - one customer grain mart (`mart_customers`) for top-customer analysis
-- - explicit date derivations for time-based dashboards

CREATE OR REPLACE VIEW mart_orders_enriched AS
WITH payment_per_order AS (
    SELECT
        order_id,
        ROUND(SUM(amount), 2) AS order_amount
    FROM stg_payments
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.customer_id,
    c.first_name,
    c.last_name,
    CAST(o.order_date AS DATE) AS order_date,
    o.status,
    COALESCE(p.order_amount, 0.0) AS order_amount,
    CASE
        WHEN EXTRACT(dow FROM CAST(o.order_date AS DATE)) IN (0, 6) THEN 'weekend'
        ELSE 'weekday'
    END AS day_type,
    CASE
        WHEN COALESCE(p.order_amount, 0.0) = 0 THEN '0'
        WHEN COALESCE(p.order_amount, 0.0) < 25 THEN '0-24.99'
        WHEN COALESCE(p.order_amount, 0.0) < 50 THEN '25-49.99'
        WHEN COALESCE(p.order_amount, 0.0) < 100 THEN '50-99.99'
        ELSE '100+'
    END AS amount_bracket
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
LEFT JOIN payment_per_order p ON o.order_id = p.order_id;

CREATE OR REPLACE VIEW mart_customers AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT o.order_id) AS number_of_orders,
    ROUND(COALESCE(SUM(p.amount), 0), 2) AS customer_lifetime_value,
    MIN(CAST(o.order_date AS DATE)) AS first_order_date,
    MAX(CAST(o.order_date AS DATE)) AS most_recent_order_date
FROM stg_customers c
LEFT JOIN stg_orders o ON c.customer_id = o.customer_id
LEFT JOIN stg_payments p ON o.order_id = p.order_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- =============================================================================
-- 4) DASHBOARD CARD QUERIES
-- =============================================================================

-- Card 1: total customers.
SELECT COUNT(*) AS total_customers
FROM mart_customers;

-- Card 2: total orders.
SELECT COUNT(*) AS total_orders
FROM mart_orders_enriched;

-- Card 3: orders by status (pie chart).
SELECT
    status,
    COUNT(*) AS order_count
FROM mart_orders_enriched
GROUP BY status
ORDER BY order_count DESC;

-- Card 4: weekend vs weekday (bar/pie).
SELECT
    day_type,
    COUNT(*) AS order_count
FROM mart_orders_enriched
GROUP BY day_type
ORDER BY order_count DESC;

-- Card 5: order volume by date (line chart).
SELECT
    order_date,
    COUNT(*) AS order_count
FROM mart_orders_enriched
GROUP BY order_date
ORDER BY order_date;

-- Card 6: order amount brackets (histogram/bar).
SELECT
    amount_bracket,
    COUNT(*) AS order_count
FROM mart_orders_enriched
GROUP BY amount_bracket
ORDER BY
    CASE amount_bracket
        WHEN '0' THEN 1
        WHEN '0-24.99' THEN 2
        WHEN '25-49.99' THEN 3
        WHEN '50-99.99' THEN 4
        WHEN '100+' THEN 5
        ELSE 99
    END;

-- Card 7: revenue by payment method.
SELECT
    payment_method,
    ROUND(SUM(amount), 2) AS total_amount
FROM stg_payments
GROUP BY payment_method
ORDER BY total_amount DESC;

-- Card 8: top customers by lifetime value.
SELECT
    customer_id,
    first_name,
    last_name,
    customer_lifetime_value,
    number_of_orders
FROM mart_customers
ORDER BY customer_lifetime_value DESC
LIMIT 10;

-- =============================================================================
-- 5) OPTIONAL: WRITE MARTS BACK TO S3 (PARQUET)
-- =============================================================================
-- Use this if you want marts as persistent parquet outputs in your lake.

COPY (SELECT * FROM mart_orders_enriched)
TO 's3://datalake/marts/mart_orders_enriched.parquet'
(FORMAT PARQUET);

COPY (SELECT * FROM mart_customers)
TO 's3://datalake/marts/mart_customers.parquet'
(FORMAT PARQUET);

-- =============================================================================
-- 6) THINGS TO BE CAREFUL ABOUT IN DASHBOARDS
-- =============================================================================
-- 1) `status` categories may include low-count classes; percentages can mislead
--    if absolute counts are tiny. Show both count and percent.
-- 2) `order_date` is staged as text; marts cast to DATE. Always chart mart date.
-- 3) Payments are event-level while orders are order-level. For order-level charts,
--    aggregate payments by order first (as done in `mart_orders_enriched`).
-- 4) Zero-value payments/orders may be valid (e.g., coupons). Keep them visible,
--    but call them out in narration.
-- 5) Small sample size makes trend charts noisy. Use this as a teaching dataset,
--    not business inference.
