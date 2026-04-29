-- ─────────────────────────────────────────────────────────────────────────
--  Dashboard Views — connect PowerBI / Streamlit to these instead of
--  running heavy GROUP BYs inside the dashboard. Each view is small,
--  fast, and named so it maps directly to a dashboard tile.
-- ─────────────────────────────────────────────────────────────────────────
USE amazon_india_db;

-- ── Drop existing views (idempotent) ─────────────────────────────────────
DROP VIEW IF EXISTS vw_kpi_summary;
DROP VIEW IF EXISTS vw_yearly_revenue;
DROP VIEW IF EXISTS vw_monthly_revenue;
DROP VIEW IF EXISTS vw_category_performance;
DROP VIEW IF EXISTS vw_top_customers;
DROP VIEW IF EXISTS vw_payment_mix;
DROP VIEW IF EXISTS vw_geographic_revenue;
DROP VIEW IF EXISTS vw_festival_impact;
DROP VIEW IF EXISTS vw_brand_performance;
DROP VIEW IF EXISTS vw_prime_vs_nonprime;
DROP VIEW IF EXISTS vw_returns_by_category;
DROP VIEW IF EXISTS vw_delivery_performance;
DROP VIEW IF EXISTS vw_discount_effectiveness;
DROP VIEW IF EXISTS vw_age_group_behavior;
DROP VIEW IF EXISTS vw_customer_cohorts;

-- ── 1. KPI summary (single-row, lights up the executive dashboard) ───────
CREATE VIEW vw_kpi_summary AS
SELECT
  COUNT(*)                            AS total_orders,
  COUNT(DISTINCT customer_id)         AS active_customers,
  COUNT(DISTINCT product_id)          AS active_products,
  ROUND(SUM(final_amount_inr), 2)     AS total_revenue_inr,
  ROUND(AVG(final_amount_inr), 2)     AS avg_order_value_inr,
  ROUND(AVG(customer_rating), 2)      AS avg_customer_rating,
  ROUND(SUM(is_festival_sale) / COUNT(*) * 100, 2) AS festival_share_pct,
  ROUND(SUM(is_prime_member)  / COUNT(*) * 100, 2) AS prime_share_pct
FROM fact_transactions;

-- ── 2. Yearly revenue + YoY growth ───────────────────────────────────────
CREATE VIEW vw_yearly_revenue AS
SELECT
  t.year,
  ROUND(SUM(f.final_amount_inr), 2)                    AS revenue_inr,
  COUNT(*)                                              AS orders,
  COUNT(DISTINCT f.customer_id)                         AS active_customers,
  ROUND(AVG(f.final_amount_inr), 2)                     AS avg_order_value
FROM fact_transactions f
JOIN dim_time t ON t.date_id = f.date_id
GROUP BY t.year
ORDER BY t.year;

-- ── 3. Monthly revenue (year-month grain for time-series tiles) ──────────
CREATE VIEW vw_monthly_revenue AS
SELECT
  t.year,
  t.month,
  t.month_name,
  ROUND(SUM(f.final_amount_inr), 2) AS revenue_inr,
  COUNT(*)                          AS orders
FROM fact_transactions f
JOIN dim_time t ON t.date_id = f.date_id
GROUP BY t.year, t.month, t.month_name
ORDER BY t.year, t.month;

-- ── 4. Category-wise performance ─────────────────────────────────────────
CREATE VIEW vw_category_performance AS
SELECT
  p.category,
  COUNT(*)                                                          AS orders,
  ROUND(SUM(f.final_amount_inr), 2)                                 AS revenue_inr,
  ROUND(AVG(f.final_amount_inr), 2)                                 AS avg_order_value,
  ROUND(SUM(f.final_amount_inr) /
        (SELECT SUM(final_amount_inr) FROM fact_transactions) * 100, 2) AS market_share_pct
FROM fact_transactions f
JOIN dim_products p ON p.product_id = f.product_id
GROUP BY p.category
ORDER BY revenue_inr DESC;

-- ── 5. Top customers (CLV-style) ─────────────────────────────────────────
CREATE VIEW vw_top_customers AS
SELECT
  f.customer_id,
  c.customer_city,
  c.age_group,
  COUNT(*)                              AS orders,
  ROUND(SUM(f.final_amount_inr), 2)     AS lifetime_value_inr,
  ROUND(AVG(f.final_amount_inr), 2)     AS avg_order_value,
  MAX(f.order_date)                     AS last_order_date
FROM fact_transactions f
JOIN dim_customers c ON c.customer_id = f.customer_id
GROUP BY f.customer_id, c.customer_city, c.age_group
ORDER BY lifetime_value_inr DESC
LIMIT 1000;

-- ── 6. Payment method evolution ──────────────────────────────────────────
CREATE VIEW vw_payment_mix AS
SELECT
  t.year,
  f.payment_method,
  COUNT(*)                          AS orders,
  ROUND(SUM(f.final_amount_inr), 2) AS revenue_inr
FROM fact_transactions f
JOIN dim_time t ON t.date_id = f.date_id
GROUP BY t.year, f.payment_method
ORDER BY t.year, orders DESC;

-- ── 7. Geographic revenue (city + state + tier) ──────────────────────────
CREATE VIEW vw_geographic_revenue AS
SELECT
  c.customer_state,
  c.customer_city,
  c.city_tier,
  COUNT(*)                              AS orders,
  COUNT(DISTINCT f.customer_id)         AS unique_customers,
  ROUND(SUM(f.final_amount_inr), 2)     AS revenue_inr,
  ROUND(AVG(f.final_amount_inr), 2)     AS avg_order_value
FROM fact_transactions f
JOIN dim_customers c ON c.customer_id = f.customer_id
GROUP BY c.customer_state, c.customer_city, c.city_tier
ORDER BY revenue_inr DESC;

-- ── 8. Festival impact ───────────────────────────────────────────────────
CREATE VIEW vw_festival_impact AS
SELECT
  t.year,
  CASE WHEN f.is_festival_sale = 1 THEN COALESCE(NULLIF(f.festival_name,''), 'Festival')
       ELSE 'Normal' END                AS period_type,
  COUNT(*)                              AS orders,
  ROUND(SUM(f.final_amount_inr), 2)     AS revenue_inr,
  ROUND(AVG(f.final_amount_inr), 2)     AS avg_order_value,
  ROUND(AVG(f.discount_percent), 2)     AS avg_discount_pct
FROM fact_transactions f
JOIN dim_time t ON t.date_id = f.date_id
GROUP BY t.year, period_type
ORDER BY t.year, revenue_inr DESC;

-- ── 9. Brand performance (top 25) ────────────────────────────────────────
CREATE VIEW vw_brand_performance AS
SELECT
  p.brand,
  p.category,
  COUNT(*)                              AS orders,
  ROUND(SUM(f.final_amount_inr), 2)     AS revenue_inr,
  ROUND(AVG(p.product_rating), 2)       AS avg_product_rating
FROM fact_transactions f
JOIN dim_products p ON p.product_id = f.product_id
GROUP BY p.brand, p.category
ORDER BY revenue_inr DESC
LIMIT 25;

-- ── 10. Prime vs non-Prime ───────────────────────────────────────────────
CREATE VIEW vw_prime_vs_nonprime AS
SELECT
  CASE WHEN f.is_prime_member = 1 THEN 'Prime' ELSE 'Non-Prime' END AS member_type,
  COUNT(*)                              AS orders,
  COUNT(DISTINCT f.customer_id)         AS active_customers,
  ROUND(SUM(f.final_amount_inr), 2)     AS revenue_inr,
  ROUND(AVG(f.final_amount_inr), 2)     AS avg_order_value
FROM fact_transactions f
GROUP BY member_type;

-- ── 11. Return rate by category ──────────────────────────────────────────
CREATE VIEW vw_returns_by_category AS
SELECT
  p.category,
  COUNT(*)                                                    AS total_orders,
  SUM(CASE WHEN LOWER(f.return_status) IN ('yes','true','returned','1') THEN 1 ELSE 0 END) AS returned_orders,
  ROUND(
    SUM(CASE WHEN LOWER(f.return_status) IN ('yes','true','returned','1') THEN 1 ELSE 0 END)
      / COUNT(*) * 100, 2)                                     AS return_rate_pct
FROM fact_transactions f
JOIN dim_products p ON p.product_id = f.product_id
GROUP BY p.category
ORDER BY return_rate_pct DESC;

-- ── 12. Delivery performance by city ─────────────────────────────────────
CREATE VIEW vw_delivery_performance AS
SELECT
  c.customer_city,
  c.city_tier,
  COUNT(*)                                                    AS orders,
  ROUND(AVG(f.delivery_days), 2)                              AS avg_delivery_days,
  ROUND(SUM(CASE WHEN f.delivery_days <= 4 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS on_time_pct
FROM fact_transactions f
JOIN dim_customers c ON c.customer_id = f.customer_id
WHERE f.delivery_days IS NOT NULL
GROUP BY c.customer_city, c.city_tier
ORDER BY orders DESC;

-- ── 13. Discount effectiveness ───────────────────────────────────────────
CREATE VIEW vw_discount_effectiveness AS
SELECT
  CASE
    WHEN discount_percent = 0           THEN '0%'
    WHEN discount_percent BETWEEN 1 AND 10  THEN '1-10%'
    WHEN discount_percent BETWEEN 11 AND 20 THEN '11-20%'
    WHEN discount_percent BETWEEN 21 AND 30 THEN '21-30%'
    WHEN discount_percent BETWEEN 31 AND 50 THEN '31-50%'
    ELSE '50%+'
  END                                                       AS discount_bucket,
  COUNT(*)                                                  AS orders,
  ROUND(SUM(final_amount_inr), 2)                           AS revenue_inr,
  ROUND(AVG(final_amount_inr), 2)                           AS avg_order_value,
  ROUND(AVG(discount_percent), 2)                           AS avg_discount_pct
FROM fact_transactions
GROUP BY discount_bucket
ORDER BY FIELD(discount_bucket, '0%','1-10%','11-20%','21-30%','31-50%','50%+');

-- ── 14. Age group behaviour ──────────────────────────────────────────────
CREATE VIEW vw_age_group_behavior AS
SELECT
  c.age_group,
  COUNT(*)                              AS orders,
  COUNT(DISTINCT f.customer_id)         AS unique_customers,
  ROUND(AVG(f.final_amount_inr), 2)     AS avg_order_value,
  ROUND(SUM(f.final_amount_inr), 2)     AS revenue_inr
FROM fact_transactions f
JOIN dim_customers c ON c.customer_id = f.customer_id
GROUP BY c.age_group
ORDER BY revenue_inr DESC;

-- ── 15. Customer cohorts (acquisition month → revenue) ───────────────────
CREATE VIEW vw_customer_cohorts AS
SELECT
  DATE_FORMAT(c.first_purchase_date, '%Y-%m')               AS cohort_month,
  COUNT(DISTINCT f.customer_id)                              AS customers,
  COUNT(*)                                                   AS orders,
  ROUND(SUM(f.final_amount_inr), 2)                          AS revenue_inr,
  ROUND(SUM(f.final_amount_inr) / COUNT(DISTINCT f.customer_id), 2) AS arpu
FROM fact_transactions f
JOIN dim_customers c ON c.customer_id = f.customer_id
WHERE c.first_purchase_date IS NOT NULL
GROUP BY cohort_month
ORDER BY cohort_month;

-- ── Verification ─────────────────────────────────────────────────────────
SHOW FULL TABLES IN amazon_india_db WHERE Table_type = 'VIEW';
