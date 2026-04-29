-- ─────────────────────────────────────────────────────────────────────────
--  Amazon India: Decade of Sales Analytics — Star-Schema DDL
--  Engine: MySQL 8.0+
-- ─────────────────────────────────────────────────────────────────────────
--  Run order:
--      1. SOURCE schema.sql;
--      2. Load data via Python (src/run_db_load.py)
--      3. SOURCE dashboard_views.sql;
-- ─────────────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS amazon_india_db
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE amazon_india_db;

-- Drop in dependency order (FKs first)
DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_time;

-- ─────────────────────────────────────────────────────────────────────────
-- DIMENSION: products (one row per product_id)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE dim_products (
  product_id        VARCHAR(32)   NOT NULL,
  product_name      VARCHAR(255),
  category          VARCHAR(64),
  subcategory       VARCHAR(96),
  brand             VARCHAR(96),
  base_price_2015   DECIMAL(12,2),
  weight_kg         DECIMAL(8,3),
  product_rating    DECIMAL(3,2),
  is_prime_eligible TINYINT(1),
  launch_year       SMALLINT,
  model             VARCHAR(96),
  PRIMARY KEY (product_id),
  KEY idx_products_category (category),
  KEY idx_products_brand    (brand),
  KEY idx_products_launch   (launch_year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────
-- DIMENSION: customers (one row per customer_id; latest snapshot)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE dim_customers (
  customer_id              VARCHAR(32) NOT NULL,
  customer_city            VARCHAR(96),
  customer_state           VARCHAR(96),
  city_tier                VARCHAR(20),    -- Metro / Tier 1 / Tier 2 / Rural
  age_group                VARCHAR(16),
  is_prime_member          TINYINT(1),
  customer_spending_tier   VARCHAR(20),
  first_purchase_date      DATE,
  last_purchase_date       DATE,
  PRIMARY KEY (customer_id),
  KEY idx_cust_city  (customer_city),
  KEY idx_cust_state (customer_state),
  KEY idx_cust_tier  (city_tier),
  KEY idx_cust_age   (age_group),
  KEY idx_cust_prime (is_prime_member)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────
-- DIMENSION: time (one row per calendar date 2015-01-01 .. 2025-12-31)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE dim_time (
  date_id        INT          NOT NULL,    -- YYYYMMDD
  full_date      DATE         NOT NULL,
  year           SMALLINT     NOT NULL,
  quarter        TINYINT      NOT NULL,
  month          TINYINT      NOT NULL,
  month_name     VARCHAR(12)  NOT NULL,
  week_of_year   TINYINT      NOT NULL,
  day            TINYINT      NOT NULL,
  day_of_week    VARCHAR(12)  NOT NULL,
  is_weekend     TINYINT(1)   NOT NULL,
  PRIMARY KEY (date_id),
  UNIQUE KEY uk_full_date (full_date),
  KEY idx_time_year     (year),
  KEY idx_time_year_mon (year, month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────
-- FACT: transactions (~1M rows)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE fact_transactions (
  transaction_id      VARCHAR(40)   NOT NULL,
  customer_id         VARCHAR(32)   NOT NULL,
  product_id          VARCHAR(32)   NOT NULL,
  date_id             INT           NOT NULL,
  order_date          DATE          NOT NULL,
  payment_method      VARCHAR(32),
  original_price_inr  DECIMAL(12,2),
  discount_percent    DECIMAL(5,2),
  final_amount_inr    DECIMAL(12,2),
  delivery_charges    DECIMAL(8,2),
  delivery_days       DECIMAL(4,1),
  return_status       VARCHAR(16),
  customer_rating     DECIMAL(3,2),
  is_prime_member     TINYINT(1),
  is_festival_sale    TINYINT(1),
  festival_name       VARCHAR(64),
  is_bulk_order       TINYINT(1)   DEFAULT 0,
  bulk_qty            INT          DEFAULT 1,
  PRIMARY KEY (transaction_id),
  KEY idx_fact_customer (customer_id),
  KEY idx_fact_product  (product_id),
  KEY idx_fact_date     (date_id),
  KEY idx_fact_orderdt  (order_date),
  KEY idx_fact_payment  (payment_method),
  KEY idx_fact_festival (is_festival_sale, festival_name),
  CONSTRAINT fk_fact_customer FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
  CONSTRAINT fk_fact_product  FOREIGN KEY (product_id)  REFERENCES dim_products(product_id),
  CONSTRAINT fk_fact_date     FOREIGN KEY (date_id)     REFERENCES dim_time(date_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────
-- Smoke-test query — should return 0 rows initially
-- ─────────────────────────────────────────────────────────────────────────
SELECT
  (SELECT COUNT(*) FROM dim_products)     AS product_rows,
  (SELECT COUNT(*) FROM dim_customers)    AS customer_rows,
  (SELECT COUNT(*) FROM dim_time)         AS time_rows,
  (SELECT COUNT(*) FROM fact_transactions) AS transaction_rows;
