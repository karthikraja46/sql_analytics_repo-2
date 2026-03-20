-- ============================================================
--  QL-Driven Analytics Dashboard  |  PostgreSQL Schema
--  Covers: normalised tables, indexes, views, stored procs
-- ============================================================

-- ─── Extensions ────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;   -- query-plan analysis

-- ─── Schemas ───────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;      -- landing zone
CREATE SCHEMA IF NOT EXISTS mart;     -- analytics-ready views & materialised views
CREATE SCHEMA IF NOT EXISTS api;      -- functions exposed to FastAPI

-- ==============================================================
--  RAW TABLES  (3NF normalised)
-- ==============================================================

-- 1. Regions
CREATE TABLE IF NOT EXISTS raw.regions (
    region_id   SERIAL PRIMARY KEY,
    region_name VARCHAR(100) NOT NULL UNIQUE,
    country     VARCHAR(100) NOT NULL DEFAULT 'India'
);

-- 2. Customers
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id   UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    full_name     VARCHAR(200) NOT NULL,
    email         VARCHAR(254) NOT NULL UNIQUE,
    region_id     INT          NOT NULL REFERENCES raw.regions(region_id),
    signup_date   DATE         NOT NULL DEFAULT CURRENT_DATE,
    customer_tier VARCHAR(20)  NOT NULL DEFAULT 'standard'
                               CHECK (customer_tier IN ('standard','silver','gold','platinum'))
);

-- 3. Product Categories
CREATE TABLE IF NOT EXISTS raw.categories (
    category_id   SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    parent_id     INT REFERENCES raw.categories(category_id)  -- self-ref for sub-cats
);

-- 4. Products
CREATE TABLE IF NOT EXISTS raw.products (
    product_id    SERIAL       PRIMARY KEY,
    sku           VARCHAR(50)  NOT NULL UNIQUE,
    product_name  VARCHAR(255) NOT NULL,
    category_id   INT          NOT NULL REFERENCES raw.categories(category_id),
    unit_price    NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    cost_price    NUMERIC(12,2) NOT NULL CHECK (cost_price >= 0),
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE
);

-- 5. Orders (header)
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id      UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id   UUID         NOT NULL REFERENCES raw.customers(customer_id),
    order_date    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    status        VARCHAR(20)  NOT NULL DEFAULT 'pending'
                               CHECK (status IN ('pending','processing','shipped','delivered','cancelled','refunded')),
    channel       VARCHAR(30)  NOT NULL DEFAULT 'web'
                               CHECK (channel IN ('web','mobile','store','partner')),
    discount_pct  NUMERIC(5,2) NOT NULL DEFAULT 0 CHECK (discount_pct BETWEEN 0 AND 100)
);

-- 6. Order Line Items
CREATE TABLE IF NOT EXISTS raw.order_items (
    item_id       SERIAL        PRIMARY KEY,
    order_id      UUID          NOT NULL REFERENCES raw.orders(order_id) ON DELETE CASCADE,
    product_id    INT           NOT NULL REFERENCES raw.products(product_id),
    quantity      INT           NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12,2) NOT NULL,   -- snapshot at purchase time
    cost_price    NUMERIC(12,2) NOT NULL
);

-- 7. Marketing Campaigns
CREATE TABLE IF NOT EXISTS raw.campaigns (
    campaign_id   SERIAL       PRIMARY KEY,
    campaign_name VARCHAR(200) NOT NULL,
    channel       VARCHAR(50),
    start_date    DATE,
    end_date      DATE,
    budget        NUMERIC(14,2) DEFAULT 0,
    spend         NUMERIC(14,2) DEFAULT 0
);

-- 8. Campaign Attribution (order ↔ campaign)
CREATE TABLE IF NOT EXISTS raw.order_attribution (
    order_id    UUID NOT NULL REFERENCES raw.orders(order_id) ON DELETE CASCADE,
    campaign_id INT  NOT NULL REFERENCES raw.campaigns(campaign_id),
    PRIMARY KEY (order_id, campaign_id)
);


-- ==============================================================
--  INDEXES  (covering common analytical filter/join patterns)
-- ==============================================================

-- orders – date range scans are the #1 query pattern
CREATE INDEX IF NOT EXISTS idx_orders_date
    ON raw.orders (order_date DESC);

CREATE INDEX IF NOT EXISTS idx_orders_customer_status
    ON raw.orders (customer_id, status);

-- order_items – joining to orders + aggregating revenue
CREATE INDEX IF NOT EXISTS idx_items_order
    ON raw.order_items (order_id);

CREATE INDEX IF NOT EXISTS idx_items_product
    ON raw.order_items (product_id);

-- customers – tier + region lookups
CREATE INDEX IF NOT EXISTS idx_customers_region_tier
    ON raw.customers (region_id, customer_tier);

-- products – category browsing
CREATE INDEX IF NOT EXISTS idx_products_category
    ON raw.products (category_id) WHERE is_active = TRUE;

-- partial index: only non-cancelled orders (reduces index size ~20%)
CREATE INDEX IF NOT EXISTS idx_orders_active
    ON raw.orders (order_date, customer_id)
    WHERE status NOT IN ('cancelled', 'refunded');


-- ==============================================================
--  MART VIEWS  (pre-joined, analytics-ready)
-- ==============================================================

-- ── 1. Daily Revenue Summary ────────────────────────────────
CREATE OR REPLACE VIEW mart.v_daily_revenue AS
SELECT
    DATE_TRUNC('day', o.order_date)::DATE               AS order_day,
    COUNT(DISTINCT o.order_id)                           AS total_orders,
    COUNT(DISTINCT o.customer_id)                        AS unique_customers,
    SUM(oi.quantity * oi.unit_price
        * (1 - o.discount_pct / 100))                    AS gross_revenue,
    SUM(oi.quantity * oi.cost_price)                     AS total_cogs,
    SUM(oi.quantity * oi.unit_price
        * (1 - o.discount_pct / 100))
      - SUM(oi.quantity * oi.cost_price)                 AS gross_profit
FROM raw.orders  o
JOIN raw.order_items oi ON oi.order_id = o.order_id
WHERE o.status NOT IN ('cancelled', 'refunded')
GROUP BY 1;

-- ── 2. Product Performance ──────────────────────────────────
CREATE OR REPLACE VIEW mart.v_product_performance AS
SELECT
    p.product_id,
    p.sku,
    p.product_name,
    c.category_name,
    SUM(oi.quantity)                                        AS units_sold,
    SUM(oi.quantity * oi.unit_price)                        AS total_revenue,
    SUM(oi.quantity * (oi.unit_price - oi.cost_price))      AS total_profit,
    ROUND(
        SUM(oi.quantity * (oi.unit_price - oi.cost_price))
        / NULLIF(SUM(oi.quantity * oi.unit_price), 0) * 100
    , 2)                                                    AS margin_pct
FROM raw.products    p
JOIN raw.categories  c  ON c.category_id = p.category_id
JOIN raw.order_items oi ON oi.product_id = p.product_id
JOIN raw.orders      o  ON o.order_id    = oi.order_id
WHERE o.status NOT IN ('cancelled', 'refunded')
GROUP BY 1,2,3,4;

-- ── 3. Customer Lifetime Value ──────────────────────────────
CREATE OR REPLACE VIEW mart.v_customer_ltv AS
SELECT
    cu.customer_id,
    cu.full_name,
    cu.email,
    cu.customer_tier,
    r.region_name,
    COUNT(DISTINCT o.order_id)                              AS total_orders,
    SUM(oi.quantity * oi.unit_price * (1 - o.discount_pct / 100)) AS lifetime_value,
    MIN(o.order_date)::DATE                                 AS first_order,
    MAX(o.order_date)::DATE                                 AS last_order,
    NOW()::DATE - MAX(o.order_date)::DATE                   AS days_since_last_order
FROM raw.customers  cu
JOIN raw.regions    r  ON r.region_id   = cu.region_id
LEFT JOIN raw.orders      o  ON o.customer_id = cu.customer_id
                             AND o.status NOT IN ('cancelled','refunded')
LEFT JOIN raw.order_items oi ON oi.order_id   = o.order_id
GROUP BY 1,2,3,4,5;

-- ── 4. Campaign ROI ─────────────────────────────────────────
CREATE OR REPLACE VIEW mart.v_campaign_roi AS
SELECT
    ca.campaign_id,
    ca.campaign_name,
    ca.channel,
    ca.budget,
    ca.spend,
    COUNT(DISTINCT oa.order_id)                             AS attributed_orders,
    COALESCE(SUM(oi.quantity * oi.unit_price), 0)           AS attributed_revenue,
    ROUND(
        (COALESCE(SUM(oi.quantity * oi.unit_price), 0) - ca.spend)
        / NULLIF(ca.spend, 0) * 100
    , 2)                                                    AS roi_pct
FROM raw.campaigns       ca
LEFT JOIN raw.order_attribution oa ON oa.campaign_id = ca.campaign_id
LEFT JOIN raw.orders             o  ON o.order_id     = oa.order_id
                                    AND o.status NOT IN ('cancelled','refunded')
LEFT JOIN raw.order_items       oi ON oi.order_id     = o.order_id
GROUP BY 1,2,3,4,5;

-- ── 5. Channel Mix ──────────────────────────────────────────
CREATE OR REPLACE VIEW mart.v_channel_mix AS
SELECT
    o.channel,
    DATE_TRUNC('month', o.order_date)::DATE                AS month,
    COUNT(DISTINCT o.order_id)                             AS orders,
    SUM(oi.quantity * oi.unit_price * (1 - o.discount_pct/100)) AS revenue
FROM raw.orders o
JOIN raw.order_items oi ON oi.order_id = o.order_id
WHERE o.status NOT IN ('cancelled','refunded')
GROUP BY 1,2;


-- ==============================================================
--  MATERIALISED VIEW  (expensive agg, refreshed on schedule)
-- ==============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_monthly_kpis AS
SELECT
    DATE_TRUNC('month', dr.order_day)::DATE  AS month,
    SUM(dr.total_orders)                     AS orders,
    SUM(dr.unique_customers)                 AS customers,
    ROUND(SUM(dr.gross_revenue)::NUMERIC, 2) AS revenue,
    ROUND(SUM(dr.gross_profit)::NUMERIC,  2) AS profit,
    ROUND(
        SUM(dr.gross_profit) / NULLIF(SUM(dr.gross_revenue), 0) * 100
    , 2)                                     AS margin_pct
FROM mart.v_daily_revenue dr
GROUP BY 1
ORDER BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS uidx_mv_monthly_kpis_month
    ON mart.mv_monthly_kpis (month);


-- ==============================================================
--  STORED PROCEDURES / FUNCTIONS  (API schema)
-- ==============================================================

-- ── A. KPI Summary (last N days) ────────────────────────────
CREATE OR REPLACE FUNCTION api.get_kpi_summary(p_days INT DEFAULT 30)
RETURNS TABLE (
    metric          TEXT,
    current_value   NUMERIC,
    previous_value  NUMERIC,
    pct_change      NUMERIC
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_now   TIMESTAMPTZ := NOW();
    v_start TIMESTAMPTZ := v_now - (p_days || ' days')::INTERVAL;
    v_prev  TIMESTAMPTZ := v_start - (p_days || ' days')::INTERVAL;
BEGIN
    RETURN QUERY
    WITH cur AS (
        SELECT
            COUNT(DISTINCT o.order_id)::NUMERIC               AS orders,
            COALESCE(SUM(oi.quantity * oi.unit_price * (1 - o.discount_pct/100)), 0) AS revenue,
            COALESCE(SUM(oi.quantity * (oi.unit_price - oi.cost_price)), 0)          AS profit,
            COUNT(DISTINCT o.customer_id)::NUMERIC            AS customers
        FROM raw.orders o
        JOIN raw.order_items oi ON oi.order_id = o.order_id
        WHERE o.order_date BETWEEN v_start AND v_now
          AND o.status NOT IN ('cancelled','refunded')
    ),
    prv AS (
        SELECT
            COUNT(DISTINCT o.order_id)::NUMERIC               AS orders,
            COALESCE(SUM(oi.quantity * oi.unit_price * (1 - o.discount_pct/100)), 0) AS revenue,
            COALESCE(SUM(oi.quantity * (oi.unit_price - oi.cost_price)), 0)          AS profit,
            COUNT(DISTINCT o.customer_id)::NUMERIC            AS customers
        FROM raw.orders o
        JOIN raw.order_items oi ON oi.order_id = o.order_id
        WHERE o.order_date BETWEEN v_prev AND v_start
          AND o.status NOT IN ('cancelled','refunded')
    )
    SELECT 'Total Orders',   cur.orders,    prv.orders,
           ROUND((cur.orders    - prv.orders)    / NULLIF(prv.orders,    0) * 100, 2) FROM cur, prv
    UNION ALL
    SELECT 'Gross Revenue',  cur.revenue,   prv.revenue,
           ROUND((cur.revenue   - prv.revenue)   / NULLIF(prv.revenue,   0) * 100, 2) FROM cur, prv
    UNION ALL
    SELECT 'Gross Profit',   cur.profit,    prv.profit,
           ROUND((cur.profit    - prv.profit)    / NULLIF(prv.profit,    0) * 100, 2) FROM cur, prv
    UNION ALL
    SELECT 'Unique Customers', cur.customers, prv.customers,
           ROUND((cur.customers - prv.customers) / NULLIF(prv.customers, 0) * 100, 2) FROM cur, prv;
END;
$$;

-- ── B. Top-N Products by Revenue ────────────────────────────
CREATE OR REPLACE FUNCTION api.get_top_products(
    p_limit   INT  DEFAULT 10,
    p_days    INT  DEFAULT 30
)
RETURNS TABLE (
    product_name  TEXT,
    category_name TEXT,
    units_sold    BIGINT,
    revenue       NUMERIC,
    margin_pct    NUMERIC
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.product_name::TEXT,
        c.category_name::TEXT,
        SUM(oi.quantity)::BIGINT,
        ROUND(SUM(oi.quantity * oi.unit_price * (1 - o.discount_pct/100))::NUMERIC, 2),
        ROUND(
            SUM(oi.quantity * (oi.unit_price - oi.cost_price))
            / NULLIF(SUM(oi.quantity * oi.unit_price), 0) * 100
        , 2)
    FROM raw.order_items oi
    JOIN raw.orders   o ON o.order_id    = oi.order_id
    JOIN raw.products p ON p.product_id  = oi.product_id
    JOIN raw.categories c ON c.category_id = p.category_id
    WHERE o.order_date >= NOW() - (p_days || ' days')::INTERVAL
      AND o.status NOT IN ('cancelled','refunded')
    GROUP BY p.product_name, c.category_name
    ORDER BY 4 DESC
    LIMIT p_limit;
END;
$$;

-- ── C. Refresh Materialised View (call from scheduler) ──────
CREATE OR REPLACE PROCEDURE api.refresh_marts()
LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_monthly_kpis;
    RAISE NOTICE 'Materialised views refreshed at %', NOW();
END;
$$;
