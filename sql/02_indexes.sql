-- ============================================================
--  Migration 02: Additional Indexes & Performance Tuning
--  Run after 01_schema.sql
-- ============================================================

-- ── Covering indexes for common API query patterns ──────────

-- v_customer_ltv: sort by lifetime_value DESC
CREATE INDEX IF NOT EXISTS idx_order_items_order_product
    ON raw.order_items (order_id, product_id)
    INCLUDE (quantity, unit_price, cost_price);

-- orders: channel-based filtering (channel mix view)
CREATE INDEX IF NOT EXISTS idx_orders_channel_date
    ON raw.orders (channel, order_date DESC)
    WHERE status NOT IN ('cancelled', 'refunded');

-- customers: full-text search on name/email
CREATE INDEX IF NOT EXISTS idx_customers_name_trgm
    ON raw.customers USING gin (full_name gin_trgm_ops);  -- requires pg_trgm

-- campaigns: date range lookups
CREATE INDEX IF NOT EXISTS idx_campaigns_dates
    ON raw.campaigns (start_date, end_date);


-- ── Query plan helpers (run in psql for analysis) ───────────

-- Show slow queries (requires pg_stat_statements):
-- SELECT query, calls, mean_exec_time, total_exec_time
-- FROM pg_stat_statements
-- ORDER BY mean_exec_time DESC LIMIT 20;

-- Identify missing indexes:
-- SELECT schemaname, tablename, attname, n_distinct, correlation
-- FROM pg_stats
-- WHERE tablename IN ('orders','order_items','customers')
-- ORDER BY n_distinct DESC;

-- Check index usage:
-- SELECT indexrelname, idx_scan, idx_tup_read, idx_tup_fetch
-- FROM pg_stat_user_indexes
-- WHERE schemaname = 'raw'
-- ORDER BY idx_scan DESC;
