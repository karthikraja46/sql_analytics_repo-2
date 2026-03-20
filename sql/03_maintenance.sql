-- ============================================================
--  Migration 03: Scheduled Maintenance & Monitoring
--  Requires: pg_cron extension (or call from external scheduler)
-- ============================================================

-- ── Enable pg_cron (superuser required) ─────────────────────
-- CREATE EXTENSION IF NOT EXISTS pg_cron;

-- ── Hourly mat-view refresh ──────────────────────────────────
-- SELECT cron.schedule(
--     'refresh-mart-views',
--     '0 * * * *',          -- every hour
--     $$ CALL api.refresh_marts() $$
-- );

-- ── Daily VACUUM ANALYZE ─────────────────────────────────────
-- SELECT cron.schedule(
--     'vacuum-analytics',
--     '30 2 * * *',          -- 02:30 daily
--     $$ VACUUM ANALYZE raw.orders, raw.order_items, raw.customers $$
-- );


-- ── Monitoring views ─────────────────────────────────────────

-- Table sizes
CREATE OR REPLACE VIEW mart.v_table_sizes AS
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename))       AS table_size,
    pg_size_pretty(
        pg_total_relation_size(schemaname||'.'||tablename)
        - pg_relation_size(schemaname||'.'||tablename)
    )                                                                   AS index_size
FROM pg_tables
WHERE schemaname IN ('raw','mart')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Index efficiency
CREATE OR REPLACE VIEW mart.v_index_health AS
SELECT
    indexrelname            AS index_name,
    relname                 AS table_name,
    idx_scan                AS scans,
    idx_tup_read            AS rows_read,
    idx_tup_fetch           AS rows_fetched,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    CASE WHEN idx_scan = 0 THEN '⚠ UNUSED' ELSE '✓ ACTIVE' END AS status
FROM pg_stat_user_indexes
JOIN pg_indexes ON indexrelname = pg_indexes.indexname
WHERE schemaname = 'raw'
ORDER BY idx_scan DESC;
