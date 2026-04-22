-- ============================================================
-- FIX: concurrent_refresh_fix.sql
-- Problem: REFRESH CONCURRENTLY requires a unique index with
--          NO expression/WHERE clause. COALESCE() in the index
--          definition is an expression → breaks CONCURRENTLY.
-- Solution: Drop expression indexes, add a plain unique index
--           by coalescing category_name at the VIEW level.
-- ============================================================


-- ============================================================
-- 1. FIX weekly_stats
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS weekly_stats CASCADE;

CREATE MATERIALIZED VIEW weekly_stats AS
SELECT
    date_trunc('week', d.date)                  AS week_start,
    d.stream,
    d.content_type,
    COALESCE(v.category_name, '__null__')        AS category_name,  -- ← sentinel in the view
    COUNT(DISTINCT d.video_id)                  AS videos_active,
    SUM(d.views_gain)                           AS total_views_gained,
    SUM(d.likes_gain)                           AS total_likes_gained,
    SUM(d.comments_gain)                        AS total_comments_gained,
    ROUND(AVG(d.views_gain))                    AS avg_views_per_video,
    MAX(d.views_gain)                           AS max_views_single_video,
    ROUND(
        AVG(
            CASE WHEN v.views > 0
            THEN (v.likes::FLOAT + v.comments_count) / v.views * 100
            ELSE 0 END
        )::NUMERIC, 2
    )                                           AS avg_er_pct,
    COUNT(DISTINCT v.channel_id)                AS active_channels
FROM daily_delta d
JOIN videos v ON d.video_id = v.id
WHERE d.views_gain IS NOT NULL
  AND d.views_gain >= 0
GROUP BY 1, 2, 3, 4;

-- Plain unique index — no expression, no WHERE → works with CONCURRENTLY
CREATE UNIQUE INDEX idx_weekly_stats_pk
    ON weekly_stats (week_start, stream, content_type, category_name);

COMMENT ON MATERIALIZED VIEW weekly_stats IS
    'category_name uses sentinel __null__ instead of NULL so REFRESH CONCURRENTLY works';


-- ============================================================
-- 2. FIX monthly_stats
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS monthly_stats CASCADE;

CREATE MATERIALIZED VIEW monthly_stats AS
SELECT
    date_trunc('month', d.date)                 AS month_start,
    d.stream,
    d.content_type,
    COALESCE(v.category_name, '__null__')        AS category_name,  -- ← sentinel in the view
    COUNT(DISTINCT d.video_id)                  AS videos_active,
    SUM(d.views_gain)                           AS total_views_gained,
    SUM(d.likes_gain)                           AS total_likes_gained,
    SUM(d.comments_gain)                        AS total_comments_gained,
    ROUND(AVG(d.views_gain))                    AS avg_views_per_video,
    MAX(d.views_gain)                           AS max_views_single_video,
    ROUND(
        AVG(
            CASE WHEN v.views > 0
            THEN (v.likes::FLOAT + v.comments_count) / v.views * 100
            ELSE 0 END
        )::NUMERIC, 2
    )                                           AS avg_er_pct,
    COUNT(DISTINCT v.channel_id)                AS active_channels
FROM daily_delta d
JOIN videos v ON d.video_id = v.id
WHERE d.views_gain IS NOT NULL
  AND d.views_gain >= 0
GROUP BY 1, 2, 3, 4;

CREATE UNIQUE INDEX idx_monthly_stats_pk
    ON monthly_stats (month_start, stream, content_type, category_name);

COMMENT ON MATERIALIZED VIEW monthly_stats IS
    'category_name uses sentinel __null__ instead of NULL so REFRESH CONCURRENTLY works';


-- ============================================================
-- 3. FIX intraday_chart (from add_hourly_snapshot.sql)
--    Its index is already clean (no expression), but re-confirm.
-- ============================================================
-- No change needed — idx_intraday_pk is a plain column index. ✓


-- ============================================================
-- 4. Re-create refresh_all_views() (already done in
--    add_hourly_snapshot.sql, included here for completeness)
-- ============================================================
CREATE OR REPLACE FUNCTION refresh_all_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY weekly_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY monthly_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY intraday_chart;
END;
$$ LANGUAGE plpgsql;