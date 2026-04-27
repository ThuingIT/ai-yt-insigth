-- ============================================================
-- phase2_upgrades.sql
-- Migration cho Phase 2: Channel subscribers + Sentiment insight type
-- Chạy trong Supabase SQL Editor sau cleanup_old_data.sql
-- ============================================================


-- ============================================================
-- 1. Thêm insight_type 'sentiment' vào CHECK constraint
-- Phải DROP constraint cũ rồi ADD lại (PostgreSQL không ALTER CHECK)
-- ============================================================

-- Xem tên constraint hiện tại
-- SELECT conname FROM pg_constraint WHERE conrelid = 'insights'::regclass AND contype = 'c';

ALTER TABLE insights DROP CONSTRAINT IF EXISTS insights_insight_type_check;

ALTER TABLE insights ADD CONSTRAINT insights_insight_type_check
    CHECK (insight_type IN (
        'weekly_summary',
        'monthly_summary',
        'anomaly',
        'trend',
        'recommendation',
        'content_gap',
        'viral_alert',
        'trend_lag',
        'sentiment'          -- ★ Mới: comment sentiment analysis
    ));

COMMENT ON COLUMN insights.insight_type IS
    'Loại insight: weekly_summary | anomaly | recommendation | content_gap | viral_alert | trend_lag | sentiment';


-- ============================================================
-- 2. Thêm cột subscribers vào channels (nếu chưa có)
-- Schema gốc đã có cột này nhưng chưa được populate từ API
-- ============================================================
ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS subscribers      BIGINT  DEFAULT 0,
    ADD COLUMN IF NOT EXISTS total_views      BIGINT  DEFAULT 0,
    ADD COLUMN IF NOT EXISTS video_count      INT     DEFAULT 0;

-- Index để tìm channel nổi bật
CREATE INDEX IF NOT EXISTS idx_channels_subscribers
    ON channels(subscribers DESC);

COMMENT ON COLUMN channels.subscribers IS
    'Số subscriber hiện tại — crawl hàng ngày từ channels.list(part=statistics)';


-- ============================================================
-- 3. Function: get_top_channels_by_subscribers(stream, limit)
-- Dùng để hiển thị leaderboard kênh trên dashboard
-- ============================================================
CREATE OR REPLACE FUNCTION get_top_channels_by_subscribers(
    p_stream    TEXT    DEFAULT 'VN',
    p_limit     INT     DEFAULT 10
)
RETURNS TABLE (
    channel_id      TEXT,
    channel_name    TEXT,
    subscribers     BIGINT,
    video_count_db  BIGINT,
    avg_views_gain  NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.id                                AS channel_id,
        c.name                              AS channel_name,
        c.subscribers,
        COUNT(v.id)                         AS video_count_db,
        ROUND(AVG(dd.views_gain))           AS avg_views_gain
    FROM channels c
    JOIN videos v ON v.channel_id = c.id AND v.stream = p_stream
    LEFT JOIN daily_delta dd ON dd.video_id = v.id
        AND dd.date >= CURRENT_DATE - INTERVAL '7 days'
        AND dd.views_gain IS NOT NULL
    WHERE c.stream = p_stream
      AND c.subscribers > 0
    GROUP BY c.id, c.name, c.subscribers
    ORDER BY c.subscribers DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_top_channels_by_subscribers IS
    'Top N channels theo subscriber count, kèm avg views_gain 7 ngày gần nhất';


-- ============================================================
-- KIỂM TRA
-- ============================================================
-- SELECT * FROM get_top_channels_by_subscribers('VN', 5);
-- SELECT insight_type, COUNT(*) FROM insights GROUP BY 1;
-- SELECT column_name FROM information_schema.columns
--   WHERE table_name = 'channels' ORDER BY ordinal_position;
-- ============================================================
