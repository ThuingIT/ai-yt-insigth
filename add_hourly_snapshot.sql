-- ============================================================
-- MIGRATION: add_hourly_snapshot.sql
-- Chạy trong Supabase SQL Editor SAU khi đã có create_supabase.sql
-- An toàn: dùng IF NOT EXISTS / IF EXISTS ở mọi nơi
-- ============================================================


-- ============================================================
-- 1. BẢNG MỚI: hourly_snapshot
-- Lưu snapshot mỗi giờ thay vì mỗi ngày.
-- Đây là nguồn dữ liệu cho intraday chart & momentum.
-- ============================================================
CREATE TABLE IF NOT EXISTS hourly_snapshot (
    id              BIGSERIAL   PRIMARY KEY,
    video_id        TEXT        NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    snapshot_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- Thời điểm crawl chính xác
    -- Stats tại thời điểm crawl
    views           BIGINT      DEFAULT 0,
    likes           BIGINT      DEFAULT 0,
    comments        BIGINT      DEFAULT 0,
    -- Delta so với snapshot giờ trước (NULL nếu là snapshot đầu tiên)
    views_delta_1h  BIGINT,
    likes_delta_1h  BIGINT,
    -- Metadata
    stream          TEXT        NOT NULL CHECK (stream IN ('VN', 'Global')),
    content_type    TEXT        NOT NULL,
    region          TEXT,
    UNIQUE (video_id, snapshot_at)  -- Tránh duplicate trong cùng 1 giờ
);

COMMENT ON TABLE hourly_snapshot IS
    'Snapshot mỗi giờ per video — dùng cho intraday chart, velocity, momentum detection';
COMMENT ON COLUMN hourly_snapshot.views_delta_1h IS
    'views tăng trong 1 giờ vừa qua. NULL = giờ đầu tiên thấy video hôm nay';


-- ============================================================
-- 2. CỘT MỚI trong daily_delta
-- Tổng hợp từ hourly_snapshot cuối mỗi ngày.
-- ============================================================

-- Giờ đạt đỉnh trong ngày (0-23)
ALTER TABLE daily_delta
    ADD COLUMN IF NOT EXISTS peak_hour          SMALLINT;

-- Views gain lớn nhất trong 1 giờ (không phải cả ngày)
ALTER TABLE daily_delta
    ADD COLUMN IF NOT EXISTS peak_views_gain_1h BIGINT;

-- Điểm tăng tốc: dương = đang tăng, âm = đang giảm
-- Tính: avg(velocity_last_3h) - avg(velocity_prev_3h)
ALTER TABLE daily_delta
    ADD COLUMN IF NOT EXISTS acceleration_score NUMERIC(10,2);

-- Trạng thái momentum: accelerating | peaking | decelerating | fading | new
ALTER TABLE daily_delta
    ADD COLUMN IF NOT EXISTS momentum_status    TEXT
        CHECK (momentum_status IN (
            'accelerating', 'peaking', 'decelerating', 'fading', 'new', NULL
        ));

-- Thời điểm đầu tiên phát hiện spike (views_delta_1h > 5× baseline)
ALTER TABLE daily_delta
    ADD COLUMN IF NOT EXISTS spike_detected_at  TIMESTAMPTZ;

COMMENT ON COLUMN daily_delta.momentum_status IS
    'accelerating=đang tăng tốc | peaking=ở đỉnh | decelerating=qua đỉnh | fading=tàn dần | new=chưa đủ data';


-- ============================================================
-- 3. INDEXES cho hourly_snapshot
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_hourly_video_time
    ON hourly_snapshot (video_id, snapshot_at DESC);

CREATE INDEX IF NOT EXISTS idx_hourly_stream_time
    ON hourly_snapshot (stream, snapshot_at DESC);

-- Index đặc biệt: tìm "hot right now" — video có velocity cao nhất trong 1h qua
CREATE INDEX IF NOT EXISTS idx_hourly_velocity
    ON hourly_snapshot (stream, snapshot_at DESC, views_delta_1h DESC NULLS LAST)
    WHERE views_delta_1h IS NOT NULL;

-- Index để tổng hợp intraday chart theo giờ
CREATE INDEX IF NOT EXISTS idx_hourly_date_trunc
    ON hourly_snapshot (date_trunc('hour', snapshot_at), stream);


-- ============================================================
-- 4. FUNCTION: compute_momentum_status(video_id, as_of)
-- Tính momentum từ 6 hourly_snapshot gần nhất.
-- Gọi từ 02_load_supabase.py sau mỗi lần upsert hourly_snapshot.
-- ============================================================
CREATE OR REPLACE FUNCTION compute_momentum_status(
    p_video_id  TEXT,
    p_as_of     TIMESTAMPTZ DEFAULT NOW()
)
RETURNS TEXT AS $$
DECLARE
    v_snapshots     NUMERIC[];
    v_last3_avg     NUMERIC;
    v_prev3_avg     NUMERIC;
    v_accel         NUMERIC;
    v_count         INT;
BEGIN
    -- Lấy 6 giá trị views_delta_1h gần nhất (không NULL)
    SELECT ARRAY_AGG(views_delta_1h ORDER BY snapshot_at DESC)
    INTO v_snapshots
    FROM (
        SELECT views_delta_1h, snapshot_at
        FROM hourly_snapshot
        WHERE video_id = p_video_id
          AND snapshot_at <= p_as_of
          AND views_delta_1h IS NOT NULL
        ORDER BY snapshot_at DESC
        LIMIT 6
    ) t;

    v_count := COALESCE(ARRAY_LENGTH(v_snapshots, 1), 0);

    IF v_count < 2 THEN
        RETURN 'new';  -- Chưa đủ data để tính
    END IF;

    -- 3 giờ gần nhất vs 3 giờ trước đó
    v_last3_avg := (
        COALESCE(v_snapshots[1], 0) +
        COALESCE(v_snapshots[2], 0) +
        COALESCE(v_snapshots[3], 0)
    ) / LEAST(v_count, 3)::NUMERIC;

    v_prev3_avg := CASE
        WHEN v_count >= 4 THEN (
            COALESCE(v_snapshots[4], 0) +
            COALESCE(v_snapshots[5], 0) +
            COALESCE(v_snapshots[6], 0)
        ) / (v_count - 3)::NUMERIC
        ELSE v_last3_avg  -- Không có prev3 → so sánh bằng nhau
    END;

    v_accel := v_last3_avg - v_prev3_avg;

    -- Phân loại momentum
    IF v_prev3_avg = 0 THEN
        RETURN 'accelerating';  -- Từ 0 lên có views = đang bùng
    ELSIF v_accel > v_prev3_avg * 0.2 THEN
        RETURN 'accelerating';  -- Tăng > 20% so với trước
    ELSIF v_accel >= 0 AND v_accel <= v_prev3_avg * 0.2 THEN
        RETURN 'peaking';       -- Tăng nhẹ hoặc flat ở vùng cao
    ELSIF v_accel < 0 AND v_last3_avg > v_prev3_avg * 0.4 THEN
        RETURN 'decelerating';  -- Giảm nhưng vẫn còn > 40% đỉnh
    ELSE
        RETURN 'fading';        -- Giảm mạnh < 40% đỉnh
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION compute_momentum_status IS
    'Tính momentum từ 6 hourly snapshots gần nhất. '
    'accelerating >20%↑ | peaking ~flat | decelerating >40% | fading <40%';


-- ============================================================
-- 5. FUNCTION: get_hot_right_now(stream, limit)
-- Query nhanh để lấy "hot right now" feed cho dashboard.
-- ============================================================
CREATE OR REPLACE FUNCTION get_hot_right_now(
    p_stream    TEXT    DEFAULT 'VN',
    p_limit     INT     DEFAULT 10,
    p_hours_ago INT     DEFAULT 1
)
RETURNS TABLE (
    video_id        TEXT,
    title           TEXT,
    thumbnail_url   TEXT,
    category_name   TEXT,
    content_type    TEXT,
    views_delta_1h  BIGINT,
    total_views     BIGINT,
    momentum_status TEXT,
    snapshot_at     TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        h.video_id,
        v.title,
        v.thumbnail_url,
        v.category_name,
        h.content_type,
        h.views_delta_1h,
        h.views                 AS total_views,
        compute_momentum_status(h.video_id) AS momentum_status,
        h.snapshot_at
    FROM hourly_snapshot h
    JOIN videos v ON h.video_id = v.id
    WHERE h.stream = p_stream
      AND h.snapshot_at >= NOW() - (p_hours_ago || ' hours')::INTERVAL
      AND h.views_delta_1h IS NOT NULL
      AND h.views_delta_1h > 0
    ORDER BY h.views_delta_1h DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_hot_right_now IS
    'Trả về top N video có views_delta_1h cao nhất trong p_hours_ago giờ gần nhất';


-- ============================================================
-- 6. MATERIALIZED VIEW: intraday_chart
-- Tổng views_delta_1h theo giờ trong ngày hôm nay (UTC).
-- Refresh sau mỗi lần upsert hourly_snapshot.
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS intraday_chart AS
SELECT
    date_trunc('hour', snapshot_at)     AS hour_bucket,
    stream,
    content_type,
    SUM(views_delta_1h)                 AS total_views_gained,
    COUNT(DISTINCT video_id)            AS active_videos,
    MAX(views_delta_1h)                 AS max_single_video,
    ROUND(AVG(views_delta_1h))          AS avg_per_video
FROM hourly_snapshot
WHERE views_delta_1h IS NOT NULL
  AND snapshot_at >= CURRENT_DATE     -- Chỉ giữ ngày hôm nay
GROUP BY 1, 2, 3;

CREATE UNIQUE INDEX IF NOT EXISTS idx_intraday_pk
    ON intraday_chart (hour_bucket, stream, content_type);

COMMENT ON MATERIALIZED VIEW intraday_chart IS
    'Tổng views per giờ trong ngày — dùng cho intraday line chart trên dashboard. '
    'Refresh: SELECT refresh_all_views();';


-- ============================================================
-- 7. CẬP NHẬT refresh_all_views() để bao gồm intraday_chart
-- ============================================================
CREATE OR REPLACE FUNCTION refresh_all_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY weekly_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY monthly_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY intraday_chart;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_all_views IS
    'Refresh tất cả materialized views. Gọi sau khi 02_load_supabase.py hoàn thành.';


-- ============================================================
-- KIỂM TRA SAU KHI CHẠY MIGRATION
-- ============================================================
-- SELECT table_name FROM information_schema.tables
--   WHERE table_schema = 'public' ORDER BY table_name;
--
-- SELECT column_name FROM information_schema.columns
--   WHERE table_name = 'daily_delta' ORDER BY ordinal_position;
--
-- SELECT matviewname FROM pg_matviews WHERE schemaname = 'public';
-- ============================================================
