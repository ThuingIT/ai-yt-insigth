-- ============================================================
-- YouTube Analytics Pipeline — Supabase Schema
-- Run this file in Supabase SQL Editor (Dashboard → SQL Editor)
-- ============================================================
-- Thứ tự chạy: extensions → tables → indexes → views → functions
-- ============================================================


-- ============================================================
-- 0. EXTENSIONS
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- ============================================================
-- 1. CHANNELS
-- Lưu thông tin kênh. Upsert hàng ngày để track growth.
-- ============================================================
CREATE TABLE IF NOT EXISTS channels (
    id                  TEXT        PRIMARY KEY,        -- YouTube channel ID (UCxxxx)
    name                TEXT        NOT NULL,
    description         TEXT,
    custom_url          TEXT,
    country             TEXT,
    subscribers         BIGINT      DEFAULT 0,
    total_views         BIGINT      DEFAULT 0,
    video_count         INT         DEFAULT 0,
    thumbnail_url       TEXT,
    stream              TEXT        NOT NULL CHECK (stream IN ('VN', 'Global')),
    first_seen_at       TIMESTAMPTZ DEFAULT NOW(),
    last_updated_at     TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE channels IS 'Snapshot hàng ngày của thông tin kênh YouTube';
COMMENT ON COLUMN channels.stream IS 'VN = kênh Việt Nam, Global = kênh quốc tế';


-- ============================================================
-- 2. VIDEOS
-- Metadata video. views/likes/comments_count là snapshot MỚI NHẤT.
-- Lịch sử thay đổi hàng ngày được lưu trong daily_delta.
-- ============================================================
CREATE TABLE IF NOT EXISTS videos (
    id                  TEXT        PRIMARY KEY,        -- YouTube video ID (11 ký tự)
    channel_id          TEXT        REFERENCES channels(id) ON DELETE SET NULL,
    title               TEXT        NOT NULL,
    description         TEXT,
    tags                TEXT[]      DEFAULT '{}',
    category_id         INT,
    category_name       TEXT,
    published_at        TIMESTAMPTZ,
    duration_seconds    INT         DEFAULT 0,
    content_type        TEXT        NOT NULL CHECK (content_type IN ('video', 'stream', 'shorts')),
    stream              TEXT        NOT NULL CHECK (stream IN ('VN', 'Global')),
    region              TEXT,                           -- Region nơi video được trending (VN/US/KR...)
    thumbnail_url       TEXT,
    -- Stats snapshot mới nhất (cập nhật mỗi ngày)
    views               BIGINT      DEFAULT 0,
    likes               BIGINT      DEFAULT 0,
    comments_count      BIGINT      DEFAULT 0,
    first_seen_at       TIMESTAMPTZ DEFAULT NOW(),
    last_updated_at     TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE videos IS 'Metadata video YouTube, stats là snapshot ngày crawl gần nhất';
COMMENT ON COLUMN videos.content_type IS 'video = VOD thường, stream = live đã kết thúc, shorts = video ngắn ≤60s';
COMMENT ON COLUMN videos.views IS 'Số views tích lũy tại lần crawl gần nhất — dùng daily_delta để tính tăng trưởng';


-- ============================================================
-- 3. STREAM_DETAILS
-- Chỉ dành cho content_type = 'stream'. Thông tin riêng của live.
-- ============================================================
CREATE TABLE IF NOT EXISTS stream_details (
    video_id                TEXT        PRIMARY KEY REFERENCES videos(id) ON DELETE CASCADE,
    actual_start_time       TIMESTAMPTZ,
    actual_end_time         TIMESTAMPTZ,
    scheduled_start_time    TIMESTAMPTZ,
    concurrent_viewers_peak BIGINT,                     -- Đỉnh người xem đồng thời
    total_chat_count        BIGINT,                     -- Tổng tin nhắn live chat
    stream_duration_seconds INT
);

COMMENT ON TABLE stream_details IS 'Chi tiết riêng cho video dạng live stream đã kết thúc';


-- ============================================================
-- 4. DAILY_DELTA  ← Bảng quan trọng nhất!
-- views YouTube là số tích lũy không giảm.
-- Delta = phần tăng thêm mỗi ngày → mới có ý nghĩa thống kê.
-- ============================================================
CREATE TABLE IF NOT EXISTS daily_delta (
    id              BIGSERIAL   PRIMARY KEY,
    video_id        TEXT        NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    date            DATE        NOT NULL,
    -- Snapshot tuyệt đối tại ngày đó
    views_total     BIGINT      DEFAULT 0,
    likes_total     BIGINT      DEFAULT 0,
    comments_total  BIGINT      DEFAULT 0,
    -- Delta so với ngày hôm trước (NULL nếu là ngày đầu tiên thấy video)
    views_gain      BIGINT,
    likes_gain      BIGINT,
    comments_gain   BIGINT,
    -- Metadata
    stream          TEXT        NOT NULL CHECK (stream IN ('VN', 'Global')),
    content_type    TEXT        NOT NULL,
    region          TEXT,
    UNIQUE(video_id, date)      -- Mỗi video chỉ có 1 row mỗi ngày
);

COMMENT ON TABLE daily_delta IS 'Tracking thay đổi views/likes/comments hàng ngày per video. views_gain = views_total - views_total_hôm_qua';
COMMENT ON COLUMN daily_delta.views_gain IS 'NULL nếu là ngày đầu tiên thấy video (chưa có baseline)';


-- ============================================================
-- 5. COMMENTS
-- Top comments của video (50 comments/video, không crawl tất cả)
-- ============================================================
CREATE TABLE IF NOT EXISTS comments (
    id              TEXT        PRIMARY KEY,            -- YouTube comment ID
    video_id        TEXT        NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    text            TEXT        NOT NULL,
    like_count      INT         DEFAULT 0,
    published_at    TIMESTAMPTZ,
    crawled_at      TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE comments IS 'Top 50 comments per video, dùng cho sentiment analysis ở tầng 2';


-- ============================================================
-- 6. CRAWL_LOGS
-- Log mỗi lần chạy pipeline để dễ debug.
-- ============================================================
CREATE TABLE IF NOT EXISTS crawl_logs (
    id                  BIGSERIAL   PRIMARY KEY,
    run_id              TEXT        NOT NULL,           -- UUID, giống nhau trong cùng 1 lần chạy pipeline
    script              TEXT        NOT NULL,           -- '01_crawl', '02_load', '03_analyze', '04_html'
    stream              TEXT,                           -- 'VN' / 'Global' / NULL (nếu không liên quan)
    status              TEXT        CHECK (status IN ('started', 'success', 'failed')),
    records_processed   INT,
    quota_used          INT,                            -- YouTube API units đã dùng (ước tính)
    error_message       TEXT,
    started_at          TIMESTAMPTZ DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    -- Tổng thời gian chạy (seconds), tự tính khi update
    duration_seconds    INT GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (finished_at - started_at))::INT
    ) STORED
);

COMMENT ON TABLE crawl_logs IS 'Log từng bước pipeline để debug GitHub Actions failures';


-- ============================================================
-- 7. INSIGHTS
-- Kết quả phân tích từ Gemini 2.5 (Tầng 2).
-- payload chứa JSON structured, narrative chứa text tự nhiên.
-- ============================================================
CREATE TABLE IF NOT EXISTS insights (
    id              BIGSERIAL   PRIMARY KEY,
    run_id          TEXT,
    insight_type    TEXT        NOT NULL CHECK (insight_type IN (
                        'weekly_summary',
                        'monthly_summary',
                        'anomaly',
                        'trend',
                        'recommendation',
                        'content_gap',
                        'viral_alert',
                        'trend_lag'
                    )),
    scope           TEXT        CHECK (scope IN ('VN', 'Global', 'both')),
    content_type    TEXT,                               -- 'video'/'stream'/'shorts'/'all'
    period_start    DATE,
    period_end      DATE,
    payload         JSONB       NOT NULL DEFAULT '{}',  -- Structured data từ Gemini
    narrative       TEXT,                               -- Text tự nhiên để hiển thị trên HTML
    generated_at    TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE insights IS 'Output từ Gemini 2.5 — narrative + structured JSON per insight type';


-- ============================================================
-- 8. INDEXES
-- Tối ưu query phổ biến nhất.
-- ============================================================

-- videos
CREATE INDEX IF NOT EXISTS idx_videos_channel_id     ON videos(channel_id);
CREATE INDEX IF NOT EXISTS idx_videos_stream          ON videos(stream);
CREATE INDEX IF NOT EXISTS idx_videos_content_type   ON videos(content_type);
CREATE INDEX IF NOT EXISTS idx_videos_published_at   ON videos(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_videos_stream_type    ON videos(stream, content_type);

-- daily_delta (query nhiều nhất)
CREATE INDEX IF NOT EXISTS idx_delta_video_date      ON daily_delta(video_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_delta_date            ON daily_delta(date DESC);
CREATE INDEX IF NOT EXISTS idx_delta_stream_date     ON daily_delta(stream, date DESC);
CREATE INDEX IF NOT EXISTS idx_delta_gain            ON daily_delta(views_gain DESC NULLS LAST);

-- comments
CREATE INDEX IF NOT EXISTS idx_comments_video_id     ON comments(video_id);
CREATE INDEX IF NOT EXISTS idx_comments_like_count   ON comments(video_id, like_count DESC);

-- insights
CREATE INDEX IF NOT EXISTS idx_insights_type         ON insights(insight_type, generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_insights_scope        ON insights(scope, period_start DESC);

-- crawl_logs
CREATE INDEX IF NOT EXISTS idx_logs_run_id           ON crawl_logs(run_id);
CREATE INDEX IF NOT EXISTS idx_logs_started_at       ON crawl_logs(started_at DESC);


-- ============================================================
-- 9. MATERIALIZED VIEWS
-- Tính weekly/monthly stats từ daily_delta.
-- Chạy: REFRESH MATERIALIZED VIEW CONCURRENTLY weekly_stats;
-- ============================================================

-- WEEKLY STATS
-- So sánh WoW (week-over-week) — dùng date_trunc('week', date) để group theo tuần ISO
CREATE MATERIALIZED VIEW IF NOT EXISTS weekly_stats AS
SELECT
    date_trunc('week', d.date)          AS week_start,
    d.stream,
    d.content_type,
    v.category_name,
    COUNT(DISTINCT d.video_id)          AS videos_active,
    SUM(d.views_gain)                   AS total_views_gained,
    SUM(d.likes_gain)                   AS total_likes_gained,
    SUM(d.comments_gain)                AS total_comments_gained,
    ROUND(AVG(d.views_gain))            AS avg_views_per_video,
    MAX(d.views_gain)                   AS max_views_single_video,
    -- Engagement Rate trung bình trong tuần (tính trên snapshot mới nhất của video)
    ROUND(
        AVG(
            CASE WHEN v.views > 0
            THEN (v.likes::FLOAT + v.comments_count) / v.views * 100
            ELSE 0 END
        )::NUMERIC, 2
    )                                   AS avg_er_pct,
    COUNT(DISTINCT v.channel_id)        AS active_channels
FROM daily_delta d
JOIN videos v ON d.video_id = v.id
WHERE d.views_gain IS NOT NULL
  AND d.views_gain >= 0               -- Lọc data lỗi (views không thể giảm)
GROUP BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS idx_weekly_stats_pk
    ON weekly_stats(week_start, stream, content_type, COALESCE(category_name, '__null__'));

COMMENT ON MATERIALIZED VIEW weekly_stats IS
    'Refresh sau mỗi lần pipeline chạy: REFRESH MATERIALIZED VIEW CONCURRENTLY weekly_stats';


-- MONTHLY STATS
CREATE MATERIALIZED VIEW IF NOT EXISTS monthly_stats AS
SELECT
    date_trunc('month', d.date)         AS month_start,
    d.stream,
    d.content_type,
    v.category_name,
    COUNT(DISTINCT d.video_id)          AS videos_active,
    SUM(d.views_gain)                   AS total_views_gained,
    SUM(d.likes_gain)                   AS total_likes_gained,
    SUM(d.comments_gain)                AS total_comments_gained,
    ROUND(AVG(d.views_gain))            AS avg_views_per_video,
    MAX(d.views_gain)                   AS max_views_single_video,
    ROUND(
        AVG(
            CASE WHEN v.views > 0
            THEN (v.likes::FLOAT + v.comments_count) / v.views * 100
            ELSE 0 END
        )::NUMERIC, 2
    )                                   AS avg_er_pct,
    COUNT(DISTINCT v.channel_id)        AS active_channels
FROM daily_delta d
JOIN videos v ON d.video_id = v.id
WHERE d.views_gain IS NOT NULL
  AND d.views_gain >= 0
GROUP BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS idx_monthly_stats_pk
    ON monthly_stats(month_start, stream, content_type, COALESCE(category_name, '__null__'));

COMMENT ON MATERIALIZED VIEW monthly_stats IS
    'Refresh sau mỗi lần pipeline chạy: REFRESH MATERIALIZED VIEW CONCURRENTLY monthly_stats';


-- ============================================================
-- 10. HELPER FUNCTIONS
-- ============================================================

-- Tính Virality Index: view_gain của video so với trung bình kênh đó
-- Dùng trong script 03_analyze để flag viral videos
CREATE OR REPLACE FUNCTION get_virality_index(
    p_video_id TEXT,
    p_date DATE DEFAULT CURRENT_DATE
)
RETURNS NUMERIC AS $$
DECLARE
    v_gain          BIGINT;
    v_channel_avg   NUMERIC;
    v_channel_id    TEXT;
BEGIN
    -- Lấy view_gain hôm nay của video
    SELECT views_gain INTO v_gain
    FROM daily_delta
    WHERE video_id = p_video_id AND date = p_date;

    IF v_gain IS NULL THEN RETURN NULL; END IF;

    -- Lấy channel_id
    SELECT channel_id INTO v_channel_id FROM videos WHERE id = p_video_id;

    -- Tính trung bình view_gain 30 ngày của tất cả video cùng kênh
    SELECT AVG(d.views_gain) INTO v_channel_avg
    FROM daily_delta d
    JOIN videos v ON d.video_id = v.id
    WHERE v.channel_id = v_channel_id
      AND d.date >= p_date - INTERVAL '30 days'
      AND d.date < p_date
      AND d.views_gain IS NOT NULL
      AND d.views_gain >= 0;

    IF v_channel_avg IS NULL OR v_channel_avg = 0 THEN RETURN NULL; END IF;

    RETURN ROUND((v_gain::NUMERIC / v_channel_avg), 2);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_virality_index IS
    'Trả về Virality Index của video so với baseline kênh 30 ngày. >5 = viral.';


-- Refresh tất cả materialized views (gọi sau mỗi lần 02_load chạy xong)
CREATE OR REPLACE FUNCTION refresh_all_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY weekly_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY monthly_stats;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_all_views IS 'Gọi function này sau khi 02_load_supabase.py hoàn thành';


-- ============================================================
-- 11. ROW LEVEL SECURITY (optional — bật nếu dùng Supabase client-side)
-- ============================================================
-- Bật RLS cho các bảng (service_role key vẫn bypass được)
ALTER TABLE channels     ENABLE ROW LEVEL SECURITY;
ALTER TABLE videos       ENABLE ROW LEVEL SECURITY;
ALTER TABLE daily_delta  ENABLE ROW LEVEL SECURITY;
ALTER TABLE insights     ENABLE ROW LEVEL SECURITY;
ALTER TABLE crawl_logs   ENABLE ROW LEVEL SECURITY;
ALTER TABLE comments     ENABLE ROW LEVEL SECURITY;
ALTER TABLE stream_details ENABLE ROW LEVEL SECURITY;

-- Policy: anon chỉ đọc được, không ghi được
-- Script Python dùng service_role key → bypass RLS hoàn toàn
CREATE POLICY "anon_read_channels"    ON channels     FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read_videos"      ON videos       FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read_delta"       ON daily_delta  FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read_insights"    ON insights     FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read_comments"    ON comments     FOR SELECT TO anon USING (true);


-- ============================================================
-- DONE
-- Kiểm tra bảng đã tạo:
--   SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
-- Kiểm tra views:
--   SELECT matviewname FROM pg_matviews WHERE schemaname = 'public';
-- ============================================================
