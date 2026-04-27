-- ============================================================
-- cleanup_old_data.sql
-- Migration: Thêm function cleanup data cũ để tránh DB phình to.
-- Chạy trong Supabase SQL Editor 1 lần.
-- ============================================================
-- Mục tiêu:
--   - hourly_snapshot: xóa sau 7 ngày (đủ cho trend analysis ngắn hạn)
--   - crawl_logs: xóa sau 30 ngày (debugging window)
--   - insights: giữ lại 90 insight gần nhất mỗi type (weekly_summary giữ nhiều hơn)
-- ============================================================


-- ============================================================
-- FUNCTION: cleanup_old_data()
-- Gọi từ daily pipeline sau job load.
-- ============================================================
CREATE OR REPLACE FUNCTION cleanup_old_data(
    p_hourly_retention_days INT DEFAULT 7,
    p_logs_retention_days   INT DEFAULT 30,
    p_insights_keep         INT DEFAULT 90
)
RETURNS TABLE (
    table_name  TEXT,
    rows_deleted BIGINT
) AS $$
DECLARE
    v_hourly_cutoff   TIMESTAMPTZ;
    v_logs_cutoff     TIMESTAMPTZ;
    v_deleted         BIGINT;
BEGIN
    v_hourly_cutoff := NOW() - (p_hourly_retention_days || ' days')::INTERVAL;
    v_logs_cutoff   := NOW() - (p_logs_retention_days   || ' days')::INTERVAL;

    -- 1. Xóa hourly_snapshot cũ hơn 7 ngày
    DELETE FROM hourly_snapshot
    WHERE snapshot_at < v_hourly_cutoff;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    table_name  := 'hourly_snapshot';
    rows_deleted := v_deleted;
    RETURN NEXT;

    -- 2. Xóa crawl_logs cũ hơn 30 ngày
    DELETE FROM crawl_logs
    WHERE started_at < v_logs_cutoff;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    table_name  := 'crawl_logs';
    rows_deleted := v_deleted;
    RETURN NEXT;

    -- 3. Trim insights: giữ 90 insights mới nhất per type
    --    (trừ weekly_summary giữ nhiều hơn vì là lịch sử AI insight)
    DELETE FROM insights
    WHERE id IN (
        SELECT id FROM (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY insight_type
                       ORDER BY generated_at DESC
                   ) AS rn,
                   insight_type
            FROM insights
        ) ranked
        WHERE (insight_type = 'weekly_summary' AND rn > 365)   -- giữ 1 năm weekly summary
           OR (insight_type != 'weekly_summary' AND rn > p_insights_keep)
    );
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    table_name  := 'insights';
    rows_deleted := v_deleted;
    RETURN NEXT;

END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cleanup_old_data IS
    'Dọn dẹp data cũ: hourly_snapshot (7 ngày), crawl_logs (30 ngày), insights (90 per type). '
    'Gọi sau mỗi lần daily pipeline chạy xong.';


-- ============================================================
-- Kiểm tra function hoạt động:
--   SELECT * FROM cleanup_old_data();
--
-- Kết quả mong đợi:
--   table_name       | rows_deleted
--   -----------------+-------------
--   hourly_snapshot  | 0     (nếu chưa có data cũ)
--   crawl_logs       | 0
--   insights         | 0
-- ============================================================
