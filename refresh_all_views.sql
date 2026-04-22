-- Function mới: không dùng CONCURRENTLY, an toàn với mọi index
CREATE OR REPLACE FUNCTION refresh_all_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW weekly_stats;
    REFRESH MATERIALIZED VIEW monthly_stats;
END;
$$ LANGUAGE plpgsql;