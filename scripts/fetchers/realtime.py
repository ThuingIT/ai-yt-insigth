"""
fetchers/realtime.py — Real-time và near-real-time data:
  - Hot Right Now (từ hourly_snapshot / RPC)
  - Momentum summary (accelerating/peaking/decelerating/fading/new)
  - Intraday chart (views per hour hôm nay, ICT)
"""

import logging
from supabase import Client
from .base import TODAY, utc_hour_to_ict

log = logging.getLogger("04_html.fetchers.realtime")


def fetch_hot_right_now(sb: Client, stream: str, limit: int = 8) -> list:
    """
    Top N video có views_delta_1h cao nhất trong 2 giờ qua.
    Dùng Postgres RPC get_hot_right_now(). Fallback về [] nếu chưa có hourly data.
    """
    try:
        res = sb.rpc("get_hot_right_now", {
            "p_stream":    stream,
            "p_limit":     limit,
            "p_hours_ago": 2,
        }).execute()
        rows = res.data or []
        result = [
            {
                "video_id":       r["video_id"],
                "title":          r["title"] or "Không rõ",
                "thumbnail":      r["thumbnail_url"] or "",
                "category":       r["category_name"] or "",
                "content_type":   r["content_type"],
                "views_delta_1h": r["views_delta_1h"] or 0,
                "total_views":    r["total_views"] or 0,
                "momentum":       r["momentum_status"] or "new",
                "yt_url":         f"https://youtube.com/watch?v={r['video_id']}",
            }
            for r in rows
        ]
        log.info(f"fetch_hot_right_now({stream}): {len(result)} videos")
        return result
    except Exception as e:
        log.warning(f"get_hot_right_now({stream}) failed: {e!r} — returning []")
        return []


def fetch_momentum_summary(sb: Client, stream: str) -> dict:
    """
    Đếm số video theo momentum_status hôm nay.
    Returns: {accelerating: N, peaking: N, decelerating: N, fading: N, new: N}
    """
    res = (
        sb.table("daily_delta")
        .select("momentum_status")
        .eq("stream", stream)
        .eq("date", TODAY.isoformat())
        .execute()
    )
    counts: dict = {"accelerating": 0, "peaking": 0, "decelerating": 0, "fading": 0, "new": 0}
    for r in (res.data or []):
        status = r.get("momentum_status") or "new"
        counts[status] = counts.get(status, 0) + 1
    log.info(f"fetch_momentum_summary({stream}): {counts}")
    return counts


def fetch_intraday_chart(sb: Client, stream: str) -> list:
    """
    Views_delta_1h theo từng giờ trong ngày hôm nay (ICT).
    Primary: query materialized view intraday_chart.
    Fallback: query hourly_snapshot trực tiếp nếu view chưa refresh.
    Giờ hiển thị đã được convert sang ICT (UTC+7).
    """
    try:
        res = (
            sb.table("intraday_chart")
            .select("hour_bucket, total_views_gained, active_videos")
            .eq("stream", stream)
            .gte("hour_bucket", TODAY.isoformat())
            .order("hour_bucket", desc=False)
            .execute()
        )
        rows = res.data or []
        if rows:
            return [
                {
                    "hour":   _fmt_ict_from_bucket(r["hour_bucket"]),
                    "views":  r["total_views_gained"] or 0,
                    "videos": r["active_videos"] or 0,
                }
                for r in rows
            ]
    except Exception as e:
        log.warning(f"intraday_chart view failed ({stream}): {e!r} — falling back to hourly_snapshot")

    # Fallback: query hourly_snapshot trực tiếp
    try:
        res = (
            sb.table("hourly_snapshot")
            .select("snapshot_at, views_delta_1h")
            .eq("stream", stream)
            .gte("snapshot_at", TODAY.isoformat())
            .not_.is_("views_delta_1h", "null")
            .order("snapshot_at", desc=False)
            .execute()
        )
        by_hour: dict = {}
        for r in (res.data or []):
            utc_h = int(r["snapshot_at"][11:13])
            h     = f"{utc_hour_to_ict(utc_h):02d}:00"
            by_hour[h] = by_hour.get(h, 0) + (r["views_delta_1h"] or 0)
        return [{"hour": h, "views": v, "videos": 0} for h, v in sorted(by_hour.items())]
    except Exception as e:
        log.warning(f"hourly_snapshot fallback ({stream}) also failed: {e!r}")
        return []


def _fmt_ict_from_bucket(bucket_str: str) -> str:
    """
    Convert hour_bucket timestamp (UTC, format '2024-01-15T07:00:00+00:00')
    sang ICT HH:MM display string.
    """
    try:
        utc_h = int(bucket_str[11:13])
        return f"{utc_hour_to_ict(utc_h):02d}:00"
    except Exception:
        return bucket_str[11:16]
