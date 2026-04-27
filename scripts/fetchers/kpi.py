"""
fetchers/kpi.py — KPI Cards và Summary metrics
"""

import logging
from supabase import Client
from .base import TODAY, coalesce_gain

log = logging.getLogger("04_html.fetchers.kpi")


def fetch_kpi_cards(sb: Client) -> dict:
    """
    KPI tổng hợp cho 2 stream (VN + Global).

    FIX Day 1 Problem: Không filter NOT NULL.
    Dùng COALESCE(views_gain, views_total) cho mọi aggregation.
    """
    result = {}
    for stream in ("VN", "Global"):
        gain_res = (
            sb.table("daily_delta")
            .select("views_gain, likes_gain, comments_gain, views_total, likes_total, comments_total, content_type")
            .eq("stream", stream)
            .eq("date", TODAY.isoformat())
            .execute()
        )
        rows = gain_res.data or []

        total_views_gain    = sum(coalesce_gain(r, "views_gain",    "views_total")    for r in rows)
        total_likes_gain    = sum(coalesce_gain(r, "likes_gain",    "likes_total")    for r in rows)
        total_comments_gain = sum(coalesce_gain(r, "comments_gain", "comments_total") for r in rows)

        type_counts = {}
        for r in rows:
            ct = r["content_type"]
            type_counts[ct] = type_counts.get(ct, 0) + 1

        all_null = all(r.get("views_gain") is None for r in rows) if rows else False

        # Total videos (không chỉ hôm nay)
        vid_res = (
            sb.table("videos")
            .select("id", count="exact")
            .eq("stream", stream)
            .execute()
        )
        total_videos = vid_res.count or 0

        # Avg ER — sample 200 videos gần nhất
        er_res = (
            sb.table("videos")
            .select("views, likes, comments_count")
            .eq("stream", stream)
            .gt("views", 0)
            .limit(200)
            .execute()
        )
        er_vals = [
            (r["likes"] + r["comments_count"]) / r["views"] * 100
            for r in (er_res.data or []) if r["views"] > 0
        ]
        avg_er = round(sum(er_vals) / len(er_vals), 2) if er_vals else 0.0

        result[stream] = {
            "total_videos":          total_videos,
            "views_gained_today":    total_views_gain,
            "likes_gained_today":    total_likes_gain,
            "comments_gained_today": total_comments_gain,
            "avg_er":                avg_er,
            "active_today":          len(rows),
            "type_counts":           type_counts,
            "is_first_day":          all_null,
        }

    log.info(f"KPI: VN active={result['VN']['active_today']}, "
             f"Global active={result['Global']['active_today']}")
    return result
