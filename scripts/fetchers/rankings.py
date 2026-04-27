"""
fetchers/rankings.py — Top Video rankings và Category leaderboard.

FIX N+1: Tất cả fetch video detail dùng batch_video_lookup()
thay vì query riêng từng video.
"""

import logging
from supabase import Client
from .base import TODAY, MONTH_AGO, batch_video_lookup, coalesce_gain

log = logging.getLogger("04_html.fetchers.rankings")


def _compute_er(video_row: dict) -> float:
    views = video_row.get("views", 0) or 0
    if views <= 0:
        return 0.0
    return round((video_row.get("likes", 0) + video_row.get("comments_count", 0)) / views * 100, 2)


def _enrich_delta_rows(rows: list[dict], video_map: dict[str, dict],
                       stream: str) -> list[dict]:
    """
    Shared helper: enrich daily_delta rows với video metadata từ batch lookup.
    Không N+1 vì video_map đã được fetch 1 lần bên ngoài.
    """
    enriched = []
    for r in rows:
        v = video_map.get(r["video_id"]) or {}
        disp_gain  = coalesce_gain(r, "views_gain",  "views_total")
        disp_likes = coalesce_gain(r, "likes_gain",  "likes_total")
        enriched.append({
            "video_id":       r["video_id"],
            "title":          v.get("title", "Không rõ"),
            "thumbnail":      v.get("thumbnail_url", ""),
            "category":       v.get("category_name", ""),
            "content_type":   r["content_type"],
            "views_gain":     disp_gain,
            "likes_gain":     disp_likes,
            "total_views":    v.get("views", 0),
            "total_likes":    v.get("likes", 0),
            "total_comments": v.get("comments_count", 0),
            "er_pct":         _compute_er(v),
            "yt_url":         f"https://youtube.com/watch?v={r['video_id']}",
        })
    return enriched


def fetch_top_videos(sb: Client, stream: str, top_n: int = 10) -> list:
    """Top N video theo views_total hôm nay với full info. [N+1 FIXED]"""
    res = (
        sb.table("daily_delta")
        .select("video_id, views_gain, likes_gain, views_total, likes_total, content_type")
        .eq("stream", stream)
        .eq("date", TODAY.isoformat())
        .order("views_total", desc=True)
        .limit(top_n)
        .execute()
    )
    rows = res.data or []
    video_map = batch_video_lookup(sb, [r["video_id"] for r in rows])
    result = _enrich_delta_rows(rows, video_map, stream)
    log.info(f"fetch_top_videos({stream}): {len(result)} videos")
    return result


def fetch_top_by_content_type(sb: Client, stream: str,
                               content_type: str, top_n: int = 10) -> list:
    """Top N video của 1 content_type cụ thể (video/stream/shorts). [N+1 FIXED]"""
    res = (
        sb.table("daily_delta")
        .select("video_id, views_gain, likes_gain, views_total, content_type")
        .eq("stream", stream)
        .eq("date", TODAY.isoformat())
        .eq("content_type", content_type)
        .order("views_total", desc=True)
        .limit(top_n)
        .execute()
    )
    rows = res.data or []
    video_map = batch_video_lookup(sb, [r["video_id"] for r in rows])
    result = _enrich_delta_rows(rows, video_map, stream)
    log.info(f"fetch_top_by_content_type({stream}, {content_type}): {len(result)} videos")
    return result


def fetch_category_performance(sb: Client, stream: str) -> list:
    """
    Per-category: tổng views_gain, avg ER, số video, top video.
    Trả về list sorted by total_views_gain desc. [N+1 FIXED]
    """
    res = (
        sb.table("daily_delta")
        .select("video_id, views_gain, views_total, content_type")
        .eq("stream", stream)
        .eq("date", TODAY.isoformat())
        .execute()
    )
    delta_rows = res.data or []
    delta_map  = {r["video_id"]: coalesce_gain(r) for r in delta_rows}
    if not delta_map:
        return []

    # Batch video lookup — 1 query thay vì N queries
    video_map = batch_video_lookup(
        sb, list(delta_map.keys()),
        fields="id, category_name, views, likes, comments_count, title, thumbnail_url, content_type"
    )

    by_cat: dict = {}
    for vid_id, v in video_map.items():
        cat  = v.get("category_name") or "Other"
        gain = delta_map.get(vid_id, 0)
        er   = _compute_er(v)

        if cat not in by_cat:
            by_cat[cat] = {
                "category":     cat,
                "total_gain":   0,
                "er_vals":      [],
                "video_count":  0,
                "stream_count": 0,
                "top_video":    None,
            }
        by_cat[cat]["total_gain"]  += gain
        by_cat[cat]["er_vals"].append(er)
        by_cat[cat]["video_count"] += 1
        if v.get("content_type") == "stream":
            by_cat[cat]["stream_count"] += 1

        # Track top video in category
        cur_top = by_cat[cat]["top_video"]
        if cur_top is None or gain > delta_map.get(cur_top["video_id"], 0):
            by_cat[cat]["top_video"] = {
                "video_id":   vid_id,
                "title":      v.get("title", ""),
                "thumbnail":  v.get("thumbnail_url", ""),
                "views_gain": gain,
                "yt_url":     f"https://youtube.com/watch?v={vid_id}",
            }

    result = [
        {
            "category":     cat,
            "total_gain":   d["total_gain"],
            "avg_er":       round(sum(d["er_vals"]) / len(d["er_vals"]), 2) if d["er_vals"] else 0,
            "video_count":  d["video_count"],
            "stream_count": d["stream_count"],
            "top_video":    d["top_video"],
        }
        for cat, d in by_cat.items()
    ]
    result.sort(key=lambda x: x["total_gain"], reverse=True)
    log.info(f"fetch_category_performance({stream}): {len(result)} categories")
    return result[:8]


def fetch_stream_vs_video_stats(sb: Client, stream: str) -> dict:
    """So sánh trực tiếp Video vs Stream vs Shorts: tổng views, avg ER, số lượng."""
    res = (
        sb.table("videos")
        .select("content_type, views, likes, comments_count")
        .eq("stream", stream)
        .execute()
    )
    data: dict = {}
    for r in (res.data or []):
        ct  = r["content_type"]
        er  = _compute_er(r)
        v   = r["views"] or 0
        if ct not in data:
            data[ct] = {"count": 0, "total_views": 0, "er_vals": []}
        data[ct]["count"]       += 1
        data[ct]["total_views"] += v
        data[ct]["er_vals"].append(er)

    grand_total = sum(d["total_views"] for d in data.values()) or 1
    result = {
        ct: {
            "count":       d["count"],
            "total_views": d["total_views"],
            "pct_views":   round(d["total_views"] / grand_total * 100, 1),
            "avg_er":      round(sum(d["er_vals"]) / len(d["er_vals"]), 2) if d["er_vals"] else 0,
        }
        for ct, d in data.items()
    }
    log.info(f"fetch_stream_vs_video_stats({stream}): {list(result.keys())}")
    return result


def fetch_post_time_heatmap(sb: Client, stream: str) -> dict:
    """
    Avg views_gain per hour of day (0-23, ICT) trong 30 ngày gần nhất.
    Dùng vẽ bar chart 'giờ vàng'. [N+1 FIXED via batch join]
    """
    from .base import utc_hour_to_ict

    res = (
        sb.table("daily_delta")
        .select("video_id, views_gain")
        .eq("stream", stream)
        .gte("date", MONTH_AGO.isoformat())
        .not_.is_("views_gain", "null")
        .execute()
    )
    delta_map = {r["video_id"]: r["views_gain"] for r in (res.data or [])}
    if not delta_map:
        return {}

    # Batch fetch published_at — 1 query
    vid_res = (
        sb.table("videos")
        .select("id, published_at")
        .in_("id", list(delta_map.keys()))
        .not_.is_("published_at", "null")
        .execute()
    )
    hour_gains: dict = {}
    for row in (vid_res.data or []):
        try:
            utc_hour = int(row["published_at"][11:13])
            ict_hour = utc_hour_to_ict(utc_hour)
            gain = delta_map.get(row["id"])
            if gain is not None:
                hour_gains.setdefault(ict_hour, []).append(gain)
        except (IndexError, ValueError, TypeError):
            continue

    return {
        str(h): round(sum(v) / len(v), 0)
        for h in range(24)
        for v in [hour_gains.get(h, [])]
        if v
    }
