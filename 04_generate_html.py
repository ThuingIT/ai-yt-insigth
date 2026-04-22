#!/usr/bin/env python3
"""
04_generate_html.py
===================
Bước 4 (cuối): Query Supabase → render HTML dashboard → deploy GitHub Pages.

Input:
    data/analysis_output.json  (từ bước 03)
    Supabase tables: insights, daily_delta, videos, weekly_stats, monthly_stats

Output:
    docs/index.html  ← GitHub Pages serve từ đây

Cách chạy:
    export SUPABASE_URL="https://xxxx.supabase.co"
    export SUPABASE_SERVICE_KEY="eyJ..."
    export OUTPUT_DIR="data"
    python 04_generate_html.py
"""

import os
import json
import logging
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape

from supabase import create_client, Client


# ============================================================
# CONFIG
# ============================================================

SUPABASE_URL: str         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY: str = os.environ["SUPABASE_SERVICE_KEY"]
OUTPUT_DIR: str           = os.environ.get("OUTPUT_DIR", "data")
DOCS_DIR: str             = os.environ.get("DOCS_DIR", "docs")
TEMPLATE_DIR: str         = os.environ.get("TEMPLATE_DIR", "templates")

TODAY: date      = date.today()
WEEK_AGO: date   = TODAY - timedelta(days=7)
MONTH_AGO: date  = TODAY - timedelta(days=30)

# ICT = UTC+7
ICT = timezone(timedelta(hours=7))


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("04_html")


# ============================================================
# TIMEZONE HELPERS
# ============================================================

def _to_ict_str(utc_str: str) -> str:
    """Convert UTC ISO string sang ICT display string."""
    if not utc_str:
        return ""
    try:
        dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        ict = dt.astimezone(ICT)
        return ict.strftime("%d/%m/%Y %H:%M ICT")
    except Exception:
        return utc_str[:16].replace("T", " ") + " UTC"


def _utc_hhmm_to_ict(hhmm: str) -> str:
    """Convert 'HH:MM' UTC sang ICT (UTC+7), xử lý wrap qua midnight."""
    try:
        h, m = int(hhmm[:2]), int(hhmm[3:5])
        h_ict = (h + 7) % 24
        return f"{h_ict:02d}:{m:02d}"
    except Exception:
        return hhmm


# ============================================================
# DATA FETCHERS
# ============================================================

def fetch_kpi_cards(sb: Client) -> dict:
    """
    KPI tổng hợp cho 2 stream.

    FIX Day 1 Problem: Không filter NOT NULL nữa.
    Dùng views_gain nếu có, fallback sang views_total nếu NULL.
    """
    result = {}
    for stream in ("VN", "Global"):
        # Lấy TẤT CẢ rows hôm nay (kể cả NULL gain)
        gain_res = (
            sb.table("daily_delta")
            .select("views_gain, likes_gain, comments_gain, views_total, likes_total, comments_total, content_type")
            .eq("stream", stream)
            .eq("date", TODAY.isoformat())
            .execute()
        )
        rows = gain_res.data or []

        # COALESCE: ưu tiên gain, fallback sang total (Day 1 case)
        def _v(r, gain_col, total_col):
            v = r.get(gain_col)
            return v if v is not None else (r.get(total_col) or 0)

        total_views_gain    = sum(_v(r, "views_gain",    "views_total")    for r in rows)
        total_likes_gain    = sum(_v(r, "likes_gain",    "likes_total")    for r in rows)
        total_comments_gain = sum(_v(r, "comments_gain", "comments_total") for r in rows)

        type_counts = {}
        for r in rows:
            ct = r["content_type"]
            type_counts[ct] = type_counts.get(ct, 0) + 1

        all_null = all(r.get("views_gain") is None for r in rows) if rows else False

        vid_res = (
            sb.table("videos")
            .select("id", count="exact")
            .eq("stream", stream)
            .execute()
        )
        total_videos = vid_res.count or 0

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
        avg_er = round(sum(er_vals) / len(er_vals), 2) if er_vals else 0

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

    return result


def fetch_trend_line(sb: Client, stream: str, days: int = 30) -> list:
    """
    Views gain tổng theo ngày (30 ngày gần nhất) cho stream.
    FIX: Lấy cả views_total để fallback khi views_gain NULL (Day 1).
    """
    from_date = (TODAY - timedelta(days=days)).isoformat()
    res = (
        sb.table("daily_delta")
        .select("date, views_gain, views_total")
        .eq("stream", stream)
        .gte("date", from_date)
        .order("date", desc=False)
        .execute()
    )

    # Group by date — COALESCE gain → total
    by_date: dict = {}
    for r in (res.data or []):
        d = r["date"]
        val = r["views_gain"] if r["views_gain"] is not None else (r["views_total"] or 0)
        by_date[d] = by_date.get(d, 0) + val

    return [{"date": d, "views": v} for d, v in sorted(by_date.items())]


def fetch_top_videos(sb: Client, stream: str, top_n: int = 10) -> list:
    """Top N video theo views_gain hôm nay với full info."""
    # FIX: Lấy cả views_total, sort theo COALESCE(views_gain, views_total)
    res = (
        sb.table("daily_delta")
        .select("video_id, views_gain, likes_gain, views_total, likes_total, content_type")
        .eq("stream", stream)
        .eq("date", TODAY.isoformat())
        .order("views_total", desc=True)   # Sort theo total thay vì gain để tránh bug NULL sort
        .limit(top_n)
        .execute()
    )
    rows = res.data or []

    enriched = []
    for r in rows:
        v = sb.table("videos").select(
            "title, thumbnail_url, views, likes, comments_count, category_name, published_at, channel_id"
        ).eq("id", r["video_id"]).maybe_single().execute()

        er = 0.0
        if v.data and v.data.get("views", 0) > 0:
            er = round((v.data.get("likes", 0) + v.data.get("comments_count", 0))
                       / v.data["views"] * 100, 2)

        # COALESCE: dùng gain nếu có, fallback sang total
        disp_gain  = r["views_gain"] if r["views_gain"] is not None else (r.get("views_total") or 0)
        disp_likes = r.get("likes_gain") if r.get("likes_gain") is not None else (r.get("likes_total") or 0)

        enriched.append({
            "video_id":       r["video_id"],
            "title":          v.data.get("title", "Không rõ") if v.data else "Không rõ",
            "thumbnail":      v.data.get("thumbnail_url", "") if v.data else "",
            "category":       v.data.get("category_name", "") if v.data else "",
            "content_type":   r["content_type"],
            "views_gain":     disp_gain,
            "likes_gain":     disp_likes,
            "total_views":    v.data.get("views", 0) if v.data else 0,
            "total_likes":    v.data.get("likes", 0) if v.data else 0,
            "total_comments": v.data.get("comments_count", 0) if v.data else 0,
            "er_pct":         er,
            "yt_url":         f"https://youtube.com/watch?v={r['video_id']}",
        })

    return enriched


def fetch_wow_chart(sb: Client) -> dict:
    """WoW data cho cả VN và Global, phân theo content_type."""
    result = {}
    for stream in ("VN", "Global"):
        def week_sum(start, end):
            r = (
                sb.table("daily_delta")
                .select("content_type, views_gain")
                .eq("stream", stream)
                .gte("date", start.isoformat())
                .lt("date", end.isoformat())
                .not_.is_("views_gain", "null")
                .execute()
            )
            totals = {}
            for row in (r.data or []):
                ct = row["content_type"]
                totals[ct] = totals.get(ct, 0) + (row["views_gain"] or 0)
            return totals

        current  = week_sum(WEEK_AGO, TODAY)
        previous = week_sum(TODAY - timedelta(days=14), WEEK_AGO)
        result[stream] = {"current": current, "previous": previous}

    return result


def fetch_post_time_heatmap(sb: Client, stream: str) -> dict:
    """
    Avg views_gain per hour of day (0-23, ICT) trong 30 ngày gần nhất.
    Dùng vẽ bar chart "giờ vàng".
    """
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
            # Convert published_at UTC hour → ICT (UTC+7)
            utc_hour = int(row["published_at"][11:13])
            ict_hour = (utc_hour + 7) % 24
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


def fetch_category_donut(sb: Client, stream: str) -> dict:
    """Category distribution cho donut chart."""
    res = (
        sb.table("videos")
        .select("category_name")
        .eq("stream", stream)
        .execute()
    )
    dist: dict = {}
    for r in (res.data or []):
        cat = r.get("category_name") or "Other"
        dist[cat] = dist.get(cat, 0) + 1
    return dict(sorted(dist.items(), key=lambda x: x[1], reverse=True)[:8])


def fetch_latest_insights(sb: Client) -> dict:
    """Lấy insights mới nhất từ Gemini cho từng loại."""
    result = {}
    for itype in ("weekly_summary", "viral_alert", "content_gap",
                  "recommendation", "trend_lag"):
        res = (
            sb.table("insights")
            .select("insight_type, scope, narrative, payload, generated_at, period_start, period_end")
            .eq("insight_type", itype)
            .order("generated_at", desc=True)
            .limit(1)
            .execute()
        )
        if res.data:
            result[itype] = res.data[0]

    return result


def fetch_monthly_chart(sb: Client, stream: str) -> list:
    """Monthly views_gained cho 6 tháng gần nhất."""
    from_date = (TODAY - timedelta(days=180)).isoformat()
    res = (
        sb.table("monthly_stats")
        .select("month_start, total_views_gained, content_type")
        .eq("stream", stream)
        .gte("month_start", from_date)
        .order("month_start", desc=False)
        .execute()
    )
    # Group by month
    by_month: dict = {}
    for r in (res.data or []):
        m = r["month_start"][:7]  # "2024-01"
        by_month[m] = by_month.get(m, 0) + (r["total_views_gained"] or 0)
    return [{"month": m, "views": v} for m, v in sorted(by_month.items())]


# ============================================================
# HTML RENDERER
# ============================================================

def fetch_top_by_content_type(sb: Client, stream: str, content_type: str, top_n: int = 10) -> list:
    """Top N video của 1 content_type cụ thể (video/stream)."""
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
    enriched = []
    for r in rows:
        v = sb.table("videos").select(
            "title, thumbnail_url, views, likes, comments_count, category_name, published_at"
        ).eq("id", r["video_id"]).maybe_single().execute()
        er = 0.0
        if v.data and v.data.get("views", 0) > 0:
            er = round((v.data.get("likes", 0) + v.data.get("comments_count", 0))
                       / v.data["views"] * 100, 2)
        gain = r["views_gain"] if r["views_gain"] is not None else (r.get("views_total") or 0)
        enriched.append({
            "video_id":       r["video_id"],
            "title":          v.data.get("title", "?") if v.data else "?",
            "thumbnail":      v.data.get("thumbnail_url", "") if v.data else "",
            "category":       v.data.get("category_name", "") if v.data else "",
            "content_type":   r["content_type"],
            "views_gain":     gain,
            "total_views":    v.data.get("views", 0) if v.data else 0,
            "total_likes":    v.data.get("likes", 0) if v.data else 0,
            "total_comments": v.data.get("comments_count", 0) if v.data else 0,
            "er_pct":         er,
            "yt_url":         f"https://youtube.com/watch?v={r['video_id']}",
        })
    return enriched


def fetch_category_performance(sb: Client, stream: str) -> list:
    """
    Per-category: tổng views_gain, avg ER, số video, top video.
    Trả về list sorted by total_views_gain desc.
    """
    res = (
        sb.table("daily_delta")
        .select("video_id, views_gain, views_total, content_type")
        .eq("stream", stream)
        .eq("date", TODAY.isoformat())
        .execute()
    )
    delta_map = {
        r["video_id"]: (r["views_gain"] if r["views_gain"] is not None else (r.get("views_total") or 0))
        for r in (res.data or [])
    }
    if not delta_map:
        return []

    vid_res = (
        sb.table("videos")
        .select("id, category_name, views, likes, comments_count, title, thumbnail_url, content_type")
        .in_("id", list(delta_map.keys()))
        .execute()
    )

    by_cat: dict = {}
    for v in (vid_res.data or []):
        cat  = v.get("category_name") or "Other"
        gain = delta_map.get(v["id"], 0)
        er   = (v["likes"] + v["comments_count"]) / v["views"] * 100 if v["views"] > 0 else 0
        if cat not in by_cat:
            by_cat[cat] = {"category": cat, "total_gain": 0, "er_vals": [],
                           "video_count": 0, "stream_count": 0, "top_video": None}
        by_cat[cat]["total_gain"] += gain
        by_cat[cat]["er_vals"].append(er)
        by_cat[cat]["video_count"] += 1
        if v.get("content_type") == "stream":
            by_cat[cat]["stream_count"] += 1
        if by_cat[cat]["top_video"] is None or gain > delta_map.get(by_cat[cat]["top_video"]["video_id"], 0):
            by_cat[cat]["top_video"] = {
                "video_id":   v["id"],
                "title":      v.get("title", ""),
                "thumbnail":  v.get("thumbnail_url", ""),
                "views_gain": gain,
                "yt_url":     f"https://youtube.com/watch?v={v['id']}",
            }

    result = []
    for cat, d in by_cat.items():
        result.append({
            "category":     cat,
            "total_gain":   d["total_gain"],
            "avg_er":       round(sum(d["er_vals"]) / len(d["er_vals"]), 2) if d["er_vals"] else 0,
            "video_count":  d["video_count"],
            "stream_count": d["stream_count"],
            "top_video":    d["top_video"],
        })
    result.sort(key=lambda x: x["total_gain"], reverse=True)
    return result[:8]


def fetch_stream_vs_video_stats(sb: Client, stream: str) -> dict:
    """
    So sánh trực tiếp Video vs Stream (Shorts không dùng):
    - Tổng views, avg ER, số lượng, % tổng
    """
    res = (
        sb.table("videos")
        .select("content_type, views, likes, comments_count")
        .eq("stream", stream)
        .execute()
    )
    data: dict = {}
    for r in (res.data or []):
        ct  = r["content_type"]
        er  = (r["likes"] + r["comments_count"]) / r["views"] * 100 if r["views"] > 0 else 0
        v   = r["views"]
        if ct not in data:
            data[ct] = {"count": 0, "total_views": 0, "er_vals": []}
        data[ct]["count"]       += 1
        data[ct]["total_views"] += v
        data[ct]["er_vals"].append(er)

    result = {}
    grand_total = sum(d["total_views"] for d in data.values()) or 1
    for ct, d in data.items():
        result[ct] = {
            "count":       d["count"],
            "total_views": d["total_views"],
            "pct_views":   round(d["total_views"] / grand_total * 100, 1),
            "avg_er":      round(sum(d["er_vals"]) / len(d["er_vals"]), 2) if d["er_vals"] else 0,
        }
    return result


def fetch_intraday_chart(sb: Client, stream: str) -> list:
    """
    Views_delta_1h theo từng giờ trong ngày hôm nay (ICT).
    Fallback: query trực tiếp hourly_snapshot nếu view chưa refresh.
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
        return [
            {
                # Convert UTC "HH:MM" → ICT
                "hour":   _utc_hhmm_to_ict(r["hour_bucket"][11:16]),
                "views":  r["total_views_gained"] or 0,
                "videos": r["active_videos"] or 0,
            }
            for r in rows
        ]
    except Exception as e:
        log.warning(f"intraday_chart fallback: {e}")
        # Fallback: query hourly_snapshot trực tiếp
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
            h = f"{(utc_h + 7) % 24:02d}:00"
            by_hour[h] = by_hour.get(h, 0) + (r["views_delta_1h"] or 0)
        return [{"hour": h, "views": v, "videos": 0} for h, v in sorted(by_hour.items())]


def fetch_hot_right_now(sb: Client, stream: str, limit: int = 8) -> list:
    """
    Top N video có views_delta_1h cao nhất trong 2 giờ qua.
    Dùng Postgres RPC get_hot_right_now() đã tạo trong migration SQL.
    """
    try:
        res = sb.rpc("get_hot_right_now", {
            "p_stream":    stream,
            "p_limit":     limit,
            "p_hours_ago": 2,
        }).execute()
        rows = res.data or []
        return [
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
    except Exception as e:
        log.warning(f"get_hot_right_now error: {e}")
        return []


def fetch_momentum_summary(sb: Client, stream: str) -> dict:
    """
    Đếm số video theo momentum_status hôm nay.
    Trả về {accelerating: N, peaking: N, decelerating: N, fading: N, new: N}
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
    return counts


def build_template_context(sb: Client, analysis: dict) -> dict:
    """Tổng hợp tất cả dữ liệu cần thiết cho Jinja2 template."""
    log.info("Fetching KPI cards...")
    kpi = fetch_kpi_cards(sb)

    log.info("Fetching trend lines...")
    trend_vn     = fetch_trend_line(sb, "VN", days=30)
    trend_global = fetch_trend_line(sb, "Global", days=30)

    log.info("Fetching top videos...")
    top_vn     = fetch_top_videos(sb, "VN", top_n=10)
    top_global = fetch_top_videos(sb, "Global", top_n=10)

    log.info("Fetching WoW chart...")
    wow = fetch_wow_chart(sb)

    log.info("Fetching post time heatmap...")
    post_time_vn     = fetch_post_time_heatmap(sb, "VN")
    post_time_global = fetch_post_time_heatmap(sb, "Global")

    log.info("Fetching category distribution...")
    cat_vn     = fetch_category_donut(sb, "VN")
    cat_global = fetch_category_donut(sb, "Global")

    log.info("Fetching insights...")
    insights = fetch_latest_insights(sb)

    log.info("Fetching monthly chart...")
    monthly_vn     = fetch_monthly_chart(sb, "VN")
    monthly_global = fetch_monthly_chart(sb, "Global")

    log.info("Fetching top by content type...")
    top_vn_video   = fetch_top_by_content_type(sb, "VN",     "video",  10)
    top_vn_stream  = fetch_top_by_content_type(sb, "VN",     "stream", 10)
    top_gl_video   = fetch_top_by_content_type(sb, "Global", "video",  10)
    top_gl_stream  = fetch_top_by_content_type(sb, "Global", "stream", 10)

    log.info("Fetching category performance...")
    cat_perf_vn     = fetch_category_performance(sb, "VN")
    cat_perf_global = fetch_category_performance(sb, "Global")

    log.info("Fetching stream vs video stats...")
    svv_vn     = fetch_stream_vs_video_stats(sb, "VN")
    svv_global = fetch_stream_vs_video_stats(sb, "Global")

    log.info("Fetching intraday charts (VN only)...")
    intraday_vn = fetch_intraday_chart(sb, "VN")

    log.info("Fetching hot right now...")
    hot_vn     = fetch_hot_right_now(sb, "VN", limit=8)
    hot_global = fetch_hot_right_now(sb, "Global", limit=8)

    log.info("Fetching momentum summary...")
    momentum_vn     = fetch_momentum_summary(sb, "VN")
    momentum_global = fetch_momentum_summary(sb, "Global")

    # Tier1 & Tier2: ưu tiên Supabase insights, fallback sang analysis_output.json
    tier1 = analysis.get("tier1", {})
    tier2 = analysis.get("tier2", {})

    # FIX: Lấy Gemini narrative từ Supabase insights (tồn tại cả khi hourly run)
    def _insight_field(itype: str, field: str, default=""):
        row = insights.get(itype, {})
        if field == "narrative":
            return row.get("narrative") or default
        return (row.get("payload") or {}).get(field, default)

    weekly_narrative = (
        tier2.get("weekly_narrative")
        or _insight_field("weekly_summary", "narrative")
        or ""
    )
    anomalies = (
        tier2.get("anomalies")
        or [r for itype in ["anomaly", "viral_alert"]
            for r in [insights.get(itype, {}).get("payload", {})]
            if r]
        or []
    )
    recommendations = (
        tier2.get("recommendations")
        or (insights.get("recommendation", {}).get("payload") or {}).get("recommendations", [])
    )
    content_gaps = (
        tier2.get("content_gaps")
        or (insights.get("content_gap", {}).get("payload") or {}).get("gaps", [])
    )
    trend_forecast = (
        tier2.get("trend_forecast")
        or ""
    )

    # Convert Gemini generated_at sang ICT
    raw_gemini_ts = (insights.get("weekly_summary") or {}).get("generated_at", "")
    gemini_generated_at_ict = _to_ict_str(raw_gemini_ts)

    now_ict = datetime.now(ICT)

    return {
        # Meta
        "generated_at":  now_ict.strftime("%d/%m/%Y %H:%M ICT"),
        "today":         now_ict.strftime("%d/%m/%Y"),
        "run_id":        analysis.get("run_id", ""),

        # KPI
        "kpi": kpi,

        # Charts data (serialized to JSON for Chart.js)
        "trend_vn_json":       json.dumps(trend_vn,     ensure_ascii=False),
        "trend_global_json":   json.dumps(trend_global, ensure_ascii=False),
        "monthly_vn_json":     json.dumps(monthly_vn,   ensure_ascii=False),
        "monthly_global_json": json.dumps(monthly_global, ensure_ascii=False),
        "wow_json":            json.dumps(wow,           ensure_ascii=False),
        "post_time_vn_json":   json.dumps(post_time_vn, ensure_ascii=False),
        "cat_vn_json":         json.dumps(cat_vn,       ensure_ascii=False),
        "cat_global_json":     json.dumps(cat_global,   ensure_ascii=False),

        # Top videos
        "top_vn":     top_vn,
        "top_global": top_global,

        # Tier1 analytics
        "viral_vn":     tier1.get("VN", {}).get("viral", [])[:5],
        "viral_global": tier1.get("Global", {}).get("viral", [])[:5],
        "trend_lag":    tier1.get("trend_lag", {}),
        "er_vn":        tier1.get("VN", {}).get("er_by_type", {}),
        "er_global":    tier1.get("Global", {}).get("er_by_type", {}),

        # Tier2 Gemini
        "weekly_narrative":    weekly_narrative,
        "anomalies":           anomalies,
        "recommendations":     recommendations,
        "content_gaps":        content_gaps,
        "trend_forecast":      trend_forecast,
        "gemini_generated_at": gemini_generated_at_ict,

        # Insights từ Supabase
        "insights": insights,

        # Intraday (VN only — hourly không crawl Global)
        "intraday_vn_json": json.dumps(intraday_vn, ensure_ascii=False),

        # Hot right now
        "hot_vn":     hot_vn,
        "hot_global": hot_global,

        # Momentum summary
        "momentum_vn":     momentum_vn,
        "momentum_global": momentum_global,

        # Last updated time (ICT)
        "last_updated_ts": now_ict.strftime("%H:%M ICT"),

        # Top by content type
        "top_vn_video":   top_vn_video,
        "top_vn_stream":  top_vn_stream,
        "top_gl_video":   top_gl_video,
        "top_gl_stream":  top_gl_stream,

        # Category performance
        "cat_perf_vn_json":     json.dumps(cat_perf_vn,     ensure_ascii=False, default=str),
        "cat_perf_global_json": json.dumps(cat_perf_global, ensure_ascii=False, default=str),

        # Stream vs Video stats
        "svv_vn_json":     json.dumps(svv_vn,     ensure_ascii=False),
        "svv_global_json": json.dumps(svv_global, ensure_ascii=False),
        "svv_vn_data":     svv_vn,
        "svv_global_data": svv_global,
    }


def render_html(context: dict) -> str:
    """Render HTML từ Jinja2 template."""
    env = Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template("dashboard.html")
    return template.render(**context)


# ============================================================
# MAIN
# ============================================================

def main():
    log.info(f"=== 04_generate_html START | date={TODAY} ===")

    # Đọc analysis output từ bước 03
    analysis_path = os.path.join(OUTPUT_DIR, "analysis_output.json")
    if not os.path.exists(analysis_path):
        raise FileNotFoundError(
            f"Không tìm thấy {analysis_path} — đảm bảo 03_analyze đã chạy thành công"
        )
    with open(analysis_path, "r", encoding="utf-8") as f:
        analysis = json.load(f)
    log.info(f"Loaded analysis: {analysis_path}")

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # Build context
    context = build_template_context(sb, analysis)
    log.info("Template context built")

    # Render HTML
    html = render_html(context)
    log.info("HTML rendered")

    # Ghi file
    os.makedirs(DOCS_DIR, exist_ok=True)
    output_path = os.path.join(DOCS_DIR, "index.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = Path(output_path).stat().st_size // 1024
    log.info(f"Dashboard saved → {output_path} ({size_kb} KB)")
    log.info(f"=== 04_generate_html DONE ===")


if __name__ == "__main__":
    main()
