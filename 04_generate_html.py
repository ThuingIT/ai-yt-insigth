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
# DATA FETCHERS
# ============================================================

def fetch_kpi_cards(sb: Client) -> dict:
    """
    KPI tổng hợp cho 2 stream.
    Trả về: total_videos, total_views_gained_today, top_category, avg_er
    """
    result = {}
    for stream in ("VN", "Global"):
        # Tổng views gain hôm nay
        gain_res = (
            sb.table("daily_delta")
            .select("views_gain, likes_gain, comments_gain, content_type")
            .eq("stream", stream)
            .eq("date", TODAY.isoformat())
            .not_.is_("views_gain", "null")
            .execute()
        )
        rows = gain_res.data or []
        total_views_gain    = sum(r["views_gain"] or 0 for r in rows)
        total_likes_gain    = sum(r["likes_gain"] or 0 for r in rows)
        total_comments_gain = sum(r["comments_gain"] or 0 for r in rows)

        # Phân bố content type hôm nay
        type_counts = {}
        for r in rows:
            ct = r["content_type"]
            type_counts[ct] = type_counts.get(ct, 0) + 1

        # Tổng số video trong DB của stream này
        vid_res = (
            sb.table("videos")
            .select("id", count="exact")
            .eq("stream", stream)
            .execute()
        )
        total_videos = vid_res.count or 0

        # ER trung bình
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
            "total_videos":       total_videos,
            "views_gained_today": total_views_gain,
            "likes_gained_today": total_likes_gain,
            "comments_gained_today": total_comments_gain,
            "avg_er":             avg_er,
            "active_today":       len(rows),
            "type_counts":        type_counts,
        }

    return result


def fetch_trend_line(sb: Client, stream: str, days: int = 30) -> list:
    """
    Views gain tổng theo ngày (30 ngày gần nhất) cho stream.
    Dùng vẽ line chart.
    """
    from_date = (TODAY - timedelta(days=days)).isoformat()
    res = (
        sb.table("daily_delta")
        .select("date, views_gain")
        .eq("stream", stream)
        .gte("date", from_date)
        .not_.is_("views_gain", "null")
        .order("date", desc=False)
        .execute()
    )

    # Group by date
    by_date: dict = {}
    for r in (res.data or []):
        d = r["date"]
        by_date[d] = by_date.get(d, 0) + (r["views_gain"] or 0)

    return [{"date": d, "views": v} for d, v in sorted(by_date.items())]


def fetch_top_videos(sb: Client, stream: str, top_n: int = 10) -> list:
    """Top N video theo views_gain hôm nay với full info."""
    res = (
        sb.table("daily_delta")
        .select("video_id, views_gain, likes_gain, content_type")
        .eq("stream", stream)
        .eq("date", TODAY.isoformat())
        .not_.is_("views_gain", "null")
        .order("views_gain", desc=True)
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

        enriched.append({
            "video_id":     r["video_id"],
            "title":        v.data.get("title", "Không rõ") if v.data else "Không rõ",
            "thumbnail":    v.data.get("thumbnail_url", "") if v.data else "",
            "category":     v.data.get("category_name", "") if v.data else "",
            "content_type": r["content_type"],
            "views_gain":   r["views_gain"],
            "likes_gain":   r.get("likes_gain", 0),
            "total_views":  v.data.get("views", 0) if v.data else 0,
            "er_pct":       er,
            "yt_url":       f"https://youtube.com/watch?v={r['video_id']}",
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
    Avg views_gain per hour of day (0-23) trong 30 ngày gần nhất.
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
            hour = int(row["published_at"][11:13])
            gain = delta_map.get(row["id"])
            if gain is not None:
                hour_gains.setdefault(hour, []).append(gain)
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

    # Tier1 & Tier2 từ analysis_output.json
    tier1  = analysis.get("tier1", {})
    tier2  = analysis.get("tier2", {})

    return {
        # Meta
        "generated_at":  datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC"),
        "today":         TODAY.strftime("%d/%m/%Y"),
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
        "weekly_narrative": tier2.get("weekly_narrative", ""),
        "anomalies":        tier2.get("anomalies", []),
        "recommendations":  tier2.get("recommendations", []),
        "content_gaps":     tier2.get("content_gaps", []),
        "trend_forecast":   tier2.get("trend_forecast", ""),

        # Insights từ Supabase (bổ sung cho display)
        "insights": insights,
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
