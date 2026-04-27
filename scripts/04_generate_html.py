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

Kiến trúc:
    - scripts/fetchers/     ← tất cả Supabase query logic
    - scripts/fetchers/base.py  ← safe_fetch(), batch_video_lookup()
    - 04_generate_html.py   ← thin orchestrator, chỉ glue code
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from supabase import create_client

# Thêm thư mục scripts vào sys.path để import fetchers package
_SCRIPTS_DIR = Path(__file__).parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from fetchers.base import TODAY, ICT, safe_fetch, to_ict_str
from fetchers.kpi import fetch_kpi_cards
from fetchers.rankings import (
    fetch_top_videos,
    fetch_top_by_content_type,
    fetch_category_performance,
    fetch_stream_vs_video_stats,
    fetch_post_time_heatmap,
)
from fetchers.charts import (
    fetch_trend_line,
    fetch_wow_chart,
    fetch_monthly_chart,
    fetch_category_donut,
)
from fetchers.realtime import (
    fetch_hot_right_now,
    fetch_momentum_summary,
    fetch_intraday_chart,
)
from fetchers.insights import fetch_latest_insights, build_insight_context


# ============================================================
# CONFIG
# ============================================================

SUPABASE_URL: str         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY: str = os.environ["SUPABASE_SERVICE_KEY"]
OUTPUT_DIR: str           = os.environ.get("OUTPUT_DIR", "data")
DOCS_DIR: str             = os.environ.get("DOCS_DIR", "docs")
TEMPLATE_DIR: str         = os.environ.get("TEMPLATE_DIR", "templates")


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
# BUILD TEMPLATE CONTEXT — thin orchestrator
# ============================================================

def build_template_context(sb, analysis: dict) -> dict:
    """
    Orchestrate tất cả fetch calls và assemble context cho Jinja2.

    Mỗi fetch được wrap bởi safe_fetch() → dashboard luôn render
    được dù 1-2 section bị lỗi DB.
    """
    tier1    = analysis.get("tier1", {})
    tier2    = analysis.get("tier2", {})

    log.info("Fetching KPI cards...")
    kpi = safe_fetch(fetch_kpi_cards, sb,
                     default={"VN": _empty_kpi(), "Global": _empty_kpi()},
                     label="kpi_cards")

    log.info("Fetching trend lines...")
    trend_vn     = safe_fetch(fetch_trend_line, sb, "VN",     30, default=[], label="trend_vn")
    trend_global = safe_fetch(fetch_trend_line, sb, "Global", 30, default=[], label="trend_global")

    log.info("Fetching top videos (VN + Global)...")
    top_vn     = safe_fetch(fetch_top_videos, sb, "VN",     10, default=[], label="top_vn")
    top_global = safe_fetch(fetch_top_videos, sb, "Global", 10, default=[], label="top_global")

    log.info("Fetching top by content type...")
    top_vn_video   = safe_fetch(fetch_top_by_content_type, sb, "VN",     "video",  10, default=[], label="top_vn_video")
    top_vn_stream  = safe_fetch(fetch_top_by_content_type, sb, "VN",     "stream", 10, default=[], label="top_vn_stream")
    top_gl_video   = safe_fetch(fetch_top_by_content_type, sb, "Global", "video",  10, default=[], label="top_gl_video")
    top_gl_stream  = safe_fetch(fetch_top_by_content_type, sb, "Global", "stream", 10, default=[], label="top_gl_stream")

    log.info("Fetching WoW chart...")
    wow = safe_fetch(fetch_wow_chart, sb, default={}, label="wow")

    log.info("Fetching monthly chart...")
    monthly_vn     = safe_fetch(fetch_monthly_chart, sb, "VN",     default=[], label="monthly_vn")
    monthly_global = safe_fetch(fetch_monthly_chart, sb, "Global", default=[], label="monthly_global")

    log.info("Fetching post time heatmap...")
    post_time_vn = safe_fetch(fetch_post_time_heatmap, sb, "VN", default={}, label="post_time_vn")

    log.info("Fetching category data...")
    cat_vn     = safe_fetch(fetch_category_donut,       sb, "VN",     default={}, label="cat_vn")
    cat_global = safe_fetch(fetch_category_donut,       sb, "Global", default={}, label="cat_global")
    cat_perf_vn     = safe_fetch(fetch_category_performance, sb, "VN",     default=[], label="cat_perf_vn")
    cat_perf_global = safe_fetch(fetch_category_performance, sb, "Global", default=[], label="cat_perf_global")

    log.info("Fetching stream vs video stats...")
    svv_vn     = safe_fetch(fetch_stream_vs_video_stats, sb, "VN",     default={}, label="svv_vn")
    svv_global = safe_fetch(fetch_stream_vs_video_stats, sb, "Global", default={}, label="svv_global")

    log.info("Fetching intraday chart (VN only)...")
    intraday_vn = safe_fetch(fetch_intraday_chart, sb, "VN", default=[], label="intraday_vn")

    log.info("Fetching hot right now...")
    hot_vn     = safe_fetch(fetch_hot_right_now, sb, "VN",     8, default=[], label="hot_vn")
    hot_global = safe_fetch(fetch_hot_right_now, sb, "Global", 8, default=[], label="hot_global")

    log.info("Fetching momentum summary...")
    momentum_vn     = safe_fetch(fetch_momentum_summary, sb, "VN",     default=_empty_momentum(), label="momentum_vn")
    momentum_global = safe_fetch(fetch_momentum_summary, sb, "Global", default=_empty_momentum(), label="momentum_global")

    log.info("Fetching AI insights...")
    insights     = safe_fetch(fetch_latest_insights, sb, default={}, label="insights")
    insight_ctx  = build_insight_context(tier2, insights)

    now_ict = datetime.now(ICT)

    return {
        # Meta
        "generated_at": now_ict.strftime("%d/%m/%Y %H:%M ICT"),
        "today":        now_ict.strftime("%d/%m/%Y"),
        "run_id":       analysis.get("run_id", ""),

        # KPI
        "kpi": kpi,

        # Charts (serialized JSON for Chart.js)
        "trend_vn_json":        json.dumps(trend_vn,      ensure_ascii=False),
        "trend_global_json":    json.dumps(trend_global,  ensure_ascii=False),
        "monthly_vn_json":      json.dumps(monthly_vn,    ensure_ascii=False),
        "monthly_global_json":  json.dumps(monthly_global, ensure_ascii=False),
        "wow_json":             json.dumps(wow,            ensure_ascii=False),
        "post_time_vn_json":    json.dumps(post_time_vn,  ensure_ascii=False),
        "cat_vn_json":          json.dumps(cat_vn,        ensure_ascii=False),
        "cat_global_json":      json.dumps(cat_global,    ensure_ascii=False),
        "cat_perf_vn_json":     json.dumps(cat_perf_vn,   ensure_ascii=False, default=str),
        "cat_perf_global_json": json.dumps(cat_perf_global, ensure_ascii=False, default=str),
        "svv_vn_json":          json.dumps(svv_vn,        ensure_ascii=False),
        "svv_global_json":      json.dumps(svv_global,    ensure_ascii=False),
        "intraday_vn_json":     json.dumps(intraday_vn,   ensure_ascii=False),

        # Top videos
        "top_vn":        top_vn,
        "top_global":    top_global,
        "top_vn_video":  top_vn_video,
        "top_vn_stream": top_vn_stream,
        "top_gl_video":  top_gl_video,
        "top_gl_stream": top_gl_stream,

        # SVV data (direct dicts, for Jinja2 template)
        "svv_vn_data":     svv_vn,
        "svv_global_data": svv_global,

        # Tier1 analytics (từ analysis_output.json)
        "viral_vn":     tier1.get("VN",     {}).get("viral", [])[:5],
        "viral_global": tier1.get("Global", {}).get("viral", [])[:5],
        "trend_lag":    tier1.get("trend_lag", {}),
        "er_vn":        tier1.get("VN",     {}).get("er_by_type", {}),
        "er_global":    tier1.get("Global", {}).get("er_by_type", {}),

        # Hot right now
        "hot_vn":     hot_vn,
        "hot_global": hot_global,

        # Momentum
        "momentum_vn":     momentum_vn,
        "momentum_global": momentum_global,

        # AI Insights
        **insight_ctx,
        "insights": insights,

        # Last updated (ICT)
        "last_updated_ts": now_ict.strftime("%H:%M ICT"),
    }


def _empty_kpi() -> dict:
    """Default KPI khi fetch fail."""
    return {
        "total_videos": 0, "views_gained_today": 0, "likes_gained_today": 0,
        "comments_gained_today": 0, "avg_er": 0.0, "active_today": 0,
        "type_counts": {}, "is_first_day": False,
    }


def _empty_momentum() -> dict:
    return {"accelerating": 0, "peaking": 0, "decelerating": 0, "fading": 0, "new": 0}


# ============================================================
# HTML RENDERER
# ============================================================

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

    # Build context — tất cả fetch errors đã được handle bởi safe_fetch
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
    log.info("=== 04_generate_html DONE ===")


if __name__ == "__main__":
    main()
