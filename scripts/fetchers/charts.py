"""
fetchers/charts.py — Time-series và aggregation data cho Chart.js widgets:
  - Trend 30 ngày (line chart)
  - WoW week-over-week (bar chart)
  - Monthly 6 tháng (line chart)
  - Category donut
"""

import logging
from datetime import timedelta
from supabase import Client
from .base import TODAY, WEEK_AGO, MONTH_AGO, coalesce_gain

log = logging.getLogger("04_html.fetchers.charts")


def fetch_trend_line(sb: Client, stream: str, days: int = 30) -> list:
    """
    Views gain tổng theo ngày (30 ngày gần nhất) cho stream.
    COALESCE(views_gain, views_total) cho ngày đầu tiên.
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
    # Group by date với COALESCE
    by_date: dict = {}
    for r in (res.data or []):
        d   = r["date"]
        val = coalesce_gain(r)
        by_date[d] = by_date.get(d, 0) + val

    result = [{"date": d, "views": v} for d, v in sorted(by_date.items())]
    log.info(f"fetch_trend_line({stream}): {len(result)} days")
    return result


def fetch_wow_chart(sb: Client) -> dict:
    """WoW data cho cả VN và Global, phân theo content_type."""

    def week_sum(stream: str, start, end) -> dict:
        r = (
            sb.table("daily_delta")
            .select("content_type, views_gain")
            .eq("stream", stream)
            .gte("date", start.isoformat())
            .lt("date", end.isoformat())
            .not_.is_("views_gain", "null")
            .execute()
        )
        totals: dict = {}
        for row in (r.data or []):
            ct = row["content_type"]
            totals[ct] = totals.get(ct, 0) + (row["views_gain"] or 0)
        return totals

    result = {}
    two_weeks_ago = TODAY - timedelta(days=14)
    for stream in ("VN", "Global"):
        result[stream] = {
            "current":  week_sum(stream, WEEK_AGO, TODAY),
            "previous": week_sum(stream, two_weeks_ago, WEEK_AGO),
        }

    log.info("fetch_wow_chart done")
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
    by_month: dict = {}
    for r in (res.data or []):
        m = r["month_start"][:7]  # "2024-01"
        by_month[m] = by_month.get(m, 0) + (r["total_views_gained"] or 0)
    return [{"month": m, "views": v} for m, v in sorted(by_month.items())]


def fetch_category_donut(sb: Client, stream: str) -> dict:
    """Category distribution cho donut chart (top 8 categories)."""
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
