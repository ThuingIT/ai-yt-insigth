"""
fetchers/insights.py — Gemini AI insights từ Supabase insights table.
"""

import logging
from supabase import Client
from .base import to_ict_str

log = logging.getLogger("04_html.fetchers.insights")

_INSIGHT_TYPES = (
    "weekly_summary", "viral_alert", "content_gap",
    "recommendation", "trend_lag", "anomaly",
)


def fetch_latest_insights(sb: Client) -> dict:
    """
    Lấy insight mới nhất từ Supabase cho từng loại.
    Returns dict[insight_type → row dict].
    """
    result = {}
    for itype in _INSIGHT_TYPES:
        try:
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
        except Exception as e:
            log.warning(f"fetch_latest_insights({itype}) failed: {e!r}")

    log.info(f"fetch_latest_insights: got {list(result.keys())}")
    return result


def build_insight_context(tier2: dict, insights: dict) -> dict:
    """
    Merge Tier2 Gemini output (analysis_output.json) với Supabase insights.
    Ưu tiên Tier2 nếu có (daily run), fallback sang Supabase (hourly run).

    Returns dict với các key: weekly_narrative, anomalies, recommendations,
    content_gaps, trend_forecast, gemini_generated_at.
    """
    def _insight_narrative(itype: str) -> str:
        row = insights.get(itype, {})
        return row.get("narrative") or ""

    def _insight_payload(itype: str, field: str, default=None):
        row = insights.get(itype, {})
        return (row.get("payload") or {}).get(field, default)

    weekly_narrative = (
        tier2.get("weekly_narrative")
        or _insight_narrative("weekly_summary")
        or ""
    )
    anomalies = (
        tier2.get("anomalies")
        or [p for itype in ["anomaly", "viral_alert"]
            for p in [insights.get(itype, {}).get("payload", {})]
            if p]
        or []
    )
    recommendations = (
        tier2.get("recommendations")
        or _insight_payload("recommendation", "recommendations", [])
    )
    content_gaps = (
        tier2.get("content_gaps")
        or _insight_payload("content_gap", "gaps", [])
    )
    trend_forecast = tier2.get("trend_forecast") or ""

    raw_ts = (insights.get("weekly_summary") or {}).get("generated_at", "")
    gemini_generated_at = to_ict_str(raw_ts)

    return {
        "weekly_narrative":    weekly_narrative,
        "anomalies":           anomalies,
        "recommendations":     recommendations,
        "content_gaps":        content_gaps,
        "trend_forecast":      trend_forecast,
        "gemini_generated_at": gemini_generated_at,
    }
