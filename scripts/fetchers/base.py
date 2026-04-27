"""
fetchers/base.py — Shared utilities cho tất cả fetchers:
  - safe_fetch(): wrapper với graceful degradation + logging
  - batch_video_lookup(): giải quyết N+1 bằng cách batch query videos table
  - coalesce_gain(): COALESCE(views_gain, views_total) logic
  - Constants dùng chung (TODAY, WEEK_AGO, MONTH_AGO, ICT)
"""

import logging
from datetime import datetime, timezone, date, timedelta
from typing import Any, Callable, TypeVar

from supabase import Client


# ── Constants dùng chung ─────────────────────────────────────────────────────

ICT       = timezone(timedelta(hours=7))
TODAY     = date.today()
WEEK_AGO  = TODAY - timedelta(days=7)
MONTH_AGO = TODAY - timedelta(days=30)

log = logging.getLogger("04_html.fetchers")

T = TypeVar("T")


# ── Graceful degradation wrapper ─────────────────────────────────────────────

def safe_fetch(fn: Callable[..., T], *args, default: Any = None, label: str = "", **kwargs) -> T:
    """
    Gọi fn(*args, **kwargs). Nếu exception → log warning và trả về default.

    Đảm bảo dashboard luôn render được dù 1-2 section fail.
    Tất cả fetcher functions nên được wrap bằng safe_fetch.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        lbl = label or getattr(fn, "__name__", str(fn))
        log.warning(f"[safe_fetch] {lbl} failed: {e!r} — returning default")
        return default if default is not None else _default_for_fn(fn)


def _default_for_fn(fn: Callable) -> Any:
    """Đoán default value hợp lý dựa trên return type hint."""
    hints = getattr(fn, "__annotations__", {})
    ret   = hints.get("return", None)
    if ret is None:
        return None
    origin = getattr(ret, "__origin__", ret)
    if origin is list:
        return []
    if origin is dict:
        return {}
    return None


# ── Batch video lookup — FIX N+1 ─────────────────────────────────────────────

def batch_video_lookup(sb: Client, video_ids: list[str],
                       fields: str = "id, title, thumbnail_url, views, likes, comments_count, category_name, published_at, channel_id"
                       ) -> dict[str, dict]:
    """
    Thay thế vòng lặp query 1 video/request bằng 1 query batch duy nhất.

    Trước đây (N+1):
        for vid_id in video_ids:
            sb.table("videos").select(...).eq("id", vid_id).execute()   # N queries!

    Sau (1 query):
        video_map = batch_video_lookup(sb, video_ids)

    Returns:
        dict[video_id → video row dict]
    """
    if not video_ids:
        return {}
    try:
        res = (
            sb.table("videos")
            .select(fields)
            .in_("id", video_ids)
            .execute()
        )
        return {row["id"]: row for row in (res.data or [])}
    except Exception as e:
        log.warning(f"batch_video_lookup failed: {e!r}")
        return {}


# ── COALESCE helper ───────────────────────────────────────────────────────────

def coalesce_gain(row: dict, gain_col: str = "views_gain",
                  total_col: str = "views_total") -> int:
    """
    COALESCE(views_gain, views_total) — FIX Day 1 Problem.
    Ngày đầu tiên views_gain = NULL → dùng views_total làm fallback.
    """
    v = row.get(gain_col)
    return v if v is not None else (row.get(total_col) or 0)


# ── ICT conversion helpers ────────────────────────────────────────────────────

def to_ict_str(utc_str: str) -> str:
    """Convert UTC ISO string sang ICT display string."""
    if not utc_str:
        return ""
    try:
        dt  = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        ict = dt.astimezone(ICT)
        return ict.strftime("%d/%m/%Y %H:%M ICT")
    except Exception:
        return utc_str[:16].replace("T", " ") + " UTC"


def utc_hour_to_ict(utc_hour: int) -> int:
    """Convert UTC hour (0-23) sang ICT hour (0-23)."""
    return (utc_hour + 7) % 24
