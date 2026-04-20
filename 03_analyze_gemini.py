#!/usr/bin/env python3
"""
03_analyze_gemini.py
====================
Bước 3 của pipeline: Phân tích 2 tầng.

TẦNG 1 — Thống kê thuần (SQL + Python, không tốn API):
    Top videos by views_gain | ER by content_type | Virality Index
    Optimal posting window | WoW growth | Trend Lag | Category distribution

TẦNG 2 — AI Insight (Gemini 2.5 Flash, 1 call/ngày):
    Weekly narrative | Anomaly detection | Content gap | Recommendations

Input:
    data/load_summary.json  (từ bước 02)
    Supabase: videos, daily_delta, channels

Output:
    Supabase table: insights
    data/analysis_output.json  (đọc bởi bước 04_generate_html)

Cách chạy:
    export SUPABASE_URL="https://xxxx.supabase.co"
    export SUPABASE_SERVICE_KEY="eyJ..."
    export GEMINI_API_KEY="AIza..."
    python 03_analyze_gemini.py
"""

import os
import json
import uuid
import logging
from datetime import datetime, timezone, date, timedelta
from typing import Optional

import google.generativeai as genai
from supabase import create_client, Client


# ============================================================
# CONFIG
# ============================================================

SUPABASE_URL: str         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY: str = os.environ["SUPABASE_SERVICE_KEY"]
GEMINI_API_KEY: str       = os.environ["GEMINI_API_KEY"]
OUTPUT_DIR: str           = os.environ.get("OUTPUT_DIR", "data")
RUN_ID: str               = os.environ.get("PIPELINE_RUN_ID", str(uuid.uuid4()))

TODAY: date         = date.today()
WEEK_AGO: date      = TODAY - timedelta(days=7)
TWO_WEEKS_AGO: date = TODAY - timedelta(days=14)

VIRAL_THRESHOLD = 5.0           # Virality Index >= này thì coi là viral
GEMINI_MODEL    = "gemini-2.5-flash"


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("03_analyze")


# ============================================================
# CLIENTS
# ============================================================

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def get_gemini():
    """Khởi tạo Gemini theo cách đã test thành công."""
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(GEMINI_MODEL)
    log.info(f"Gemini model: {GEMINI_MODEL}")
    return model


# ============================================================
# CRAWL LOG HELPERS
# ============================================================

def log_start(sb: Client, script: str, stream: Optional[str] = None) -> int:
    res = sb.table("crawl_logs").insert({
        "run_id":     RUN_ID,
        "script":     script,
        "stream":     stream,
        "status":     "started",
        "started_at": datetime.now(timezone.utc).isoformat(),
    }).execute()
    return res.data[0]["id"]


def log_finish(sb: Client, log_id: int, records: int = 0, error: Optional[str] = None):
    sb.table("crawl_logs").update({
        "status":            "failed" if error else "success",
        "records_processed": records,
        "error_message":     error,
        "finished_at":       datetime.now(timezone.utc).isoformat(),
    }).eq("id", log_id).execute()


def save_insight(sb: Client, insight_type: str, scope: str, payload: dict,
                 narrative: str, period_start: date, period_end: date,
                 content_type: str = "all"):
    sb.table("insights").insert({
        "run_id":       RUN_ID,
        "insight_type": insight_type,
        "scope":        scope,
        "content_type": content_type,
        "period_start": period_start.isoformat(),
        "period_end":   period_end.isoformat(),
        "payload":      payload,
        "narrative":    narrative,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }).execute()


# ============================================================
# TẦNG 1: THỐNG KÊ THUẦN
# ============================================================

def t1_top_videos_by_gain(sb: Client, stream: str, top_n: int = 10) -> list:
    """Top N video có views_gain cao nhất hôm nay."""
    res = (
        sb.table("daily_delta")
        .select("video_id, views_gain, likes_gain, comments_gain, content_type, region")
        .eq("stream", stream)
        .eq("date", TODAY.isoformat())
        .not_.is_("views_gain", "null")
        .order("views_gain", desc=True)
        .limit(top_n)
        .execute()
    )
    rows = res.data or []

    enriched = []
    for row in rows:
        vid = sb.table("videos").select(
            "title, channel_id, likes, views, comments_count, category_name"
        ).eq("id", row["video_id"]).maybe_single().execute()

        er = 0.0
        if vid.data and vid.data.get("views", 0) > 0:
            er = round(
                (vid.data.get("likes", 0) + vid.data.get("comments_count", 0))
                / vid.data["views"] * 100, 2
            )

        enriched.append({
            "video_id":      row["video_id"],
            "title":         vid.data.get("title", "") if vid.data else "",
            "channel_id":    vid.data.get("channel_id", "") if vid.data else "",
            "category":      vid.data.get("category_name", "") if vid.data else "",
            "content_type":  row["content_type"],
            "region":        row.get("region"),
            "views_gain":    row["views_gain"],
            "likes_gain":    row.get("likes_gain"),
            "comments_gain": row.get("comments_gain"),
            "er_pct":        er,
        })

    log.info(f"  [T1] top_videos({stream}): {len(enriched)} videos")
    return enriched


def t1_engagement_by_type(sb: Client, stream: str) -> dict:
    """Trung bình ER phân theo content_type."""
    res = (
        sb.table("videos")
        .select("content_type, views, likes, comments_count")
        .eq("stream", stream)
        .gt("views", 0)
        .execute()
    )
    by_type: dict = {}
    for row in (res.data or []):
        ct = row["content_type"]
        er = (row["likes"] + row["comments_count"]) / row["views"] * 100
        by_type.setdefault(ct, []).append(er)

    result = {
        ct: round(sum(v) / len(v), 3)
        for ct, v in by_type.items() if v
    }
    log.info(f"  [T1] er_by_type({stream}): {result}")
    return result


def t1_virality_index(sb: Client, stream: str) -> list:
    """Videos có Virality Index >= VIRAL_THRESHOLD hôm nay."""
    today_res = (
        sb.table("daily_delta")
        .select("video_id, views_gain")
        .eq("stream", stream)
        .eq("date", TODAY.isoformat())
        .not_.is_("views_gain", "null")
        .execute()
    )
    today_gains = {r["video_id"]: r["views_gain"] for r in (today_res.data or [])}
    if not today_gains:
        return []

    video_ids = list(today_gains.keys())
    baseline_res = (
        sb.table("daily_delta")
        .select("video_id, views_gain")
        .in_("video_id", video_ids)
        .gte("date", (TODAY - timedelta(days=30)).isoformat())
        .lt("date", TODAY.isoformat())
        .not_.is_("views_gain", "null")
        .execute()
    )

    sums: dict = {}
    for r in (baseline_res.data or []):
        sums.setdefault(r["video_id"], []).append(r["views_gain"])

    viral = []
    for vid_id, gain_today in today_gains.items():
        baseline_vals = sums.get(vid_id, [])
        if not baseline_vals:
            continue
        avg = sum(baseline_vals) / len(baseline_vals)
        if avg <= 0:
            continue
        vi = round(gain_today / avg, 2)
        if vi >= VIRAL_THRESHOLD:
            title_res = sb.table("videos").select(
                "title, category_name"
            ).eq("id", vid_id).maybe_single().execute()
            viral.append({
                "video_id":       vid_id,
                "title":          title_res.data.get("title", "") if title_res.data else "",
                "category":       title_res.data.get("category_name", "") if title_res.data else "",
                "views_gain":     gain_today,
                "avg_baseline":   round(avg, 0),
                "virality_index": vi,
            })

    viral.sort(key=lambda x: x["virality_index"], reverse=True)
    log.info(f"  [T1] virality({stream}): {len(viral)} viral videos")
    return viral


def t1_optimal_post_time(sb: Client, stream: str) -> dict:
    """
    Khung giờ đăng bài có avg views_gain cao nhất (phân tích 30 ngày gần nhất).
    Trả về dict {hour_str: avg_gain} sorted desc.
    """
    res = (
        sb.table("daily_delta")
        .select("video_id, views_gain")
        .eq("stream", stream)
        .gte("date", (TODAY - timedelta(days=30)).isoformat())
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

    result = {
        str(h): round(sum(v) / len(v), 0)
        for h, v in sorted(hour_gains.items(), key=lambda x: sum(x[1])/len(x[1]), reverse=True)
        if v
    }
    log.info(f"  [T1] optimal_post_time({stream}): top hours = {list(result.keys())[:3]}")
    return result


def t1_wow_growth(sb: Client, stream: str) -> dict:
    """Week-over-Week growth per content_type."""
    def week_total(start: date, end: date) -> dict:
        res = (
            sb.table("daily_delta")
            .select("content_type, views_gain")
            .eq("stream", stream)
            .gte("date", start.isoformat())
            .lt("date", end.isoformat())
            .not_.is_("views_gain", "null")
            .execute()
        )
        totals: dict = {}
        for r in (res.data or []):
            ct = r["content_type"]
            totals[ct] = totals.get(ct, 0) + (r["views_gain"] or 0)
        return totals

    current  = week_total(WEEK_AGO,       TODAY)
    previous = week_total(TWO_WEEKS_AGO,  WEEK_AGO)

    wow = {}
    for ct in set(list(current.keys()) + list(previous.keys())):
        cur  = current.get(ct, 0)
        prev = previous.get(ct, 0)
        wow[ct] = round((cur - prev) / prev * 100, 1) if prev > 0 else None

    log.info(f"  [T1] wow({stream}): {wow}")
    return {"current_week": current, "previous_week": previous, "wow_pct": wow}


def t1_trend_lag(sb: Client) -> dict:
    """Tags nổi bật ở Global tuần qua nhưng chưa xuất hiện ở VN."""
    def get_tags(stream: str, from_date: date) -> set:
        res = (
            sb.table("videos")
            .select("tags")
            .eq("stream", stream)
            .gte("first_seen_at", from_date.isoformat())
            .execute()
        )
        tags = set()
        for row in (res.data or []):
            for t in (row.get("tags") or []):
                if t and len(t) > 2:
                    tags.add(t.lower().strip())
        return tags

    global_tags = get_tags("Global", WEEK_AGO)
    vn_tags     = get_tags("VN", WEEK_AGO)
    lag_tags    = list(global_tags - vn_tags)[:20]

    log.info(f"  [T1] trend_lag: {len(lag_tags)} lag topics")
    return {
        "lag_tags":    lag_tags,
        "global_only": len(global_tags - vn_tags),
        "overlap":     len(global_tags & vn_tags),
        "vn_only":     len(vn_tags - global_tags),
    }


def t1_category_distribution(sb: Client, stream: str) -> dict:
    """Phân bố video theo category."""
    res = sb.table("videos").select("category_name").eq("stream", stream).execute()
    dist: dict = {}
    for row in (res.data or []):
        cat = row.get("category_name") or "Unknown"
        dist[cat] = dist.get(cat, 0) + 1
    return dict(sorted(dist.items(), key=lambda x: x[1], reverse=True))


# ============================================================
# TẦNG 2: GEMINI AI INSIGHTS
# ============================================================

def build_gemini_prompt(stats: dict) -> str:
    stats_json = json.dumps(stats, ensure_ascii=False, indent=2)
    return f"""Bạn là chuyên gia phân tích dữ liệu YouTube 10 năm kinh nghiệm.
Ngày phân tích: {TODAY.isoformat()}

Dữ liệu thống kê:
{stats_json}

Trả về MỘT JSON object duy nhất, KHÔNG markdown, KHÔNG text thêm:

{{
  "weekly_narrative": "Tóm tắt 3-5 câu tiếng Việt về xu hướng tuần này.",
  "anomalies": [
    {{
      "type": "viral_video|unusual_drop|category_surge|other",
      "description": "Mô tả ngắn",
      "video_id": null,
      "severity": "low|medium|high"
    }}
  ],
  "content_gaps": [
    {{
      "topic": "Chủ đề hot Global chưa có creator VN làm",
      "evidence": "Dựa trên dữ liệu nào",
      "opportunity_score": 8
    }}
  ],
  "recommendations": [
    {{
      "target": "creator|analyst|all",
      "action": "Hành động cụ thể",
      "reasoning": "Lý do ngắn",
      "priority": "low|medium|high"
    }}
  ],
  "trend_forecast": "Dự đoán 1-2 câu cho tuần tới.",
  "data_quality_notes": "Ghi chú nếu có vấn đề với dữ liệu"
}}""".strip()


def call_gemini(model, prompt: str, max_retries: int = 3) -> dict:
    """Gọi Gemini và parse JSON response. Retry nếu parse lỗi."""
    last_error = None
    raw_text   = ""
    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"  [T2] Gọi Gemini (attempt {attempt}/{max_retries})...")
            response = model.generate_content(prompt)
            raw_text = response.text.strip()

            # Loại bỏ markdown fences phòng ngừa
            if raw_text.startswith("```"):
                lines    = raw_text.split("\n")
                raw_text = "\n".join(l for l in lines if not l.startswith("```")).strip()

            parsed = json.loads(raw_text)
            log.info("  [T2] Gemini response parsed OK")
            return parsed

        except json.JSONDecodeError as e:
            last_error = e
            log.warning(f"  [T2] JSON parse lỗi (attempt {attempt}): {e}")

        except Exception as e:
            last_error = e
            log.error(f"  [T2] Gemini API lỗi (attempt {attempt}): {e}")

    # Fallback nếu không parse được sau 3 lần
    log.error(f"  [T2] Fallback sau {max_retries} lần thất bại")
    return {
        "weekly_narrative":   raw_text or "Không thể tạo narrative",
        "anomalies":          [],
        "content_gaps":       [],
        "recommendations":    [],
        "trend_forecast":     "",
        "data_quality_notes": f"JSON parse failed: {last_error}",
    }


# ============================================================
# ORCHESTRATOR
# ============================================================

def run_analysis(sb: Client, model) -> dict:
    log_id = log_start(sb, "03_analyze")
    result = {
        "run_id":       RUN_ID,
        "date":         TODAY.isoformat(),
        "tier1":        {},
        "tier2":        {},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        # ── TẦNG 1 ───────────────────────────────────────────
        log.info("=== TẦNG 1: Thống kê ===")

        for stream in ("VN", "Global"):
            log.info(f"[T1] Stream: {stream}")
            result["tier1"][stream] = {
                "top_videos": t1_top_videos_by_gain(sb, stream, top_n=10),
                "er_by_type": t1_engagement_by_type(sb, stream),
                "viral":      t1_virality_index(sb, stream),
                "post_time":  t1_optimal_post_time(sb, stream),
                "wow":        t1_wow_growth(sb, stream),
                "categories": t1_category_distribution(sb, stream),
            }

        result["tier1"]["trend_lag"] = t1_trend_lag(sb)

        # Lưu viral alerts
        for stream in ("VN", "Global"):
            viral_list = result["tier1"][stream]["viral"]
            if viral_list:
                save_insight(
                    sb, "viral_alert", stream,
                    payload={"viral_videos": viral_list},
                    narrative=(f"{len(viral_list)} video viral (VI≥{VIRAL_THRESHOLD}x): " +
                               ", ".join(v["title"][:30] for v in viral_list[:3])),
                    period_start=TODAY, period_end=TODAY,
                )

        # Lưu trend_lag
        tl = result["tier1"].get("trend_lag", {})
        if tl.get("lag_tags"):
            save_insight(
                sb, "trend_lag", "both",
                payload=tl,
                narrative=(f"{tl['global_only']} topics trending Global chưa có ở VN: " +
                           ", ".join(tl["lag_tags"][:5])),
                period_start=WEEK_AGO, period_end=TODAY,
            )

        # ── TẦNG 2 ───────────────────────────────────────────
        log.info("=== TẦNG 2: Gemini AI Insights ===")

        gemini_input = {
            "date": TODAY.isoformat(),
            "vn": {
                "top_5_videos":   result["tier1"]["VN"]["top_videos"][:5],
                "er_by_type":     result["tier1"]["VN"]["er_by_type"],
                "wow_pct":        result["tier1"]["VN"]["wow"]["wow_pct"],
                "top_categories": dict(list(result["tier1"]["VN"]["categories"].items())[:5]),
                "viral_count":    len(result["tier1"]["VN"]["viral"]),
            },
            "global": {
                "top_5_videos":   result["tier1"]["Global"]["top_videos"][:5],
                "er_by_type":     result["tier1"]["Global"]["er_by_type"],
                "wow_pct":        result["tier1"]["Global"]["wow"]["wow_pct"],
                "top_categories": dict(list(result["tier1"]["Global"]["categories"].items())[:5]),
            },
            "trend_lag": {
                "lag_count":  tl.get("global_only", 0),
                "top_topics": tl.get("lag_tags", [])[:10],
            },
        }

        prompt    = build_gemini_prompt(gemini_input)
        ai_output = call_gemini(model, prompt)
        result["tier2"] = ai_output

        # Lưu weekly summary
        save_insight(
            sb, "weekly_summary", "both",
            payload=ai_output,
            narrative=ai_output.get("weekly_narrative", ""),
            period_start=WEEK_AGO, period_end=TODAY,
        )

        # Lưu high-severity anomalies
        for anom in ai_output.get("anomalies", []):
            if anom.get("severity") == "high":
                save_insight(
                    sb, "anomaly", "both",
                    payload=anom,
                    narrative=anom.get("description", ""),
                    period_start=TODAY, period_end=TODAY,
                )

        # Lưu recommendations
        recs = ai_output.get("recommendations", [])
        if recs:
            save_insight(
                sb, "recommendation", "both",
                payload={"recommendations": recs},
                narrative="\n".join(
                    f"[{r.get('priority','?').upper()}] {r.get('action','')}"
                    for r in recs[:5]
                ),
                period_start=TODAY, period_end=TODAY,
            )

        # Lưu content gaps
        gaps = ai_output.get("content_gaps", [])
        if gaps:
            save_insight(
                sb, "content_gap", "both",
                payload={"gaps": gaps},
                narrative="\n".join(
                    f"• {g.get('topic','')} (opportunity: {g.get('opportunity_score','')})"
                    for g in gaps[:5]
                ),
                period_start=WEEK_AGO, period_end=TODAY,
            )

        log_finish(sb, log_id, records=4 + len(recs) + len(gaps))
        return result

    except Exception as e:
        log.error(f"Analysis failed: {e}")
        log_finish(sb, log_id, records=0, error=str(e))
        raise


# ============================================================
# MAIN
# ============================================================

def main():
    log.info(f"=== 03_analyze_gemini START | run_id={RUN_ID} | date={TODAY} ===")

    # Đảm bảo bước 02 đã chạy xong
    summary_path = os.path.join(OUTPUT_DIR, "load_summary.json")
    if not os.path.exists(summary_path):
        raise FileNotFoundError(
            f"Không tìm thấy {summary_path} — đảm bảo 02_load_supabase đã chạy thành công"
        )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sb    = get_supabase()
    model = get_gemini()

    # Verify Gemini hoạt động
    log.info("Kiểm tra kết nối Gemini...")
    test = model.generate_content("Reply with one word: READY")
    if "READY" not in test.text.upper():
        raise RuntimeError(f"Gemini API test thất bại: {test.text}")
    log.info("Gemini API READY ✓")

    result = run_analysis(sb, model)

    # Lưu output
    output_path = os.path.join(OUTPUT_DIR, "analysis_output.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"Analysis output → {output_path}")

    t2 = result.get("tier2", {})
    log.info("=== SUMMARY ===")
    log.info(f"  Tier1 VN top videos:     {len(result['tier1'].get('VN',{}).get('top_videos',[]))}")
    log.info(f"  Tier1 Global top videos: {len(result['tier1'].get('Global',{}).get('top_videos',[]))}")
    log.info(f"  Viral VN:     {len(result['tier1'].get('VN',{}).get('viral',[]))}")
    log.info(f"  Viral Global: {len(result['tier1'].get('Global',{}).get('viral',[]))}")
    log.info(f"  Gemini anomalies:     {len(t2.get('anomalies',[]))}")
    log.info(f"  Gemini recommendations: {len(t2.get('recommendations',[]))}")
    log.info(f"  Gemini content gaps:  {len(t2.get('content_gaps',[]))}")
    log.info(f"=== 03_analyze_gemini DONE ===")


if __name__ == "__main__":
    main()
