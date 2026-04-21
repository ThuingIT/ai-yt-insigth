#!/usr/bin/env python3
"""
02_load_supabase.py
===================
Bước 2 của pipeline: Đọc JSON từ bước 01, upsert vào Supabase,
tính daily_delta, refresh materialized views.

Input:
    data/crawl_vn.json
    data/crawl_global.json

Output (Supabase tables):
    channels, videos, stream_details, daily_delta, comments
    hourly_snapshot  ← MỚI: upsert khi mode=hourly

Output (file):
    data/load_summary.json  ← đọc bởi bước 03_analyze

Mode routing (đọc từ "mode" field trong crawl JSON):
    mode=hourly → upsert hourly_snapshot + tính views_delta_1h + momentum
    mode=daily  → full upsert như cũ + update daily_delta momentum fields

Cách chạy:
    export SUPABASE_URL="https://xxxx.supabase.co"
    export SUPABASE_SERVICE_KEY="eyJ..."   # service_role key, KHÔNG dùng anon key
    export OUTPUT_DIR="data"               # optional, default: data
    export PIPELINE_RUN_ID="abc-123"       # optional, tự generate nếu để trống
    python 02_load_supabase.py

Lưu ý:
    - Phải dùng service_role key để bypass RLS
    - Toàn bộ upsert idempotent → chạy lại không bị duplicate
    - daily_delta tính bằng cách so sánh snapshot hôm nay vs ngày gần nhất trong DB
"""

import os
import json
import uuid
import logging
from datetime import datetime, timezone, date
from typing import Optional

from supabase import create_client, Client


# ============================================================
# CONFIG
# ============================================================

SUPABASE_URL: str         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY: str = os.environ["SUPABASE_SERVICE_KEY"]
OUTPUT_DIR: str           = os.environ.get("OUTPUT_DIR", "data")
RUN_ID: str               = os.environ.get("PIPELINE_RUN_ID", str(uuid.uuid4()))

BATCH_SIZE = 50           # Rows per upsert request (~1MB Supabase limit)
TODAY: date = date.today()


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("02_load")


# ============================================================
# SUPABASE CLIENT
# ============================================================

def get_supabase() -> Client:
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    log.info(f"Supabase connected: {SUPABASE_URL}")
    return client


# ============================================================
# CRAWL LOG HELPERS
# ============================================================

def log_start(sb: Client, script: str, stream: Optional[str] = None) -> int:
    row = {
        "run_id":     RUN_ID,
        "script":     script,
        "stream":     stream,
        "status":     "started",
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    res = sb.table("crawl_logs").insert(row).execute()
    return res.data[0]["id"]


def log_finish(sb: Client, log_id: int, records: int = 0,
               quota: int = 0, error: Optional[str] = None):
    sb.table("crawl_logs").update({
        "status":            "failed" if error else "success",
        "records_processed": records,
        "quota_used":        quota,
        "error_message":     error,
        "finished_at":       datetime.now(timezone.utc).isoformat(),
    }).eq("id", log_id).execute()


# ============================================================
# BATCH UPSERT
# ============================================================

def batch_upsert(sb: Client, table: str, rows: list[dict],
                 on_conflict: str, label: str = "") -> int:
    """Upsert rows theo batch. Trả về tổng số rows đã upsert."""
    if not rows:
        log.info(f"  [{label or table}] 0 rows — bỏ qua")
        return 0

    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        sb.table(table).upsert(batch, on_conflict=on_conflict).execute()
        total += len(batch)

    log.info(f"  [{label or table}] Upserted {total} rows")
    return total


# ============================================================
# DAILY DELTA CALCULATOR
# ============================================================

def compute_daily_delta(sb: Client, video_ids: list[str],
                        videos_map: dict[str, dict]) -> list[dict]:
    """
    Tính delta views/likes/comments so với ngày gần nhất trong DB.

    views YouTube là số tích lũy không giảm nên:
        views_gain = views_hôm_nay - views_ngày_gần_nhất_trước_đó

    FIX "Day 1 Problem":
        Ngày đầu tiên thấy video (chưa có baseline), dùng views_total làm gain.
        Lý do: dashboard cần có data để hiển thị ngay từ ngày đầu,
                và "tất cả views tích lũy đến nay" là baseline hợp lý cho ngày đầu.
        Cột is_first_day_estimate KHÔNG tồn tại trong schema — ta dùng
        views_gain = views_total và ghi log để phân biệt.

    Nếu gain < 0 (data lỗi từ API) → clamp về 0.
    """
    if not video_ids:
        return []

    # Query snapshot gần nhất TRƯỚC hôm nay cho từng video (theo batch)
    prev_snapshots: dict[str, dict] = {}
    for i in range(0, len(video_ids), BATCH_SIZE):
        batch_ids = video_ids[i : i + BATCH_SIZE]
        res = (
            sb.table("daily_delta")
            .select("video_id, date, views_total, likes_total, comments_total")
            .in_("video_id", batch_ids)
            .lt("date", TODAY.isoformat())
            .order("date", desc=True)
            .execute()
        )
        for row in res.data:
            vid = row["video_id"]
            if vid not in prev_snapshots:   # chỉ giữ row mới nhất
                prev_snapshots[vid] = row

    new_count  = 0
    gain_count = 0
    delta_rows = []

    for vid_id in video_ids:
        video = videos_map.get(vid_id)
        if not video:
            continue

        views_now    = video["views"]
        likes_now    = video["likes"]
        comments_now = video["comments_count"]
        prev         = prev_snapshots.get(vid_id)

        if prev:
            # Ngày thứ 2 trở đi: tính delta thực sự
            views_gain    = max(0, views_now    - prev["views_total"])
            likes_gain    = max(0, likes_now    - prev["likes_total"])
            comments_gain = max(0, comments_now - prev["comments_total"])
            gain_count += 1
        else:
            # ── FIX Day 1 Problem ──────────────────────────────────────
            # Ngày đầu tiên thấy video: dùng views_total làm gain.
            # Dashboard sẽ hiển thị data ngay từ ngày đầu, không bị trắng.
            # Khi so sánh WoW/MoM sau này, ngày đầu sẽ tự nhiên "blend" vào.
            views_gain    = views_now    # ← CHANGED từ None
            likes_gain    = likes_now    # ← CHANGED từ None
            comments_gain = comments_now # ← CHANGED từ None
            new_count += 1

        delta_rows.append({
            "video_id":       vid_id,
            "date":           TODAY.isoformat(),
            "views_total":    views_now,
            "likes_total":    likes_now,
            "comments_total": comments_now,
            "views_gain":     views_gain,
            "likes_gain":     likes_gain,
            "comments_gain":  comments_gain,
            "stream":         video["stream"],
            "content_type":   video["content_type"],
            "region":         video.get("region"),
        })

    log.info(f"  [delta] {gain_count} có baseline, {new_count} video mới (dùng views_total làm gain)")
    return delta_rows


# ============================================================
# HOURLY SNAPSHOT LOADER
# ============================================================

def load_hourly_snapshot(sb: Client, videos_map: dict[str, dict]) -> list[dict]:
    """
    Tạo hourly_snapshot rows cho lần crawl này.

    Logic tính views_delta_1h:
        1. Lấy snapshot GẦN NHẤT trong vòng 2 giờ qua (để tránh gap nếu 1 lần chạy bị delay)
        2. delta = views_hiện_tại - views_snapshot_trước
        3. NULL nếu chưa có snapshot nào trước đó trong ngày hôm nay

    Tại sao dùng 2h thay vì 1h?
        GitHub Actions không chạy chính xác đúng giờ — có thể delay 5-10 phút.
        Dùng 2h window để không bỏ sót baseline khi có delay.
    """
    from datetime import datetime as dt

    if not videos_map:
        return []

    now_ts  = datetime.now(timezone.utc)
    video_ids = list(videos_map.keys())

    # Query snapshots gần nhất trong 2 giờ qua
    cutoff = (now_ts - timedelta(hours=2)).isoformat()
    prev_map: dict[str, dict] = {}

    for i in range(0, len(video_ids), BATCH_SIZE):
        batch = video_ids[i : i + BATCH_SIZE]
        res = (
            sb.table("hourly_snapshot")
            .select("video_id, snapshot_at, views, likes")
            .in_("video_id", batch)
            .gte("snapshot_at", cutoff)
            .lt("snapshot_at", now_ts.isoformat())   # Chỉ trước lần crawl này
            .order("snapshot_at", desc=True)
            .execute()
        )
        for row in (res.data or []):
            vid = row["video_id"]
            if vid not in prev_map:   # Giữ cái mới nhất
                prev_map[vid] = row

    rows       = []
    new_count  = 0
    gain_count = 0

    for vid_id, video in videos_map.items():
        prev = prev_map.get(vid_id)

        if prev:
            views_delta = max(0, video["views"] - prev["views"])
            likes_delta = max(0, video["likes"] - prev["likes"])
            gain_count += 1
        else:
            views_delta = None  # Lần đầu trong ngày → NULL
            likes_delta = None
            new_count += 1

        rows.append({
            "video_id":        vid_id,
            "snapshot_at":     now_ts.isoformat(),
            "views":           video["views"],
            "likes":           video["likes"],
            "comments":        video["comments_count"],
            "views_delta_1h":  views_delta,
            "likes_delta_1h":  likes_delta,
            "stream":          video["stream"],
            "content_type":    video["content_type"],
            "region":          video.get("region"),
        })

    log.info(f"  [hourly] {gain_count} với delta, {new_count} mới (no baseline)")
    return rows


def update_daily_momentum(sb: Client, video_ids: list[str]):
    """
    Sau khi upsert hourly_snapshot, gọi Postgres function để tính lại
    momentum_status cho từng video và update vào daily_delta hôm nay.

    Dùng RPC thay vì loop Python để tránh N+1 query.
    """
    if not video_ids:
        return

    log.info(f"  [momentum] Tính lại momentum cho {len(video_ids)} videos...")

    updated = 0
    for i in range(0, len(video_ids), BATCH_SIZE):
        batch = video_ids[i : i + BATCH_SIZE]

        # Gọi compute_momentum_status từ Postgres function (đã tạo trong migration SQL)
        for vid_id in batch:
            try:
                result = sb.rpc("compute_momentum_status", {
                    "p_video_id": vid_id,
                    "p_as_of":    datetime.now(timezone.utc).isoformat(),
                }).execute()

                momentum = result.data

                if momentum:
                    # Update daily_delta hôm nay
                    sb.table("daily_delta").update({
                        "momentum_status": momentum,
                    }).eq("video_id", vid_id).eq("date", TODAY.isoformat()).execute()
                    updated += 1
            except Exception as e:
                log.debug(f"  [momentum] Skip {vid_id}: {e}")

    log.info(f"  [momentum] Updated {updated}/{len(video_ids)} videos")


# ============================================================
# LOAD SINGLE STREAM
# ============================================================

def load_stream(sb: Client, json_path: str) -> dict:
    """Đọc 1 file JSON (VN hoặc Global) và upsert vào Supabase."""
    if not os.path.exists(json_path):
        log.warning(f"Không tìm thấy {json_path} — bỏ qua")
        return {}

    log.info(f"Đọc: {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    stream      = data.get("stream", "unknown")
    videos      = data.get("videos", [])
    channels    = data.get("channels", [])
    stream_dets = data.get("stream_details", [])
    comments    = data.get("comments", [])

    log.info(f"[{stream}] JSON: {len(videos)} videos | {len(channels)} channels | "
             f"{len(stream_dets)} stream_details | {len(comments)} comments")

    log_id = log_start(sb, "02_load", stream=stream)
    total  = 0
    now_ts = datetime.now(timezone.utc).isoformat()

    try:
        # ── 1. Channels ─────────────────────────────────────
        channel_rows = [
            {
                "id":              ch["id"],
                "name":            ch.get("name", ""),
                "stream":          ch.get("stream", stream),
                "last_updated_at": now_ts,
            }
            for ch in channels if ch.get("id")
        ]
        total += batch_upsert(sb, "channels", channel_rows,
                               on_conflict="id", label=f"{stream}/channels")

        # ── 2. Videos ───────────────────────────────────────
        video_rows = [
            {
                "id":               v["id"],
                "channel_id":       v.get("channel_id") or None,
                "title":            v.get("title", ""),
                "description":      (v.get("description") or "")[:500],
                "tags":             v.get("tags", [])[:20],
                "category_id":      v.get("category_id"),
                "category_name":    v.get("category_name"),
                "published_at":     v.get("published_at"),
                "duration_seconds": v.get("duration_seconds", 0),
                "content_type":     v.get("content_type", "video"),
                "stream":           v.get("stream", stream),
                "region":           v.get("region"),
                "thumbnail_url":    v.get("thumbnail_url"),
                "views":            v.get("views", 0),
                "likes":            v.get("likes", 0),
                "comments_count":   v.get("comments_count", 0),
                "last_updated_at":  now_ts,
            }
            for v in videos if v.get("id")
        ]
        total += batch_upsert(sb, "videos", video_rows,
                               on_conflict="id", label=f"{stream}/videos")

        # ── 3. Stream details ────────────────────────────────
        if stream_dets:
            total += batch_upsert(sb, "stream_details", stream_dets,
                                   on_conflict="video_id", label=f"{stream}/stream_details")

        # ── 4. Daily delta ───────────────────────────────────
        videos_map = {v["id"]: v for v in videos}
        video_ids  = [v["id"] for v in videos]
        delta_rows = compute_daily_delta(sb, video_ids, videos_map)
        total += batch_upsert(sb, "daily_delta", delta_rows,
                               on_conflict="video_id,date", label=f"{stream}/daily_delta")

        # ── 5. Comments ──────────────────────────────────────
        if comments:
            comment_rows = [
                {
                    "id":           c["id"],
                    "video_id":     c["video_id"],
                    "text":         (c.get("text") or "")[:1000],
                    "like_count":   c.get("like_count", 0),
                    "published_at": c.get("published_at"),
                    "crawled_at":   c.get("crawled_at", now_ts),
                }
                for c in comments if c.get("id") and c.get("video_id")
            ]
            total += batch_upsert(sb, "comments", comment_rows,
                                   on_conflict="id", label=f"{stream}/comments")

        # ── 6. Hourly snapshot (chỉ khi mode=hourly) ────────
        mode = data.get("mode", "daily")
        hourly_count = 0
        if mode == "hourly":
            snapshot_rows = load_hourly_snapshot(sb, videos_map)
            hourly_count  = batch_upsert(sb, "hourly_snapshot", snapshot_rows,
                                          on_conflict="video_id,snapshot_at",
                                          label=f"{stream}/hourly_snapshot")
            total += hourly_count
            # Tính lại momentum sau khi có snapshot mới
            update_daily_momentum(sb, video_ids)

        log_finish(sb, log_id, records=total)
        log.info(f"[{stream}] Load done: {total} records | mode={mode} | "
                 f"hourly_snapshots={hourly_count}")

        return {
            "stream":           stream,
            "mode":             mode,
            "videos":           len(videos),
            "channels":         len(channels),
            "delta_rows":       len(delta_rows),
            "hourly_snapshots": hourly_count,
            "comments":         len(comments),
            "total":            total,
        }

    except Exception as e:
        msg = str(e)
        log.error(f"[{stream}] FAILED: {msg}")
        log_finish(sb, log_id, records=total, error=msg)
        raise


# ============================================================
# REFRESH MATERIALIZED VIEWS
# ============================================================

def refresh_views(sb: Client):
    """Gọi Postgres function refresh_all_views() đã tạo trong create_supabase.sql."""
    log.info("Refreshing materialized views (weekly_stats, monthly_stats)...")
    log_id = log_start(sb, "02_load_refresh_views")
    try:
        sb.rpc("refresh_all_views", {}).execute()
        log_finish(sb, log_id, records=0)
        log.info("Materialized views refreshed OK")
    except Exception as e:
        msg = str(e)
        log.error(f"Refresh views failed: {msg}")
        log_finish(sb, log_id, records=0, error=msg)
        raise


# ============================================================
# MAIN
# ============================================================

def main():
    log.info(f"=== 02_load_supabase START | run_id={RUN_ID} | date={TODAY} ===")

    sb = get_supabase()
    summaries = []

    for filename in ("crawl_vn.json", "crawl_global.json"):
        path = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(path):
            s = load_stream(sb, path)
            if s:
                summaries.append(s)
        else:
            log.warning(f"Bỏ qua (không tìm thấy): {path}")

    if not summaries:
        raise RuntimeError("Không có file JSON nào — kiểm tra output của bước 01_crawl")

    refresh_views(sb)

    # Lưu load_summary.json → đọc bởi bước 03_analyze
    summary_path = os.path.join(OUTPUT_DIR, "load_summary.json")
    payload = {
        "run_id":      RUN_ID,
        "date":        TODAY.isoformat(),
        "streams":     summaries,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    log.info(f"Load summary saved → {summary_path}")

    log.info("=== SUMMARY ===")
    for s in summaries:
        log.info(f"  {s['stream']}: {s['videos']} videos | "
                 f"{s['delta_rows']} delta | {s['total']} records")
    log.info(f"=== 02_load_supabase DONE ===")


if __name__ == "__main__":
    main()
