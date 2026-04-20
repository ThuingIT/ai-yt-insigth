#!/usr/bin/env python3
"""
01_crawl_youtube.py
===================
Bước 1 của pipeline: Crawl dữ liệu từ YouTube Data API v3.

Stream A — Vietnam:   Top 50 trending videos (regionCode=VN)
Stream B — Global:    Top 10/market × 5 markets = 50 videos (sampled)

Output:
    data/crawl_vn.json
    data/crawl_global.json

Quota YouTube API ước tính mỗi lần chạy:
    VN:     videos.list chart × 1 + videos.list detail × 1 = ~5 units
    Global: videos.list chart × 5 + videos.list detail × 1 = ~10 units
    Tổng:   ~15 units / ngày  (giới hạn: 10,000 units/ngày)

Cách chạy:
    export YOUTUBE_API_KEY="AIza..."
    export OUTPUT_DIR="data"          # optional, default: data
    export ENABLE_COMMENTS="false"    # optional, default: false (tốn thêm quota)
    python 01_crawl_youtube.py

Trong GitHub Actions:
    - YOUTUBE_API_KEY lưu trong GitHub Secrets
    - OUTPUT_DIR = "data" (artifact được dùng bởi job 02_load)
"""

import os
import re
import json
import time
import uuid
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ============================================================
# CONFIG
# ============================================================

YOUTUBE_API_KEY: str = os.environ["YOUTUBE_API_KEY"]
OUTPUT_DIR: str      = os.environ.get("OUTPUT_DIR", "data")
ENABLE_COMMENTS: bool = os.environ.get("ENABLE_COMMENTS", "false").lower() == "true"

# ID duy nhất cho lần chạy pipeline này (dùng chung với 02_load, 03_analyze)
RUN_ID: str = os.environ.get("PIPELINE_RUN_ID", str(uuid.uuid4()))

# Vietnam stream: chỉ top 50 trending
VN_CONFIG = {
    "stream":              "VN",
    "region_code":         "VN",
    "relevance_language":  "vi",
    "max_results":         50,       # Theo yêu cầu: top 50 VN thôi
}

# Global stream: 5 markets × 10 videos mỗi market = 50 total (trước khi dedup)
GLOBAL_MARKETS = [
    {"region_code": "US", "max_results": 10},
    {"region_code": "KR", "max_results": 10},
    {"region_code": "JP", "max_results": 10},
    {"region_code": "GB", "max_results": 10},
    {"region_code": "IN", "max_results": 10},
]

# YouTube category ID → tên (API trả về ID dạng string)
CATEGORY_MAP: dict[str, str] = {
    "1":  "Film & Animation",
    "2":  "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "18": "Short Movies",
    "19": "Travel & Events",
    "20": "Gaming",
    "21": "Videoblogging",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
    "29": "Nonprofits & Activism",
}

# Retry settings cho API calls
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("01_crawl")


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def parse_iso8601_duration(duration: str) -> int:
    """
    Chuyển ISO 8601 duration sang giây.
    Ví dụ: 'PT4M13S' → 253, 'PT1H2M30S' → 3750, 'PT0S' → 0
    """
    if not duration:
        return 0
    pattern = r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
    match = re.match(pattern, duration)
    if not match:
        return 0
    hours   = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def detect_content_type(item: dict) -> str:
    """
    Phân loại video thành 'video', 'stream', hoặc 'shorts'.

    Logic:
    - Có liveStreamingDetails → stream (live đã kết thúc)
    - duration ≤ 60s VÀ có #shorts trong tags/description → shorts
    - Còn lại → video thường
    """
    live_details = item.get("liveStreamingDetails", {})
    if live_details:
        return "stream"

    content_details = item.get("contentDetails", {})
    duration_str    = content_details.get("duration", "PT0S")
    duration_secs   = parse_iso8601_duration(duration_str)

    snippet     = item.get("snippet", {})
    tags        = snippet.get("tags", [])
    description = snippet.get("description", "")
    tags_lower  = " ".join(tags).lower()

    if duration_secs <= 60 and (
        "#shorts" in tags_lower or
        "#shorts" in description.lower()
    ):
        return "shorts"

    return "video"


def safe_int(value, default: int = 0) -> int:
    """Chuyển string sang int an toàn (API trả về string cho view counts)."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def api_call_with_retry(fn, *args, **kwargs):
    """
    Gọi YouTube API với retry logic.
    Tự retry khi gặp lỗi 500/503 (server error), không retry khi 403/404.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs).execute()
        except HttpError as e:
            status = e.resp.status
            if status in (500, 503) and attempt < MAX_RETRIES:
                log.warning(f"API error {status}, retry {attempt}/{MAX_RETRIES} sau {RETRY_DELAY}s")
                time.sleep(RETRY_DELAY * attempt)
            elif status == 403:
                # Quota exceeded hoặc API key không hợp lệ → fail ngay
                log.error(f"API 403 Forbidden: {e.error_details}")
                raise
            else:
                log.error(f"API error {status}: {e}")
                raise
    return None


# ============================================================
# CORE CRAWL FUNCTIONS
# ============================================================

def get_trending_video_ids(
    youtube,
    region_code: str,
    max_results: int,
    language: Optional[str] = None,
) -> list[str]:
    """
    Lấy danh sách ID video trending của một region.
    Dùng videos.list với chart=mostPopular.
    Quota: ~1 unit per 50 results (rất rẻ).
    """
    params = {
        "part":        "id",
        "chart":       "mostPopular",
        "regionCode":  region_code,
        "maxResults":  min(max_results, 50),  # API giới hạn 50/request
    }
    if language:
        params["relevanceLanguage"] = language

    response = api_call_with_retry(youtube.videos().list, **params)
    if not response:
        return []

    ids = [item["id"] for item in response.get("items", [])]

    # Nếu cần hơn 50 và có nextPageToken (hiếm gặp với trending)
    next_token = response.get("nextPageToken")
    while next_token and len(ids) < max_results:
        params["pageToken"] = next_token
        response = api_call_with_retry(youtube.videos().list, **params)
        if not response:
            break
        ids.extend([item["id"] for item in response.get("items", [])])
        next_token = response.get("nextPageToken")

    return ids[:max_results]


def get_video_details(youtube, video_ids: list[str]) -> list[dict]:
    """
    Lấy full details cho danh sách video IDs.
    API cho phép tối đa 50 IDs/request.
    Parts: snippet, statistics, contentDetails, liveStreamingDetails.
    Quota: ~1 unit per 50 videos.
    """
    if not video_ids:
        return []

    results = []
    # Chia batch 50 ID mỗi lần
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        response = api_call_with_retry(
            youtube.videos().list,
            part="snippet,statistics,contentDetails,liveStreamingDetails",
            id=",".join(batch),
        )
        if response:
            results.extend(response.get("items", []))
        log.debug(f"  Fetched details: batch {i//50 + 1}, got {len(response.get('items', []) if response else [])} items")

    return results


def get_top_comments(youtube, video_id: str, max_results: int = 20) -> list[dict]:
    """
    Lấy top comments của một video (sắp xếp theo relevance).
    Chỉ gọi khi ENABLE_COMMENTS=true.
    Quota: ~1 unit per call.

    CẢNH BÁO: Gọi function này cho 100 videos = 100 units.
    Chỉ nên dùng cho top 10-20 videos có nhiều views nhất.
    """
    try:
        response = api_call_with_retry(
            youtube.commentThreads().list,
            part="snippet",
            videoId=video_id,
            order="relevance",
            maxResults=min(max_results, 100),
            textFormat="plainText",
        )
        if not response:
            return []

        comments = []
        for item in response.get("items", []):
            top_comment = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "id":           item["id"],
                "video_id":     video_id,
                "text":         top_comment.get("textDisplay", "")[:1000],  # Giới hạn 1000 chars
                "like_count":   safe_int(top_comment.get("likeCount", 0)),
                "published_at": top_comment.get("publishedAt"),
                "crawled_at":   datetime.now(timezone.utc).isoformat(),
            })
        return comments

    except HttpError as e:
        if e.resp.status == 403:
            # Comments disabled trên video này
            log.debug(f"  Comments disabled: video {video_id}")
            return []
        raise


def process_video_item(item: dict, stream: str, region: str) -> tuple[dict, dict | None, dict | None]:
    """
    Xử lý một YouTube video item thành format chuẩn cho pipeline.

    Returns:
        (video_dict, channel_dict, stream_details_dict | None)
    """
    content_type = detect_content_type(item)
    snippet      = item.get("snippet", {})
    stats        = item.get("statistics", {})
    live         = item.get("liveStreamingDetails", {})
    content      = item.get("contentDetails", {})

    channel_id   = snippet.get("channelId", "")
    category_id  = snippet.get("categoryId", "")
    duration_str = content.get("duration", "PT0S")

    # Thumbnail: ưu tiên medium, fallback default
    thumbnails   = snippet.get("thumbnails", {})
    thumbnail    = (
        thumbnails.get("medium", {}).get("url") or
        thumbnails.get("default", {}).get("url")
    )

    video = {
        "id":               item["id"],
        "channel_id":       channel_id,
        "title":            snippet.get("title", ""),
        "description":      snippet.get("description", "")[:500],      # Giới hạn 500 chars để tránh bloat DB
        "tags":             snippet.get("tags", [])[:20],               # Tối đa 20 tags
        "category_id":      int(category_id) if category_id else None,
        "category_name":    CATEGORY_MAP.get(category_id, "Other"),
        "published_at":     snippet.get("publishedAt"),
        "duration_seconds": parse_iso8601_duration(duration_str),
        "content_type":     content_type,
        "stream":           stream,
        "region":           region,
        "thumbnail_url":    thumbnail,
        # Stats (snapshot tại thời điểm crawl)
        "views":            safe_int(stats.get("viewCount")),
        "likes":            safe_int(stats.get("likeCount")),
        "comments_count":   safe_int(stats.get("commentCount")),
        "crawled_at":       datetime.now(timezone.utc).isoformat(),
    }

    channel = {
        "id":    channel_id,
        "name":  snippet.get("channelTitle", ""),
        "stream": stream,
    }

    stream_details = None
    if content_type == "stream" and live:
        stream_details = {
            "video_id":                 item["id"],
            "actual_start_time":        live.get("actualStartTime"),
            "actual_end_time":          live.get("actualEndTime"),
            "scheduled_start_time":     live.get("scheduledStartTime"),
            "concurrent_viewers_peak":  safe_int(live.get("concurrentViewers")) or None,
            "total_chat_count":         None,   # Không crawl live chat (tốn quota)
        }
        # Tính duration từ start/end nếu có
        if stream_details["actual_start_time"] and stream_details["actual_end_time"]:
            try:
                from datetime import datetime as dt
                fmt = "%Y-%m-%dT%H:%M:%SZ"
                start = dt.strptime(stream_details["actual_start_time"][:19], "%Y-%m-%dT%H:%M:%S")
                end   = dt.strptime(stream_details["actual_end_time"][:19], "%Y-%m-%dT%H:%M:%S")
                stream_details["stream_duration_seconds"] = max(0, int((end - start).total_seconds()))
            except Exception:
                stream_details["stream_duration_seconds"] = None
        else:
            stream_details["stream_duration_seconds"] = None

    return video, channel, stream_details


# ============================================================
# STREAM A: VIETNAM
# ============================================================

def crawl_vn(youtube) -> dict:
    """
    Crawl Top 50 trending videos tại Việt Nam.

    Quota ước tính:
        - videos.list(chart=mostPopular, regionCode=VN, n=50): 1 unit
        - videos.list(id=..., part=snippet+stats+content+live, n=50): 1 unit
        - Tổng: ~5 units (bao gồm overhead)
    """
    log.info(f"[VN] === Bắt đầu crawl Top {VN_CONFIG['max_results']} Trending ===")
    quota_used = 0

    # Bước 1: Lấy danh sách ID
    video_ids = get_trending_video_ids(
        youtube,
        region_code=VN_CONFIG["region_code"],
        max_results=VN_CONFIG["max_results"],
        language=VN_CONFIG["relevance_language"],
    )
    quota_used += 2
    log.info(f"[VN] Lấy được {len(video_ids)} video IDs")

    if not video_ids:
        log.error("[VN] Không lấy được video IDs — kiểm tra API key và quota")
        raise RuntimeError("VN crawl failed: no video IDs returned")

    # Bước 2: Lấy full details
    raw_items = get_video_details(youtube, video_ids)
    quota_used += max(1, len(video_ids) // 50)
    log.info(f"[VN] Lấy được details cho {len(raw_items)}/{len(video_ids)} videos")

    # Bước 3: Xử lý từng video
    videos        = []
    channels_map  = {}  # channel_id → channel dict (dedup)
    stream_details_list = []
    type_count    = {"video": 0, "stream": 0, "shorts": 0}

    for item in raw_items:
        try:
            video, channel, sd = process_video_item(item, stream="VN", region="VN")
            videos.append(video)
            channels_map[channel["id"]] = channel
            type_count[video["content_type"]] += 1
            if sd:
                stream_details_list.append(sd)
        except Exception as e:
            log.warning(f"[VN] Bỏ qua video {item.get('id', '?')}: {e}")

    # Bước 4 (optional): Crawl comments cho top 10 videos theo views
    comments = []
    if ENABLE_COMMENTS:
        top_videos = sorted(videos, key=lambda v: v["views"], reverse=True)[:10]
        log.info(f"[VN] Crawl comments cho top {len(top_videos)} videos...")
        for v in top_videos:
            c = get_top_comments(youtube, v["id"], max_results=20)
            comments.extend(c)
            quota_used += 1
        log.info(f"[VN] Lấy được {len(comments)} comments")

    log.info(f"[VN] Kết quả: {type_count} | Channels: {len(channels_map)} | Quota: ~{quota_used}")

    return {
        "stream":           "VN",
        "run_id":           RUN_ID,
        "crawled_at":       datetime.now(timezone.utc).isoformat(),
        "quota_used":       quota_used,
        "summary": {
            "total_videos":   len(videos),
            "by_type":        type_count,
            "channels":       len(channels_map),
        },
        "videos":           videos,
        "channels":         list(channels_map.values()),
        "stream_details":   stream_details_list,
        "comments":         comments,
    }


# ============================================================
# STREAM B: GLOBAL
# ============================================================

def crawl_global(youtube) -> dict:
    """
    Crawl Top 10 trending videos từ 5 thị trường lớn (tổng ~50 videos).
    Dedup cross-market: video nổi ở nhiều nước chỉ lưu 1 lần.

    Markets: US, KR, JP, GB, IN
    Quota ước tính:
        - videos.list(chart) × 5 markets: 5 units
        - videos.list(detail) cho ~50 unique IDs: 1 unit
        - Tổng: ~10 units
    """
    log.info("[Global] === Bắt đầu crawl 5 markets ===")
    quota_used  = 0
    all_ids     = []        # Ordered list để giữ thứ tự (first-seen = first-trending)
    id_to_region = {}       # video_id → region đầu tiên thấy

    # Bước 1: Lấy IDs từ từng market
    for market in GLOBAL_MARKETS:
        region = market["region_code"]
        try:
            ids = get_trending_video_ids(
                youtube,
                region_code=region,
                max_results=market["max_results"],
            )
            quota_used += 2
            new_ids = 0
            for vid_id in ids:
                if vid_id not in id_to_region:
                    id_to_region[vid_id] = region
                    all_ids.append(vid_id)
                    new_ids += 1
            log.info(f"[Global/{region}] {len(ids)} videos, {new_ids} unique mới")
        except HttpError as e:
            log.error(f"[Global/{region}] Lỗi API: {e} — bỏ qua market này")

    unique_ids = all_ids  # Đã dedup trong vòng lặp trên
    log.info(f"[Global] Tổng: {len(unique_ids)} unique videos từ {len(GLOBAL_MARKETS)} markets")

    if not unique_ids:
        log.error("[Global] Không lấy được video IDs nào")
        raise RuntimeError("Global crawl failed: no video IDs returned")

    # Bước 2: Lấy full details
    raw_items = get_video_details(youtube, unique_ids)
    quota_used += max(1, len(unique_ids) // 50)
    log.info(f"[Global] Lấy được details cho {len(raw_items)}/{len(unique_ids)} videos")

    # Bước 3: Xử lý
    videos             = []
    channels_map       = {}
    stream_details_list = []
    type_count         = {"video": 0, "stream": 0, "shorts": 0}

    for item in raw_items:
        try:
            region = id_to_region.get(item["id"], "unknown")
            video, channel, sd = process_video_item(item, stream="Global", region=region)
            videos.append(video)
            channels_map[channel["id"]] = channel
            type_count[video["content_type"]] += 1
            if sd:
                stream_details_list.append(sd)
        except Exception as e:
            log.warning(f"[Global] Bỏ qua video {item.get('id', '?')}: {e}")

    # Optional: comments cho Global
    comments = []
    if ENABLE_COMMENTS:
        top_videos = sorted(videos, key=lambda v: v["views"], reverse=True)[:5]
        log.info(f"[Global] Crawl comments cho top {len(top_videos)} videos...")
        for v in top_videos:
            c = get_top_comments(youtube, v["id"], max_results=10)
            comments.extend(c)
            quota_used += 1

    log.info(f"[Global] Kết quả: {type_count} | Channels: {len(channels_map)} | Quota: ~{quota_used}")

    return {
        "stream":           "Global",
        "run_id":           RUN_ID,
        "crawled_at":       datetime.now(timezone.utc).isoformat(),
        "quota_used":       quota_used,
        "markets":          [m["region_code"] for m in GLOBAL_MARKETS],
        "summary": {
            "total_videos":   len(videos),
            "by_type":        type_count,
            "channels":       len(channels_map),
            "by_region":      {
                region: sum(1 for v in videos if v["region"] == region)
                for region in id_to_region.values()
            },
        },
        "videos":           videos,
        "channels":         list(channels_map.values()),
        "stream_details":   stream_details_list,
        "comments":         comments,
    }


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="01_crawl_youtube — YouTube Data API v3 crawler")
    parser.add_argument("--stream", choices=["vn", "global", "both"], default="both",
                        help="Chỉ chạy 1 stream (mặc định: cả hai)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log config và thoát, không gọi API")
    args = parser.parse_args()

    if args.dry_run:
        log.info("=== DRY RUN ===")
        log.info(f"OUTPUT_DIR:      {OUTPUT_DIR}")
        log.info(f"RUN_ID:          {RUN_ID}")
        log.info(f"VN top results:  {VN_CONFIG['max_results']}")
        log.info(f"Global markets:  {[m['region_code'] for m in GLOBAL_MARKETS]}")
        log.info(f"Comments:        {ENABLE_COMMENTS}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    log.info(f"=== 01_crawl_youtube START | run_id={RUN_ID} ===")
    log.info(f"Output dir: {OUTPUT_DIR}")

    # Khởi tạo YouTube API client
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    total_quota = 0
    results     = {}

    # Stream A: Vietnam
    if args.stream in ("vn", "both"):
        vn_data = crawl_vn(youtube)
        vn_path = os.path.join(OUTPUT_DIR, "crawl_vn.json")
        with open(vn_path, "w", encoding="utf-8") as f:
            json.dump(vn_data, f, ensure_ascii=False, indent=2, default=str)
        total_quota += vn_data["quota_used"]
        results["vn"] = vn_data["summary"]
        log.info(f"[VN] Đã lưu → {vn_path}")

    # Stream B: Global
    if args.stream in ("global", "both"):
        global_data = crawl_global(youtube)
        global_path = os.path.join(OUTPUT_DIR, "crawl_global.json")
        with open(global_path, "w", encoding="utf-8") as f:
            json.dump(global_data, f, ensure_ascii=False, indent=2, default=str)
        total_quota += global_data["quota_used"]
        results["global"] = global_data["summary"]
        log.info(f"[Global] Đã lưu → {global_path}")

    # Summary
    log.info("=== SUMMARY ===")
    for stream, summary in results.items():
        log.info(f"  {stream.upper()}: {summary}")
    log.info(f"  Tổng quota dùng: ~{total_quota} / 10,000 units/ngày")
    log.info(f"=== 01_crawl_youtube DONE | run_id={RUN_ID} ===")


if __name__ == "__main__":
    main()
