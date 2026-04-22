# PROJECT_CONTEXT.md — YouTube Analytics Pipeline
> **Dành cho AI (Claude).** Đây là file tổng quan toàn bộ project để AI hiểu cấu trúc, debug và phát triển mà không cần xem lại toàn bộ repo mỗi lần.
> Cập nhật file này mỗi khi có thay đổi lớn về schema, logic, hoặc thêm tính năng mới.

---

## 1. MỤC ĐÍCH DỰ ÁN

Pipeline tự động thu thập dữ liệu trending YouTube, lưu vào Supabase, phân tích bằng Gemini AI, và publish dashboard lên GitHub Pages.

- **Stream VN**: Top 50 trending Việt Nam
- **Stream Global**: Top 10 × 5 thị trường (US, KR, JP, GB, IN) = ~50 video sau dedup

---

## 2. KIẾN TRÚC TỔNG QUAN

```
GitHub Actions
│
├── hourly.yml  (05 * * * * — mỗi giờ)
│   ├── 01_crawl_youtube.py  --mode hourly  →  crawl_vn.json (mode=hourly)
│   ├── 02_load_supabase.py                 →  upsert + hourly_snapshot + momentum
│   └── 04_generate_html.py                 →  render HTML từ Supabase, deploy gh-pages
│
└── daily.yml   (0 23 * * * UTC = 06:00 ICT)
    ├── 01_crawl_youtube.py  --mode daily   →  crawl_vn.json + crawl_global.json (mode=daily)
    ├── 02_load_supabase.py                 →  full upsert + daily_delta + refresh views
    ├── 03_analyze_gemini.py                →  Tier1 stats + Tier2 Gemini → insights table
    └── 04_generate_html.py                 →  render HTML đầy đủ với AI insights
```

**Artifacts flow giữa các job:**
- `crawl_vn.json` / `crawl_global.json` → artifact `daily-crawl-{run_id}` / `hourly-crawl-{run_id}`
- `load_summary.json` → artifact `daily-load-summary-{run_id}` / `hourly-load-summary-{run_id}`
- `analysis_output.json` → artifact `daily-analysis-{run_id}`

---

## 3. FILES TRONG REPO

| File | Vai trò | Ghi chú |
|------|---------|---------|
| `01_crawl_youtube.py` | Crawl YouTube Data API v3 | Modes: daily/hourly |
| `02_load_supabase.py` | Upsert Supabase, tính delta, hourly snapshot | Routing theo `mode` field trong JSON |
| `03_analyze_gemini.py` | Tier1 stats + Tier2 Gemini AI | Chỉ chạy trong daily.yml |
| `04_generate_html.py` | Render Jinja2 template → HTML | Query Supabase trực tiếp |
| `templates/dashboard.html` | Jinja2 template, Chart.js | Self-contained, không tách CSS/JS |
| `create_supabase.sql` | Schema ban đầu | Chạy 1 lần |
| `add_hourly_snapshot.sql` | Migration thêm hourly_snapshot | Chạy sau create_supabase |
| `concurrent_refresh_fix.sql` | Fix CONCURRENTLY index | Chạy sau add_hourly_snapshot |
| `refresh_all_views.sql` | Override function không dùng CONCURRENTLY | Chạy sau concurrent_refresh_fix nếu vẫn lỗi |
| `requirements.txt` | Python deps | google-api-python-client, supabase, google-generativeai, Jinja2 |
| `.github/workflows/daily.yml` | CI daily | 4 jobs: crawl→load→analyze→publish |
| `.github/workflows/hourly.yml` | CI hourly | 3 jobs: crawl→load→publish |

---

## 4. SUPABASE SCHEMA

### Bảng chính

```
channels         id (PK TEXT), name, stream, subscribers, last_updated_at
videos           id (PK TEXT), channel_id→channels, title, tags[], category_id/name,
                 published_at, duration_seconds, content_type, stream, region,
                 thumbnail_url, views, likes, comments_count, first_seen_at, last_updated_at
stream_details   video_id (PK→videos), actual_start_time, actual_end_time,
                 concurrent_viewers_peak, stream_duration_seconds
daily_delta      (video_id, date) UNIQUE, views_total, likes_total, comments_total,
                 views_gain (NULL nếu ngày đầu), likes_gain, comments_gain,
                 stream, content_type, region,
                 peak_hour, peak_views_gain_1h, acceleration_score,
                 momentum_status (accelerating|peaking|decelerating|fading|new),
                 spike_detected_at
hourly_snapshot  (video_id, snapshot_at) UNIQUE, views, likes, comments,
                 views_delta_1h (NULL nếu snapshot đầu tiên trong ngày),
                 likes_delta_1h, stream, content_type, region
comments         id (PK TEXT→YouTube comment ID), video_id, text, like_count, published_at
insights         id BIGSERIAL, run_id, insight_type, scope, content_type,
                 period_start, period_end, payload JSONB, narrative TEXT, generated_at
crawl_logs       id BIGSERIAL, run_id, script, stream, status, records_processed,
                 quota_used, error_message, started_at, finished_at, duration_seconds (computed)
```

### Materialized Views

```
weekly_stats     week_start, stream, content_type, category_name (sentinel '__null__'),
                 videos_active, total_views_gained, avg_er_pct, active_channels
monthly_stats    (tương tự weekly_stats nhưng theo tháng)
intraday_chart   hour_bucket, stream, content_type, total_views_gained, active_videos,
                 max_single_video, avg_per_video
```

### Functions quan trọng

```sql
refresh_all_views()           -- Refresh 3 materialized views (không dùng CONCURRENTLY)
compute_momentum_status(video_id, as_of)  -- Tính momentum từ 6 hourly snapshots
get_hot_right_now(stream, limit, hours_ago)  -- Top N video theo views_delta_1h
get_virality_index(video_id, date)  -- So sánh views_gain vs baseline kênh 30 ngày
```

### Thứ tự chạy SQL migrations

1. `create_supabase.sql` — schema gốc
2. `add_hourly_snapshot.sql` — thêm hourly_snapshot + momentum columns
3. `concurrent_refresh_fix.sql` — fix weekly/monthly_stats views (dùng sentinel `__null__` thay vì NULL)
4. `refresh_all_views.sql` — override function bỏ CONCURRENTLY nếu vẫn còn lỗi unique index

---

## 5. LOGIC NGHIỆP VỤ QUAN TRỌNG

### Day 1 Problem Fix
- Ngày đầu video được thấy: `views_gain = NULL` (chưa có baseline)
- **FIX**: Mọi query dùng `COALESCE(views_gain, views_total)` hoặc `views_gain if not None else views_total`
- `is_first_day` flag trong KPI cards để hiển thị badge "Day 1" trên dashboard

### Mode Routing trong 02_load
- `crawl JSON có "mode": "hourly"` → upsert `hourly_snapshot` + gọi `compute_momentum_status` cho từng video
- `mode: "daily"` → chỉ upsert daily tables, không upsert hourly_snapshot

### Virality Index
- `VI = views_gain_hôm_nay / avg(views_gain_30_ngày_trước)`
- Threshold: VI ≥ 5.0 → viral
- Nếu không có baseline 30 ngày → bỏ qua

### Momentum Status Logic
```
≥ 6 hourly snapshots → so sánh avg 3h gần nhất vs avg 3h trước đó
  accel > 20% → "accelerating"
  -20% ~ +20% → "peaking"
  giảm nhưng still > 40% đỉnh → "decelerating"
  < 40% đỉnh → "fading"
< 2 snapshots → "new"
```

### Gemini Insights (Tier 2)
- Input: top 5 videos mỗi stream, ER by type, WoW%, top categories, viral count, trend lag
- Output JSON: `weekly_narrative`, `anomalies[]`, `content_gaps[]`, `recommendations[]`, `trend_forecast`
- Lưu vào `insights` table
- Hourly run KHÔNG gọi Gemini — đọc insights cũ từ Supabase (`weekly_summary` mới nhất)

### Trend Lag
- Tags trending ở Global stream trong 7 ngày qua nhưng chưa xuất hiện ở VN stream
- Dùng `videos.first_seen_at` để filter

---

## 6. GITHUB ACTIONS — LƯU Ý

### Secrets cần thiết
```
YOUTUBE_API_KEY       — YouTube Data API v3
SUPABASE_URL          — https://xxxx.supabase.co
SUPABASE_SERVICE_KEY  — service_role key (bypass RLS), KHÔNG dùng anon key
GEMINI_API_KEY        — Google AI Studio
GITHUB_TOKEN          — tự động có sẵn
```

### Concurrency
- `group: youtube-hourly` và `group: youtube-daily` — không cancel-in-progress để tránh race condition vào Supabase
- Daily và hourly có thể chạy đồng thời (different concurrency groups)

### `run_id`
- UUID generate bởi Python tại job crawl
- Truyền qua `jobs.<job>.outputs.run_id` → các job sau dùng để tìm đúng artifact
- Dùng chung trong cùng 1 lần chạy pipeline (crawl_logs, insights đều ghi run_id này)

### Skip Gemini
- `workflow_dispatch` có input `skip_gemini: true` → job `analyze` bị skip
- Dùng khi debug nhanh mà không muốn tốn Gemini quota

---

## 7. QUOTA YOUTUBE API

| Operation | Units |
|-----------|-------|
| `videos.list(chart=mostPopular)` | ~1/50 results |
| `videos.list(id=..., parts=4)` | ~1/50 videos |
| `commentThreads.list` | ~1/video |
| **Hourly mode (VN only)** | ~5 units/lần → 120/ngày |
| **Daily mode (VN+Global)** | ~15 units/lần |
| **Daily quota limit** | 10,000 units |

---

## 8. DASHBOARD HTML — CẤU TRÚC

Template: `templates/dashboard.html` (Jinja2 + Chart.js 4.4.1 từ CDN)

### Sections theo thứ tự
1. Header (badges, generated_at)
2. KPI Cards (4 cards: videos VN, views VN, ER VN, views Global)
3. Content Type Battle (Video vs Stream vs Shorts — VN và Global)
4. Top Rankings (tabs: All / Video / Stream / Shorts)
5. Category Leaderboard (JS render từ `cat_perf_vn_json`)
6. Intraday Charts (hourly views — cần hourly.yml đã chạy ít nhất 1 lần)
7. Trend 30 ngày (line charts)
8. WoW + Monthly + Post Time heatmap
9. Hot Right Now (từ `get_hot_right_now()` RPC)
10. Momentum Status grid
11. Gemini AI Insights (narrative, forecast, anomalies)
12. Content Gap + Recommendations
13. Viral Early Warning (chỉ hiện khi có viral)
14. Trend Lag Radar
15. Category + ER Charts (donuts)

### Template variables quan trọng
```python
kpi                  # dict {VN: {...}, Global: {...}}
trend_vn_json        # JSON string cho Chart.js
top_vn / top_global  # list of video dicts
svv_vn_data          # dict {video: {count, pct_views, avg_er}, stream: ..., shorts: ...}
intraday_vn_json     # [{hour: "HH:MM", views: N, videos: N}]
hot_vn / hot_global  # từ get_hot_right_now() RPC
momentum_vn/global   # {accelerating: N, peaking: N, ...}
weekly_narrative     # string từ Gemini hoặc Supabase insights
```

---

## 9. DEBUG CHECKLIST

### Pipeline fail tại job Crawl
- Kiểm tra `YOUTUBE_API_KEY` secret còn hợp lệ không
- Xem log: tìm `API 403 Forbidden` → quota exceeded hoặc key sai
- Chạy local: `export YOUTUBE_API_KEY=... && python 01_crawl_youtube.py --dry-run`

### Pipeline fail tại job Load
- Kiểm tra `SUPABASE_URL` và `SUPABASE_SERVICE_KEY`
- Lỗi thường gặp: table chưa tồn tại → chạy lại SQL migrations theo đúng thứ tự
- Lỗi `duplicate key` → upsert đang conflict, kiểm tra `on_conflict` argument
- Lỗi `refresh_all_views` → thường do CONCURRENTLY với expression index → chạy `refresh_all_views.sql`

### Pipeline fail tại job Analyze
- Lỗi `FileNotFoundError: load_summary.json` → job Load chưa upload artifact đúng tên
- Lỗi Gemini `READY` test fail → kiểm tra `GEMINI_API_KEY`
- JSON parse error từ Gemini → có retry 3 lần, nếu vẫn fail thì fallback (narrative = raw text)

### Pipeline fail tại job Publish
- Lỗi `FileNotFoundError: analysis_output.json` → hourly run tạo placeholder tự động, daily run cần job analyze xong trước
- Lỗi Jinja2 template → kiểm tra template syntax, thường do variable None không được handle
- GitHub Pages không cập nhật → kiểm tra `publish_branch: gh-pages` và permissions `contents: write`

### Dashboard hiển thị "Chưa có hourly data"
- Hot Right Now / Intraday charts cần `hourly.yml` đã chạy ít nhất 1 lần
- Kiểm tra `hourly_snapshot` table trong Supabase có data không
- Kiểm tra `intraday_chart` materialized view có được refresh không

### Tất cả views_gain = NULL (Day 1)
- Bình thường với video mới thấy lần đầu
- Dashboard dùng `views_total` làm fallback — KPI vẫn hiển thị số
- Ngày hôm sau sẽ có delta thực sự

---

## 10. PHÁT TRIỂN THÊM — HƯỚNG DẪN

### Thêm market mới vào Global stream
1. `01_crawl_youtube.py` → thêm vào `GLOBAL_MARKETS` list
2. Không cần thay đổi DB hay template

### Thêm một loại insight mới từ Gemini
1. `03_analyze_gemini.py` → thêm field vào `build_gemini_prompt()` output JSON schema
2. Thêm `save_insight()` call với `insight_type` mới (cần add vào CHECK constraint trong schema)
3. `04_generate_html.py` → thêm fetch trong `fetch_latest_insights()` và truyền vào context
4. `templates/dashboard.html` → thêm section render

### Thêm cột mới vào bảng
1. Viết migration SQL với `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
2. Cập nhật Python script tương ứng
3. Nếu cột dùng trong materialized view → DROP và CREATE lại view, tạo unique index plain (không expression)
4. Cập nhật `refresh_all_views()` nếu cần

### Thêm chart mới vào dashboard
1. `04_generate_html.py` → thêm fetch function và thêm vào `build_template_context()`
2. `templates/dashboard.html` → thêm `<canvas>` element và JS Chart.js code

### Thay Gemini model
- `03_analyze_gemini.py` → sửa `GEMINI_MODEL = "gemini-2.5-flash"` (hiện tại)
- Chú ý: model phải support JSON output đủ complex

---

## 11. FILES CẦN XEM KHI DEBUG (yêu cầu gửi lại)

Khi gặp vấn đề phức tạp, yêu cầu user gửi file tương ứng:

| Vấn đề | File cần xem |
|--------|-------------|
| Dashboard layout/styling | `templates/dashboard.html` |
| Logic tính delta/momentum | `02_load_supabase.py` |
| Gemini prompt / insight structure | `03_analyze_gemini.py` |
| HTML template variables không đủ | `04_generate_html.py` |
| Schema conflict / migration | `create_supabase.sql`, `add_hourly_snapshot.sql`, `concurrent_refresh_fix.sql` |
| Crawl data sai / thiếu | `01_crawl_youtube.py` |
| Job dependencies / artifact names | `.github/workflows/daily.yml` hoặc `hourly.yml` |

---

## 12. TRẠNG THÁI HIỆN TẠI & TODO

### Đã hoạt động
- [x] Hourly crawl VN + load + publish
- [x] Daily crawl VN+Global + Gemini analysis + publish
- [x] Hourly snapshot + momentum tracking
- [x] Day 1 problem fix (COALESCE fallback)
- [x] Concurrent refresh fix (sentinel `__null__`)
- [x] Hot Right Now feed
- [x] Content Type Battle (Video vs Stream vs Shorts)
- [x] Category Leaderboard

### Biết trước sẽ empty khi mới deploy
- Hot Right Now / Intraday: cần ≥1 hourly run
- WoW chart: cần ≥2 tuần data
- Monthly chart: cần ≥1 tháng data
- Viral alerts: cần ≥30 ngày baseline
- Trend Lag: cần video có `first_seen_at` trong 7 ngày qua

### Potential improvements (chưa làm)
- [ ] Channel subscriber tracking (crawl channel details)
- [ ] Comments sentiment analysis (hiện tại crawl comments nhưng chưa analyze)
- [ ] Email/Slack notification khi có viral alert
- [ ] Shorts-specific metrics (view velocity khác với video thường)
- [ ] Multi-language support cho Gemini narrative (hiện chỉ tiếng Việt)

---

*File này được tạo tự động — cập nhật khi có thay đổi lớn.*
