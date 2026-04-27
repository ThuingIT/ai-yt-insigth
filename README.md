# YouTube Analytics Pipeline

Hệ thống tự động crawl YouTube → phân tích với Gemini 2.5 → dashboard HTML hàng ngày.

## Kiến trúc

```
GitHub Actions (06:00 ICT)
  └─ Job 1: 01_crawl_youtube.py     ← YouTube API → data/*.json
  └─ Job 2: 02_load_supabase.py     ← upsert + daily_delta + refresh views
  └─ Job 3: 03_analyze_gemini.py    ← Tầng 1 stats + Tầng 2 Gemini
  └─ Job 4: 04_generate_html.py     ← Jinja2 → docs/index.html → GitHub Pages
```

## Cấu trúc thư mục

```
├── .github/workflows/
│   ├── daily.yml          ← crawl→load+cleanup→analyze→publish
│   └── hourly.yml         ← crawl→load→publish (mỗi giờ)
├── scripts/               ← ★ Mới (Phase 1 refactor)
│   ├── 04_generate_html.py ← thin orchestrator
│   └── fetchers/
│       ├── base.py        ← safe_fetch(), batch_video_lookup()
│       ├── kpi.py
│       ├── rankings.py    ← N+1 fixed
│       ├── charts.py
│       ├── realtime.py    ← 2-tier fallback
│       └── insights.py
├── 01_crawl_youtube.py
├── 02_load_supabase.py
├── 03_analyze_gemini.py   ← N+1 fixed
├── templates/
│   └── dashboard.html
├── docs/                  ← GitHub Pages (auto-generated)
│   └── index.html
├── data/                  ← Artifacts tạm (không commit)
├── cleanup_old_data.sql   ← ★ Mới: chạy 1 lần trong Supabase
├── requirements.txt
└── README.md
```

## Setup (1 lần)

### 1. Supabase
- Tạo project tại https://supabase.com
- Vào SQL Editor → chạy theo thứ tự:
  1. `create_supabase.sql`
  2. `add_hourly_snapshot.sql`
  3. `concurrent_refresh_fix.sql`
  4. `cleanup_old_data.sql` ← **Mới** (Phase 1)
- Lấy **Project URL** và **service_role key** (Settings → API)

### 2. YouTube API Key
- Vào https://console.cloud.google.com
- Enable "YouTube Data API v3"
- Tạo API Key (không cần OAuth)

### 3. Gemini API Key
- Vào https://aistudio.google.com/app/apikey
- Tạo API key miễn phí

### 4. GitHub Secrets
Vào **Settings → Secrets and variables → Actions → New repository secret**:

| Secret name          | Giá trị                        |
|----------------------|--------------------------------|
| `YOUTUBE_API_KEY`    | AIza...                        |
| `SUPABASE_URL`       | https://xxxx.supabase.co       |
| `SUPABASE_SERVICE_KEY` | eyJ... (service_role key)   |
| `GEMINI_API_KEY`     | AIza...                        |

### 5. GitHub Pages
- Settings → Pages → Source: **Deploy from branch** → Branch: `gh-pages`
- Dashboard sẽ có tại: `https://<username>.github.io/<repo-name>/`

## Chạy thủ công

Actions → YouTube Analytics Pipeline → **Run workflow**

Options:
- `stream`: `both` / `vn` / `global`
- `skip_analyze`: `true` để bỏ qua Gemini (debug nhanh)

## Debug từng bước local

```bash
# Setup
pip install -r requirements.txt
export YOUTUBE_API_KEY="AIza..."
export SUPABASE_URL="https://xxxx.supabase.co"
export SUPABASE_SERVICE_KEY="eyJ..."
export GEMINI_API_KEY="AIza..."
export OUTPUT_DIR="data"
export DOCS_DIR="docs"
export TEMPLATE_DIR="templates"
mkdir -p data docs

# Bước 1 — chỉ VN
python 01_crawl_youtube.py --stream vn

# Bước 1 — dry run (không gọi API)
python 01_crawl_youtube.py --dry-run

# Bước 2
python 02_load_supabase.py

# Bước 3
python 03_analyze_gemini.py

# Bước 4
python 04_generate_html.py
open docs/index.html
```

## Quota YouTube API

| Nguồn             | Units/ngày |
|-------------------|------------|
| VN Top 50         | ~5         |
| Global 5 markets  | ~10        |
| **Tổng**          | **~15**    |
| Giới hạn miễn phí | 10,000     |
| Dư                | 9,985      |

## Chi phí Gemini

Chỉ gọi **1 lần/ngày** với ~2,000 tokens input + ~1,000 tokens output.
Gemini 2.5 Flash: ~$0.0001/lần → **< $0.05/tháng**.
