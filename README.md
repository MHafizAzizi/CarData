# CarData

Car listing data scraped from [Mudah.my](https://www.mudah.my), Malaysia's largest classifieds site. The master dataset is updated regularly as listings change frequently.

> **Note:** Mudah.my caps browsable listings at ~250 pages. Re-running every 1–2 days is recommended to keep data fresh.

---

## Data Pipeline

The project supports two collection paths. The **hybrid scraper** (`scraper.py`) is the new primary collector; the **HTML-only scraper** (`script.py`) is kept as a fallback.

### Sources

| Page / endpoint | URL pattern | Used by |
|---|---|---|
| EagleSearch JSON API | `https://search.mudah.my/v1/search?category={1020\|1040}` | `scraper.py` (Phase 1) |
| Individual listing detail | `https://www.mudah.my/{ads_id}.htm` | `scraper.py` (Phase 2), `script.py`, `recheck.py` |
| Listing search results (HTML) | `https://www.mudah.my/malaysia/{cars\|motorcycles}-for-sale?o={page}` | `script.py` |
| Category landing (filters) | `https://www.mudah.my/malaysia/cars-for-sale?o=1` | `scrape_makes_models.py` |

EagleSearch is Mudah's internal JSON API (discovered via JS bundle inspection). Returns 200 ads/request with rich metadata that the HTML pages don't expose (`old_price`, `year_verified`, `bundle`, `car_loan_*`, etc.).

### What is extracted

**Hybrid scraper (`scraper.py`)** — two-phase pipeline:
- **Phase 1 (API)** — paginates EagleSearch (offset/limit 200/req). Returns normalized records with: `ads_id`, `subject`, `price`, `make`, `model`, `mileage_bucket`, `region`, `subarea`, `condition`, plus API-only fields (`old_price`, `year_verified`, `store_verified`, `bundle`, `media_count`, `car_loan_eligible`, `car_loan_payment`, `car_loan_tenure`, `has_car_grant`).
- **Phase 2 (HTML)** — fetches each `.htm` detail page only when needed. Adds: `body` (full description), exact `mileage`, chassis specs (`kw`, `torque`, `length`, `wheelbase`, `kerbwt`, `fueltk`, `brake_*`, `suspension_*`, `tyres_*`, `wheel_rim_*`, `steering`), `family`, `variant`, `series`, `style`, `seat`, `country_origin`, `engine`.

**HTML-only scraper (`script.py`)** — reads `__NEXT_DATA__` from HTML search pages (40 listings/page) and detail pages. Slower (~3-4s throttle per request) but kept as a complete fallback if EagleSearch is ever auth'd or blocked.

**Reference data (`scrape_makes_models.py`)** — pulls canonical make/model lists from category landing pages.

### Flow

```
EagleSearch API (search.mudah.my)
        │  Phase 1: 200 ads/req, <500ms
        │
        ▼                          .htm detail pages
                              ┌────────────│
                              │  Phase 2 (body, exact mileage, chassis specs;
                              │   only when --depth != none)
                              ▼
                          scraper.py
                              │
                              ▼
                  data/raw/<category>/*.csv
                              │
                              ▼
                  migrate_xlsx_to_db.py
                              │
                              ▼
                 data/master/cardata_*.db
                              │
                      ┌───────┴────────┐
                      ▼                ▼
                   clean.py        recheck.py
                (normalize data)  (track availability)
```

Reference data (makes/models) is a separate one-shot flow:

```
Mudah.my category pages
        │
        ▼
scrape_makes_models.py
        │
        ▼
data/reference/{cars,motorcycles}_{makes,models}.json
```

### Performance comparison

| Scraper | Listings | Wall time | Per listing | HTTP requests |
|---|---|---|---|---|
| `scraper.py --depth none` (API only) | 50 | ~0.1s | ~2ms | 1 |
| `scraper.py --depth missing` (default) | 50 | ~150s | ~3s | 1 + N HTML |
| `script.py` (HTML only) | 50 | ~150s | ~3s | 1 search + N detail |

API-only mode is ~1500× faster per listing. Hybrid mode matches HTML speed but adds 7+ API-only columns and avoids the 40-listing-per-page Cloudflare challenge that occasionally throttles `script.py`.

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Project Structure

```
CarData/
├── src/                                   # All scripts
│   ├── scraper.py                         # NEW: hybrid scraper (API + HTML)
│   ├── eagle_client.py                    # NEW: EagleSearch API wrapper
│   ├── detail_client.py                   # NEW: HTML detail-page parser
│   ├── script.py                          # HTML-only scraper (fallback)
│   ├── recheck.py                         # Availability re-checker
│   ├── migrate_xlsx_to_db.py              # CSV/Excel → SQLite migration
│   ├── db.py                              # Database connection helper
│   ├── clean.py                           # Data cleanup utility
│   ├── scrape_makes_models.py             # Reference make/model list scraper
│   └── mudah_client.py                    # Shared HTTP client (HTML, 3-4s throttle)
│
├── migrations/                            # NEW: schema migration runners
│   └── run_migrations.py                  # v1 → v2: adds API-only columns
│
├── docs/                                  # Documentation
│   └── brands.md                          # List of available car/motorcycle brands
│
├── data/
│   ├── raw/                               # Raw CSV outputs from scrapers
│   ├── reference/                         # Canonical make/model lists from Mudah
│   │   ├── cars_makes.json
│   │   ├── cars_models.json
│   │   ├── motorcycles_makes.json
│   │   └── motorcycles_models.json
│   └── master/                            # Production SQLite databases
│       ├── cardata_cars.db
│       ├── cardata_motorcycles.db
│       └── MasterMudahCarData.xlsx        # Legacy Excel master (optional)
│
├── logs/                                  # Log files
│   ├── scraper_hybrid.log                 # scraper.py logs (NEW)
│   ├── scraper.log                        # script.py logs
│   ├── recheck.log                        # recheck.py logs
│   └── migrate.log                        # migrate_xlsx_to_db.py logs
│
├── tests/                                 # Unit + integration tests
│   ├── test_scraper.py                    # NEW: HybridScraper tests
│   ├── test_eagle_client.py               # NEW: API wrapper tests
│   ├── test_detail_client.py              # NEW: HTML parser tests
│   ├── test_migrations.py                 # NEW: schema migration tests
│   ├── compare_outputs.py                 # NEW: integration diff (new vs old)
│   ├── test_clean.py
│   └── test_parser.py
├── schema_cars.sql                        # SQLite schema for cars (v2)
├── schema_motorcycles.sql                 # SQLite schema for motorcycles (v2)
├── requirements.txt
├── README.md
└── .gitignore
```

---

## Usage

### Hybrid scraper — `scraper.py` (recommended)

```bash
python src/scraper.py                                            # interactive
python src/scraper.py --category cars --max-ads 1000             # default depth=missing
python src/scraper.py --category motorcycles --max-ads 500 --depth none   # API only, fastest
python src/scraper.py --category cars --max-ads 200 --depth all  # always re-fetch HTML
```

Flags:

| Flag | Default | Description |
|---|---|---|
| `--category` | *prompted* | `cars` or `motorcycles` |
| `--max-ads` | *prompted* | Max ads to collect via API (Phase 1 cap) |
| `--depth` | `missing` | `none` = API only; `missing` = HTML only when `body` absent; `all` = HTML for every ad |
| `--checkpoint-every` | `100` | Write Phase-2 partial CSV every N detail fetches |
| `--output-dir` | `data/raw/<category>/` | Where to save CSVs |

CSV filename: `mudah_eagle_{category}_final_{timestamp}.csv`.

### HTML-only scraper — `script.py` (fallback)

Original page-by-page scraper. Use when EagleSearch is unavailable or to scrape with state/brand filters.

```bash
python src/script.py
```

Interactive prompts:
1. **Category** — pick by number (`1. cars`, `2. motorcycles`); default `1`
2. **State** — pick by number from a numbered list of `malaysia` plus the 13 states and 3 federal territories; default `1` (`malaysia` = nationwide)
3. **Brand** — type the brand slug, or leave blank for all
4. **Start page** — default `1`
5. **How many pages to scrape**

Flags:

| Flag | Default | Description |
|---|---|---|
| `--category` | *prompted* | Category: `cars` or `motorcycles` |
| `--state` | *prompted* | State to scrape (e.g. `selangor`, `johor`) |
| `--brand` | *prompted* | Brand filter (see `docs/brands.md` for available brands) |
| `--start` | *prompted* | Start page number |
| `--end` | *prompted* | End page number (max ~250) |
| `--pages` | — | Number of pages to scrape from `--start` (alternative to `--end`) |
| `--workers` | `2` | Concurrent workers for fetching listing details |
| `--output-dir` | `data/raw/<category>/` | Directory to save the output CSV |

### Examples

**Scrape cars — all Toyota listings in Selangor:**
```bash
python src/script.py --category cars --state selangor --brand toyota --start 1 --end 50
```

**Scrape motorcycles — all brands nationwide, first 5 pages:**
```bash
python src/script.py --category motorcycles --brand "" --start 1 --pages 5
```

**Scrape all car brands nationwide, then load into database:**
```bash
python src/script.py --category cars --start 1 --end 250 --workers 4
python src/migrate_xlsx_to_db.py --category cars
```

**Save output to a specific folder:**
```bash
python src/script.py --state kuala-lumpur --brand honda --start 1 --end 30 --output-dir data/raw/cars
```

See [`docs/brands.md`](docs/brands.md) for a complete list of available brands.

---

## Output Columns

Each row in the output CSV/Excel represents one listing. Columns vary slightly between cars and motorcycles.

### Common fields (both categories)

| Column | Description |
|---|---|
| `url` | Full Mudah.my listing URL |
| `ads_id` | Unique Mudah listing ID |
| `subject` | Full listing title |
| `body` | Full seller-written description (HTML `<br>` collapsed to newlines) |
| `price` | Listed price (MYR) |
| `condition` | New / Used |
| `manufactured_date` | Year of manufacture |
| `location` | State + area (e.g. "Johor - Skudai") |
| `region` | State only |
| `subregion` | Area only |
| `seller_name` | Dealer or seller name |
| `company_ad` | `1` = dealer listing, blank = private |
| `published` | Listing date/time, parsed to full datetime (`YYYY-MM-DD HH:MM`) |
| `Tarikh_Kemaskini` | Date the row was scraped (`YYYY-MM-DD`) |

### Cars — additional fields

| Column | Description |
|---|---|
| `make` / `model` | Brand and model (e.g. Toyota / Vios) |
| `variant` / `series` / `family` / `style` | Trim and body classification |
| `mileage` | Odometer reading |
| `transmission` | Auto / Manual |
| `engine_capacity`, `cc`, `kw`, `torque`, `comp_ratio` | Engine specs |
| `engine`, `fuel_type` | Engine code + fuel type |
| `car_type` | Body type (Sedan, SUV, etc.) |
| `seat`, `country_origin` | Seats + country of manufacture |
| `length`, `width`, `height`, `wheelbase`, `kerbwt`, `fueltk` | Dimensions and weights |
| `brake_front`, `brake_rear`, `suspension_front`, `suspension_rear`, `steering` | Chassis specs |
| `tyres_front`, `tyres_rear`, `wheel_rim_front`, `wheel_rim_rear` | Wheels and tyres |

### Motorcycles — additional fields

| Column | Description |
|---|---|
| `motorcycle_make` / `motorcycle_model` | Brand and model (e.g. Yamaha / Y15ZR) |

> Motorcycle listings expose fewer technical specs on Mudah than cars. Fields from the cars table will appear as blanks in motorcycle output.

### API-only fields (`scraper.py`, schema v2+)

These columns are populated by the EagleSearch API. They are absent from `script.py` output.

| Column | Description |
|---|---|
| `old_price` | Original price before discount (deal indicator) — sparse, only on price-cut listings |
| `year_verified` | `1` when Mudah verified manufacture year, `0` otherwise |
| `store_verified` | `verified` / `unverified` for dealer accounts |
| `bundle` | Paid-tier listing identifier (NULL for free posts) |
| `media_count` | Image count on listing |
| `mileage_bucket` | API mileage range (e.g. `50000-59999`); use alongside exact `mileage` from HTML |
| `car_loan_eligible` | `1` if eligible for instant car loan calc (cars only) |
| `car_loan_payment` | Monthly payment estimate in MYR (cars only) |
| `car_loan_tenure` | Loan tenure in years (cars only) |
| `has_car_grant` | `1` if grant-eligible (cars only) |
| `last_detail_fetched_at` | ISO timestamp of last HTML detail fetch (`NULL` when `depth=none`) |
| `detail_fetch_status` | `ok` / `skipped` / `error` for HTML enrichment |

---

## Rate limiting & retries

To stay friendly with mudah.my and avoid 403 blocks, both scrapers pace themselves:

**EagleSearch API (`scraper.py` Phase 1):**
- **0.5–1s** between calls (lighter — JSON endpoint, not Cloudflare-protected)
- Same fixed-schedule retries: **2s → 3s → 5s**

**HTML scraping (`scraper.py` Phase 2 + `script.py`):**
- **3–4 seconds** between requests, globally serialized via a shared lock — increasing `--workers` does NOT increase request rate
- **Listing-page retries:** 2s → 3s → 5s
- **Detail-page retries:** same schedule

The two clients (`eagle_client.py` and `mudah_client.py`) own independent locks so the API throttle never blocks the HTML throttle (or vice versa) when running in the same process.

Logs go to `logs/scraper_hybrid.log` (new) and `logs/scraper.log` (legacy), both UTF-8 to preserve non-ASCII titles.

---

## Master Data

The SQLite databases (`data/master/cardata_cars.db` and `data/master/cardata_motorcycles.db`) are now the primary storage. Use `--update-db` flag in `script.py` to upsert new results — duplicates are automatically removed based on `ads_id`.

Legacy Excel file (`data/master/MasterMudahCarData.xlsx`) can still be migrated into SQLite using `migrate_xlsx_to_db.py` — see below.

The databases are **not tracked in git** due to their size. Store them locally or in shared cloud storage (e.g. Google Drive, S3).

---

## SQLite Storage

The project uses per-category SQLite databases as the source of truth for tracking and re-checks:

- `data/master/cardata_cars.db`
- `data/master/cardata_motorcycles.db`

Each DB has two tables:
- **`listings`** — one row per listing (keyed on `ads_id`), including the availability tracking columns `first_seen_at`, `last_seen_at`, `last_checked_at`, `availability_status`.
- **`availability_checks`** — append-only audit log of every probe.

### Migrating an existing master xlsx into SQLite

`migrate_xlsx_to_db.py` reads the master Excel file, classifies rows into cars vs. motorcycles (by which `make`/`motorcycle_make` column is populated), and inserts them into the appropriate DB. Duplicates (same `ads_id`) are skipped.

```bash
# migrate everything in the master xlsx into both DBs
python src/migrate_xlsx_to_db.py --xlsx ../data/master/MasterMudahCarData.xlsx

# only migrate the motorcycle rows
python src/migrate_xlsx_to_db.py --xlsx ../data/master/MasterMudahCarData.xlsx --category motorcycles
```

> Rows from the legacy master xlsx have no `url` column — they can be inserted but cannot be re-checked until a URL is backfilled (e.g. by re-scraping). Rows from a fresh `src/script.py` run already include `url`.

---

## Reference Data — Makes & Models

`scrape_makes_models.py` pulls Mudah's canonical make + model lists from the cars and motorcycles category landing pages and writes them to `data/reference/` as JSON. Run manually whenever the brand list needs refreshing (Mudah adds new makes roughly once a year).

```bash
python src/scrape_makes_models.py
```

### Output files

| File | Shape | Sample |
|---|---|---|
| `cars_makes.json` | List of `{id, name, slug, founding_country, icon_png, icon_svg}` | 128 entries (Alfa Romeo, Alpine, ...) |
| `cars_models.json` | `{make_slug: [{id, name, slug}, ...]}` | Toyota: 99 models, total 1,533 |
| `motorcycles_makes.json` | List of `{id, name, slug}` (no `founding_country`) | 93 entries (Adiva, Aprilia, ...) |
| `motorcycles_models.json` | `{make_slug: [{id, name, slug}, ...]}` | Yamaha: 109 models, total 1,362 |

### How it works

Two HTTP requests total (one per category). Both lists are embedded in the search-page HTML inside `__NEXT_DATA__`-style filter blobs:

```
"<name>":{"filter":{...inner config...},"values":[...data...]}
```

The script anchors on the filter object (`"make":{`, `"motorcycle_make":{`, `"model":{`, `"motorcycle_model":{`), then bracket-matches the next `"values":[ ... ]` array.

Mudah serves a stripped HTML variant (no filterOptions blob) most of the time. The script retries with a fresh `cloudscraper` session up to 8 times, detecting the full response by presence of a known make name (`"Alfa Romeo"` for cars, `"Adiva"` for motos).

### Suggested uses

- **Validation** — `clean.py` could fuzzy-match scraped `make`/`model` strings against the canonical list to fix typos and casing variants.
- **Coverage planning** — feed the make slugs into a state × brand grid runner to bypass Mudah's 250-page-per-query cap.
- **URL construction** — slugs map directly to Mudah filter URLs (`/cars-for-sale/{make_slug}/{model_slug}`).

---

## Data Cleaning

`clean.py` normalizes raw scraped fields, strips noise, redacts PII, and removes duplicate listings. Cleans a category SQLite DB in place.

```bash
# clean motorcycles DB
python src/clean.py --category motorcycles

# preview without writing
python src/clean.py --category motorcycles --dry-run

# clean cars DB
python src/clean.py --category cars
```

### What it cleans

**Numeric normalization**
| Field | Transform |
|---|---|
| `price` | `"RM 3,500"` → `"3500"` |
| `mileage` | `"50,000 km"` → `"50000"` |
| `engine_capacity` / `cc` | `"1500cc"` → `"1500"` |
| `manufactured_date` | Extract 4-digit year. `"1995 or older"` → `"1995"` |
| `company_ad` | `'0'`/`'1'` strings normalized to integer |
| `kw`, `torque`, `length`, `width`, `height`, `wheelbase`, `kerbwt`, `fueltk` | Strip non-numeric chars → float |

**Text normalization**
| Field | Transform |
|---|---|
| `make`, `model`, `variant`, `family`, `series`, `motorcycle_make`, `motorcycle_model`, `location`, `condition`, `car_type`, `transmission`, `fuel_type`, `country_origin` | Strip whitespace + title-case |

**Subject cleaning**
- Strip leading/trailing whitespace
- Strip emoji (🌕 ✅ 😊 …)
- Strip trailing dealer sales noise after `-`/`~`/`|` separator: `Full Loan`, `Ready Stock`, `Low Deposit`, `90% Credit`, `Free Delivery`, `Ready`, `KHM <location>`

**Body cleaning**
- Redact Malaysian phone numbers (`01x-xxxxxxx`, `+601x…`) → `[PHONE]`
- Strip emoji
- Strip decorative separator lines (`====`, `****`, `----`, `~~~~` of length 5+)
- Collapse 3+ blank lines → 1
- Null-out effectively-empty body (< 20 chars after cleanup, e.g. `"Pm"`, `"Nego"`)

**Row-level**
- Drop rows missing all of `ads_id`/`make`/`model` (or `motorcycle_make`/`motorcycle_model`)
- Dedup exact `ads_id` collisions, keep last
- Dedup true reposts: same `(subject + price + make)` with different `ads_id`, keep highest `ads_id`
- Flag (warn) any row with `price < 1000` MYR — likely spam/test listings, not auto-deleted

**DB side effects**
- Updates listing rows in place (preserves `first_seen_at`, `last_seen_at`, `last_checked_at`, `availability_status`, `url`)
- Deletes rows removed by dedup/drop logic (cascades to `availability_checks`)
- Stamps `meta.last_cleaned_at` with the run timestamp

### Legacy xlsx mode

Still supported for the old `MasterMudahCarData.xlsx` workflow:

```bash
python src/clean.py --input data/master/MasterMudahCarData.xlsx --output cleaned.xlsx
```

---

## Availability Re-checking

`recheck.py` revisits previously scraped listings to track whether each URL is still live. It does **not** claim listings are "sold" — only that the URL is currently `available` or `unavailable`.

```bash
# normal cadenced run on both DBs (recommended)
python src/recheck.py

# only motorcycles
python src/recheck.py --category motorcycles

# re-check every row regardless of policy, capped to 50 per category
python src/recheck.py --all --limit 50

# preview what would be checked, no HTTP requests
python src/recheck.py --dry-run
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--category` | `both` | Which DB to re-check: `cars`, `motorcycles`, or `both` (sequential through one shared client) |
| `--limit` | — | Max rows to probe this run (per category) |
| `--all` | off | Skip cadence policy, probe every row that has a URL |
| `--dry-run` | off | Compute the due set and log it; make no HTTP requests |

### Cadence policy

| Listing age (since `first_seen_at`) | Re-check interval |
|---|---|
| 0–7 days | ~daily (≥ 20 hours since last check) |
| 7–30 days | every 3 days |
| 30+ days | weekly |
| `unavailable` for 14+ days | give up — stop re-checking |

### `listings` availability columns

| Column | Meaning |
|---|---|
| `first_seen_at` | First time the listing was probed (or scraped, if pre-populated) |
| `last_seen_at` | Last time the listing was confirmed `available` |
| `last_checked_at` | Last time we probed (regardless of outcome) |
| `availability_status` | `available` \| `unavailable` \| `unknown` |

### `availability_checks` audit log

Every probe appends one row to the `availability_checks` table in the same DB:

| Column | Description |
|---|---|
| `id` | Auto-increment primary key |
| `ads_id` | Listing ID (FK → `listings.ads_id`) |
| `checked_at` | Timestamp |
| `http_status` | HTTP response code (NULL on connection error/timeout) |
| `detected_status` | `available` \| `soft_404` \| `removed` \| `blocked` \| `transient` |

### Detection rules

- **`available`** — HTTP 200 *and* the ad's `adDetails` block is present in `__NEXT_DATA__`
- **`soft_404`** — HTTP 200 but Mudah serves a "listing not found" page (no ad block)
- **`removed`** — HTTP 404 or 410
- **`blocked`** — HTTP 403 (Cloudflare bot block) — treated as **transient**, the previous `availability_status` is preserved (we don't lie about ads being unavailable just because we got blocked)
- **`transient`** — 5xx, timeout, connection error — also preserves previous status

`recheck.py` shares the same global 3–4s throttle as the scraper through `mudah_client.py`, so running both concurrently won't blow the request budget. Logs go to `logs/recheck.log`.

---

## Changelog

This section tracks every revision of this README. Add a new entry at the top whenever the document is updated.

### 2026-05-02
- **Hybrid scraper landed.** New `src/scraper.py` orchestrates a two-phase pipeline: Phase 1 pulls 200 ads/request from the discovered EagleSearch JSON API (`https://search.mudah.my/v1/search`, ~500ms for 200 ads) and Phase 2 fetches `.htm` pages only for ads needing deep fields (body, exact mileage, chassis specs). New `--depth {none|missing|all}` flag controls Phase 2 scope. API-only mode is ~1500× faster per listing than `script.py`. Also added `src/eagle_client.py` (API wrapper with own 0.5–1s throttle) and `src/detail_client.py` (HTML parser carved from `script.py`). `script.py` retained as fallback when EagleSearch is unavailable.
- **Schema migration v1 → v2.** New `migrations/run_migrations.py` adds API-only columns to both DBs: `old_price`, `year_verified`, `store_verified`, `bundle`, `media_count`, `mileage_bucket`, `last_detail_fetched_at`, `detail_fetch_status` (shared); `car_loan_eligible`, `car_loan_payment`, `car_loan_tenure`, `has_car_grant` (cars only). Idempotent — re-running is a no-op. Documented under **API-only fields** in Output Columns.
- **Test coverage:** 109 new tests across `test_scraper.py` (24), `test_eagle_client.py` (40), `test_detail_client.py` (33), `test_migrations.py` (12). Plus `tests/compare_outputs.py` integration diff that runs both scrapers on the same ad URLs and asserts shared-field parity (`subject`, `body`, `make`, `model`). Verified 0 mismatches on a 5-ad sample (only difference is `make` casing — `Toyota` from API vs `TOYOTA` from HTML — already handled by `clean.py`).
- Updated **Project Structure** to list new files. Updated **Data Pipeline** with hybrid two-phase flow diagram and performance comparison table. Split **Usage** into "Hybrid scraper" (recommended) and "HTML-only scraper" (fallback) sections. Updated **Rate limiting & retries** to document independent throttles for the API client (0.5–1s) and HTML client (3–4s).

### 2026-05-01
- Added **Data Pipeline** section documenting source URLs (search results, listing detail, category landing), what each page yields, URL filter patterns (state/brand), and an ASCII flow diagram showing script.py → CSV → SQLite → clean.py/recheck.py plus the separate reference-data flow.

### 2026-04-30
- Added `src/scrape_makes_models.py` and the **Reference Data — Makes & Models** section. Two-request scraper pulls Mudah's canonical make + model lists for cars (128 makes / 1,533 models) and motorcycles (93 makes / 1,362 models) from the category landing pages and writes them to `data/reference/{cars,motorcycles}_{makes,models}.json`. Mudah serves a stripped HTML variant most of the time, so the script retries with a fresh `cloudscraper` session up to 8 times, detecting the full response by presence of a known make name. Filter-object anchors plus a bracket-matching JSON array extractor avoid relying on full `__NEXT_DATA__` parsing (Next.js 13 RSC streaming would break that anyway).

### 2026-04-29
- Added the **Data Cleaning** section. `clean.py` rewritten to clean SQLite DBs in place via `--category {cars,motorcycles}`. Adds: `manufactured_date` `"1995 or older"` mapping, `company_ad` int normalization, `subject` emoji + trailing sales-noise stripping, `body` phone-number redaction + emoji + separator-line + blank-line collapse + short-body NULLing, true-repost deduplication on `(subject + price + make)`, price-outlier flagging at < 1000 MYR. Stamps `meta.last_cleaned_at`. Legacy xlsx mode preserved.
- Fixed scraper returning only ~12 listings per page instead of the real 40. `fake_useragent.UserAgent.random` was returning mobile UAs (e.g. iPhone) which made Mudah serve the mobile listing variant. Constrained to desktop UAs via `UserAgent(platforms=['pc'])` and bumped `expected_urls_per_page` default from 20 → 40 in `scrape_cars`.

### 2026-04-27
- Removed `--update-db` flag from Usage docs — flag was dropped in the CSV-first workflow switch (`5ed2b29`); correct workflow is scrape → `migrate_xlsx_to_db.py`.
- Fixed `--output-dir` default value (`../data/raw` → `data/raw/<category>/`) and example path.

### 2026-04-26
- **Migrated to per-category SQLite storage** (`data/master/cardata_cars.db`, `data/master/cardata_motorcycles.db`). Added the **SQLite Storage** section and rewrote **Availability Re-checking** to match: `recheck.py` now reads/writes a `listings` table and appends to an `availability_checks` audit table; replaced `--master` with `--category {cars,motorcycles,both}`. Documented `migrate_xlsx_to_db.py` for one-time backfill from the legacy master xlsx.
- Added the **Availability Re-checking** section documenting `recheck.py`, its CLI flags, cadence policy, new master columns (`first_seen_at`, `last_seen_at`, `last_checked_at`, `availability_status`), the `data/master/availability_log.csv` audit log, and the response-classification rules (incl. 403/timeouts treated as transient).
- Documented the new `body` column (full seller description from `attributes.body`, with `<br>` collapsed to newlines).

### 2026-04-25
- Added the **Changelog** section.
- Documented the new `url` column in the common-fields output table.
- Added the **Rate limiting & retries** section (3–4s global throttle, 2s/3s/5s retry schedule, UTF-8 log location).
- Updated the interactive-prompt description to reflect numbered category/state menus.
- Updated default flag values: `--workers 2`, `--output-dir data/raw`, `--master data/master/MasterMudahCarData.xlsx`.
- Updated master-file path reference to `data/master/MasterMudahCarData.xlsx` to match the new project layout.
