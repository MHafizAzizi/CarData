# CarData

Car listing data scraped from [Mudah.my](https://www.mudah.my), Malaysia's largest classifieds site. The master dataset is updated regularly as listings change frequently.

> **Note:** Mudah.my listings expire after ~66 days. A **weekly scrape + daily recheck** is the recommended cadence — see Quick Start below.

---

## Quick Start — what to run and when

### Weekly scrape (refresh listings + ad_expiry)

```bash
# 1. Scrape new listings → data/raw/<category>/
python src/1_scrape.py --category motorcycles

# 2. Load CSVs into SQLite
python src/2_migrate.py --category motorcycles

# 3. Normalize the DB (strip noise, dedup)
python src/3_clean.py --category motorcycles
```

Swap `--category motorcycles` → `--category cars` for the cars DB.

### Daily recheck (detect sold listings)

```bash
# Probe existing listings; sets availability_status + sold_inference
python src/recheck.py --category motorcycles
```

### One-time / occasional

| Script | When to run |
|---|---|
| `migrations/run_migrations.py --category both` | Once after cloning or wiping a DB (brings schema to current version) |
| `src/scrape_makes_models.py` | Once to seed `data/reference/`; re-run when Mudah adds new brands (~annually) |
| `src/scrape_makes_models.py --variants --all-makes` | Once to bootstrap `cars_variants.json`; re-run after a major re-scrape |
| `src/3_clean.py --category cars --enrich-variants` | After running `--variants` above, to fill NULL variant rows in the DB |
| `src/backfill_ad_expiry.py --category motorcycles` | One-off to backfill `ad_expiry` on rows scraped before v5 schema |

### Script reference at a glance

| Script | Role | Run it? |
|---|---|---|
| `src/1_scrape.py` | **Primary scraper** — EagleSearch API (Phase 1 only) | Yes — weekly |
| `src/2_migrate.py` | Load scraped CSVs into SQLite | Yes — after each scrape |
| `src/3_clean.py` | Normalize / dedup the DB | Yes — after each migrate |
| `src/recheck.py` | Re-check availability + infer sold/expired | Yes — daily |
| `src/backfill_ad_expiry.py` | One-off backfill of `ad_expiry` for pre-v5 rows | Once after upgrade |
| `src/dashboard_aggregate.py` | Generate `mockups/dashboard.html` from DB | On demand |
| `src/scrape_makes_models.py` | Refresh make/model reference lists | Occasionally |
| `src/eagle_client.py` | EagleSearch API wrapper | Library — do not run directly |
| `src/mudah_client.py` | Shared HTTP client (used by `recheck.py`) | Library — do not run directly |
| `src/db.py` | Database connection helper | Library — do not run directly |
| `src/test_eaglesearch.py` | Manual API connectivity check | Dev/debug only |

---

## Data Pipeline

Single collection path: the **EagleSearch API scraper** (`1_scrape.py`) is the sole collector.

### Sources

| Page / endpoint | URL pattern | Used by |
|---|---|---|
| EagleSearch JSON API | `https://search.mudah.my/v1/search?category={1020\|1040}` | `1_scrape.py`, `recheck.py` (availability check) |
| Individual listing detail | `https://www.mudah.my/{ads_id}.htm` | `recheck.py` |
| Category landing (filters) | `https://www.mudah.my/malaysia/cars-for-sale?o=1` | `scrape_makes_models.py` |

EagleSearch is Mudah's internal JSON API (discovered via JS bundle inspection). Returns 200 ads/request with rich metadata that the HTML pages don't expose (`old_price`, `year_verified`, `bundle`, `car_loan_*`, `ad_expiry`, etc.).

### What is extracted

**Primary scraper (`1_scrape.py`)** — paginates EagleSearch (200 ads/request). Returns normalized records with: `ads_id`, `subject`, `price`, `make`, `model`, `mileage_bucket`, `region`, `subarea`, `condition`, `ad_expiry`, plus API-only fields (`old_price`, `year_verified`, `store_verified`, `bundle`, `media_count`, `car_loan_eligible`, `car_loan_payment`, `car_loan_tenure`, `has_car_grant`). API-only mode — no HTML detail-page fetching.

**Reference data (`scrape_makes_models.py`)** — pulls canonical make/model lists from category landing pages.

### Flow

```
EagleSearch API (search.mudah.my)
        │  200 ads/req, <500ms
        │
        ▼
   1_scrape.py
        │
        ▼
data/raw/<category>/*.csv
        │
        ▼
   2_migrate.py
        │
        ▼
data/master/cardata_*.db
        │
    ┌───┴───────────────┐
    ▼                   ▼
 3_clean.py         recheck.py
(normalize data)  (track availability
                   + infer sold/expired)
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
| `1_scrape.py` (API only) | 50 | ~0.1s | ~2ms | 1 |

`1_scrape.py` collects 200 ads/request and captures 7+ API-only columns the HTML pages never exposed.

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
│   ├── 1_scrape.py                        # Primary scraper (EagleSearch API)
│   ├── 2_migrate.py                       # CSV → SQLite migration
│   ├── 3_clean.py                         # Data normalization + dedup
│   ├── recheck.py                         # Availability re-checker + sold inference
│   ├── backfill_ad_expiry.py              # One-off: backfill ad_expiry for pre-v5 rows
│   ├── dashboard_aggregate.py             # Generate mockups/dashboard.html from DB
│   ├── scrape_makes_models.py             # Reference make/model list scraper (occasional)
│   ├── eagle_client.py                    # EagleSearch API wrapper (library)
│   ├── mudah_client.py                    # Shared HTTP client — used by recheck.py (library)
│   ├── db.py                              # Database connection helper (library)
│   └── test_eaglesearch.py                # Manual API connectivity check (dev)
│
├── migrations/                            # Schema migration runners
│   └── run_migrations.py                  # v1 → v5: idempotent schema upgrade
│
├── docs/                                  # Documentation
│   └── brands.md                          # List of available car/motorcycle brands
│
├── data/
│   ├── raw/                               # Raw CSV outputs from scrapers
│   ├── reference/                         # Canonical make/model lists from Mudah
│   │   ├── cars_makes.json
│   │   ├── cars_models.json
│   │   ├── cars_variants.json             # Variant/trim vocabulary (985 models)
│   │   ├── motorcycles_makes.json
│   │   └── motorcycles_models.json
│   └── master/                            # Production SQLite databases
│       ├── cardata_cars.db
│       └── cardata_motorcycles.db
│
├── logs/                                  # Log files
│   ├── scraper_hybrid.log                 # 1_scrape.py logs
│   ├── recheck.log                        # recheck.py logs
│   └── migrate.log                        # 2_migrate.py logs
│
├── tests/                                 # Unit + integration tests
│   ├── test_scraper.py                    # HybridScraper tests
│   ├── test_eagle_client.py               # API wrapper tests
│   ├── test_migrations.py                 # schema migration tests
│   └── test_clean.py
├── schema_cars.sql                        # SQLite schema for cars (v5)
├── schema_motorcycles.sql                 # SQLite schema for motorcycles (v5)
├── requirements.txt
├── README.md
└── .gitignore
```

---

## Usage

### Primary scraper — `1_scrape.py`

```bash
python src/1_scrape.py                              # interactive
python src/1_scrape.py --category cars              # all cars listings
python src/1_scrape.py --category motorcycles       # all motorcycle listings
python src/1_scrape.py --category cars --max-ads 1000   # cap at 1000 ads
```

Flags:

| Flag | Default | Description |
|---|---|---|
| `--category` | *prompted* | `cars` or `motorcycles` |
| `--make` | *prompted* | Make slug(s), comma-separated (`toyota` or `toyota,honda`). Prompts with a numbered picker if omitted |
| `--model` | — | Model slug (e.g. `vios`). Requires `--make` |
| `--max-ads` | *prompted* | Max ads to collect (API pagination cap) |
| `--all-makes` | off | Scrape every reference make non-interactively (used by `run_pipeline.bat`) |
| `--smart` | off | **Depth-cap evasion** — probe each make and split any make over ~9,500 listings into per-model queries (see below) |
| `--list-active-makes` | off | Probe every make, print those with >0 listings sorted by count (discovery only, no scrape) |
| `--output-dir` | `data/raw/<category>/` | Where to save CSVs |

CSV filename: `mudah_eagle_{category}_final_{timestamp}.csv` (per-make runs add a `{make}` tag; `--smart` model splits add a `{make}_{model}` tag).

### Beating the 10,000-listing depth cap — `--smart`

A single EagleSearch query paginates to **at most ~10,000 ads** (`from >= 10000` returns empty). Any make with more inventory loses the overflow — e.g. **Toyota cars ≈ 18,800 listings**, so a plain make-level scrape captures only ~53%.

`--smart` fixes this: it probes each make's `total-results` (one cheap `limit=1` call) and, for any make over ~9,500, scrapes **one query per model** instead. Each model is well under the cap, so the make's full inventory is captured. Per-model totals sum exactly to the make total (Mudah forces sellers to pick a known model), so the split is lossless. Makes under the cap stay a single query.

```bash
# full, complete scrape of every make (auto-splits the big ones)
python src/1_scrape.py --category cars --all-makes --smart
```

`run_pipeline.bat` already passes `--smart`. Cost is one extra probe call per make; net pages fetched are roughly unchanged (you fetch the same ads, just partitioned).

### Examples

**Scrape a single make:**
```bash
python src/1_scrape.py --category cars --make toyota
```

**Scrape several makes (one CSV per make):**
```bash
python src/1_scrape.py --category cars --make toyota,honda,perodua
```

**Scrape all makes (full coverage past the 10k cap), then load into database:**
```bash
python src/1_scrape.py --category cars --all-makes --smart
python src/2_migrate.py --category cars
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

### API-only fields (`1_scrape.py`, schema v2+)

These columns are populated by the EagleSearch API.

| Column | Description |
|---|---|
| `old_price` | Original price before discount (deal indicator) — sparse, only on price-cut listings |
| `year_verified` | `1` when Mudah verified manufacture year, `0` otherwise |
| `store_verified` | `verified` / `unverified` for dealer accounts |
| `bundle` | Paid-tier listing identifier (NULL for free posts) |
| `media_count` | Image count on listing |
| `mileage_bucket` | API mileage range (e.g. `50000-59999`) |
| `car_loan_eligible` | `1` if eligible for instant car loan calc (cars only) |
| `car_loan_payment` | Monthly payment estimate in MYR (cars only) |
| `car_loan_tenure` | Loan tenure in years (cars only) |
| `has_car_grant` | `1` if grant-eligible (cars only) |
| `last_detail_fetched_at` | Legacy (Phase 2 removed) — NULL for all rows going forward |
| `detail_fetch_status` | Legacy (Phase 2 removed) — NULL for all rows going forward |

### Availability / sold tracking fields (schema v5+)

These columns are written by `recheck.py` and `backfill_ad_expiry.py`.

| Column | Description |
|---|---|
| `ad_expiry` | ISO datetime when the ad's paid slot expires, as returned by the API. Populated by `1_scrape.py` (v5+) and `backfill_ad_expiry.py` for older rows. NULL means the listing disappeared before it could be captured. |
| `sold_inference` | Set by `recheck.py` when a listing goes offline: `likely_sold` (disappeared before `ad_expiry`), `likely_expired` (disappeared at/after `ad_expiry`), `unknown` (no `ad_expiry` to compare against) |
| `availability_status` | `available` \| `unavailable` \| `unknown` — current HTTP-probe result |
| `first_seen_at` | Timestamp when the listing was first scraped |
| `last_seen_at` | Timestamp of last confirmed `available` probe |
| `last_checked_at` | Timestamp of last probe attempt (regardless of result) |

---

## Rate limiting & retries

To stay friendly with mudah.my and avoid 403 blocks, the clients pace themselves:

**EagleSearch API (`1_scrape.py`):**
- **0.5–1s** between calls (lighter — JSON endpoint, not Cloudflare-protected)
- Fixed-schedule retries: **2s → 3s → 5s**

**HTML probes (`recheck.py`):**
- **3–4 seconds** between requests, globally serialized via a shared lock
- **Retries:** 2s → 3s → 5s

The two clients (`eagle_client.py` and `mudah_client.py`) own independent locks so the API throttle never blocks the HTML throttle (or vice versa) when running in the same process.

**429 Too Many Requests** is handled separately from the fixed retry schedule. If the server sends `Retry-After`, the client sleeps for that many seconds (or until that HTTP-date), capped at 5 minutes. Without the header, the client falls back to a 60s back-off. This applies to both `eagle_client.py` and `mudah_client.py`.

Logs go to `logs/scraper_hybrid.log`, UTF-8 to preserve non-ASCII titles.

---

## Master Data

The SQLite databases (`data/master/cardata_cars.db` and `data/master/cardata_motorcycles.db`) are the primary storage. Scraped CSVs are loaded via `2_migrate.py` (upsert mode) — duplicates are automatically removed based on `ads_id`.

Legacy Excel file (`data/master/MasterMudahCarData.xlsx`) can still be migrated into SQLite using `2_migrate.py` — see below.

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

`2_migrate.py` reads the master Excel file, classifies rows into cars vs. motorcycles (by which `make`/`motorcycle_make` column is populated), and inserts them into the appropriate DB. Duplicates (same `ads_id`) are skipped.

```bash
# migrate everything in the master xlsx into both DBs
python src/2_migrate.py --xlsx ../data/master/MasterMudahCarData.xlsx

# only migrate the motorcycle rows
python src/2_migrate.py --xlsx ../data/master/MasterMudahCarData.xlsx --category motorcycles
```

> Rows from the legacy master xlsx have no `url` column — they can be inserted but cannot be re-checked until a URL is backfilled (e.g. by re-scraping). Rows from a fresh `src/1_scrape.py` run already include `url`.

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
| `cars_models.json` | `{make_slug: [{id, name, slug}, ...]}` | Toyota: 99 models, total 1,540 |
| `motorcycles_makes.json` | List of `{id, name, slug}` (no `founding_country`) | 96 entries (Adiva, Aprilia, ...) |
| `motorcycles_models.json` | `{make_slug: [{id, name, slug}, ...]}` | total 1,434 |
| `cars_variants.json` | `{model_slug: {make, model, by_cc: {cc: [tokens]}, _all: [tokens]}}` | 985 models, 9,723 variant tokens |

### How it works

Two HTTP requests total (one per category). Both lists are embedded in the search-page HTML inside `__NEXT_DATA__`-style filter blobs:

```
"<name>":{"filter":{...inner config...},"values":[...data...]}
```

The script anchors on the filter object (`"make":{`, `"motorcycle_make":{`, `"model":{`, `"motorcycle_model":{`), then bracket-matches the next `"values":[ ... ]` array.

Mudah serves a stripped HTML variant (no filterOptions blob) most of the time. The script retries with a fresh `cloudscraper` session up to 8 times, detecting the full response by presence of a known make name (`"Alfa Romeo"` for cars, `"Adiva"` for motos).

### Variant vocabulary — `--variants` mode

`scrape_makes_models.py --variants` builds a trim/variant vocabulary per car model by sampling listing subjects from the EagleSearch API (filtered by `model_id`). The API path avoids Cloudflare protection that blocks model-specific HTML pages.

```bash
# Bootstrap vocabulary for all 128 makes (~30 min, resumable)
python src/scrape_makes_models.py --variants --all-makes

# Limit to specific makes for a faster refresh
python src/scrape_makes_models.py --variants --makes toyota honda perodua proton
```

Extraction logic: strips year, make, model name, engine size, transmission, and seller noise from each listing subject — the residual is the variant token (e.g. `"2020 Toyota ALPHARD 2.5 SC (A)"` → `"SC"`). Tokens are grouped by `engine_capacity` and deduplicated by frequency, keeping the top 15 per cc bucket.

Once `cars_variants.json` exists, apply it to NULL variant rows in the DB:

```bash
python src/3_clean.py --category cars --enrich-variants
```

This fills ~83% of NULL variant rows using longest-match against the vocabulary. The remaining ~17% are either pure seller-noise listings (no variant token extractable) or rare variants not seen in the 50-listing sample.

### Suggested uses

- **Validation** — `3_clean.py` could fuzzy-match scraped `make`/`model` strings against the canonical list to fix typos and casing variants.
- **Coverage planning** — the model lists power `1_scrape.py --smart`, which splits over-cap makes into per-model queries to bypass Mudah's ~10,000-listing (250-page) depth cap. A single model still over the cap would need a further axis (year/region).
- **URL construction** — slugs map directly to Mudah filter URLs (`/cars-for-sale/{make_slug}/{model_slug}`).

---

## Data Cleaning

`3_clean.py` normalizes raw scraped fields, strips noise, redacts PII, and removes duplicate listings. Cleans a category SQLite DB in place.

```bash
# clean motorcycles DB
python src/3_clean.py --category motorcycles

# preview without writing
python src/3_clean.py --category motorcycles --dry-run

# clean cars DB
python src/3_clean.py --category cars

# clean + fill NULL variant rows from cars_variants.json vocabulary
python src/3_clean.py --category cars --enrich-variants
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
python src/3_clean.py --input data/master/MasterMudahCarData.xlsx --output cleaned.xlsx
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
| `ad_expiry` | ISO datetime the paid listing slot expires (from EagleSearch API). Used to distinguish sold vs expired. |
| `sold_inference` | Set when a listing goes offline: `likely_sold` = vanished before `ad_expiry`; `likely_expired` = vanished at/after `ad_expiry`; `unknown` = `ad_expiry` was NULL |

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

### 2026-06-13
- **`--smart` depth-cap evasion.** New `1_scrape.py --smart` flag probes each make's `total-results` and splits any make over ~9,500 listings into per-model queries, capturing the full inventory past the ~10,000-listing EagleSearch depth cap (e.g. Toyota cars ≈ 18,800 — a plain make-level scrape captured only ~53%). The split is lossless (per-model totals sum to the make total). `run_pipeline.bat` scrape step now passes `--all-makes --smart`. Added a "Beating the 10,000-listing depth cap" subsection, expanded the scraper flags table (`--make`, `--model`, `--all-makes`, `--smart`, `--list-active-makes`), and updated the all-makes example. 6 new tests (`TestExpandCappedMakes`), 218 passing.
- **Reference data refreshed.** Re-ran `scrape_makes_models.py`: cars 1,533 → 1,540 models; motorcycles 93 → 96 makes (+cyclone, peugeot, zxmoto) and 1,362 → 1,434 models. Updated the reference output-table counts.

### 2026-05-28
- **Schema v5: `ad_expiry` + `sold_inference`.** Added two columns to both DBs via `migrations/run_migrations.py` (now at `TARGET_VERSION=5`). `ad_expiry TEXT` stores the ISO datetime when a listing's paid slot expires (returned by EagleSearch as `ad_expiry`). `sold_inference TEXT` is written by `recheck.py` when a listing goes offline: `likely_sold` if it vanished before `ad_expiry`, `likely_expired` if it vanished at/after, or `unknown` if `ad_expiry` was NULL. Added `ad_expiry` field capture to `eagle_client.py` so all future scrapes populate it automatically.
- **`backfill_ad_expiry.py` (new script).** One-off tool to retroactively populate `ad_expiry` for the 21,712 existing motorcycle rows scraped before v5. Queries EagleSearch make-by-make (since the all-category endpoint is capped at 10,000 of ~25,000+ listings), collects `{ads_id: ad_expiry}` pairs, and batch-patches the DB. Run against the live motorcycles DB: 21,051 / 21,712 rows (97%) updated in ~3 min 16 s; the remaining 661 NULL rows had already sold or expired overnight.
- **Phase 2 removed from primary workflow.** HTML detail-page enrichment (Phase 2) was profiled: only 0.9% of rows gain any new field, and `mileage` from Phase 2 is the same bucket range as `mileage_bucket` reformatted — no precision gain. `scraper.py` now runs API-only (`--depth none` equivalent) by default. `detail_client.py` is retained as a legacy library. Updated Quick Start, flow diagram, scraper flags table, and performance comparison accordingly.
- **Cadence updated.** Recommended cadence is now weekly scrape + daily recheck (Mudah listings expire after ~66 days, average lifespan ~66 days measured across the motorcycles DB).
- **Test coverage.** Updated `test_eagle_client.py` to assert `ad_expiry` is captured. Updated `test_migrations.py` to cover v4→v5 migration (`TestV5AddExpiry` class with 3 tests) and to verify schema reaches v5 in all paths.
- **README.** Consolidated Quick Start into weekly/daily/one-time sections. Removed stale Phase 2 flags. Added `ad_expiry` / `sold_inference` to Output Columns and Availability Re-checking tables.

### 2026-05-27
- **Variant vocabulary system.** `scrape_makes_models.py` gained `--variants` / `--all-makes` flags. Samples listing subjects via EagleSearch API (filtered by `model_id`) instead of model-specific HTML pages — the latter are blocked by Cloudflare. Ran `--all-makes` across all 128 makes: `data/reference/cars_variants.json` now contains 985 models and 9,723 variant tokens. `clean.py` gained `--enrich-variants`: applies the vocabulary to NULL variant rows using longest-match, filling ~83% of candidates.
- **`car_type` fix.** `eagle_client.py` was silently dropping the `car_type_name` field from every API response. Added `car_type_name` → `car_type` to `_RENAME_CARS`; all future scrapes populate `car_type`.
- **Phase 2 profiled and deprioritised.** Only 0.9% of fresh rows gain any new field from Phase 2 HTML enrichment. Phase 2 `mileage` is the same bucket range as `mileage_bucket` reformatted as text — no precision gain. Chassis specs are model-level constants. Recommendation: use `--depth none` for all regular scraping.
- **DB cleaned.** `clean.py --category cars --enrich-variants` removed 13,527 repost duplicates; DB trimmed from 23,776 → 10,249 rows. 805 NULL variant rows filled.
- Added `cars_variants.json` to project structure and reference data output table. Documented `--variants` mode and `--enrich-variants` flag.

### 2026-05-26
- **Scraping smoothness pass.** Six resilience improvements to `scraper.py`, `eagle_client.py`, `mudah_client.py`:
  - **`--resume` flag** auto-detects the latest `phase2_partial_*.csv` (or `phase1_*.csv`) in the output dir and continues from there; `depth=missing` skips already-enriched ads naturally.
  - **429 / Retry-After awareness** in both HTTP clients — honors the header (capped at 5 min) or falls back to 60s, independent of the normal 2s/3s/5s retry schedule.
  - **End-of-Phase-2 retry pass** re-attempts ads marked `detail_fetch_status='error'` once before final write — recovers most transient blips.
  - **Incremental Phase 1 checkpoint** — `phase1_*.csv` is rewritten per page during Phase 1 so a mid-Phase-1 crash still leaves usable data on disk.
  - **Cleaned up partial checkpoints** — `phase2_partial_*.csv` files are now removed after the final CSV is written (previously they leaked).
  - **Added `Referer` header** to `MudahClient` and synced `Sec-Fetch-Site=same-origin` for parity with `EagleClient`.
- **Test coverage:** +24 tests across `test_scraper.py` (15) and `test_eagle_client.py` (9). Total: 184 passing.

### 2026-05-02
- **Hybrid scraper landed.** New `src/1_scrape.py` orchestrates a two-phase pipeline: Phase 1 pulls 200 ads/request from the discovered EagleSearch JSON API (`https://search.mudah.my/v1/search`, ~500ms for 200 ads) and Phase 2 fetches `.htm` pages only for ads needing deep fields (body, exact mileage, chassis specs). New `--depth {none|missing|all}` flag controls Phase 2 scope. API-only mode is ~1500× faster per listing than `script.py`. Also added `src/eagle_client.py` (API wrapper with own 0.5–1s throttle) and `src/detail_client.py` (HTML parser carved from `script.py`). `script.py` retained as fallback when EagleSearch is unavailable.
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
