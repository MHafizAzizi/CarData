# CarData

Car listing data scraped from [Mudah.my](https://www.mudah.my), Malaysia's largest classifieds site. The master dataset is updated regularly as listings change frequently.

> **Note:** Mudah.my caps browsable listings at ~250 pages. Re-running every 1–2 days is recommended to keep data fresh.

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
│   ├── script.py                          # Main scraper
│   ├── recheck.py                         # Availability re-checker
│   ├── migrate_xlsx_to_db.py              # Excel → SQLite migration
│   ├── db.py                              # Database connection helper
│   ├── clean.py                           # Data cleanup utility
│   └── mudah_client.py                    # Shared HTTP client
│
├── docs/                                  # Documentation
│   └── brands.md                          # List of available car/motorcycle brands
│
├── data/
│   ├── raw/                               # Raw CSV outputs from script.py
│   └── master/                            # Production SQLite databases
│       ├── cardata_cars.db
│       ├── cardata_motorcycles.db
│       └── MasterMudahCarData.xlsx        # Legacy Excel master (optional)
│
├── logs/                                  # Log files
│   ├── scraper.log                        # script.py logs
│   ├── recheck.log                        # recheck.py logs
│   └── migrate.log                        # migrate_xlsx_to_db.py logs
│
├── tests/                                 # Unit tests
├── schema_cars.sql                        # SQLite schema for cars
├── schema_motorcycles.sql                 # SQLite schema for motorcycles
├── requirements.txt
├── README.md
└── .gitignore
```

---

## Usage

### Interactive mode (recommended)

Run the script with no arguments and it will prompt you for inputs:

```bash
python src/script.py
```

You'll be asked for:
1. **Category** — pick by number (`1. cars`, `2. motorcycles`); default `1`
2. **State** — pick by number from a numbered list of `malaysia` plus the 13 states and 3 federal territories; default `1` (`malaysia` = nationwide)
3. **Brand** — type the brand slug, or leave blank for all
4. **Start page** — default `1`
5. **How many pages to scrape**

### Non-interactive mode (for automation)

Pass any of the flags below to skip the corresponding prompt:

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

---

## Rate limiting & retries

To stay friendly with mudah.my and avoid 403 blocks, the scraper paces itself:

- **Per-request throttle:** all outgoing requests are spaced **3–4 seconds** apart globally (a shared lock across workers), so increasing `--workers` does not increase request rate.
- **Listing-page retries:** if a search results page returns fewer URLs than expected, retries wait **2s → 3s → 5s**.
- **Detail-page retries:** failed listing fetches retry on the same **2s → 3s → 5s** schedule before giving up.

Logs are written to `logs/scraper.log` (UTF-8) so non-ASCII characters in titles are preserved.

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
