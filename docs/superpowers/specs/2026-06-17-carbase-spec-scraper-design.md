# carbase.my Spec Scraper — Phase 1 Design

> Status: approved 2026-06-17. Phase 1 of 2. Phase 2 (matching `model_specs` →
> Mudah `listings`) is a separate spec, deferred.

## Goal

Build a standalone scraper that crawls the full carbase.my catalog (paultan.org's
Malaysian car buyer's guide) and stores per-variant car specifications in a
dedicated reference database. This gives the project an OEM-grade, Malaysia-local
spec source — covering Proton/Perodua, which external spec APIs (e.g. CarQuery)
handle poorly — to later enrich the Mudah listings with engine/power/torque/
dimensions data for price modeling.

Phase 1 delivers the spec catalog as a self-contained, queryable artifact. It does
**not** join specs to listings — that fuzzy matching problem is Phase 2.

## Recon basis (verified 2026-06-17, see `src/probe_carbase.py`)

- **No Cloudflare block.** cloudscraper returns HTTP 200 on every request; 0× 403,
  0× 429 across the probe. ~0.5s/page fresh.
- **Server-rendered HTML** (no `__NEXT_DATA__`). Specs live in label/value HTML
  tables. A `ld+json @type:Vehicle` blob is present but **buggy/incomplete**
  (`driveWheelConfiguration` carries the wheelbase value; transmission empty; no
  power/torque numbers) — so it is **not** used for spec extraction.
- **URL structure:** `/<make>/<model>/<generation>/<variant-year>`, e.g.
  `/honda/civic/fe-facelift/1.5-rs-turbo-2025`. Real variant pages end in a
  `-YYYY` year suffix; the same depth also hosts noise pages
  (`user-review`, `exterior`, `interior`, `owner-reviews`, `generations`) which
  the year anchor excludes.
- **robots.txt** disallows only `/cars-for-sale/` (their classifieds). Spec pages
  are crawlable.
- **Catalog size:** 73 brands; ~3.8 variants/model observed. Full catalog is on the
  order of thousands of variant pages; est. ~1–2 hrs at polite pacing.

## Decisions (locked with user)

1. **Field scope:** curated ~20 queryable columns **+ a `raw_specs` JSON blob**
   holding every scraped label/value pair (mine later without re-scraping).
2. **Crawl scope:** full carbase catalog → standalone reference (decoupled from
   listings), not just DB-present combos.
3. **No-match handling:** N/A in Phase 1 (no matching). Belongs to Phase 2.
4. **Brand list:** scraped live from `/car-brands-malaysia` each run (stays fresh),
   not hardcoded.
5. **DB tracking:** `carbase_specs.db` is gitignored (rebuildable from the scraper),
   consistent with `cardata_*.db`.

## Storage

New file **`data/reference/carbase_specs.db`** (separate from the wipe-prone
`cardata_cars.db`; survives listings re-scrapes). One table:

```
model_specs
  -- natural-identity columns
  make            TEXT     -- carbase make slug, e.g. "honda"
  model           TEXT     -- carbase model slug, e.g. "civic"
  generation      TEXT     -- e.g. "fe-facelift"
  variant         TEXT     -- variant slug w/o year, e.g. "1.5-rs-turbo"
  year            INTEGER  -- model year from the -YYYY suffix

  -- curated queryable specs (~20)
  body_type       TEXT
  fuel_type       TEXT
  engine_cc       INTEGER
  engine_tech     TEXT
  power_hp        INTEGER
  torque_nm       INTEGER
  transmission    TEXT
  driveline       TEXT
  accel_0_100     REAL     -- seconds
  top_speed_kmh   INTEGER
  fuel_economy_l100 REAL   -- L/100 km
  length_mm       INTEGER
  width_mm        INTEGER
  height_mm       INTEGER
  wheelbase_mm    INTEGER
  kerb_weight_kg  INTEGER
  boot_l          INTEGER
  seats           INTEGER
  doors           INTEGER
  price_peninsular INTEGER -- RM, Peninsular Malaysia

  -- everything scraped, for later mining
  raw_specs       TEXT     -- JSON: {"<table label>": "<value>", ...}

  -- provenance
  source_url      TEXT     -- UNIQUE; one variant page = one row (upsert key)
  scraped_at      TEXT     -- ISO timestamp
```

`source_url` is the unique upsert key: re-running the scraper refreshes existing
rows in place rather than duplicating. A `CREATE UNIQUE INDEX` on `source_url`
backs the upsert.

## Components

### `src/carbase_client.py` (new library)
A small HTTP client mirroring `mudah_client.py`: a `cloudscraper` session, a global
throttle (default ~1.5–2.0s between requests — conservative because sustained-load
behavior is the one untested recon risk), fixed-schedule retry backoff, and
`Retry-After`/429 honoring (reuse `parse_retry_after` + `RATE_LIMIT_*` from
`mudah_client.py`, same as `eagle_client.py` does). Exposes `get(path) -> Response`
and `get_status(path) -> (code, text)`.

### `src/scrape_carbase_specs.py` (new, occasional tool)
Not part of the weekly `run_pipeline.bat` loop — an occasional tool like
`scrape_makes_models.py`, run quarterly. Crawl flow:

1. **Brands:** GET `/car-brands-malaysia` → list of make slugs.
2. **Models:** for each make, GET `/<make>` → model links (`^/<make>/[^/]+/?$`).
3. **Variants:** for each model, GET `/<make>/<model>` → variant links
   (`^/<make>/[^/]+/[^/]+/[^/]+-(?:19|20)\d\d/?$`, which excludes noise pages).
4. **Specs:** for each variant link, GET the page → `parse_variant_page(html)` →
   upsert one `model_specs` row.

**Checkpointing / `--resume`:** because a full run is thousands of pages over
~1–2 hrs, the scraper records progress (the set of `source_url`s already written is
itself the checkpoint — on `--resume`/by default, skip variant URLs already in the
DB). A `--refresh` flag forces re-fetch of existing rows.

**CLI:**
- (no args) — full catalog, skipping already-stored variant URLs
- `--makes honda,toyota` — restrict to given make slugs
- `--limit N` — stop after N variant pages (smoke testing)
- `--refresh` — re-fetch and overwrite rows already in the DB

### Parser: `parse_variant_page(html) -> dict`
Lives in `scrape_carbase_specs.py` (or a small `carbase_parse.py` if it grows).
- Iterate `<table>` rows, collect every `label -> value` pair (confirmed page
  structure from recon).
- Map known labels to curated columns; normalize numerics:
  `"1,498 cc" -> 1498`, `"240 Nm" -> 240`, `"11.0 seconds" -> 11.0`,
  `"1,108 kg" -> 1108`, `"RM 89,900.00" -> 89900`.
- `raw_specs` = JSON of all collected label/value pairs (superset of the curated
  columns).
- Pull `make/model/generation/variant/year` from the source URL path, not the body.
- Missing/`-` values become `NULL`.

## Data flow

```
/car-brands-malaysia ──> [make slugs]
   for each make:  /<make> ──> [model links]
      for each model: /<make>/<model> ──> [variant links (year-suffixed)]
         for each variant: GET page ──> parse_variant_page ──> upsert model_specs
```

All fetches go through `CarbaseClient` (throttle + retry). The DB is the checkpoint:
already-stored `source_url`s are skipped unless `--refresh`.

## Error handling

- **Non-200 on a brand/model page:** log + skip that subtree, continue (one dead
  model must not abort the catalog). Mirrors the per-make resilience in `1_scrape.py`.
- **Non-200 / parse failure on a variant page:** log + skip that variant, continue.
  Counted in the end-of-run summary.
- **403/429:** handled inside `CarbaseClient` (backoff + retry). If a page still
  fails after retries, it is skipped (not stored) so `--resume` retries it next run.
- **End-of-run summary:** brands/models/variants seen, rows upserted, variants
  skipped (with reason counts).

## Testing (`tests/test_carbase.py`)

Use the already-dumped `data/raw/carbase_probe/variant_sample.html` as a fixture:
- `parse_variant_page` extracts the expected curated fields (engine_cc=1498,
  power/torque, dimensions, kerb weight, seats, etc.) from the fixture.
- Numeric normalization helpers (cc/Nm/kg/seconds/RM-price) round-trip correctly,
  incl. `-` → None and comma stripping.
- Variant-link regex: accepts `.../rs-2023`, rejects `.../user-review`,
  `.../exterior`, `.../generations`.
- Model-link regex: accepts `/honda/civic`, rejects `/honda` and
  `/honda/civic/fe-facelift/...`.
- Upsert idempotency: inserting the same `source_url` twice yields one row, second
  write refreshes fields.

`CarbaseClient` network calls are not unit-tested (live-network, like the other
clients); resilience is exercised manually via the scraper.

## Out of scope (Phase 2, separate spec)

- Matching `model_specs` rows to Mudah `listings` on (make, model, variant, year)
  — fuzzy, error-prone, deserves its own design + tests.
- Denormalizing `power_hp`/`torque_nm`/`kerb_weight_kg` into `listings` as
  enrichment-only columns.
- The carbase NVIC market-value (used-car valuation) tool — endpoints mapped in
  `probe_carbase.py` but gated behind browser-seeded session; needs a separate
  browser-XHR-capture recon.

## File summary

| File | Status | Role |
|---|---|---|
| `src/carbase_client.py` | new | throttled/retrying HTTP client for carbase.my |
| `src/scrape_carbase_specs.py` | new | catalog crawler + variant parser + upsert |
| `data/reference/carbase_specs.db` | new (gitignored) | `model_specs` table |
| `tests/test_carbase.py` | new | parser + regex + normalize + upsert tests |
| `.gitignore` | edit | ignore `carbase_specs.db` |
| `requirements.txt` | (no change) | cloudscraper + bs4 already present |
