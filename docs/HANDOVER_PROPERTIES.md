# Handover — Mudah Rental Properties Scraper

> **Audience:** A developer building a Mudah.my scraper for **rental properties**, using the existing CarData repo as a reference implementation.
>
> **Goal of this doc:** Get you productive in a day. Tells you what to copy verbatim, what to rebuild, what landmines to avoid, and what you need to discover on day one.

---

## 1. What this repo actually is

CarData scrapes vehicle listings (cars + motorcycles) from **mudah.my**. It is a complete pipeline:

```
Mudah EagleSearch API → CSV → SQLite → cleaned/dedup'd → availability rechecked
```

The same site (`mudah.my`) hosts **properties for rent** under a sister section:
- `https://www.mudah.my/malaysia/properties-for-rent`

This means **the infrastructure is reusable** — same API endpoint, same auth model, same throttling, same Cloudflare protections, same general HTML structure. **The semantics (fields, filters, schema) are different**.

The goal of your project is to build `RentData` (or fold rentals into this repo as a third category) using the same architecture, only with the property-specific schema and filters swapped in.

---

## 2. The architectural spine — copy this verbatim

These are the patterns that took several iterations to land. Don't redesign them.

### 2.1 The four-stage pipeline

```
1. SCRAPE      → src/scraper.py   → writes CSV per run
2. MIGRATE     → src/migrate_*.py → loads CSVs into SQLite (upsert)
3. CLEAN       → src/clean.py     → normalizes in-place
4. RECHECK     → src/recheck.py   → daily availability probe
```

**Why CSV-first instead of writing directly to the DB:**
- Crash-safe — partial scrapes still produce a usable CSV checkpoint
- Inspectable — you can `head -50` a CSV and immediately see what's wrong
- Decouples scraping from DB writes (different failure modes)
- Easy to replay an old scrape without re-hitting the API

Keep this pattern. Resist the temptation to "just write straight to SQLite."

### 2.2 Per-category SQLite, not one shared DB

```
data/master/cardata_cars.db
data/master/cardata_motorcycles.db
data/master/rentdata_properties.db   ← your new file
```

Why: the schemas diverge enough that one shared table becomes a mess of nullable category-specific columns. Per-category DBs keep queries simple and let each category evolve independently.

### 2.3 The "EagleSearch first, HTML fallback" hierarchy

Mudah has two access paths:

| Path | Endpoint | Speed | Coverage |
|---|---|---|---|
| **EagleSearch JSON API** | `search.mudah.my/v1/search` | ~500ms / 200 ads | Most fields, no Cloudflare |
| HTML search pages | `www.mudah.my/{section}-for-rent?o={page}` | ~3-4s / 40 ads | All fields, Cloudflare-protected |

`scraper.py` is API-only. `script.py` is the HTML fallback. **Always prefer the API.** The HTML scraper exists only because the API could one day be locked down — if that happens, you swap to `script.py`.

### 2.4 The reference-data layer

```
data/reference/
  ├── cars_makes.json          ← list of makes (id, name, slug, …)
  ├── cars_models.json         ← {make_slug: [{id, name, slug}, …]}
  └── cars_variants.json       ← variant/trim vocabulary
```

For rentals, the equivalent is **property types**, **location hierarchy**, and **furnishing levels**:

```
data/reference/
  ├── properties_types.json       ← Apartment, Condominium, Terrace House, …
  ├── properties_subareas.json    ← state → area → subarea hierarchy
  └── properties_amenities.json   ← if Mudah exposes structured amenity tags
```

Build this layer first. It powers the make-by-make scrape pattern you'll need (Section 4.2).

### 2.5 The throttling architecture

`src/eagle_client.py` and `src/mudah_client.py` each own a **separate** `threading.Lock` + last-request timestamp. They throttle independently:

- EagleSearch: 0.5–1s between calls
- HTML pages: 3–4s between calls

Don't merge them. Mixing throttles in one shared lock means slow HTML pages block fast API calls.

### 2.6 The cadence-aware recheck

`recheck.py` doesn't just probe everything. It uses a **decay policy**:

| Age since `first_seen_at` | Re-check interval |
|---|---|
| 0–7 days | daily |
| 7–30 days | every 3 days |
| 30+ days | weekly |
| Unavailable for 14+ days | stop checking |

Copy this for rentals. The exact intervals may need tweaking (rentals turn over differently than used cars) but the **decay shape** is right.

---

## 3. Mudah-specific traps (battle scars)

Every one of these is a real bug that took hours to diagnose. Internalize before writing code.

### 3.1 The EagleSearch API is undocumented and was discovered via JS bundle inspection

```
GET https://search.mudah.my/v1/search?category={cat_id}&type=sell&from={offset}&limit={n}
```

There's no public docs. The URL was found by opening the listings page in browser DevTools → Network → filtering for `search.mudah.my`. **Do the same thing for rentals** — load `mudah.my/malaysia/properties-for-rent`, watch the network panel. You'll see the same endpoint with a different `category` ID and likely a different `type` parameter (`for-rent` not `sell`).

### 3.2 The category ID system

| Category | ID | Where used |
|---|---|---|
| Cars | `1020` | `eagle_client.py CATEGORY_IDS` |
| Motorcycles | `1040` | same |
| Properties for rent | **unknown — discover via network tab** | new entry |
| Properties for sale | unknown | new entry (if relevant) |

Educated guess: probably `2010` or `2020` (Mudah's category IDs increment by tens across top-level sections), but **verify before coding**.

### 3.3 The 10,000-listing depth cap

```python
MAX_OFFSET = 10_000   # Empirical depth cap; offset >= this returns empty
```

Mudah's API silently caps at offset 9,999. Request `from=10000` and you get an empty `data` array, no error. This matches the HTML UI's 250-page cap (250 × 40 = 10k).

**Workaround pattern (already implemented in `scraper.py`):** scrape per-filter so each query has its own 10k window. For cars we scrape per-make. For rentals you'd scrape **per state**, per **property type**, or per **price band** — whichever filter axis gives the best coverage.

The new multi-make picker in `scraper.py` (commit `92346b0` on `feat/new-feature` branch) is the reusable pattern: numbered list, comma-separated filter values, per-filter CSV naming.

### 3.4 Filter parameter names differ across categories

```python
_FILTER_PARAM_NAMES: Dict[str, Dict[str, str]] = {
    "cars":        {"make_id": "make_id",            "model_id": "model_id"},
    "motorcycles": {"make_id": "motorcycle_make_id", "model_id": "motorcycle_model_id"},
}
```

Cars use bare `make_id`/`model_id`. Motorcycles use prefixed `motorcycle_make_id`. **This was a silent bug** — passing `make_id` to category=1040 returns 0 results. It took an API probe to discover.

Assume properties will have its own quirk. Probable filter names: `property_type_id`, `subarea_id`, `state_id`, `room_count`, `furnishing`, `price_min`, `price_max`. **Probe and add an entry to `_FILTER_PARAM_NAMES`.**

### 3.5 Cloudflare protection is uneven

- **EagleSearch JSON API**: **not** Cloudflare-protected. Reliable.
- **HTML search pages (`www.mudah.my/.../page=N`)**: lightly protected. Usually returns 200, sometimes 403.
- **HTML category landing pages**: heavily protected. Returns a **stripped HTML variant** (no `filterOptions` blob) most of the time. `scrape_makes_models.py` retries up to 8× with fresh `cloudscraper` sessions and detects success by presence of a known sentinel string (`"Alfa Romeo"` for cars).
- **HTML model-specific pages**: aggressively blocked. The variant scraper had to be rewritten to use the API instead.

For rentals, expect the same pattern. **Use the API. Only fall back to HTML when you can't.**

### 3.6 The mobile-user-agent trap

```python
# In mudah_client.py
ua = UserAgent(platforms=['pc'])   # NOT UserAgent().random
```

`fake_useragent.UserAgent().random` returns mobile UAs roughly 40% of the time. When Mudah sees a mobile UA, it serves a **stripped mobile HTML variant with ~12 listings per page instead of 40**. This caused a "scraper is missing 70% of listings" bug. Always constrain to desktop UAs.

### 3.7 Retry-After and rate limiting

Both clients honor the `Retry-After` header on 429 responses, capped at 5 minutes. Without the header they sleep 60s. This is **separate** from the normal 2s/3s/5s retry schedule. Copy this logic verbatim — Mudah does send Retry-After sometimes.

### 3.8 The 66-day listing expiry

Mudah expires listings ~66 days after publishing, regardless of sold status. This is a **listing policy**, not a pagination quirk. Verified in the May 28 motorcycles analysis. The implication:

- A weekly scrape captures every new listing well before it expires
- `recheck.py` uses `ad_expiry` to distinguish sold (vanished early) vs expired (vanished at/after expiry)
- **Verify rentals have the same policy.** It might be 90 days or longer for properties.

### 3.9 `__NEXT_DATA__` is Next.js 13 RSC streamed — don't parse it as plain JSON

The HTML pages embed a `__NEXT_DATA__` blob. With Next.js 13 RSC, it's split across multiple chunks and isn't valid standalone JSON. `scrape_makes_models.py` works around this with bracket-matched extraction, anchoring on `"<filter_name>":{`. Don't write naive `json.loads(html.split('__NEXT_DATA__')[1])` — it will break randomly.

---

## 4. Step-by-step playbook for rentals

### Phase 0 — Discovery (Day 1, ~4 hours)

Don't write code yet. Do this:

1. **Open `mudah.my/malaysia/properties-for-rent` with DevTools Network tab.** Filter for `search.mudah.my`. Note the API request — capture the URL, query params, headers, and a sample response. Save the response as `docs/sample_property_response.json`.
2. **Note the `category` ID.** It's in the query string.
3. **Click filters in the UI** (state, property type, room count, price band). Each click triggers a new API call with new params. Map each UI filter to its query param name. This becomes your `_FILTER_PARAM_NAMES` for properties.
4. **Read 5 individual listings** to understand the data shape. Specifically: what fields exist (bedrooms, bathrooms, sqft, furnishing, deposit, tenure type, monthly rent, agent details)? Are they in the API response or only in the HTML detail page?
5. **Probe the depth cap.** Call `from=9000&limit=200` and `from=10000&limit=200`. Confirm the second returns empty (same as cars).
6. **Check for `ad_expiry`.** Look for an `ad_expiry`, `expires_at`, or `expiry_date` field in the response. If present, your `sold_inference` logic from cars/motorcycles maps directly.
7. **Document everything in a `docs/api_discovery_properties.md`** before writing a single line of code.

### Phase 1 — Schema design (Day 1 afternoon)

Based on Phase 0, draft a `schema_properties.sql` mirroring `schema_cars.sql`'s structure:

```sql
CREATE TABLE listings (
    -- Common to all categories (copy from schema_cars.sql)
    ads_id TEXT PRIMARY KEY,
    url TEXT,
    subject TEXT,
    price INTEGER,
    region TEXT, subregion TEXT,
    seller_name TEXT, company_ad INTEGER,
    published TEXT,
    first_seen_at TEXT, last_seen_at TEXT, last_checked_at TEXT,
    availability_status TEXT,

    -- Property-specific (new)
    property_type TEXT,         -- Apartment, Condominium, Bungalow, …
    bedrooms INTEGER,
    bathrooms INTEGER,
    sqft INTEGER,
    furnishing TEXT,            -- Fully / Partially / Unfurnished
    tenure TEXT,                -- Freehold / Leasehold (relevant?)
    monthly_rent INTEGER,       -- if different from `price`
    deposit INTEGER,
    listing_type TEXT,          -- Agent / Owner

    -- Availability tracking (copy from v5)
    ad_expiry TEXT,
    sold_inference TEXT
);
```

Resist the urge to add every possible field. Start with what's in the API response. Add columns later via migration.

### Phase 2 — Client + scraper (Day 2)

Files to create or fork:

| New file | Forked from | Changes |
|---|---|---|
| `src/eagle_client.py` (extend) | existing | Add `properties` to `CATEGORY_IDS` and `_FILTER_PARAM_NAMES`. Add `_RENAME_PROPERTIES` mapping API field names → DB column names. |
| `src/scraper.py` (extend) | existing | Add `"properties"` to `CATEGORY_CHOICES`. The interactive picker abstraction already supports any category if you give it a reference data file. |
| `src/scrape_property_filters.py` (new) | `scrape_makes_models.py` | Pull canonical property types + subarea hierarchy from Mudah's property landing page. Save to `data/reference/`. |
| `migrations/v6_add_properties.sql` | — | Bring the new DB to current schema. |

Run order:
1. `python migrations/run_migrations.py --category properties` → creates the DB with current schema
2. `python src/scrape_property_filters.py` → populates `data/reference/properties_*.json`
3. `python src/scraper.py --category properties` → first scrape (start small with `--max-ads 100`)

### Phase 3 — Clean + recheck (Day 3)

`src/clean.py` and `src/recheck.py` need category-aware extensions but the **shape is identical**. Specifically:

- `clean.py` numeric normalization: add `bedrooms`, `bathrooms`, `sqft`, `monthly_rent` to the integer-coercion list. Add `furnishing` to the title-case list.
- `clean.py` subject cleaning: rental subjects have their own noise patterns ("FREE WIFI", "NEAR LRT", agent contact numbers embedded). Audit 100 raw subjects, build a noise pattern list, regex it.
- `recheck.py`: should work out of the box. The detection rules (200+adDetails / soft_404 / 404 / 403=transient) apply to rental listings too.

### Phase 4 — Testing (Day 3-4)

Copy the test structure:
- `tests/test_eagle_client.py` → add tests for property field renames
- `tests/test_scraper.py` → add a fixture with a sample property API response
- `tests/test_migrations.py` → assert schema reaches current version

Manual integration test:
- Scrape 100 properties → migrate → clean → recheck → eyeball the DB
- Compare 5 random rows against their live Mudah listings — every field should match

### Phase 5 — Production cadence

Same as cars/motorcycles (see CONTEXT.md and Q&A in the README):
- Daily `recheck.py`
- Weekly full scrape
- Monthly reference-data refresh

---

## 5. What to keep, what to change — file-by-file

| File | Action | Notes |
|---|---|---|
| `src/mudah_client.py` | **Keep as-is** | Generic HTTP client with throttling, retries, UA management. Category-agnostic. |
| `src/eagle_client.py` | **Extend** | Add `properties` entries to `CATEGORY_IDS`, `_FILTER_PARAM_NAMES`. Add `_RENAME_PROPERTIES` dict. The pagination + retry logic is identical. |
| `src/scraper.py` | **Extend** | Add `"properties"` to `CATEGORY_CHOICES`. The multi-filter picker (now numbered-list-based) is already generic if the reference data file exists. |
| `src/migrate_xlsx_to_db.py` | **Light edit** | Add property-row classification. The upsert/mode logic is reusable. |
| `src/clean.py` | **Extend** | Add property-specific normalization rules. The dedup + repost logic is reusable. |
| `src/recheck.py` | **Keep mostly as-is** | The HTTP detection rules apply to any Mudah listing. Add `--category properties` routing. |
| `src/scrape_makes_models.py` | **Don't reuse** | Properties don't have makes/models. Build `src/scrape_property_filters.py` from scratch. The bracket-matching JSON extractor pattern *is* reusable. |
| `migrations/run_migrations.py` | **Extend** | Add v6 (or fresh schema) for properties. Idempotent migration pattern stays. |
| `src/detail_client.py` | **Likely don't need** | Phase 2 (HTML detail-page enrichment) is removed for cars/motos because it only added 0.9% data. If properties expose more detail in HTML than API, you may revive this. Profile first. |
| `src/dashboard_aggregate.py` | **Build separately** | Hard-coded to cars. Build a properties dashboard from scratch if needed. |
| `src/script.py` | **Reference only** | Legacy HTML-only scraper. Use as a fallback template if EagleSearch ever blocks you. |

---

## 6. Discovery checklist — answer these on day 1

Before writing code:

- [ ] **Category ID** for `properties-for-rent` (likely 2-prefix)
- [ ] **Filter param names** — map every UI filter to its API query param
- [ ] **Sort order** — is there a `sort` param? What are the values?
- [ ] **Depth cap** — confirm offset 10,000 returns empty (probably true)
- [ ] **`ad_expiry` field** — does the API return it? If yes, sold_inference works.
- [ ] **`published` field name** — is it `published`, `created_at`, `posted_at`?
- [ ] **Listing expiry policy** — confirm the 66-day window (or whatever it is for rentals)
- [ ] **HTML detail-page fields** — what does the detail page have that the API doesn't?
- [ ] **Cloudflare aggressiveness** — does the category landing page return stripped HTML like cars?
- [ ] **Sample response shape** — capture and save 1 raw response per filter combo you intend to use

---

## 7. Risks & open questions

### 7.1 Mudah may close the EagleSearch API at any time

There's no contract. They could add auth tomorrow. The whole codebase has `EagleAuthError` as a first-class concept — if 401/403 starts coming back from the API, fall back to HTML-only. **This is the single biggest risk** to the project. Don't write code that assumes the API will always be open.

### 7.2 Schema drift in API responses

The API field names have changed once already (we caught it via `test_eaglesearch.py`, a manual connectivity check kept just for this purpose). Build the equivalent for properties. Run it before every weekly scrape. If field names changed, you'll know on day one instead of after a corrupted CSV ships into the DB.

### 7.3 Rentals have higher churn than used cars

Rental turnover is faster than used-car sales. A weekly scrape might miss listings that publish + rent in 3 days. Consider tightening to twice-weekly if the data shows high churn (re-evaluate after first month of data).

### 7.4 Privacy / PII

Rental listings contain agent phone numbers, sometimes addresses. `clean.py` for cars already redacts phone numbers in the `body` field. Mirror this for properties — Malaysia's PDPA applies. The redaction logic is in `clean.py` (`redact_pii` or similar — search for phone-regex patterns).

### 7.5 Map / geo data

Property listings sometimes include lat/long. Cars don't have this concept. If the API returns coordinates, **store them** even if you don't have a use yet — backfilling geo data later is expensive.

### 7.6 The `body` column was removed in cars

Cars used to have a `body` column for full seller description. It was dropped in schema v3 because it was rarely useful for cars. Properties are different — the body of a rental listing often contains amenity details ("free wifi", "near LRT", "covered parking") not in structured fields. **Keep the body column for properties.** Add an NLP enrichment pass later if useful.

---

## 8. Quick-reference: commands you'll use

```bash
# Initial setup
pip install -r requirements.txt
python migrations/run_migrations.py --category properties

# Discovery
python src/test_eaglesearch.py        # Adapt this for properties API smoke test

# Daily workflow (once running)
python src/scraper.py --category properties
python src/migrate_xlsx_to_db.py --category properties
python src/clean.py --category properties
python src/recheck.py --category properties

# Multi-filter scrape (analog to multi-make for cars)
python src/scraper.py --category properties --filter "kuala-lumpur,selangor,johor"
```

---

## 9. Where to read more

- **[README.md](../README.md)** — top-level user docs for the existing pipeline. Read **Quick Start**, **Data Pipeline**, **Rate limiting & retries**.
- **[CONTEXT.md](../CONTEXT.md)** — session log + architectural decisions + gotchas. The "Known Gotchas" section is the war diary.
- **[src/eagle_client.py](../src/eagle_client.py)** — read this end-to-end before touching API code. It's 500 lines and worth every minute.
- **[src/scraper.py](../src/scraper.py)** — the multi-make picker is your template for any multi-filter workflow.
- **[docs/availability_tracking_summary.md](availability_tracking_summary.md)** — explains the `availability_status` + `sold_inference` model.

---

## 10. Who to ask

The original author can answer:
- Why a specific architecture decision was made (the **Architecture Decisions** table in CONTEXT.md catalogs the big ones)
- Mudah-specific quirks not documented here
- The history of any test or migration

For Mudah-specific behavior changes (filter names, response shape), the source of truth is **the live API**. Don't trust this doc if it disagrees with what the API returns today. Probe and update.

---

*Last updated 2026-05-28. Update this doc as you discover things — the discovery checklist in Section 6 should fill with specifics within your first week.*
