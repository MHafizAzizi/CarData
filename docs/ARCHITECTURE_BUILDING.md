# CarData — Architecture & Build Roadmap

> Forward-looking build plan for taking the repo past "mature scraper" into **product + scaled infra**.
> Companion to [CONTEXT.md](../CONTEXT.md) (current state / session log). This doc = where we're going and in what order.
> Living document — update phase status as work lands.

---

## Where We Are

Mature single-site pipeline: scrape Mudah.my → SQLite → clean → recheck → static dashboard.
- `cardata_cars.db` — 102,728 rows, specs 61% filled.
- `cardata_motorcycles.db` — 29,077 rows, specs 85% filled.
- Pipeline runs **manually, locally**. Dashboard is cars-only v1 (static regen).
- 316 tests pass (run manually on dev machine).

**The trap to avoid:** further spec-fill grinding is near its ceiling (last batch = 13 models for +0.8%). Next phase is *automation + product*, not more spec %.

---

## North Star

**We own data no Malaysian car site publishes.** `first_seen_at` + `ad_expiry` + `sold_inference` + 66-day avg lifespan = we know **what actually sells, at what price, how fast**. Competitors show only *asking price*.

Every product decision points back to this. Lead with liquidity + fair-price, not another listings table.

---

## Two Tracks

### Track A — Productize the data

Highest-leverage first.

| # | Build | Why it wins | Leans on |
|---|---|---|---|
| A1 | **Fair-price / deal-finder** | Killer feature. Flag underpriced live listings. | `price × manufactured_date × mileage_bucket × specs × region` — training set already exists |
| A2 | **Liquidity / time-on-market** | Nobody publishes "Civic sells in 18d, Estima sits 90d" | `first_seen_at → ad_expiry → sold_inference` |
| A3 | **Dashboard v2** | Both categories, interactive filters | Mechanical once A1/A2 exist |
| A4 | **Read API** | Only if a consumer exists | YAGNI until then |

**Sequence:** A1 as a script/notebook against the DB first. Prove the price model has signal *before* any UI. Then A2, then wrap both in A3.

### Track B — Harden / scale infra

Reality-checked. Don't build ahead of measured pain.

| # | Build | When | Note |
|---|---|---|---|
| B1 | ~~**WAL mode**~~ ✅ | done | Already set in `src/db.py:84` (`PRAGMA journal_mode=WAL`). |
| B2 | ~~**`.gitattributes`**~~ ✅ | done | Already present + correct (`* text=auto eol=lf`). Stale CONTEXT TODO — drop it. |
| B3 | **GitHub Actions CI (`pytest`)** | Now | 316 tests only run locally. Protect codebase as product code lands. |
| B4 | **Scheduled incremental scrape** | After A1 | Actions cron / Task Scheduler. Freshness for a live product. |
| B5 | **Proxy rotation pool** | When freshness demands it | **The real ceiling** — recheck stalls ~every 600 rows on Cloudflare 403/429. Caps everything before the DB does. |
| B6 | **Postgres migration** | When write contention measured | NOT on vibes. SQLite does millions of rows; we're at 130K. Migrate only when multi-process concurrent scraping forces it. |

---

## Phased Roadmap

**Phase 0 — Unblock (hours).** B1 ✅ + B2 ✅ already done in repo — only B3 (CI) remains.

**Phase 1 — Prove (1–2 weeks).** A1 prototype only. One script: fit per-model fair-price curve, output underpriced live listings. **Go/no-go gate** — if the price model has no signal, the rest isn't worth scaling for.

**Phase 2 — Build (weeks).** A2 + A3. Surface liquidity + deal-finder in dashboard v2 (both categories). B4 for freshness.

**Phase 3 — Scale (when warranted).** B5 proxy pool, then A4 API, then B6 Postgres — each only when the prior phase proves demand.

---

## Decisions & Guardrails

Stops us building the wrong thing.

- **SQLite stays** until write contention is *measured*, not assumed. WAL covers read-during-scrape. Postgres = multi-process scraping era only.
- **Prove before UI.** A1 is a script first. No web app until the model has signal.
- **No API until a consumer exists.** A4 is last for a reason.
- **Proxy pool is the true scaling bottleneck**, not the database. Spend engineering there before infra elsewhere.
- **Spec-fill grinding is done.** ~85% moto / ~61% car is the working ceiling. Don't reopen unless a new bulk source appears.

---

## Implementation Plan

Concrete steps, grounded in actual files. Phase 0 + 1 detailed (near-term, well-specced); 2 + 3 sketched (detail when reached).

### Phase 0 — CI (hours)

Only B3 left (B1/B2 already in repo).

1. New `.github/workflows/ci.yml` — on push + PR:
   - `pip install -r requirements.txt`
   - `pytest -q`
   - Single Python (3.11). DBs aren't git-tracked, so tests already run on fixtures / in-memory (316 pass) — no data setup needed.
2. One file, zero code change. Done when a PR shows a green check.

### Phase 1 — Prove A1 fair-price (the gate, 1–2 weeks)

Build as a script, **not** UI. This phase exists to answer one question: *does the price model have signal?*

1. **`src/fair_price.py`** — read cars DB via `db.connect("cars")` (reuse existing helper).
2. **Baseline (not ML):** group available rows by `(make, model, year, mileage_bucket)` → `median(price)` + sample count `n`.
   - `year` derived from `manufactured_date` (INT).
   - Drop groups with `n < ~8` (too thin to trust).
   - ponytail: median-by-group first. Regression **only if** buckets prove too coarse — don't import sklearn before the median fails.
3. **Score live listings:** `residual = (price − group_median) / group_median`; flag `residual < −0.15` AND `availability_status IN ('available','unknown')`.
4. **Output:** ranked table/CSV — top deals by `discount × sample-confidence`.
5. **`tests/test_fair_price.py`:** in-memory DB, seed one obviously-underpriced row in a group → assert flagged; assert a fair-priced row is not. (Matches existing test style.)
6. **Gate decision:** eyeball top ~50.
   - Real deals → proceed to Phase 2.
   - Salvage / typos / wrong-trim noise → spec gaps (61% car fill) are the cause; improve features or stop. **This is the discovery Phase 1 is for.**

**Known risk surfaced here:** if price variance is driven by trim/spec the buckets can't see, baseline underperforms — better to learn that now than after building UI on it.

### Phase 2 — Build (sketch)

- **A2 liquidity:** `julianday(ad_expiry) − julianday(first_seen_at)` per model; join `sold_inference` for sold-vs-expired split.
- **A3 dashboard v2:** feed A1 + A2 into `src/dashboard_aggregate.py` (extend to both categories; currently cars-only).
- **B4 scheduled scrape:** Actions cron or Task Scheduler for incremental freshness.

### Phase 3 — Scale (sketch)

B5 proxy rotation pool (the real ceiling) → A4 read API → B6 Postgres. Each gated on prior-phase demand, never pre-built.

---

## Open Questions

- A1: which model? Start simple — median-by-(make, model, year-bucket, mileage-bucket) baseline, then regression only if baseline is too coarse. (ponytail: don't reach for ML before the median fails.)
- Hosting target for a live product — local box + tunnel, or cloud? Decides B4/B5 shape.
- Public vs private — does the product face users, or is it your own analysis tool? Changes A3/A4 weight entirely.
