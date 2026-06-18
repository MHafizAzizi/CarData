# carbase.my Spec Matching — Phase 2 Design

**Date:** 2026-06-18
**Status:** Approved, ready for implementation plan
**Depends on:** Phase 1 (PR #33) — `data/reference/carbase_specs.db` populated (1,368 variant rows).

## Goal

Enrich cars `listings` with hard specs from the carbase.my catalog. Match each
Mudah listing to a `model_specs` variant row on `(make, model, variant, year)`
and denormalize `power_hp`, `torque_nm`, `kerb_weight_kg` into `listings` as
enrichment-only columns (mirrors the `vehicle_type`/`type_group` pattern). Cars
only — `model_specs` has no motorcycles.

## Non-goals

- No motorcycle support (no spec source).
- No cross-generation/year guessing (precision over coverage).
- Not part of `run_pipeline.bat` — occasional tool, run after a spec refresh.
- No new third-party dependency — variant fuzz uses stdlib `difflib`.

## Architecture

New standalone script `src/enrich_specs.py`:

- Reads spec catalog from `data/reference/carbase_specs.db` (`model_specs`).
- Reads + writes the cars DB `listings` table via `db.connect("cars")`.
- Standalone (not a `3_clean.py` step) because it joins two databases; the
  existing `--enrich-types` step is single-DB.
- CLI: `--dry-run` (preview + stats, no write), `--refresh` (re-match rows that
  already have a `spec_match`), `--db` (override specs DB path), `--report`
  (print coverage breakdown). Default run fills only rows where `spec_match IS
  NULL`.

### Schema migration v9 (cars only)

Add to `listings`:

| Column | Type | Notes |
|---|---|---|
| `power_hp` | INTEGER | from `model_specs.power_hp` |
| `torque_nm` | INTEGER | from `model_specs.torque_nm` |
| `kerb_weight_kg` | INTEGER | from `model_specs.kerb_weight_kg` |
| `spec_match` | TEXT | match tier: `'variant'` / `'model_year'` / NULL |

All four added to `ENRICHMENT_ONLY_COLUMNS["cars"]` in `src/2_migrate.py` so a
re-scrape upsert never clobbers them. Migration is idempotent (guarded
`ALTER TABLE` add-column, mirroring existing migrations). `schema_cars.sql`
updated to match. Motorcycles schema untouched.

## Matching algorithm

### Index

`build_spec_index(model_specs rows)` → dict keyed `(make_norm, model_norm,
year)` → list of variant spec dicts (`variant_norm`, `power_hp`, `torque_nm`,
`kerb_weight_kg`).

`normalize_key(s)`: casefold, strip, collapse slug/space/punctuation to a single
token form so carbase `honda`/`civic` aligns with Mudah `Honda`/`Civic`. Variant
normalization additionally reduces to a token set for fuzzy comparison.

### Per-listing match

Inputs: `make`, `model`, `manufactured_date` (year), `variant`.

1. Look up candidates by `(make_norm, model_norm, year)`. No candidates → NULL.
2. **Tier `variant`** — score listing variant against each candidate's variant
   with `difflib.SequenceMatcher` on normalized token sets. Best score ≥
   `VARIANT_THRESHOLD` (default 0.6) AND an unambiguous winner (best clearly
   beats second-best) → use that variant's specs. `spec_match = 'variant'`.
3. **Tier `model_year`** — no confident variant hit. For each spec column
   independently: if all candidate variants agree on a value (or only one
   candidate exists), fill it. `spec_match = 'model_year'`. Per-column: power may
   agree (fill) while weight differs (leave that column NULL).
4. **NULL** — candidates disagree and variant unresolved → leave specs and
   `spec_match` NULL.

Year must match exactly; an absent year in the index is a miss, not a
cross-generation guess.

### Reporting

`--report` / end-of-run summary: candidate count, filled-by-`variant`,
filled-by-`model_year`, unmatched count, and the top unmatched `(make, model)`
pairs by listing count — surfacing catalog gaps the same way `--enrich-types`
surfaces unmapped pairs.

## Testing

Pure-function core, table-driven, in-memory SQLite (mirrors `test_migrate.py`):

- `normalize_key` / variant tokenization: slug↔space, case, punctuation.
- `build_spec_index`: groups variants by `(make, model, year)`.
- `match_listing`: variant hit; ambiguous variant → `model_year` fallback;
  per-column agreement (mixed fill); total miss → NULL. Each tier asserted.
- Migration v9 idempotent (re-run safe); new cols present in
  `ENRICHMENT_ONLY_COLUMNS["cars"]`.

## Out of scope / future

- Tuning `VARIANT_THRESHOLD` against real coverage numbers post-first-run.
- Backfilling specs for new makes/models that surface as unmatched pairs (feeds
  back into a Phase 1 refresh).
