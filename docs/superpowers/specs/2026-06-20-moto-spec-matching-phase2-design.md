# Motorcycle Spec Matching — Phase 2 Design

**Date:** 2026-06-20
**Status:** DESIGN LOCKED, build not started
**Counterpart:** mirrors [carbase Phase 2](2026-06-18-carbase-spec-matching-phase2-design.md), but for motorcycles + a deterministic (non-fuzzy) matcher.

## Goal

Enrich `data/master/cardata_motorcycles.db` `listings` with OEM specs from
`data/reference/motobike_specs.db` `model_specs` (530 rows, zigwheels.my new-bike
catalog), matching on `(make, model)`.

## Why deterministic, not fuzzy

Spec source is small (530 rows) and the join key is unique (0 `(make, norm(model))`
collisions). Fuzzy (difflib) was rejected as the primary tool: the unmatched tail is
dominated by displacement-number collisions (`Z 250`↔`ninja-250`, `Mt-07`↔`mt-09`,
`Vario 150`↔`vario-160`) that score high and match WRONG silently. Deterministic tiers
+ a hand-curated alias file are auditable and yield NULL instead of a confident-wrong
match. Measured coverage: **73.3%** (21,325 / 29,077 rows).

## Match pipeline (in `src/enrich_specs.py`, moto path)

Normalization (match key only — never stored; stored make/model keep `3_clean.py` `.title()`):

```python
import re
def norm(s):                       # model key: strip everything
    return re.sub(r'[^a-z0-9]', '', (s or '').lower())

MAKE_ALIAS = {"mbp morbidelli": "morbidelli", "sm sport": "sm-sport", "mbp": "morbidelli"}
def norm_make(s):                  # make key: keep hyphen (spec makes are hyphenated)
    s = (s or '').lower()
    return MAKE_ALIAS.get(s, s.replace(' ', '-'))
```

Tiers, first hit wins, records `spec_match`:
1. **exact** — `norm(model)` in make's spec pool (49.3%, 14,342 rows)
2. **prefix** — pool key startswith/endswith the normalized model, **unambiguous only** (1 candidate) (19.0%, 5,532 rows)
3. **alias** — `(make|model)` in the curated alias file (5.0%, 1,451 rows)
4. **else NULL** — no match. Do not guess. (25.5% — genuinely discontinued: Ex5, C70, RXZ, Z250/500/800, Vario 150 → motomalaysia Phase-2b territory)

`no_make_catalog` (1.1%, 333 rows): make absent from zigwheels → NULL.

Idempotent write: `... WHERE engine_cc IS NULL` so re-runs skip filled rows
(same pattern as `recheck.py` mcdParams).

## Alias file

`data/reference/moto_spec_aliases.json` (finalized 2026-06-20). 19 alias entries (1,451 rows) + the rest decided NULL. Structure: `_make_alias` map,
`aliases` keyed `"Make|Model"` with `decision` = spec slug or `"NULL"`. Hand-curated;
top-volume pairs swept twice (initial 60 + NULL re-sweep for slug-mismatch current
bikes like `Z1000Sx`→`ninja-1000sx`, `Transalp`→`xl-750-transalp`).

## Schema — migration v10 (motorcycles only; cars version-bump no-op)

Pure `ADD COLUMN` (safe, no table rebuild). 14 new cols on motorcycles `listings`.
Column set chosen by fill-rate + analytical value; detail noise dropped (recoverable
from `model_specs.raw_specs` via re-run if ever needed).

```python
V10_MOTORCYCLE_COLS = [
    # performance core
    ("engine_cc",       "REAL"),     # decimals in source (124.3)
    ("power_hp",        "REAL"),     # hp, NOT kW (cars use kw) — store as sourced
    ("torque_nm",       "REAL"),
    ("kerb_weight_kg",  "REAL"),     # 55% fill — power-to-weight input, kept
    ("fuel_tank_l",     "REAL"),
    # categorical / segmentation
    ("engine_type",     "TEXT"),
    ("transmission",    "TEXT"),     # CVT vs manual → scooter vs bike
    ("fuel_type",       "TEXT"),     # flags EVs
    ("cooling",         "TEXT"),     # air vs liquid
    # moto-relevant geometry
    ("seat_height_mm",  "INTEGER"),
    ("wheelbase_mm",    "INTEGER"),
    ("comp_ratio",      "TEXT"),     # parity with cars v9
    # provenance (set by enricher, not from specs)
    ("spec_match",      "TEXT"),     # exact / prefix / alias
    ("spec_source",     "TEXT"),     # "zigwheels" now; added early for motomalaysia Phase-2b
]
```

**Dropped on purpose:** length/width/height_mm, ground_clearance, bore_stroke,
valve_config, fuel_system, rpm_power, rpm_torque, cylinders, seating_capacity (~always 2),
fuel_consumption_kmpl (20% fill), tyres, suspension.

## Migration build — 3 known tax spots (per CONTEXT 2026-06-20)

1. `migrations/run_migrations.py`: `TARGET_VERSION 9 → 10`; add `V10_MOTORCYCLE_COLS`;
   add `_migrate_v9_to_v10` (clone of `_migrate_v8_to_v9`, cars→motorcycles); wire
   `if current < 10:`.
2. `tests/test_migrations.py`: terminal-version asserts `== 9` → `== 10`; cars now get
   no-op v9→v10.
3. `src/2_migrate.py`: append the 14 cols to `ENRICHMENT_ONLY_COLUMNS["motorcycles"]`
   (else drift test `test_every_schema_column_is_accepted_by_migrate[motorcycles]` fails).

## Build sequence

1. ✅ DONE 2026-06-20 — Migration v10 (+ the 3 tax spots) run `--category both`; both DBs at v10, 14 cols on motos, full suite 264 pass.
2. ✅ DONE 2026-06-20 — Alias file finalized → `moto_spec_aliases.json`.
3. ✅ DONE 2026-06-21 — `src/enrich_specs.py` moto path (TDD). Idempotency guard refined
   to `spec_match IS NULL` (records every tier; protects future motomalaysia fills;
   `--force` reprocesses). Source values already cleanly typed → coercion passthrough,
   only `compression_ratio`→`comp_ratio` rename.
4. ✅ DONE 2026-06-21 — `tests/test_enrich_specs.py` (17 tests: norm, match tiers, enrich,
   idempotency, alias loading).
5. ✅ DONE 2026-06-21 — ran: 21,325/29,077 matched (73.3%); idempotent re-run = 0 rows.

## Phase-2b — motomalaysia cross-fill (✅ DONE 2026-06-21)

Built `src/scrape_motomalaysia_specs.py` — curated, displacement-verified crawl of 9
motomalaysia spec pages into `model_specs` (new `source` column). `enrich_specs`
reads `spec_source` from the matched row. Coverage 73.3% → **78.0%** (+1,357 rows).
The 5 displacement traps (Vario 150, Adv 150, Dash 110, Wave 100, Kriss MR3) are NOT
in the curated list → stay NULL by design. Same deterministic exact/prefix discipline
(no fuzzy). Full suite 289 pass. Details in CONTEXT session log 2026-06-21.

Original parked notes (for reference):

**motomalaysia.com Phase-2b cross-fill** — covers the 25.5% NULL discontinued bikes
(EX5, 125ZR, PCX, Z250/800, MT-07 confirmed present; richer fields than zigwheels incl.
kerb weight + 2-strokes). Slug `/{make}-{model}-price-specs-malaysia/`, **but** year in
slug + no spec sitemap → messier enumeration than zigwheels. Build only if NULL coverage
proves to matter. imotorbike.com = used marketplace (no clean spec catalog; its value is
mileage, separate track). getbike.my = new-only, thin fields → not useful.
