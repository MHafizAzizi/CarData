# Implementation Plan — Moto spec tail, hand-transcription (largest-first)

**Status:** Draft 2026-06-25. Builds on the merged manual-loader path (PR #43).
**Goal:** push moto spec fill past 79.8% by filling the addressable null tail one
model at a time, cheapest tier first, never guessing.

---

## Background (what's already true)

- Fill = **79.8%** (24,777 / 31,039). Catalog = zigwheels 530 + motomalaysia 15
  + manual 2 (Voge).
- Bulk approaches are exhausted: zigwheels sitemap is fully captured;
  brand-page walk *regressed* matching (prefix collisions) and was reverted;
  motomalaysia lacks Chinese brands; international aggregators don't carry these
  MY-market models.
- The only remaining lever is **per-model curation** via:
  - `data/reference/moto_spec_aliases.json` — when the bike IS in a catalog
    under a name that doesn't `norm`-match.
  - `data/reference/moto_specs_manual.json` + `src/load_manual_moto_specs.py` —
    when it's in NO catalog and must be transcribed from an OEM/other page.
- `enrich_specs.py` consumes both; `--force` re-applies to already-processed rows.

---

## Per-model decision flow (the core loop)

For each target, in order — stop at the first that applies:

1. **Already in a catalog under a non-matching name?**
   Check the make's pool in `motobike_specs.db`. If exactly one candidate is
   the same bike → add a `moto_spec_aliases.json` entry (`"Make|Model":
   {"decision": "<catalog-slug>"}`). Cheapest; no transcription.
   - *Engine-shared variants* (e.g. ER-6n ↔ er-6f, same 649cc platform): alias
     only if the listing has no own displacement that would conflict and the
     shared specs are the engine core. Note the approximation in the entry.

2. **Bare / ambiguous model name** (Versys, Tiger, Ego, Ss, bare Scrambler)?
   - If every catalog variant of that name shares one displacement AND agree on
     power/torque → fill via alias to the base. (Ducati Scrambler = all 803cc.)
   - Otherwise **skip — leave NULL** (no-guess rule). Versys (X250/650/1000) and
     Tiger (660/850/900/1200) stay NULL.

3. **In NO catalog → transcribe.** Pick the source by obstacle type (below),
   read the spec sheet, transcribe a verified row into `moto_specs_manual.json`.

4. **Encode + verify.** `model` slug chosen so `norm(listing model)` hits enrich
   at exact/prefix. **cc must agree with the model name** (Naga 150 → ~150cc) or
   the row is wrong — discard. Fill only fields shown on the source; leave
   ambiguous ones (unlabelled power units, etc.) **NULL, never guessed**.

5. **Load + measure.** `python src/load_manual_moto_specs.py` →
   `python src/enrich_specs.py --force` → confirm the model's rows now carry
   `engine_cc` + `spec_source='manual'`. Batch ~5 models, then commit.

---

## Source-by-obstacle cheatsheet

| OEM site type | Brand(s) | How to read specs |
|---|---|---|
| Server-rendered | Voge (`voge.my/product/<SLUG>`) | Read HTML text directly (done). |
| JS / PDF brochure | Aveta, SYM | Check `__NEXT_DATA__` / inline JSON first; else fetch the linked brochure PDF and read it (use the `pdf` skill). |
| Wix | WMoto | Find the page-data JSON endpoint (`*.wix.com` / `pages/...thunderbolt?...json`) or the `wix-warmup-data` blob in HTML. |
| Shopify | Benda | `GET /products/<slug>.json` (Shopify product JSON) — specs often in `body_html` or metafields. |
| Offline OEM | CFMoto, QJ Motor | Fall back to a launch/review article (paultan.org, bikesrepublic) or bikez/Wikipedia for the same model-year; verify cc against the name. |

---

## Ranked worklist (top of the null tail, post-Voge)

Counts are listings that would fill. Tier: **A** alias, **S** skip (ambiguous),
**T** transcribe.

| # | Listing | n | Tier | Action / source |
|---|---|---|---|---|
| 1 | Kawasaki Er-6N | 97 | A? | Catalog has `er-6f` (same 649cc platform). Alias if engine-core fill is acceptable; verify dims differ → may leave dims NULL. |
| 2 | Sym Naga 150 | 89 | T | SYM (JS). Discontinued — try motomalaysia article / paultan / bikez; cc ~149. |
| 3 | Aveta Nova 200 | 81 | T | Aveta brochure PDF. cc per name ~177-200; verify. |
| 4 | Wmoto Cruiser Amt 125 | 73 | T | WMoto Wix data blob. cc 125. |
| 5 | Cfmoto 700Mt | 64 | T | CFMoto offline → paultan/bikez 700MT (693cc). |
| 6 | Kawasaki Versys | 57 | S | Ambiguous (X250/650/1000) — leave NULL. |
| 7 | Benda Lfc 700 Pro | 56 | T | Benda Shopify `/products/*.json`. cc ~680. |
| 8 | Qj Motor Atx250X | 56 | T | QJ offline → paultan/bikez. cc ~249. |
| 9 | Sym Cruisym 400I | 56 | T | SYM (JS) / brochure. cc ~400. |
| 10 | Yadea Gt20 | 49 | T | EV — no cc; fill power/range fields only, or leave (enrich keys on cc). |
| 11 | Aveta Dy 115 | 46 | T | Aveta brochure. cc ~115. |
| 12 | Benda Dark Flag 500 | 43 | T | Benda Shopify. cc ~500. |
| 13 | Yadea Rs20 | 43 | T | EV — as Gt20. |
| 14 | Zeeho Ae4 | 42 | T | EV. |
| 15 | Sym Vts 200 | 39 | T | SYM. cc ~200 (zigwheels page lacked it). |

Plus mainstream genuine gaps further down (Kawasaki Z1000 / 1400GTR / KLX250,
Yamaha YZF-R1, Honda CBR500R / Spacy, Harley Iron 883, Aprilia Shiver, Benelli
TNT600 / RFS150) — none in catalog; transcribe from bikez/Wikipedia (mature
models, well documented) when reached.

**EV note:** Yadea/Zeeho have no `engine_cc`. `enrich_specs` keys matching on
make+model (not cc), so an EV manual row with `engine_cc=null` + power/range
still matches and fills the non-cc fields — but `engine_cc IS NULL` means it
won't count toward the cc-fill metric. Decide whether EVs are in scope before
spending time on them.

---

## Mechanics

- **JSON row schema** (`moto_specs_manual.json` → `specs[]`): `make`, `model`
  (slug), then any of `engine_cc, power_hp, torque_nm, kerb_weight_kg,
  fuel_tank_l, engine_type, transmission, fuel_type, cooling, seat_height_mm,
  wheelbase_mm, compression_ratio`, plus `source_url`. `power_hp` in HP
  (kW × 1.34102).
- **Slug rule:** `norm(slug) == norm(listing model)` for exact, or listing-norm
  starts-with / is-started-by slug-norm for prefix. Verify with a dry-run after
  loading.
- **Cadence:** transcribe ~5 models → `load` → `enrich --force` → spot-check →
  commit (`feat(specs): manual moto rows — <brands>`). Keep batches small so a
  bad row is easy to bisect.

## Stop criteria

Stop when the remaining null rows are only: vintage 2-strokes/cubs absent
everywhere (RXZ, C70, Nouvo, KRR), the 5 deliberate displacement traps, bare
ambiguous names, and EVs (if out of scope). Realistic reachable ceiling with
full transcription ≈ **85-87%**.

## Execution log

WebSearch (not paultan fuzzy search or bikez URL-guessing) is the reliable
per-model source — it returns authoritative spec pages and clean numbers.

- **Batch 1** (→80.8%): Er-6N alias, Aveta Nova 200, CFMoto 700MT, Benda LFC700.
- **Batch 2** (→81.3%): SYM Cruisym 400i, WMoto Cruiser 125, Honda CBR500R.
- **Batch 3** (→81.4%): Ducati Scrambler.
- **Batch 4** (→82.4%): Aveta DY115, Benda Dark Flag 500, Benelli TNT600S,
  Benelli RFS150i, Brixton Classic 150I (alias→bx-150), Kawasaki KLX250,
  Kawasaki 1400GTR, CFMoto 800MT Explorer. +319 rows.

- **Batch 5** (→83.2%): Harley Iron 883, Aprilia Shiver 750, Aveta VS115,
  WMoto Hawk 200i, SYM SM Sport 110, Yamaha XJ6; GPX GR200-RR (alias→
  demon-gr200r). Skipped Ducati Diavel (bare name spans 1198/1262/1158cc gens).

- **Batch 6** (→84.0%): Benda Chinchilla 500, Modenas ZX-25R SE (Kawasaki
  rebadge), Harley Forty-Eight, SYM VFE185i, Kawasaki Z500, BMW F800GS, Aveta
  Serra 125; Vespa GTS 300 (alias→gts-classic-300). Source_url collisions
  bit twice (a manual URL equal to a catalog row's, and two manual rows sharing
  a `?s=` URL) — keep every manual `source_url` unique.

- **Batch 7** (→84.5%): Honda CB650F, Modenas GT128, Yamaha Virago 535, Kymco
  DTX250 (manual); Kawasaki Ninja 1000→ninja-1000sx, Triumph Trident→trident-660,
  Brixton Cafe Racer 150I→bx-150, Harley Sportster 48→forty-eight (aliases).
  Skipped Ducati Multistrada/Streetfighter, MV Agusta Brutale, Triumph
  Bonneville/Scrambler/Speed Twin, Vespa Super 150 (generation/displacement
  ambiguous). +152 rows.

- **Batch 8 / final** (→85.3%): MBP Morbidelli T252X/T502X, SYM Bonus 110 SR,
  Kawasaki Ninja 1100SX SE, Vespa LX 150, GPX Gentleman 200, Honda CBR650F,
  Kawasaki Vulcan 900, SYM Jet Power 125, Voge CU525, Kymco AK550 (manual);
  BMW 310GS→g-310-gs, SYM Sport Bonus 110 SR→bonus-110-sr (aliases). +258 rows.

**Final: 78.0% → 85.3%** over Phase-2c + manual loader + 8 transcription batches.
Stop here — the remaining null tail is vintage 2-strokes/cubs, EVs (no cc), the
5 displacement traps, and bare ambiguous names spanning multiple generations.

**Recurring gotcha:** the alias file already holds auto-suggested entries for
many listings with `decision: "NULL"`. A NEW key prepended to `aliases` is a
JSON duplicate — the LATER (NULL) one wins on parse, so the alias silently
no-ops. Always edit the EXISTING entry's `decision` in place (hit on Er-6N and
Brixton Classic 150I).

**Catalog collision:** a manual row whose `source_url` equals an existing
catalog row's violates `UNIQUE(source_url)` and rolls back the whole load. If a
model is already in the catalog under a different slug (Brixton bx-150), alias
to it instead of adding a manual row.

## Risks

- **Wrong-displacement fill** is the cardinal sin (the trap this whole design
  avoids). Mitigation: cc must match the model name; discard otherwise.
- **PDF/JS brittleness** — these are one-time manual reads, not a scraper, so
  brittleness is bounded to the moment of transcription.
- **Variant ambiguity** — when an OEM page lists multiple sub-variants, transcribe
  only the one matching the listing model string, else skip.
