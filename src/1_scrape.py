"""Mudah scraper — EagleSearch API (Phase 1 only).

Collects car/motorcycle listing metadata via the EagleSearch JSON API
(search.mudah.my/v1/search, 200 ads/req). Writes a timestamped CSV per run.

Usage:
    python src/1_scrape.py --category cars --max-ads 1000
    python src/1_scrape.py --category cars --make toyota --max-ads 5000
    python src/1_scrape.py --category cars --make toyota --model vios
    python src/1_scrape.py --category cars --all-makes --smart   # full coverage
    python src/1_scrape.py                          # interactive prompts

Depth-cap evasion (--smart):
    A single EagleSearch query paginates to at most ~10,000 ads. Makes with
    more inventory than that (e.g. Toyota cars ~18.8k) lose the overflow.
    --smart probes each make's total-results and splits any make over ~9,500
    into one query per model, so the full inventory is captured. Combine with
    --all-makes for a complete scrape. Costs one extra probe call per make.

Filters:
    --make  <slug>   Filter by make  (e.g. 'toyota', 'perodua'). Narrows API
                     results to that make only — useful for make-by-make runs
                     to work around the 10,000-listing depth cap.
    --model <slug>   Filter by model (e.g. 'vios', 'myvi'). Can be combined
                     with --make. Requires reference data in data/reference/.

API auth fallback:
    If EagleSearch returns 401/403, the scraper logs a warning and raises
    EagleAuthError. Use src/script.py for HTML-only scraping as a fallback.

Available makes — Cars (128):
    alfa-romeo, alpine, ariel, asia-motors, aston-martin, audi, austin,
    baic, baw, bentley, bison, bmw, boon-koon, borgward, brabus, bufori,
    byd, buick, cadillac, cam, caterham, chana, changan, chery, chevrolet,
    chrysler, citroen, daihatsu, daimler, datsun, denza, dodge, dongfeng,
    ds-automobiles, excalibur, farid, ferrari, fiat, ford, foton, gac,
    gac-aion, gac-motor, gmc, golden-dragon, great-wall, haval, higer,
    holden, honda, huanghai, hummer, hyundai, icaur, infiniti, inokom,
    isuzu, iveco, jac, jaecoo, jaguar, jeep, jetour, jmc, joylong,
    kaicene, kia, king-long, ktm, lamborghini, lancia, land-rover,
    leapmotor, lexus, lincoln, lmg, lotus, mahindra, maserati, maxus,
    mazda, mclaren, mercedes-benz, mg, mini, mitsubishi, mitsuoka,
    morgan, morris, naza, neta, nissan, oldsmobile, opel, perodua,
    peugeot, polestar, porsche, proton, renault, rolls-royce, rover,
    saab, seres, shenyang-brilliance, shineray, skoda, smart, ssangyong,
    subaru, sunbeam, sutton, suzuki, tata, td2000, tesla, tq-wuling,
    toyota, triumph, tvr, volkswagen, volvo, weststar, wolseley,
    xpeng, yangtse, zeekr, zxauto

Available makes — Motorcycles (93):
    adiva, afaz, ajs, aprilia, ariic, aveta, benda, benelli, bimota,
    bkz, blueshark, bmw, brixton, buell, can-am, cfmoto, cmc, comel,
    daelim, daiichi, demak, ducati, ebixon, ezi, fantic, gilera, gpx,
    hanway, harley-davidson, honda, husaberg, husqvarna, hyosung, indian,
    italjet, jawa, kamax, kawasaki, kayo, keeway, kove, ktm, ktns, kymco,
    lambretta, laverda, lima, lytron, mbp, mbp-morbidelli, mle, moda,
    modenas, momos, moto-guzzi, moto-morini, mv-agusta, mz, naza, nimota,
    nitro, norton, ottimo, petronas, piaggio, qj-motor, royal-alloy,
    royal-enfield, scomadi, sherco, skyteam, sm-sport, steyr-daimler,
    superlux, suzuki, sym, thunder, tm-moto, treeletrik, triumph, tvs,
    vespa, victory, voge, wmoto, x-moto, x-wedge, yadea, yamaha,
    zeeho, zero-engineering, zesparii, zontes

"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Path setup so direct script invocation works
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
sys.path.insert(0, str(_HERE))

from eagle_client import EagleAPIError, EagleAuthError, EagleClient  # noqa: E402

# Logging setup
_LOGS_DIR = _ROOT / "logs"
_LOGS_DIR.mkdir(parents=True, exist_ok=True)

import reference  # noqa: E402
from reference import (  # noqa: E402
    load_makes as _load_makes,
    load_models as _load_models,
    lookup_make,
    lookup_model,
)

CATEGORY_CHOICES = ["cars", "motorcycles"]
DEFAULT_OUTPUT_DIR = _ROOT / "data" / "raw"

# A single EagleSearch query can only paginate to ~10,000 ads (the depth cap,
# `from >= 10000` returns empty). A make whose total-results exceeds this loses
# the overflow. `--smart` mode splits any such make into one query per model so
# each sub-query stays under the cap. Threshold sits below 10k for safety
# margin: inventory shifts during a run, and totals fluctuate between probe and
# scrape. Empirically (Toyota), per-model totals sum exactly to the make total,
# so the split is lossless — Mudah requires sellers to pick a known model.
MODEL_SPLIT_THRESHOLD = 9500


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class HybridScraper:
    """EagleSearch API scraper — collects listing metadata via Phase 1 only."""

    def __init__(
        self,
        category: str,
        eagle_client: EagleClient,
        output_dir: Path,
    ) -> None:
        if category not in CATEGORY_CHOICES:
            raise ValueError(f"Unknown category: {category!r}")
        self.category = category
        self.eagle = eagle_client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        max_ads: Optional[int] = None,
        resume: bool = False,
        make_id: Optional[str] = None,
        model_id: Optional[str] = None,
        year: Optional[int] = None,
        name_tag: Optional[str] = None,
    ) -> Path:
        """Execute Phase 1 (API collection); return final CSV path.

        Args:
            make_id:  Optional Mudah make ID to filter results.
            model_id: Optional Mudah model ID to filter results.
            year:     Optional manufactured_year to filter results (3rd axis for
                      over-cap models).
            resume:   If True, skip Phase 1 and load the most recent phase1
                      checkpoint CSV in the output dir for this category.
            name_tag: Optional slug (e.g. make slug) embedded in the CSV
                      filename so multi-make runs produce distinguishable files.
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filter_info = (
            f", make_id={make_id}" if make_id else ""
        ) + (
            f", model_id={model_id}" if model_id else ""
        ) + (
            f", year={year}" if year else ""
        )
        logging.info(
            f"[{self.category}] Scraper starting "
            f"(max_ads={max_ads}, resume={resume}{filter_info}, ts={timestamp})"
        )

        checkpoint_paths: List[Path] = []

        if resume:
            resume_path = self._find_resume_checkpoint()
            if resume_path is None:
                raise FileNotFoundError(
                    f"No checkpoint CSV found for category {self.category!r} "
                    f"in {self.output_dir}. Looked for phase1_*.csv."
                )
            ads = self._load_checkpoint(resume_path)
            logging.info(
                f"[{self.category}] Resuming from {resume_path.name} "
                f"({len(ads)} ads loaded)"
            )
            checkpoint_paths.append(resume_path)
            if max_ads is not None:
                logging.info(
                    f"[{self.category}] --max-ads ignored in resume mode "
                    f"(checkpoint defines the working set)"
                )
        else:
            # _phase1_collect writes the phase1 CSV incrementally per page so
            # a mid-run crash still leaves a usable checkpoint on disk.
            try:
                ads = self._phase1_collect(
                    max_ads, timestamp=timestamp,
                    make_id=make_id, model_id=model_id, year=year,
                    name_tag=name_tag,
                )
            except EagleAuthError:
                logging.warning(
                    "EagleSearch returned auth error. Aborting. "
                    "Use src/script.py for HTML-only scraping."
                )
                raise

            if not ads:
                logging.warning(f"[{self.category}] Phase 1 returned 0 ads; aborting")
                return self._write_csv([], timestamp, suffix="empty", name_tag=name_tag)

            phase1_path = (
                self.output_dir
                / self._csv_filename("phase1", timestamp, name_tag)
            )
            logging.info(
                f"[{self.category}] Phase 1 complete — {len(ads)} ads "
                f"checkpointed to {phase1_path.name}"
            )
            checkpoint_paths.append(phase1_path)

        final_path = self._write_csv(ads, timestamp, suffix="final", name_tag=name_tag)
        self._cleanup_checkpoints(checkpoint_paths)
        logging.info(
            f"[{self.category}] Scraper complete — final CSV: {final_path.name}"
        )
        return final_path

    # ------------------------------------------------------------------
    # Phase 1: API collection
    # ------------------------------------------------------------------

    def _phase1_collect(
        self,
        max_ads: Optional[int],
        timestamp: Optional[str] = None,
        make_id: Optional[str] = None,
        model_id: Optional[str] = None,
        year: Optional[int] = None,
        name_tag: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Paginate EagleSearch and return flat list of normalized ad dicts.

        When `timestamp` is provided, the phase1 checkpoint CSV is rewritten
        after each page so a mid-run crash still leaves a usable checkpoint
        on disk (rewriting is cheap with at most ~50 pages).
        """
        all_ads: List[Dict[str, Any]] = []
        for page in self.eagle.fetch_all(
            self.category, max_ads=max_ads,
            make_id=make_id, model_id=model_id, year=year,
        ):
            all_ads.extend(page)
            if max_ads is not None and len(all_ads) >= max_ads:
                all_ads = all_ads[:max_ads]
                if timestamp is not None:
                    self._write_csv(all_ads, timestamp, suffix="phase1", name_tag=name_tag)
                break
            if timestamp is not None:
                self._write_csv(all_ads, timestamp, suffix="phase1", name_tag=name_tag)
        logging.info(f"[{self.category}] Phase 1 collected {len(all_ads)} ads")
        return all_ads

    # ------------------------------------------------------------------
    # CSV output
    # ------------------------------------------------------------------

    def _csv_filename(
        self,
        suffix: str,
        timestamp: str,
        name_tag: Optional[str] = None,
    ) -> str:
        """Build the CSV filename, optionally embedding a name tag (e.g. make slug)."""
        if name_tag:
            return f"mudah_eagle_{self.category}_{name_tag}_{suffix}_{timestamp}.csv"
        return f"mudah_eagle_{self.category}_{suffix}_{timestamp}.csv"

    def _write_csv(
        self,
        ads: List[Dict[str, Any]],
        timestamp: str,
        *,
        suffix: str = "final",
        name_tag: Optional[str] = None,
    ) -> Path:
        """Write ads list to a timestamped CSV in self.output_dir."""
        filename = self._csv_filename(suffix, timestamp, name_tag)
        path = self.output_dir / filename
        df = pd.DataFrame(ads) if ads else pd.DataFrame()
        df.to_csv(path, index=False, encoding="utf-8")
        return path

    # ------------------------------------------------------------------
    # Resume helpers
    # ------------------------------------------------------------------

    def _find_resume_checkpoint(self) -> Optional[Path]:
        """Locate the most recent phase1 checkpoint CSV to resume from."""
        # Match both old (no make tag) and new (with make tag) naming schemes.
        phase1s = sorted(
            list(self.output_dir.glob(f"mudah_eagle_{self.category}_phase1_*.csv"))
            + list(self.output_dir.glob(f"mudah_eagle_{self.category}_*_phase1_*.csv")),
            key=lambda p: p.stat().st_mtime,
        )
        if phase1s:
            return phase1s[-1]
        return None

    def _load_checkpoint(self, path: Path) -> List[Dict[str, Any]]:
        """Read a checkpoint CSV into a list of ad dicts.

        Converts pandas NaN to None so downstream checks treat missing values
        consistently (NaN is truthy, which would skew presence tests).
        """
        df = pd.read_csv(path, encoding="utf-8")
        records = df.to_dict(orient="records")
        for r in records:
            for k, v in list(r.items()):
                if isinstance(v, float) and pd.isna(v):
                    r[k] = None
        return records

    def _cleanup_checkpoints(self, paths: List[Path]) -> None:
        """Remove intermediate checkpoint CSVs after the final one is written."""
        for path in paths:
            try:
                if path.exists():
                    path.unlink()
            except OSError as e:
                logging.warning(f"Could not remove checkpoint {path}: {e}")


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------

def _prompt_choice(label: str, choices: List[str], default_index: int = 0) -> str:
    print(f"\n{label}:")
    for i, choice in enumerate(choices, 1):
        marker = " (default)" if i - 1 == default_index else ""
        print(f"  {i}. {choice}{marker}")
    while True:
        raw = input(f"Select 1-{len(choices)} [default: {default_index + 1}]: ").strip()
        if raw == "":
            return choices[default_index]
        try:
            idx = int(raw)
            if 1 <= idx <= len(choices):
                return choices[idx - 1]
        except ValueError:
            pass
        print(f"Please enter a number between 1 and {len(choices)}.")


def _prompt_max_ads(prompt: str, *, default: Optional[int] = None) -> Optional[int]:
    """Prompt for a max_ads value.

    Returns None to mean 'no caller cap' (the EagleSearch depth cap of
    10,000 per filter still applies — fetch terminates naturally when the
    API returns an empty page).

    Accepts:
      Enter        → `default` if provided, else None (= no cap)
      'max'/'all'  → None (= no cap)
      '0'          → None (= no cap)
      <integer>    → that int
    """
    while True:
        raw = input(prompt).strip().lower()
        if raw == "":
            return default
        if raw in ("max", "all", "0"):
            return None
        try:
            v = int(raw)
            if v < 1:
                print("Enter a positive number, or 'max'/'all'/Enter for no cap.")
                continue
            return v
        except ValueError:
            print("Invalid input. Enter a whole number, or 'max'/'all'/Enter for no cap.")


def _prompt_int(prompt: str, *, min_val: int = 1, default: Optional[int] = None) -> int:
    while True:
        raw = input(prompt).strip()
        if raw == "" and default is not None:
            return default
        try:
            v = int(raw)
            if v < min_val:
                print(f"Enter a number >= {min_val}.")
                continue
            return v
        except ValueError:
            print("Invalid input. Enter a whole number.")


def _prompt_makes_numbered(category: str) -> List[Tuple[str, str, str]]:
    """Show a numbered list of makes; user selects by number, range, or slug.

    Accepts: numbers (1,3,5), ranges (1-10), slugs (toyota,honda), or
    combinations (1-5,honda,8). Press Enter alone to scrape all makes.

    Returns list of (slug, make_id, make_name). Empty list means all makes.
    """
    makes = _load_makes(category)
    if not makes:
        print(f"  No reference data for {category!r} — scraping all makes.")
        return []

    col_w = max(len(m["name"]) for m in makes) + 2
    per_row = 3
    print(f"\nAvailable {category} makes ({len(makes)}):")
    for i, m in enumerate(makes, 1):
        label = f"{i:3}. {m['name']:<{col_w}}"
        print(f"  {label}", end="\n" if i % per_row == 0 or i == len(makes) else "")

    print("\n\nSelect makes to scrape:")
    print("  Numbers / ranges : 1,3,5  |  1-10  |  1-5,8,12-15")
    print("  Slugs            : toyota,honda")
    print("  Press Enter      : scrape ALL makes")

    raw = input("\nSelection: ").strip().lower()
    if not raw:
        return []

    slug_map = {m["slug"]: m for m in makes}
    selected: List[Tuple[str, str, str]] = []
    seen: set = set()

    def _add(make: dict) -> None:
        if make["id"] not in seen:
            seen.add(make["id"])
            selected.append((make["slug"], make["id"], make["name"]))

    for token in (t.strip() for t in raw.split(",") if t.strip()):
        range_m = re.match(r'^(\d+)-(\d+)$', token)
        if range_m:
            lo, hi = int(range_m.group(1)), int(range_m.group(2))
            for idx in range(lo, hi + 1):
                if 1 <= idx <= len(makes):
                    _add(makes[idx - 1])
                else:
                    print(f"  ✗ {idx} out of range (1–{len(makes)})")
        elif token.isdigit():
            idx = int(token)
            if 1 <= idx <= len(makes):
                _add(makes[idx - 1])
            else:
                print(f"  ✗ {idx} out of range (1–{len(makes)})")
        elif token in slug_map:
            _add(slug_map[token])
        else:
            print(f"  ✗ '{token}' not found — skipped")

    if selected:
        print(f"\n  Selected {len(selected)} make(s): " + ", ".join(n for _, _, n in selected))
    return selected


def _resolve_makes_from_cli(category: str, make_arg: str) -> List[Tuple[str, str, str]]:
    """Parse a comma-separated --make value into [(slug, make_id, make_name), ...]."""
    results: List[Tuple[str, str, str]] = []
    for slug in (s.strip().lower() for s in make_arg.split(",") if s.strip()):
        found = lookup_make(category, slug)
        if found:
            name, make_id = found
            results.append((slug, make_id, name))
            print(f"  ✓ {name} (make_id={make_id})")
        else:
            print(f"  ✗ Make '{slug}' not found in reference data — skipped.")
    return results


def _resolve_model(category: str, make_slug: str, model_slug: Optional[str]) -> Optional[str]:
    """Prompt for model if not supplied; return model_id or None."""
    if model_slug is None:
        raw = input(
            f"Filter by model? (slug e.g. 'vios', or Enter for all {make_slug} models): "
        ).strip().lower()
        model_slug = raw or None

    if model_slug:
        result = lookup_model(category, make_slug, model_slug)
        if result:
            model_name, model_id = result
            print(f"  ✓ {model_name} (model_id={model_id})")
            return model_id
        print(f"  ✗ Model '{model_slug}' not found under '{make_slug}' — scraping all models.")
    return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Mudah.my scraper — EagleSearch API"
    )
    p.add_argument("--category", default=None, choices=CATEGORY_CHOICES)
    p.add_argument(
        "--max-ads",
        type=int,
        default=None,
        help="Max ads to collect via API. Prompts if omitted.",
    )
    p.add_argument(
        "--make",
        default=None,
        metavar="SLUG[,SLUG...]",
        help="Filter by make slug(s), comma-separated (e.g. 'toyota' or 'toyota,honda,perodua'). "
             "Prompts with a numbered list if omitted.",
    )
    p.add_argument(
        "--model",
        default=None,
        metavar="SLUG",
        help="Filter by model slug (e.g. 'vios'). Requires --make.",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the latest phase1 checkpoint CSV in output_dir (skip re-scrape)",
    )
    p.add_argument(
        "--all-makes",
        action="store_true",
        help="Scrape every make in the reference data non-interactively (no picker "
             "prompt). max_ads defaults to all available (~10k API cap) unless "
             "--max-ads is given. Used by run_pipeline.bat.",
    )
    p.add_argument(
        "--smart",
        action="store_true",
        help="Adaptive depth-cap evasion. Probes each make's total-results and "
             "splits any make over ~9,500 listings into per-model queries so the "
             "full inventory is captured past the 10k EagleSearch depth cap. "
             "Costs one extra probe call per make; recommended for full scrapes.",
    )
    p.add_argument(
        "--list-active-makes",
        action="store_true",
        help="Probe every make in the reference data and print only those with "
             ">0 active listings, sorted by count. Requires --category. "
             "Takes ~1-2 min (one API call per make, throttled).",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Output dir for CSVs (default: data/raw/<category>/)",
    )
    # Compat flags from script.py — accepted but unused in API mode.
    # They exist so existing shell scripts can be redirected here without breaking.
    p.add_argument("--state", default=None, help="(compat) ignored when API works")
    p.add_argument("--brand", default=None, help="(compat) ignored when API works")
    p.add_argument("--start", type=int, default=None, help="(compat) ignored")
    p.add_argument("--end", type=int, default=None, help="(compat) ignored")
    p.add_argument("--pages", type=int, default=None, help="(compat) ignored")
    p.add_argument("--workers", type=int, default=1, help="(compat) ignored")
    return p.parse_args()


def list_active_makes(category: str, eagle: EagleClient) -> None:
    """Probe each make in the reference data and print those with >0 listings.

    Output is a two-column table sorted by listing count (descending), plus
    a summary line. Makes with 0 listings (or that error out) are skipped.
    """
    makes = _load_makes(category)
    if not makes:
        print(f"No reference data for category {category!r}. "
              f"Expected {reference.makes_path(category)}.")
        return

    print(f"\nProbing {len(makes)} {category} makes via EagleSearch "
          f"(~{len(makes) * 0.75:.0f}s with throttle)...\n")

    results: List[Tuple[str, str, int]] = []  # (slug, name, count)
    for i, make in enumerate(makes, 1):
        slug = make.get("slug", "")
        name = make.get("name", "")
        mid = make.get("id", "")
        try:
            _, meta = eagle.fetch_page(category, offset=0, limit=1, make_id=mid)
            total = int(meta.get("total-results", 0) or 0)
        except Exception as e:
            logging.warning(f"[{slug}] probe failed: {e}")
            continue
        if total > 0:
            results.append((slug, name, total))
        # Lightweight progress indicator every 10 makes
        if i % 10 == 0 or i == len(makes):
            print(f"  ...probed {i}/{len(makes)}", flush=True)

    results.sort(key=lambda r: r[2], reverse=True)
    grand_total = sum(c for _, _, c in results)

    print(f"\nActive makes — {category} ({len(results)} of {len(makes)}):\n")
    print(f"  {'slug':<22} {'name':<24} {'listings':>10}")
    print(f"  {'-' * 22} {'-' * 24} {'-' * 10}")
    for slug, name, count in results:
        print(f"  {slug:<22} {name:<24} {count:>10,}")

    print(
        f"\nTotal active listings across all makes: {grand_total:,}  "
        f"({len(makes) - len(results)} makes have 0 listings)"
    )



def _preview_count(
    eagle: EagleClient,
    category: str,
    make_id: Optional[str],
    model_id: Optional[str],
    year: Optional[int] = None,
) -> Optional[int]:
    """Probe EagleSearch with limit=1 to get total-results for the current filter.

    Returns the total count, or None if the probe call fails (network error,
    API down, etc.) so the caller can fall back to a generic default.
    """
    try:
        _, meta = eagle.fetch_page(
            category, offset=0, limit=1, make_id=make_id, model_id=model_id, year=year
        )
        total = meta.get("total-results")
        return int(total) if total is not None else None
    except Exception as e:
        logging.warning(f"Could not preview total count: {e}")
        return None


# Lower bound for the manufactured-year partition axis. Mudah car listings don't
# predate this in practice; the upper bound is the current year + 1 (next-year
# models appear early). Factored out so tests can stub the range.
_MODEL_YEAR_MIN = 1980


def _model_year_range() -> List[int]:
    """Years to probe when partitioning a single over-cap model by manufactured year."""
    return list(range(_MODEL_YEAR_MIN, datetime.now().year + 2))


def _expand_model_by_year(
    eagle: EagleClient,
    category: str,
    slug: str,
    make_id: str,
    name: str,
    model: Dict[str, Any],
    threshold: int,
) -> List[Tuple[str, str, str, Optional[str], Optional[int]]]:
    """Split a single over-cap model into per-(manufactured)year entries.

    `manufactured_year` partitions a model's listings losslessly (verified: per-year
    counts sum to the model total). Probes each year in `_model_year_range()`, keeps
    only years with listings. A year that itself exceeds the cap can't be split
    further with the wired axes — it's emitted anyway (best effort) with a warning.
    """
    base = (f"{slug}_{model['slug']}", make_id, f"{name} {model['name']}", model["id"], None)
    entries: List[Tuple[str, str, str, Optional[str], Optional[int]]] = []
    for year in _model_year_range():
        yt = _preview_count(eagle, category, make_id, model["id"], year=year)
        if yt is None:
            logging.warning(
                f"[{slug}_{model['slug']}] year={year} probe failed; skipping year"
            )
            continue
        if yt <= 0:
            continue
        if yt > threshold:
            logging.warning(
                f"[{slug}_{model['slug']}] year={year} has {yt:,} listings > "
                f"{threshold:,} cap and cannot be split further — will cap at ~10k"
            )
        entries.append((
            f"{slug}_{model['slug']}_{year}",
            make_id,
            f"{name} {model['name']} {year}",
            model["id"],
            year,
        ))
    # If every year probe failed, fall back to a single model-level entry.
    return entries or [base]


def _expand_capped_makes(
    eagle: EagleClient,
    category: str,
    makes_list: List[Tuple[str, str, str, Optional[str], Optional[int]]],
    threshold: int = MODEL_SPLIT_THRESHOLD,
) -> List[Tuple[str, str, str, Optional[str], Optional[int]]]:
    """Probe each make's total-results; split over-cap makes/models to stay under the cap.

    Takes the standard makes_list of (slug, make_id, name, model_id, year) tuples
    and returns a new list where any make whose total-results exceeds `threshold`
    is replaced by per-model entries; and any single model that *still* exceeds the
    threshold is further split by `manufactured_year` (3rd filter axis). So every
    downstream query stays under the EagleSearch depth cap and the make's full
    inventory is captured.

    Entries already carrying a model_id (user filtered by model explicitly) are left
    untouched — they're already at model granularity. Makes under the threshold pass
    through unchanged (single make-level query).

    Per-model entries use a composite name_tag (`<make>_<model>`, plus `_<year>` when
    year-split) so per-make CSVs don't collide, and a display name "<Make> <Model> [<Year>]".
    """
    models_by_make = _load_models(category)
    expanded: List[Tuple[str, str, str, Optional[str], Optional[int]]] = []
    for slug, make_id, name, model_id, year in makes_list:
        if model_id is not None:
            expanded.append((slug, make_id, name, model_id, year))
            continue

        total = _preview_count(eagle, category, make_id, None)
        if total is None:
            # Probe failed — keep make-level; the scrape itself will retry/log.
            logging.warning(
                f"[{slug}] could not probe total-results; scraping make-level"
            )
            expanded.append((slug, make_id, name, model_id, year))
            continue

        if total <= threshold:
            expanded.append((slug, make_id, name, model_id, year))
            continue

        models = models_by_make.get(slug, [])
        if not models:
            logging.warning(
                f"[{slug}] total-results={total:,} exceeds cap but no models in "
                f"reference data — scraping make-level (will cap at ~10k). "
                f"Run scrape_makes_models.py to refresh models."
            )
            expanded.append((slug, make_id, name, model_id, year))
            continue

        logging.info(
            f"[{slug}] total-results={total:,} > {threshold:,} cap — "
            f"splitting into {len(models)} model-level queries"
        )
        print(
            f"  [split] {name}: {total:,} listings > {threshold:,} cap "
            f"-> {len(models)} model-level queries"
        )
        for m in models:
            mtotal = _preview_count(eagle, category, make_id, m["id"])
            if mtotal is not None and mtotal > threshold:
                logging.info(
                    f"[{slug}_{m['slug']}] model total-results={mtotal:,} > "
                    f"{threshold:,} cap — splitting by manufactured_year"
                )
                print(
                    f"    [year-split] {name} {m['name']}: {mtotal:,} > {threshold:,} "
                    f"-> per-year queries"
                )
                expanded.extend(
                    _expand_model_by_year(eagle, category, slug, make_id, name, m, threshold)
                )
            else:
                expanded.append((
                    f"{slug}_{m['slug']}",
                    make_id,
                    f"{name} {m['name']}",
                    m["id"],
                    None,
                ))
    return expanded


def _ask_max_ads_with_preview(
    eagle: EagleClient,
    category: str,
    make_id: Optional[str],
    model_id: Optional[str],
) -> Optional[int]:
    """Probe the API for total count, print it, then prompt for max_ads.

    Enter / 'max' / 'all' → None (fetch all available, up to ~10k API cap).
    """
    total = _preview_count(eagle, category, make_id, model_id)
    if total is not None:
        cap_note = " (capped at 10,000 — use per-model to get more)" if total > 10_000 else ""
        print(f"\n  Available listings: {total:,}{cap_note}")
    return _prompt_max_ads(
        "Max ads to fetch [Enter / 'max' = all available, ~10k API cap]: "
    )


def _interactive_fill(args: argparse.Namespace, eagle: EagleClient) -> argparse.Namespace:
    print("\n=== Mudah Scraper ===")
    if args.category is None:
        args.category = _prompt_choice("Category", CATEGORY_CHOICES)

    if not args.resume:
        if args.all_makes:
            # Non-interactive all-makes: expand to per-make runs, no picker prompt.
            # max_ads stays as supplied (None = all available; no prompt).
            all_makes = _load_makes(args.category)
            args.makes_list = [(m["slug"], m["id"], m["name"], None, None) for m in all_makes]
            if args.output_dir is None:
                args.output_dir = str(DEFAULT_OUTPUT_DIR / args.category)
            return args

        # Resolve makes list — either from --make CLI arg or interactive picker.
        if args.make is not None:
            makes_list = _resolve_makes_from_cli(args.category, args.make)
        else:
            makes_list = _prompt_makes_numbered(args.category)

        if len(makes_list) == 1:
            # Single make: allow model filter + count preview.
            make_slug, make_id, make_name = makes_list[0]
            model_id = _resolve_model(args.category, make_slug, args.model)
            args.makes_list = [(make_slug, make_id, make_name, model_id, None)]

            if args.max_ads is None:
                args.max_ads = _ask_max_ads_with_preview(eagle, args.category, make_id, model_id)

        elif len(makes_list) == 0:
            # All makes — expand to per-make sequential runs (same as selecting each make).
            all_makes = _load_makes(args.category)
            args.makes_list = [(m["slug"], m["id"], m["name"], None, None) for m in all_makes]
            if args.max_ads is None:
                args.max_ads = _prompt_max_ads(
                    "Max ads per make [Enter / 'max' = all available, ~10k API cap]: "
                )

        else:
            # Multiple makes: no model filter, ask max_ads once (applied per make).
            args.makes_list = [(slug, mid, name, None, None) for slug, mid, name in makes_list]
            if args.max_ads is None:
                args.max_ads = _prompt_max_ads(
                    "Max ads per make [Enter / 'max' = all available, ~10k API cap]: "
                )

    else:
        args.makes_list = []
    if args.output_dir is None:
        args.output_dir = str(DEFAULT_OUTPUT_DIR / args.category)
    return args


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(_LOGS_DIR / "scraper_hybrid.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    args = parse_args()
    eagle = EagleClient()

    # --list-active-makes: discovery-only mode, no scrape.
    if args.list_active_makes:
        category = args.category or _prompt_choice("Category", CATEGORY_CHOICES)
        list_active_makes(category, eagle)
        return

    args = _interactive_fill(args, eagle)

    # --smart: probe each make and split over-cap makes into per-model queries
    # so the 10k depth cap doesn't truncate high-volume makes.
    if getattr(args, "smart", False) and args.makes_list:
        if args.max_ads is not None:
            logging.info(
                "--smart with --max-ads: the cap applies PER model query, "
                "not per make. Omit --max-ads for full coverage."
            )
        print(f"\n[smart] probing {len(args.makes_list)} make(s) for depth-cap splits...")
        args.makes_list = _expand_capped_makes(eagle, args.category, args.makes_list)

    # Compat flag warnings
    deprecated_set = [
        flag for flag, val in (
            ("--state", args.state),
            ("--brand", args.brand),
            ("--start", args.start),
            ("--end", args.end),
            ("--pages", args.pages),
        )
        if val is not None
    ]
    if deprecated_set:
        logging.info(
            f"Ignoring deprecated flags ({', '.join(deprecated_set)}) — "
            f"EagleSearch API does not use them."
        )

    scraper = HybridScraper(
        category=args.category,
        eagle_client=eagle,
        output_dir=Path(args.output_dir),
    )

    cap_display = f"{args.max_ads}" if args.max_ads is not None else "all available (~10k API cap)"

    if not args.makes_list:
        # Resume mode only — makes_list is empty, run without make filter.
        print(f"\nCategory: {args.category} | max_ads: {cap_display} | resuming | output: {args.output_dir}\n")
        final_path = scraper.run(max_ads=args.max_ads, resume=args.resume)
        print(f"\nDone. Output: {final_path}")
    else:
        n = len(args.makes_list)
        scope = "per make" if n > 1 else ""
        print(f"\nCategory: {args.category} | max_ads: {cap_display} {scope} | {n} make(s) | output: {args.output_dir}\n")
        scraped: List[Tuple[str, Path]] = []
        skipped_empty: List[str] = []
        for i, (make_slug, make_id, make_name, model_id, year) in enumerate(args.makes_list, 1):
            print(f"[{i}/{n}] {make_name}")
            try:
                final_path = scraper.run(
                    max_ads=args.max_ads,
                    resume=args.resume,
                    make_id=make_id,
                    model_id=model_id,
                    year=year,
                    name_tag=make_slug,
                )
            except EagleAuthError:
                # Auth is a global failure — don't try the remaining makes.
                raise
            except EagleAPIError as e:
                # "no results at offset=0" → this make has 0 active listings.
                logging.warning(f"[{args.category}] {make_name} skipped: {e}")
                print(f"  ⏭  0 listings — skipped")
                skipped_empty.append(make_name)
                continue
            print(f"  → {final_path}")
            scraped.append((make_name, final_path))

        print(f"\nDone. Scraped {len(scraped)}/{n} make(s).")
        if skipped_empty:
            print(f"Skipped (0 listings): {', '.join(skipped_empty)}")


if __name__ == "__main__":
    main()
