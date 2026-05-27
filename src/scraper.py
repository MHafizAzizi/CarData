"""Mudah scraper — EagleSearch API (Phase 1 only).

Collects car/motorcycle listing metadata via the EagleSearch JSON API
(search.mudah.my/v1/search, 200 ads/req). Writes a timestamped CSV per run.

Usage:
    python src/scraper.py --category cars --max-ads 1000
    python src/scraper.py --category cars --make toyota --max-ads 5000
    python src/scraper.py --category cars --make toyota --model vios
    python src/scraper.py                          # interactive prompts

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
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Path setup so direct script invocation works
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
sys.path.insert(0, str(_HERE))

from eagle_client import EagleAuthError, EagleClient  # noqa: E402

# Logging setup
_LOGS_DIR = _ROOT / "logs"
_LOGS_DIR.mkdir(parents=True, exist_ok=True)

CATEGORY_CHOICES = ["cars", "motorcycles"]
DEFAULT_OUTPUT_DIR = _ROOT / "data" / "raw"
_REF_DIR = _ROOT / "data" / "reference"


# ---------------------------------------------------------------------------
# Reference data helpers (make/model lookup)
# ---------------------------------------------------------------------------

def _load_makes(category: str) -> List[Dict]:
    """Load makes list for the given category from reference JSON."""
    path = _REF_DIR / f"{category}_makes.json"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_models(category: str) -> Dict[str, List[Dict]]:
    """Load models dict {make_slug: [{id, name, slug}]} for the given category."""
    path = _REF_DIR / f"{category}_models.json"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def lookup_make(category: str, slug: str) -> Optional[Tuple[str, str]]:
    """Return (name, id) for the given make slug, or None if not found."""
    for make in _load_makes(category):
        if make.get("slug") == slug:
            return make["name"], make["id"]
    return None


def lookup_model(category: str, make_slug: str, model_slug: str) -> Optional[Tuple[str, str]]:
    """Return (name, id) for the given model slug under a make, or None if not found."""
    models = _load_models(category)
    for model in models.get(make_slug, []):
        if model.get("slug") == model_slug:
            return model["name"], model["id"]
    return None


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
    ) -> Path:
        """Execute Phase 1 (API collection); return final CSV path.

        Args:
            make_id:  Optional Mudah make ID to filter results.
            model_id: Optional Mudah model ID to filter results.
            resume:   If True, skip Phase 1 and load the most recent phase1
                      checkpoint CSV in the output dir for this category.
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filter_info = (
            f", make_id={make_id}" if make_id else ""
        ) + (
            f", model_id={model_id}" if model_id else ""
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
                    make_id=make_id, model_id=model_id,
                )
            except EagleAuthError:
                logging.warning(
                    "EagleSearch returned auth error. Aborting. "
                    "Use src/script.py for HTML-only scraping."
                )
                raise

            if not ads:
                logging.warning(f"[{self.category}] Phase 1 returned 0 ads; aborting")
                return self._write_csv([], timestamp, suffix="empty")

            phase1_path = (
                self.output_dir
                / f"mudah_eagle_{self.category}_phase1_{timestamp}.csv"
            )
            logging.info(
                f"[{self.category}] Phase 1 complete — {len(ads)} ads "
                f"checkpointed to {phase1_path.name}"
            )
            checkpoint_paths.append(phase1_path)

        final_path = self._write_csv(ads, timestamp, suffix="final")
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
    ) -> List[Dict[str, Any]]:
        """Paginate EagleSearch and return flat list of normalized ad dicts.

        When `timestamp` is provided, the phase1 checkpoint CSV is rewritten
        after each page so a mid-run crash still leaves a usable checkpoint
        on disk (rewriting is cheap with at most ~50 pages).
        """
        all_ads: List[Dict[str, Any]] = []
        for page in self.eagle.fetch_all(
            self.category, max_ads=max_ads,
            make_id=make_id, model_id=model_id,
        ):
            all_ads.extend(page)
            if max_ads is not None and len(all_ads) >= max_ads:
                all_ads = all_ads[:max_ads]
                if timestamp is not None:
                    self._write_csv(all_ads, timestamp, suffix="phase1")
                break
            if timestamp is not None:
                self._write_csv(all_ads, timestamp, suffix="phase1")
        logging.info(f"[{self.category}] Phase 1 collected {len(all_ads)} ads")
        return all_ads

    # ------------------------------------------------------------------
    # CSV output
    # ------------------------------------------------------------------

    def _write_csv(
        self,
        ads: List[Dict[str, Any]],
        timestamp: str,
        *,
        suffix: str = "final",
    ) -> Path:
        """Write ads list to a timestamped CSV in self.output_dir."""
        filename = f"mudah_eagle_{self.category}_{suffix}_{timestamp}.csv"
        path = self.output_dir / filename
        df = pd.DataFrame(ads) if ads else pd.DataFrame()
        df.to_csv(path, index=False, encoding="utf-8")
        return path

    # ------------------------------------------------------------------
    # Resume helpers
    # ------------------------------------------------------------------

    def _find_resume_checkpoint(self) -> Optional[Path]:
        """Locate the most recent phase1 checkpoint CSV to resume from."""
        phase1s = sorted(
            self.output_dir.glob(f"mudah_eagle_{self.category}_phase1_*.csv"),
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


def _prompt_make_model(
    category: str,
    make_slug: Optional[str],
    model_slug: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Interactive make/model prompt. Returns (make_id, model_id) or (None, None)."""
    # --- Make ---
    if make_slug is None:
        raw = input("\nFilter by make? (slug e.g. 'toyota', or Enter to scrape all): ").strip().lower()
        make_slug = raw or None

    make_id: Optional[str] = None
    if make_slug:
        result = lookup_make(category, make_slug)
        if result:
            make_name, make_id = result
            print(f"  ✓ {make_name} (make_id={make_id})")
        else:
            print(f"  ✗ Make '{make_slug}' not found in reference data — scraping all makes.")
            make_slug = None

    # --- Model (only if a valid make was found) ---
    model_id: Optional[str] = None
    if make_slug:
        if model_slug is None:
            raw = input(f"Filter by model? (slug e.g. 'vios', or Enter for all {make_slug} models): ").strip().lower()
            model_slug = raw or None

        if model_slug:
            result = lookup_model(category, make_slug, model_slug)
            if result:
                model_name, model_id = result
                print(f"  ✓ {model_name} (model_id={model_id})")
            else:
                print(f"  ✗ Model '{model_slug}' not found under '{make_slug}' — scraping all models.")

    return make_id, model_id


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
        metavar="SLUG",
        help="Filter by make slug (e.g. 'toyota'). Prompts if omitted.",
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


def _preview_count(
    eagle: EagleClient,
    category: str,
    make_id: Optional[str],
    model_id: Optional[str],
) -> Optional[int]:
    """Probe EagleSearch with limit=1 to get total-results for the current filter.

    Returns the total count, or None if the probe call fails (network error,
    API down, etc.) so the caller can fall back to a generic default.
    """
    try:
        _, meta = eagle.fetch_page(
            category, offset=0, limit=1, make_id=make_id, model_id=model_id
        )
        total = meta.get("total-results")
        return int(total) if total is not None else None
    except Exception as e:
        logging.warning(f"Could not preview total count: {e}")
        return None


def _interactive_fill(args: argparse.Namespace, eagle: EagleClient) -> argparse.Namespace:
    print("\n=== Mudah Scraper ===")
    if args.category is None:
        args.category = _prompt_choice("Category", CATEGORY_CHOICES)
    # In resume mode the checkpoint defines the working set, so skip prompts.
    if not args.resume:
        # Make/model first, so we can preview the count before asking max_ads.
        args.make_id, args.model_id = _prompt_make_model(
            args.category, args.make, args.model
        )

        # Probe API for total available listings under this filter.
        if args.max_ads is None:
            total = _preview_count(eagle, args.category, args.make_id, args.model_id)
            if total is not None:
                # Inform the user about the depth cap if relevant.
                fetchable = min(total, 10_000)
                cap_note = " (capped at 10,000 — use per-model to get more)" if total > 10_000 else ""
                print(f"\n  Available listings: {total:,}{cap_note}")
                default = fetchable if fetchable > 0 else 1000
                args.max_ads = _prompt_int(
                    f"Max ads to fetch [default: {default} = all available]: ",
                    min_val=1,
                    default=default,
                )
            else:
                args.max_ads = _prompt_int(
                    "Max ads to fetch [default: 1000]: ", min_val=1, default=1000
                )
    else:
        args.make_id = None
        args.model_id = None
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
    args = _interactive_fill(args, eagle)

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

    make_label = f"make_id={args.make_id}" if args.make_id else "all makes"
    model_label = f"model_id={args.model_id}" if args.model_id else "all models"
    print(
        f"\nCategory: {args.category} | max_ads: {args.max_ads} | "
        f"{make_label} | {model_label} | output: {args.output_dir}\n"
    )

    scraper = HybridScraper(
        category=args.category,
        eagle_client=eagle,
        output_dir=Path(args.output_dir),
    )

    final_path = scraper.run(
        max_ads=args.max_ads,
        resume=args.resume,
        make_id=args.make_id,
        model_id=args.model_id,
    )
    print(f"\nDone. Output: {final_path}")


if __name__ == "__main__":
    main()
