"""Hybrid Mudah scraper — EagleSearch API + HTML detail enrichment.

Two-phase pipeline:
    Phase 1: collect basic metadata via EagleSearch /v1/search (200 ads/req)
    Phase 2: fetch HTML .htm detail page for ads needing depth (body, exact
             mileage, chassis specs)

Designed to replace src/script.py while keeping the CSV-first workflow
compatible. Output goes to data/raw/<category>/ as before.

Usage:
    python src/scraper.py --category cars --max-ads 1000 --depth missing
    python src/scraper.py --category motorcycles --max-ads 500 --depth none
    python src/scraper.py                                   # interactive prompts

--depth modes:
    none     -> API only, fastest, no body/specs
    missing  -> fetch HTML only when body is absent (default)
    all      -> fetch HTML for every ad

API auth fallback:
    If EagleSearch returns 401/403, the orchestrator logs a warning and
    delegates to the existing HTML-only path (script.py-style) so the run
    still produces output. Requires --state/--brand for URL construction.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

# Path setup so direct script invocation works
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
sys.path.insert(0, str(_HERE))

from eagle_client import EagleAuthError, EagleClient  # noqa: E402
from detail_client import DETAIL_FIELDS, DetailClient  # noqa: E402
from mudah_client import MudahClient  # noqa: E402

# Logging setup
_LOGS_DIR = _ROOT / "logs"
_LOGS_DIR.mkdir(parents=True, exist_ok=True)

CATEGORY_CHOICES = ["cars", "motorcycles"]
DEPTH_CHOICES = ["none", "missing", "all"]
DEFAULT_OUTPUT_DIR = _ROOT / "data" / "raw"


# ---------------------------------------------------------------------------
# HybridScraper
# ---------------------------------------------------------------------------

class HybridScraper:
    """Two-phase orchestrator: EagleSearch + HTML detail enrichment."""

    def __init__(
        self,
        category: str,
        eagle_client: EagleClient,
        detail_client: DetailClient,
        output_dir: Path,
    ) -> None:
        if category not in CATEGORY_CHOICES:
            raise ValueError(f"Unknown category: {category!r}")
        self.category = category
        self.eagle = eagle_client
        self.detail = detail_client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        max_ads: Optional[int] = None,
        depth: str = "missing",
        checkpoint_every: int = 100,
    ) -> Path:
        """Execute Phase 1 + Phase 2; return final CSV path."""
        if depth not in DEPTH_CHOICES:
            raise ValueError(f"Invalid depth {depth!r}. Expected {DEPTH_CHOICES}.")

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        logging.info(
            f"[{self.category}] HybridScraper starting "
            f"(max_ads={max_ads}, depth={depth}, ts={timestamp})"
        )

        # ---------- Phase 1: API ----------
        try:
            ads = self._phase1_collect(max_ads)
        except EagleAuthError:
            logging.warning(
                "EagleSearch returned auth error — falling back to HTML-only "
                "mode is not implemented in this orchestrator. Aborting. "
                "Use src/script.py for HTML-only scraping."
            )
            raise

        if not ads:
            logging.warning(f"[{self.category}] Phase 1 returned 0 ads; aborting")
            return self._write_csv([], timestamp, suffix="empty")

        # Phase-1 checkpoint: write CSV immediately so API data survives Phase 2 crash
        phase1_path = self._write_csv(ads, timestamp, suffix="phase1")
        logging.info(
            f"[{self.category}] Phase 1 complete — {len(ads)} ads "
            f"checkpointed to {phase1_path.name}"
        )

        # ---------- Phase 2: HTML enrichment ----------
        if depth == "none":
            logging.info(f"[{self.category}] Phase 2 skipped (depth=none)")
            final_path = self._write_csv(ads, timestamp, suffix="final")
            self._cleanup_checkpoint(phase1_path)
            return final_path

        ads = self._phase2_enrich(ads, depth, checkpoint_every, timestamp)

        final_path = self._write_csv(ads, timestamp, suffix="final")
        self._cleanup_checkpoint(phase1_path)
        logging.info(
            f"[{self.category}] HybridScraper complete — "
            f"final CSV: {final_path.name}"
        )
        return final_path

    # ------------------------------------------------------------------
    # Phase 1: API collection
    # ------------------------------------------------------------------

    def _phase1_collect(self, max_ads: Optional[int]) -> List[Dict[str, Any]]:
        """Paginate EagleSearch and return flat list of normalized ad dicts."""
        all_ads: List[Dict[str, Any]] = []
        for page in self.eagle.fetch_all(self.category, max_ads=max_ads):
            all_ads.extend(page)
            if max_ads is not None and len(all_ads) >= max_ads:
                all_ads = all_ads[:max_ads]
                break
        logging.info(f"[{self.category}] Phase 1 collected {len(all_ads)} ads")
        return all_ads

    # ------------------------------------------------------------------
    # Phase 2: HTML enrichment
    # ------------------------------------------------------------------

    def _needs_detail(self, ad: Dict[str, Any], depth: str) -> bool:
        """Return True if this ad should have its HTML detail page fetched.

        depth="none"    -> always False
        depth="all"     -> always True
        depth="missing" -> True when ad['body'] is absent or empty
        """
        if depth == "none":
            return False
        if depth == "all":
            return True
        # depth == "missing"
        return not ad.get("body")

    def _phase2_enrich(
        self,
        ads: List[Dict[str, Any]],
        depth: str,
        checkpoint_every: int,
        timestamp: str,
    ) -> List[Dict[str, Any]]:
        """Fetch HTML detail for ads needing it. Mutates ads in place + returns."""
        targets = [(i, ad) for i, ad in enumerate(ads) if self._needs_detail(ad, depth)]
        skipped = len(ads) - len(targets)
        logging.info(
            f"[{self.category}] Phase 2: enriching {len(targets)} ads "
            f"(skipping {skipped} already-complete or out of scope)"
        )

        for done_n, (idx, ad) in enumerate(targets, start=1):
            url = ad.get("url")
            ads_id = ad.get("ads_id")
            if not url or ads_id is None:
                logging.warning(
                    f"[{self.category}] Phase 2: ad missing url/ads_id "
                    f"(ads_id={ads_id}, url={url}); marking error"
                )
                ad["detail_fetch_status"] = "error"
                ad["last_detail_fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                continue

            detail = self.detail.fetch(url, int(ads_id))
            ad.update(detail)

            status = detail.get("detail_fetch_status", "?")
            log_interval = max(1, min(10, len(targets) // 10))
            if done_n % log_interval == 0 or done_n == 1 or done_n == len(targets):
                logging.info(
                    f"[{self.category}] Phase 2: {done_n}/{len(targets)} "
                    f"ads_id={ads_id} status={status}"
                )

            if done_n % checkpoint_every == 0:
                self._write_csv(ads, timestamp, suffix=f"phase2_partial_{done_n}")
                logging.info(
                    f"[{self.category}] Phase 2 progress: {done_n}/{len(targets)} "
                    f"detail fetches completed (checkpointed)"
                )

        # Mark non-targets as 'skipped' so DB layer can distinguish from 'error'
        for i, ad in enumerate(ads):
            if "detail_fetch_status" not in ad:
                ad["detail_fetch_status"] = "skipped"

        return ads

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
        """Write ads list to a timestamped CSV in self.output_dir.

        Filename:
            mudah_eagle_{category}_{suffix}_{timestamp}.csv
        """
        filename = f"mudah_eagle_{self.category}_{suffix}_{timestamp}.csv"
        path = self.output_dir / filename
        df = pd.DataFrame(ads) if ads else pd.DataFrame()
        df.to_csv(path, index=False, encoding="utf-8")
        return path

    def _cleanup_checkpoint(self, path: Path) -> None:
        """Remove an intermediate checkpoint CSV after the final one is written."""
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Hybrid Mudah.my scraper (EagleSearch API + HTML detail enrichment)"
    )
    p.add_argument("--category", default=None, choices=CATEGORY_CHOICES)
    p.add_argument(
        "--max-ads",
        type=int,
        default=None,
        help="Max ads to collect via API (Phase 1 cap). Prompts if omitted.",
    )
    p.add_argument(
        "--depth",
        choices=DEPTH_CHOICES,
        default="missing",
        help="HTML enrichment depth: none, missing (default), all",
    )
    p.add_argument(
        "--checkpoint-every",
        type=int,
        default=100,
        help="Write Phase-2 checkpoint CSV every N detail fetches (default: 100)",
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


def _interactive_fill(args: argparse.Namespace) -> argparse.Namespace:
    print("\n=== Hybrid Mudah Scraper ===")
    if args.category is None:
        args.category = _prompt_choice("Category", CATEGORY_CHOICES)
    if args.max_ads is None:
        args.max_ads = _prompt_int(
            "Max ads to fetch [default: 1000]: ", min_val=1, default=1000
        )
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
    args = _interactive_fill(args)

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

    print(
        f"\nCategory: {args.category} | max_ads: {args.max_ads} | "
        f"depth: {args.depth} | output: {args.output_dir}\n"
    )

    eagle = EagleClient()
    mudah = MudahClient()
    detail = DetailClient(mudah)

    scraper = HybridScraper(
        category=args.category,
        eagle_client=eagle,
        detail_client=detail,
        output_dir=Path(args.output_dir),
    )

    final_path = scraper.run(
        max_ads=args.max_ads,
        depth=args.depth,
        checkpoint_every=args.checkpoint_every,
    )
    print(f"\nDone. Output: {final_path}")


if __name__ == "__main__":
    main()
