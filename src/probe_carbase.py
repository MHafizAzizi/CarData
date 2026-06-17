"""Recon spike for carbase.my as a car-spec source.

NOT part of the pipeline. A one-off probe to decide whether to build a
`model_specs` enrichment fetcher against carbase.my (paultan.org's MY car
buyer's guide). Answers the open questions from the 2026-06-17 recon:

  1. Cloudflare / bot-block behavior  -> does cloudscraper get through?
  2. Render mode                      -> __NEXT_DATA__ JSON, ld+json, or raw HTML?
  3. Rate-limit behavior              -> time N sequential fetches, watch for 403/429
  4. Spec extraction feasibility      -> can we pull the spec table off a variant page?
  5. Sizing                           -> brand -> model -> variant fan-out for one brand
  6. Market-value (NVIC) tool         -> can the used-car valuation cascade be reached?

URL structure (confirmed via WebFetch):
    /<make>/<model>/<generation>/<variant-year>   e.g.
    /honda/civic/fe-facelift/1.5-rs-turbo-2025
robots.txt disallows ONLY /cars-for-sale/ (their classifieds) -- spec pages
are crawlable.

Market-value-guide (`/tool/car-market-value-guide`) recon findings:
    Cascading dropdowns POST to ISM/NVIC endpoints, all under /ism/:
        /ism/ajax-get-model            (make -> models)
        /ism/ajax-get-year
        /ism/ajax-get-engine-capacity
        /ism/ajax-get-transmission
        /ism/ajax-get-generation
        /ism/ajax-get-valuation-result -> JSON
    Valuation JSON fields: nvic, year, make, family, variant, style, cc,
        wm_rrr (West-MY value), em_rrr (East-MY value), wm_new_pr (new price),
        + valuation date. Official NVIC used-car valuations.
    BLOCKER (static client): every /ism/ajax-get-* returns an EMPTY <option>
    list via cloudscraper regardless of param (tried slug `honda`, carbase
    data-id `24`, name `Honda`, make_id=, brand=, GET+POST). The value-guide
    make dropdown is itself JS-seeded (empty in static HTML; the populated
    74-make list w/ data-id lives in the separate /search form), and there is
    NO /ism/ajax-get-make endpoint (404). Conclusion: the cascade is gated by
    browser-seeded session state / an internal NVIC make code we can't derive
    statically. Reversing it needs real-browser XHR capture (Chrome devtools /
    Chrome MCP) to read the actual request payloads. probe_market_value()
    reproduces the dead-end so the finding is verifiable, not just asserted.

Usage:
    python src/probe_carbase.py                 # full recon (default honda)
    python src/probe_carbase.py --make toyota   # size a different brand
    python src/probe_carbase.py --dump-html      # also write raw HTML samples
    python src/probe_carbase.py --skip-specs     # only run the market-value probe

Writes findings + raw HTML samples to data/raw/carbase_probe/.
"""

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import cloudscraper
from bs4 import BeautifulSoup

_ROOT = Path(__file__).resolve().parent.parent
_OUT_DIR = _ROOT / "data" / "raw" / "carbase_probe"

_BASE = "https://www.carbase.my"

# Polite pacing for an unfamiliar site -- start slow, the spike measures whether
# we can safely go faster. carlist.my recon (sibling project) found ~1.5s/page
# seq was safe; treat that as the optimistic floor.
_SLEEP = 2.0
_TIMEOUT = 20.0


def _new_scraper():
    return cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )


def fetch(scraper, path: str) -> tuple[int, str, float]:
    """GET a carbase path. Returns (status_code, body_text, elapsed_seconds).

    Does not raise on HTTP errors -- the whole point is to observe them.
    """
    url = urljoin(_BASE, path)
    t0 = time.monotonic()
    try:
        resp = scraper.get(url, timeout=_TIMEOUT)
        elapsed = time.monotonic() - t0
        return resp.status_code, resp.text, elapsed
    except Exception as e:  # noqa: BLE001 -- recon: log and keep going
        elapsed = time.monotonic() - t0
        logging.warning(f"fetch failed {url}: {e}")
        return -1, "", elapsed


def detect_render_mode(html: str) -> dict:
    """Classify how the page ships its data: __NEXT_DATA__ / ld+json / raw HTML."""
    soup = BeautifulSoup(html, "html.parser")

    next_data = soup.find("script", id="__NEXT_DATA__")
    ld_json = soup.find_all("script", type="application/ld+json")

    next_keys = []
    if next_data and next_data.string:
        try:
            blob = json.loads(next_data.string)
            # surface the useful nesting so we know where specs would live
            page_props = blob.get("props", {}).get("pageProps", {})
            next_keys = list(page_props.keys())
        except (json.JSONDecodeError, AttributeError):
            next_keys = ["<present but unparseable>"]

    ld_types = []
    for tag in ld_json:
        if not tag.string:
            continue
        try:
            obj = json.loads(tag.string)
            objs = obj if isinstance(obj, list) else [obj]
            ld_types += [o.get("@type") for o in objs if isinstance(o, dict)]
        except json.JSONDecodeError:
            ld_types.append("<unparseable>")

    return {
        "has_next_data": next_data is not None,
        "next_pageProps_keys": next_keys,
        "ld_json_count": len(ld_json),
        "ld_json_types": ld_types,
        "html_bytes": len(html),
    }


def extract_variant_links(html: str, make: str) -> list[str]:
    """Pull variant-spec hrefs (/make/model/generation/variant-year) from a page.

    Real variant pages end in a -YYYY year suffix (e.g. .../rs-2023). The same
    4-segment depth also hosts noise pages (user-review, exterior, interior,
    owner-reviews, generations) -- the year anchor excludes them.
    """
    soup = BeautifulSoup(html, "html.parser")
    pat = re.compile(rf"^/{re.escape(make)}/[^/]+/[^/]+/[^/]+-(?:19|20)\d\d/?$")
    seen, out = set(), []
    for a in soup.find_all("a", href=True):
        href = a["href"].split("?")[0].split("#")[0]
        if pat.match(href) and href not in seen:
            seen.add(href)
            out.append(href)
    return out


def extract_model_links(html: str, make: str) -> list[str]:
    """Pull model hrefs (/make/model) from a brand page."""
    soup = BeautifulSoup(html, "html.parser")
    pat = re.compile(rf"^/{re.escape(make)}/[^/]+/?$")
    seen, out = set(), []
    for a in soup.find_all("a", href=True):
        href = a["href"].split("?")[0].split("#")[0]
        if pat.match(href) and href not in seen:
            seen.add(href)
            out.append(href)
    return out


def count_spec_fields(html: str) -> dict:
    """Heuristic: how many label:value spec rows does a variant page expose?

    carbase renders specs as label/value pairs. We don't need a perfect parser
    for the spike -- just confirm the table is in the static HTML and roughly
    how rich it is. Looks for known spec labels.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    markers = [
        "Displacement", "Torque", "Kerb Weight", "Wheelbase", "Compression Ratio",
        "Bore", "Fuel Tank", "Boot", "Front Tyres", "Front Suspension",
        "Top Speed", "Power", "Transmission", "Driveline", "Seating",
    ]
    found = [m for m in markers if m in text]
    return {"spec_markers_found": found, "marker_hits": len(found), "total_markers": len(markers)}


# --- market-value (NVIC) recon ---------------------------------------------------
_ISM_ENDPOINTS = [
    "/ism/ajax-get-model",
    "/ism/ajax-get-year",
    "/ism/ajax-get-engine-capacity",
    "/ism/ajax-get-transmission",
    "/ism/ajax-get-generation",
    "/ism/ajax-get-valuation-result",
]
_VALUATION_JSON_FIELDS = [
    "nvic", "year", "make", "family", "variant", "style", "cc",
    "wm_rrr", "em_rrr", "wm_new_pr",
]


def probe_market_value(scraper) -> dict:
    """Recon the used-car NVIC valuation cascade. Documents the static dead-end.

    Visits the tool page (to seed cookies), then fires the discovered /ism/
    endpoints with every make-value format we can derive. Records the responses
    so the "needs browser capture" conclusion is reproducible rather than
    asserted. Returns a findings dict.
    """
    out: dict = {
        "tool_url": "/tool/car-market-value-guide",
        "ism_endpoints": _ISM_ENDPOINTS,
        "valuation_json_fields": _VALUATION_JSON_FIELDS,
    }

    logging.info("[6] GET /tool/car-market-value-guide (seed cookies + read form)")
    status, html, _ = fetch(scraper, "/tool/car-market-value-guide")
    out["tool_page_status"] = status
    if status != 200:
        out["result"] = f"tool page non-200 ({status})"
        return out

    # the populated 74-make list (slug + data-id) lives in the /search form,
    # NOT the value-guide form (whose make select is JS-seeded / empty)
    soup = BeautifulSoup(html, "html.parser")
    search_form = soup.find("form", attrs={"name": "search-form"})
    make_opts = []
    if search_form:
        sel = search_form.find("select", attrs={"name": "make"})
        if sel:
            for o in sel.find_all("option"):
                if o.get("value"):
                    make_opts.append({"slug": o.get("value"), "data_id": o.get("data-id"),
                                      "name": o.get_text(strip=True)})
    out["search_form_make_count"] = len(make_opts)

    vg_form = soup.find("form", attrs={"name": "car_market_value_guide_form"})
    vg_make = vg_form.find("select", attrs={"name": "make"}) if vg_form else None
    out["value_guide_make_options_in_static_html"] = (
        len(vg_make.find_all("option")) if vg_make else 0
    )

    # try /ism/ajax-get-model with every plausible make-value format
    honda = next((m for m in make_opts if m["name"] == "Honda"), None)
    hid = honda["data_id"] if honda else "24"
    hdr = {"X-Requested-With": "XMLHttpRequest",
           "Referer": urljoin(_BASE, "/tool/car-market-value-guide")}
    trials = [
        {"make": "honda"}, {"make": hid}, {"make_id": hid},
        {"brand": hid}, {"make": "Honda"},
    ]
    attempts = []
    for t in trials:
        try:
            resp = scraper.post(urljoin(_BASE, "/ism/ajax-get-model"),
                                data=t, headers=hdr, timeout=_TIMEOUT)
            body = resp.text.strip()
            opt_count = body.count("<option")
            attempts.append({"params": t, "status": resp.status_code,
                             "option_count": opt_count, "body_head": body[:80]})
        except Exception as e:  # noqa: BLE001
            attempts.append({"params": t, "error": str(e)})
    out["get_model_attempts"] = attempts

    # is there a make-list endpoint? (expect 404)
    probe404 = {}
    for p in ("/ism/ajax-get-make", "/ism/ajax-get-brand"):
        try:
            resp = scraper.post(urljoin(_BASE, p), data={}, headers=hdr, timeout=_TIMEOUT)
            probe404[p] = resp.status_code
        except Exception as e:  # noqa: BLE001
            probe404[p] = str(e)
    out["make_list_endpoint_probe"] = probe404

    populated = any(a.get("option_count", 0) > 1 for a in attempts)
    out["result"] = (
        "REACHABLE -- a make-value format returned models (revisit build!)"
        if populated else
        "DEAD-END (static) -- all /ism/ajax-get-model calls returned empty; "
        "cascade needs browser-seeded session. Capture XHR payloads via a real "
        "browser (Chrome MCP / devtools) to reverse the NVIC valuation tool."
    )
    return out


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    ap = argparse.ArgumentParser(description="carbase.my recon spike")
    ap.add_argument("--make", default="honda", help="brand slug to size (default honda)")
    ap.add_argument("--dump-html", action="store_true", help="write raw HTML samples")
    ap.add_argument("--max-models", type=int, default=5, help="models to crawl when sizing")
    ap.add_argument("--skip-specs", action="store_true", help="only run the market-value probe")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    scraper = _new_scraper()
    findings: dict = {"make": args.make, "base": _BASE}

    if args.skip_specs:
        findings["market_value"] = probe_market_value(scraper)
        logging.info(f"market-value: {findings['market_value']['result']}")
        (_OUT_DIR / "findings.json").write_text(
            json.dumps(findings, indent=2, ensure_ascii=False), encoding="utf-8")
        logging.info(f"findings -> {(_OUT_DIR / 'findings.json').relative_to(_ROOT)}")
        return

    # --- Q1+Q2: hit the brand page, observe status + render mode ----------------
    logging.info(f"[1] GET /{args.make} (brand page)")
    status, html, elapsed = fetch(scraper, f"/{args.make}")
    findings["brand_page"] = {"status": status, "elapsed_s": round(elapsed, 2)}
    logging.info(f"    status={status} {elapsed:.2f}s {len(html)}B")
    if status != 200:
        logging.error(f"    brand page non-200 ({status}) -- Cloudflare block? aborting recon")
        findings["verdict"] = "BLOCKED at brand page"
        (_OUT_DIR / "findings.json").write_text(json.dumps(findings, indent=2), encoding="utf-8")
        return

    findings["brand_render"] = detect_render_mode(html)
    logging.info(f"    render: {findings['brand_render']}")
    if args.dump_html:
        (_OUT_DIR / f"{args.make}_brand.html").write_text(html, encoding="utf-8")

    model_links = extract_model_links(html, args.make)
    findings["model_link_count"] = len(model_links)
    findings["model_links_sample"] = model_links[:10]
    logging.info(f"    found {len(model_links)} model links")

    # --- Q3+Q5: crawl a few models, collect variants, time every request --------
    time.sleep(_SLEEP)
    variant_links: list[str] = []
    timings: list[float] = []
    statuses: list[int] = []
    for mlink in model_links[: args.max_models]:
        logging.info(f"[3] GET {mlink} (model page)")
        st, mhtml, el = fetch(scraper, mlink)
        statuses.append(st)
        timings.append(el)
        logging.info(f"    status={st} {el:.2f}s")
        if st == 200:
            vlinks = extract_variant_links(mhtml, args.make)
            variant_links += vlinks
            logging.info(f"    +{len(vlinks)} variant links")
        time.sleep(_SLEEP)

    variant_links = list(dict.fromkeys(variant_links))  # dedupe, keep order
    findings["variant_links_from_sample"] = len(variant_links)
    findings["variant_links_sample"] = variant_links[:10]

    # --- Q4: fetch ONE variant page, prove specs are in static HTML -------------
    spec_findings = {}
    if variant_links:
        vtarget = variant_links[0]
        logging.info(f"[4] GET {vtarget} (variant spec page)")
        st, vhtml, el = fetch(scraper, vtarget)
        statuses.append(st)
        timings.append(el)
        logging.info(f"    status={st} {el:.2f}s {len(vhtml)}B")
        if st == 200:
            spec_findings = count_spec_fields(vhtml)
            spec_findings["render"] = detect_render_mode(vhtml)
            spec_findings["url"] = vtarget
            logging.info(
                f"    spec markers: {spec_findings['marker_hits']}/{spec_findings['total_markers']} "
                f"{spec_findings['spec_markers_found']}"
            )
            if args.dump_html:
                (_OUT_DIR / "variant_sample.html").write_text(vhtml, encoding="utf-8")
    findings["variant_spec"] = spec_findings

    # --- summary ----------------------------------------------------------------
    ok = [t for s, t in zip(statuses, timings) if s == 200]
    findings["timing"] = {
        "requests": len(timings),
        "http_200": statuses.count(200),
        "http_403": statuses.count(403),
        "http_429": statuses.count(429),
        "avg_s_per_ok_req": round(sum(ok) / len(ok), 2) if ok else None,
        "max_s": round(max(timings), 2) if timings else None,
    }

    # rough fan-out estimate for the whole site
    if model_links and variant_links:
        v_per_model = findings["variant_links_from_sample"] / min(args.max_models, len(model_links))
        findings["sizing_estimate"] = {
            "variants_per_model_observed": round(v_per_model, 1),
            "note": "x (models in your listings DB) = variant pages to fetch. "
                    "Specs are variant-level; fetch only DB-present combos.",
        }

    # --- Q6: market-value (NVIC) valuation cascade ------------------------------
    time.sleep(_SLEEP)
    findings["market_value"] = probe_market_value(scraper)
    logging.info(f"[6] market-value: {findings['market_value']['result']}")

    findings["verdict"] = (
        "PROCEED -- specs in static HTML, no block observed"
        if spec_findings.get("marker_hits", 0) >= 8 and findings["timing"]["http_403"] == 0
        else "INVESTIGATE -- see findings"
    )

    (_OUT_DIR / "findings.json").write_text(
        json.dumps(findings, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    logging.info(f"verdict: {findings['verdict']}")
    logging.info(f"findings -> {(_OUT_DIR / 'findings.json').relative_to(_ROOT)}")


if __name__ == "__main__":
    main()
