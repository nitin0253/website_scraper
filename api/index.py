"""
Multi-Rooftop Inventory Scraper  (parallel edition)
=====================================================
Reads a CSV of dealership rooftops and scrapes new + used inventory
(VIN + first image URL) from each one.

Speed settings (Safe & Fast mode):
  ROOFTOP_WORKERS = 3   — 3 rooftops scraped simultaneously
  PAGE_WORKERS    = 4   — 4 pages fetched in parallel per rooftop
  CRAWL_DELAY     = 0.3 — seconds between page batches

Supported platforms (auto-detected):
  1. Dealer.com / DDC  — /apis/widget/INVENTORY_LISTING.../getInventory
  2. DealerOn          — /api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/...

Input CSV columns used:
  Enterprise_Name, Rooftop_Name, New_url, used_url

Output: one pair of CSVs per rooftop
  {rooftop_slug}_new.csv
  {rooftop_slug}_used.csv

Each CSV has columns:
  enterprise_name, rooftop_name, condition, vin, first_image_url

Install:  pip install requests
Run:      python3 multi_rooftop_scraper.py
          python3 multi_rooftop_scraper.py --csv path/to/your.csv
          python3 multi_rooftop_scraper.py --csv path/to/your.csv --output ./results
          python3 multi_rooftop_scraper.py --rooftop "Genesis"   (single rooftop test)
"""

import argparse
import csv
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ── Speed settings ───────────────────────────────────────────────────────────
ROOFTOP_WORKERS = 3    # rooftops scraped in parallel
PAGE_WORKERS    = 4    # pages fetched in parallel per condition (new/used)
CRAWL_DELAY     = 0.3  # seconds between page batches

# ── HTTP settings ────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

MAX_RETRIES  = 5
BACKOFF_BASE = 10  # seconds; doubles each retry: 10, 20, 40, 80, 160

# Thread-safe print lock (prevents garbled output from parallel threads)
_print_lock = threading.Lock()

def tprint(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs)


# ════════════════════════════════════════════════════════════════════════════
# PLATFORM DETECTION
# ════════════════════════════════════════════════════════════════════════════

def detect_platform(url: str) -> str | None:
    """
    Fetch the inventory page HTML and look for platform fingerprints.
    Returns 'dealeron', 'ddc', or None (unknown).

    Strategy:
      1. Try HTML fingerprint (fast, works when page is accessible)
      2. Always fallback to direct DDC API probe (works even when HTML returns 403)
      3. Always fallback to direct DealerOn API probe
    """
    base_match = re.match(r"(https?://[^/]+)", url)
    base = base_match.group(1) if base_match else ""

    # ── Step 1: HTML fingerprint ─────────────────────────────────────────
    html_platform = None
    try:
        r = requests.get(
            url,
            headers={**HEADERS, "Accept": "text/html"},
            timeout=20,
            allow_redirects=True,
        )
        if r.status_code == 200:
            html = r.text.lower()
            if "vhcliaa" in html or "dealeron" in html:
                html_platform = "dealeron"
            elif "inventory_listing" in html or "/apis/widget/" in html or "dealer.com" in html:
                html_platform = "ddc"
        # Non-200 (403, etc.) — fall through to API probes below
    except Exception:
        pass  # Network error — fall through to API probes

    if html_platform:
        return html_platform

    # ── Step 2: Direct DDC API probe (works even behind 403 on HTML pages) ──
    if base:
        probe = (
            f"{base}/apis/widget/"
            "INVENTORY_LISTING_DEFAULT_AUTO_NEW:inventory-data-bus1"
            "/getInventory?start=0&pageSize=1&numRecords=1&new-used=N"
        )
        try:
            rp = requests.get(probe, headers=HEADERS, timeout=15)
            if rp.status_code == 200 and "pageInfo" in rp.text:
                tprint(f"    ✓ DDC confirmed via API probe for {base}")
                return "ddc"
        except Exception:
            pass

    # ── Step 3: Direct DealerOn API probe ───────────────────────────────
    if base:
        # DealerOn API path always contains /api/vhcliaa/ — probe a known pattern
        probe_do = f"{base}/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles"
        try:
            rp = requests.get(probe_do, headers=HEADERS, timeout=15)
            # DealerOn returns 400/404 with a JSON body (not HTML) for missing params
            if rp.status_code in (400, 404) and "application/json" in rp.headers.get("Content-Type", ""):
                tprint(f"    ✓ DealerOn confirmed via API probe for {base}")
                return "dealeron"
        except Exception:
            pass

    tprint(f"    ⚠ Platform not detected for {base}")
    return None


# ════════════════════════════════════════════════════════════════════════════
# PLATFORM: Dealer.com / DDC
# ════════════════════════════════════════════════════════════════════════════

DDC_PAGE_SIZE = 24

DDC_WIDGET_MAP = {
    "new":  "INVENTORY_LISTING_DEFAULT_AUTO_NEW:inventory-data-bus1",
    "used": "INVENTORY_LISTING_DEFAULT_AUTO_USED:inventory-data-bus1",
}

DDC_NEW_USED_MAP = {"new": "N", "used": "U"}


def _ddc_fetch_page(base_url: str, widget: str, new_used: str, start: int) -> tuple[list[dict], int]:
    """Fetch a single DDC page with retry/backoff. Returns (vehicles, total_count)."""
    url = (
        f"{base_url}/apis/widget/{widget}/getInventory"
        f"?start={start}&pageSize={DDC_PAGE_SIZE}&numRecords={DDC_PAGE_SIZE}"
        f"&new-used={new_used}"
    )
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 429:
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            tprint(f"      ⚠ 429 rate-limited (attempt {attempt}/{MAX_RETRIES}) — waiting {wait}s…")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        raise RuntimeError(f"DDC: gave up after {MAX_RETRIES} retries (start={start})")

    data        = resp.json()
    page_info   = data.get("pageInfo", {})
    total_count = int(page_info.get("totalCount", 0))
    inventory   = data.get("inventory", [])

    vehicles = []
    for v in inventory:
        vin = (v.get("vin") or "").strip().upper()
        if not vin or len(vin) != 17:
            continue
        images = v.get("images") or []
        img    = images[0].get("uri", "") if images else ""
        vehicles.append({"vin": vin, "img": img})

    return vehicles, total_count


def scrape_ddc(base_url: str, condition: str, label: str) -> list[dict]:
    """
    Scrape all pages for one condition (new/used) using parallel page fetches.
    Pages are fetched in batches of PAGE_WORKERS.
    """
    widget   = DDC_WIDGET_MAP[condition]
    new_used = DDC_NEW_USED_MAP[condition]

    # Page 1: get total count first
    batch0, total_count = _ddc_fetch_page(base_url, widget, new_used, start=0)
    total_pages = (total_count + DDC_PAGE_SIZE - 1) // DDC_PAGE_SIZE
    tprint(f"      [{label}] DDC {condition.upper()}: TotalCount={total_count}  TotalPages={total_pages}")

    # Collect page-1 results
    page_results = {0: batch0}

    # Remaining pages fetched in parallel batches
    remaining_starts = [(p - 1) * DDC_PAGE_SIZE for p in range(2, total_pages + 1)]

    for i in range(0, len(remaining_starts), PAGE_WORKERS):
        batch_starts = remaining_starts[i : i + PAGE_WORKERS]
        with ThreadPoolExecutor(max_workers=len(batch_starts)) as ex:
            futures = {
                ex.submit(_ddc_fetch_page, base_url, widget, new_used, s): s
                for s in batch_starts
            }
            for fut in as_completed(futures):
                s = futures[fut]
                try:
                    vehicles, _ = fut.result()
                    page_results[s] = vehicles
                except Exception as e:
                    tprint(f"      [{label}] ⚠ page start={s} failed: {e}")
                    page_results[s] = []
        time.sleep(CRAWL_DELAY)

    # Merge in order, dedup
    results, seen = [], set()
    for s in sorted(page_results):
        for h in page_results[s]:
            if h["vin"] not in seen:
                results.append(h); seen.add(h["vin"])

    tprint(f"      [{label}] {condition.upper()} done: {len(results)} vehicles")
    return results


# ════════════════════════════════════════════════════════════════════════════
# PLATFORM: DealerOn
# ════════════════════════════════════════════════════════════════════════════

DEALERON_PAGE_SIZE = 12


def _dealeron_extract_config(url: str) -> tuple[str, str, str] | None:
    """Extract host, dealerId, pageId from DealerOn SRP page source."""
    try:
        r = requests.get(
            url,
            headers={**HEADERS, "Accept": "text/html"},
            timeout=20,
            allow_redirects=True,
        )
        r.raise_for_status()
    except Exception as e:
        tprint(f"      ⚠ DealerOn config fetch failed: {e}")
        return None

    html = r.text
    base = re.match(r"(https?://[^/]+)", url).group(1)
    host = re.match(r"https?://(.+)", base).group(1)

    dealer_id = None
    for pat in [
        r'"dealerId"\s*:\s*"?(\d+)"?',
        r'dealerId[=:]\s*"?(\d+)"?',
        r'dealer[_-]?id[=:]\s*"?(\d+)"?',
        r'/vehicles/(\d+)/\d+',
    ]:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            dealer_id = m.group(1)
            break

    page_id = None
    for pat in [
        r'"pageId"\s*:\s*"?(\d+)"?',
        r'pageId[=:]\s*"?(\d+)"?',
        r'/vehicles/\d+/(\d+)',
    ]:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            page_id = m.group(1)
            break

    if not dealer_id or not page_id:
        tprint(f"      ⚠ DealerOn: could not extract dealerId/pageId from {url}")
        return None

    tprint(f"      DealerOn config: host={host}  dealerId={dealer_id}  pageId={page_id}")
    return host, dealer_id, page_id


def _dealeron_normalise_img(raw: str, base_url: str) -> str:
    img = raw.replace("/thumbs/", "/")
    img = re.sub(r"/ip/\d+\.jpg$", "/ip/1.jpg", img)
    if img and not img.startswith("http"):
        img = base_url.rstrip("/") + img
    return img


def _dealeron_fetch_page(
    base_url: str, dealer_id: str, page_id: str, host: str, page_num: int
) -> tuple[list[dict], dict]:
    url = (
        f"{base_url}/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles"
        f"/{dealer_id}/{page_id}"
        f"?pt={page_num}&host={host}&pn={DEALERON_PAGE_SIZE}"
        f"&baseFilter=e30=&displayCardsShown={DEALERON_PAGE_SIZE}"
    )
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 429:
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            tprint(f"      ⚠ 429 rate-limited (attempt {attempt}/{MAX_RETRIES}) — waiting {wait}s…")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        raise RuntimeError(f"DealerOn: gave up after {MAX_RETRIES} retries (page={page_num})")

    data   = resp.json()
    paging = (data.get("Paging") or {}).get("PaginationDataModel") or {}
    vehicles = []
    for cw in data.get("DisplayCards", []):
        vc  = cw.get("VehicleCard", {})
        m   = re.search(r"([A-HJ-NPR-Z0-9]{17})", vc.get("VehicleDetailUrl", ""))
        if not m:
            continue
        vin = m.group(1).upper()
        im  = vc.get("VehicleImageModel", {})
        img = _dealeron_normalise_img(im.get("VehiclePhotoSrc") or "", base_url)
        vehicles.append({"vin": vin, "img": img})
    return vehicles, paging


def scrape_dealeron(base_url: str, inv_url: str, condition: str, label: str) -> list[dict]:
    config = _dealeron_extract_config(inv_url)
    if not config:
        return []
    host, dealer_id, page_id = config

    # Page 1: get total pages
    batch0, paging = _dealeron_fetch_page(base_url, dealer_id, page_id, host, 1)
    total_pages  = int(paging.get("TotalPages") or 1)
    total_count  = int(paging.get("TotalCount") or 0)
    tprint(f"      [{label}] DealerOn {condition.upper()}: TotalCount={total_count}  TotalPages={total_pages}")

    page_results = {1: batch0}

    # Remaining pages in parallel batches
    remaining_pages = list(range(2, total_pages + 1))
    for i in range(0, len(remaining_pages), PAGE_WORKERS):
        batch_pages = remaining_pages[i : i + PAGE_WORKERS]
        with ThreadPoolExecutor(max_workers=len(batch_pages)) as ex:
            futures = {
                ex.submit(_dealeron_fetch_page, base_url, dealer_id, page_id, host, pg): pg
                for pg in batch_pages
            }
            for fut in as_completed(futures):
                pg = futures[fut]
                try:
                    vehicles, _ = fut.result()
                    page_results[pg] = vehicles
                except Exception as e:
                    tprint(f"      [{label}] ⚠ page {pg} failed: {e}")
                    page_results[pg] = []
        time.sleep(CRAWL_DELAY)

    results, seen = [], set()
    for pg in sorted(page_results):
        for h in page_results[pg]:
            if h["vin"] not in seen:
                results.append(h); seen.add(h["vin"])

    tprint(f"      [{label}] {condition.upper()} done: {len(results)} vehicles")
    return results


# ════════════════════════════════════════════════════════════════════════════
# CSV OUTPUT
# ════════════════════════════════════════════════════════════════════════════

def write_csv(
    hits: list[dict],
    enterprise_name: str,
    rooftop_name: str,
    condition: str,
    output_path: str,
) -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["enterprise_name", "rooftop_name", "condition", "vin", "first_image_url"],
        )
        writer.writeheader()
        for h in hits:
            writer.writerow({
                "enterprise_name": enterprise_name,
                "rooftop_name":    rooftop_name,
                "condition":       condition,
                "vin":             h["vin"],
                "first_image_url": h["img"],
            })
    tprint(f"      ✓ Saved → {output_path}  ({len(hits)} rows)")


# ════════════════════════════════════════════════════════════════════════════
# ROOFTOP ORCHESTRATOR
# ════════════════════════════════════════════════════════════════════════════

def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def scrape_rooftop(row: dict, output_dir: str, index: int, total: int) -> dict:
    """Scrape one rooftop (new + used). Thread-safe. Returns status summary dict."""
    enterprise = row["Enterprise_Name"].strip()
    rooftop    = row["Rooftop_Name"].strip()
    new_url    = (row.get("New_url") or "").strip().rstrip("/")
    used_url   = (row.get("used_url") or "").strip().rstrip("/")
    base_url   = (row.get("Website Link") or new_url or used_url or "")
    m          = re.match(r"(https?://[^/]+)", base_url)
    base_url   = m.group(1) if m else ""
    label      = f"{index}/{total} {rooftop}"

    status = {"rooftop": rooftop, "new": "skipped", "used": "skipped", "platform": "unknown"}

    tprint(f"\n{'─'*60}")
    tprint(f"  [{label}]")
    tprint(f"  Base: {base_url}")

    detect_url = new_url or used_url
    if not detect_url:
        tprint(f"  ✗ No URLs — skipping")
        status["new"] = status["used"] = "no_url"
        return status

    platform = detect_platform(detect_url)
    if platform is None:
        tprint(f"  ✗ Platform not detected — skipping")
        status["new"] = status["used"] = "unknown_platform"
        return status

    status["platform"] = platform
    tprint(f"  ✓ Platform: {platform.upper()}")

    rooftop_slug = slug(rooftop)

    # ── NEW + USED scraped concurrently within the rooftop ───────────────
    def do_new():
        if not new_url:
            return "skipped", 0
        try:
            if platform == "ddc":
                hits = scrape_ddc(base_url, "new", label)
            elif platform == "dealeron":
                hits = scrape_dealeron(base_url, new_url, "new", label)
            else:
                return "unknown_platform", 0
            out = os.path.join(output_dir, f"{rooftop_slug}_new.csv")
            write_csv(hits, enterprise, rooftop, "new", out)
            return f"ok ({len(hits)} vehicles)", len(hits)
        except Exception as e:
            tprint(f"      [{label}] ✗ NEW failed: {e}")
            return f"error: {e}", 0

    def do_used():
        if not used_url:
            return "skipped", 0
        try:
            if platform == "ddc":
                hits = scrape_ddc(base_url, "used", label)
            elif platform == "dealeron":
                hits = scrape_dealeron(base_url, used_url, "used", label)
            else:
                return "unknown_platform", 0
            out = os.path.join(output_dir, f"{rooftop_slug}_used.csv")
            write_csv(hits, enterprise, rooftop, "used", out)
            return f"ok ({len(hits)} vehicles)", len(hits)
        except Exception as e:
            tprint(f"      [{label}] ✗ USED failed: {e}")
            return f"error: {e}", 0

    # Run new + used in parallel within the rooftop
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_new  = ex.submit(do_new)
        f_used = ex.submit(do_used)
        status["new"],  _ = f_new.result()
        status["used"], _ = f_used.result()

    return status


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Multi-rooftop inventory scraper (parallel)")
    parser.add_argument(
        "--csv",
        default="website_scrapper_data_-_Sheet1.csv",
        help="Path to input CSV (default: website_scrapper_data_-_Sheet1.csv)",
    )
    parser.add_argument(
        "--output",
        default="./output",
        help="Directory for output CSVs (default: ./output)",
    )
    parser.add_argument(
        "--rooftop",
        default=None,
        help="Only scrape rooftops matching this name (partial, case-insensitive)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=ROOFTOP_WORKERS,
        help=f"Parallel rooftop workers (default: {ROOFTOP_WORKERS})",
    )
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print(f"✗ Input CSV not found: {args.csv}")
        sys.exit(1)

    with open(args.csv, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    rows = [{k.strip(): v for k, v in row.items()} for row in rows]

    if args.rooftop:
        rows = [r for r in rows if args.rooftop.lower() in r.get("Rooftop_Name", "").lower()]
        if not rows:
            print(f"✗ No rooftop matching '{args.rooftop}' found")
            sys.exit(1)

    os.makedirs(args.output, exist_ok=True)
    total = len(rows)

    print(f"Multi-Rooftop Scraper  (parallel edition)")
    print(f"Input    : {args.csv}  ({total} rooftops)")
    print(f"Output   : {args.output}")
    print(f"Workers  : {args.workers} rooftops × {PAGE_WORKERS} pages × 2 conditions")
    print(f"Delay    : {CRAWL_DELAY}s between page batches")

    start_time = time.time()
    summary    = [None] * total

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(scrape_rooftop, row, args.output, i + 1, total): i
            for i, row in enumerate(rows)
        }
        for fut in as_completed(futures):
            i = futures[fut]
            try:
                summary[i] = fut.result()
            except Exception as e:
                summary[i] = {
                    "rooftop":  rows[i].get("Rooftop_Name", "?"),
                    "platform": "error",
                    "new":      f"error: {e}",
                    "used":     f"error: {e}",
                }

    elapsed = time.time() - start_time

    print(f"\n\n{'═'*70}")
    print("SUMMARY")
    print(f"{'═'*70}")
    print(f"{'Rooftop':<42} {'Platform':<10} {'New':<22} {'Used'}")
    print(f"{'-'*42} {'-'*10} {'-'*22} {'-'*22}")
    for s in summary:
        if s:
            print(f"{s['rooftop']:<42} {s['platform']:<10} {s['new']:<22} {s['used']}")

    print(f"\n  Total time : {elapsed:.1f}s  ({elapsed/60:.1f} min)")
    print(f"  CSVs saved : {args.output}/")


if __name__ == "__main__":
    main()
