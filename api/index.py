"""
Scraper API — FastAPI backend
==============================
Drop this file into your repo as  api/index.py

Speed model (sweet spot):
  - 3 rooftops in parallel           (ROOFTOP_WORKERS)
  - NEW + USED run concurrently per rooftop
  - 2 pages fetched in parallel      (PAGE_WORKERS)
  - 0.2s delay between page batches  (CRAWL_DELAY)

This avoids the two failure modes:
  - Too sequential → slow (old issue)
  - Too parallel   → 429 storms + backoff waits (parallel edition issue)

Endpoints:
  POST /api/scrape          — upload CSV, start scraping in background
  GET  /api/progress        — SSE stream of live log lines
  GET  /api/results         — list all result CSVs
  GET  /api/download/{name} — download a single CSV
  GET  /api/download-all    — download all CSVs as a zip
  GET  /api/status          — running / done / total counts
"""

import csv
import io
import json
import os
import re
import tempfile
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Generator

import requests as http
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Output directory ─────────────────────────────────────────────────────────
OUTPUT_DIR = Path(tempfile.gettempdir()) / "scraper_output"
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Shared run state ─────────────────────────────────────────────────────────
_run_state: dict = {"running": False, "log": [], "summary": [], "total": 0, "done": 0}
_state_lock = threading.Lock()

def emit(msg: str, level: str = "info"):
    with _state_lock:
        _run_state["log"].append({"ts": time.time(), "level": level, "msg": msg})


# ════════════════════════════════════════════════════════════════════════════
# SETTINGS
# ════════════════════════════════════════════════════════════════════════════

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

ROOFTOP_WORKERS  = 3    # rooftops scraped in parallel
PAGE_WORKERS     = 2    # pages fetched in parallel per condition
CRAWL_DELAY      = 0.2  # seconds between page batches
MAX_RETRIES      = 5
BACKOFF_BASE     = 10   # seconds; doubles each retry: 10, 20, 40, 80, 160

DDC_PAGE_SIZE    = 24
DDC_WIDGET_MAP   = {
    "new":  "INVENTORY_LISTING_DEFAULT_AUTO_NEW:inventory-data-bus1",
    "used": "INVENTORY_LISTING_DEFAULT_AUTO_USED:inventory-data-bus1",
}
DDC_NEW_USED_MAP = {"new": "N", "used": "U"}
DEALERON_PAGE_SIZE = 12


# ════════════════════════════════════════════════════════════════════════════
# PLATFORM DETECTION
# ════════════════════════════════════════════════════════════════════════════

def detect_platform(url: str) -> str | None:
    """
    Returns 'ddc', 'dealeron', or None.
    1. HTML fingerprint
    2. Direct DDC API probe  (works even when HTML returns 403)
    3. Direct DealerOn API probe
    """
    base_match = re.match(r"(https?://[^/]+)", url)
    base = base_match.group(1) if base_match else ""

    try:
        r = http.get(url, headers={**HEADERS, "Accept": "text/html"},
                     timeout=20, allow_redirects=True)
        if r.status_code == 200:
            html = r.text.lower()
            if "vhcliaa" in html or "dealeron" in html:
                return "dealeron"
            if "inventory_listing" in html or "/apis/widget/" in html or "dealer.com" in html:
                return "ddc"
    except Exception:
        pass

    if base:
        try:
            probe = (f"{base}/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_NEW:inventory-data-bus1"
                     f"/getInventory?start=0&pageSize=1&numRecords=1&new-used=N")
            rp = http.get(probe, headers=HEADERS, timeout=15)
            if rp.status_code == 200 and "pageInfo" in rp.text:
                emit(f"DDC confirmed via API probe: {base}")
                return "ddc"
        except Exception:
            pass

    if base:
        try:
            rp = http.get(f"{base}/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles",
                          headers=HEADERS, timeout=15)
            if rp.status_code in (400, 404) and "application/json" in rp.headers.get("Content-Type", ""):
                emit(f"DealerOn confirmed via API probe: {base}")
                return "dealeron"
        except Exception:
            pass

    return None


# ════════════════════════════════════════════════════════════════════════════
# PLATFORM: Dealer.com / DDC
# ════════════════════════════════════════════════════════════════════════════

def _ddc_fetch_page(base_url: str, widget: str, new_used: str, start: int) -> tuple[list[dict], int]:
    url = (f"{base_url}/apis/widget/{widget}/getInventory"
           f"?start={start}&pageSize={DDC_PAGE_SIZE}&numRecords={DDC_PAGE_SIZE}&new-used={new_used}")
    for attempt in range(1, MAX_RETRIES + 1):
        resp = http.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 429:
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            emit(f"429 rate-limited — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})", "warn")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        raise RuntimeError(f"DDC: gave up after {MAX_RETRIES} retries (start={start})")

    data        = resp.json()
    total_count = int(data.get("pageInfo", {}).get("totalCount", 0))
    vehicles    = []
    for v in data.get("inventory", []):
        vin = (v.get("vin") or "").strip().upper()
        if not vin or len(vin) != 17:
            continue
        images = v.get("images") or []
        img    = images[0].get("uri", "") if images else ""
        vehicles.append({"vin": vin, "img": img})
    return vehicles, total_count


def scrape_ddc(base_url: str, condition: str, label: str) -> list[dict]:
    """
    Fetch page 1 to get total, then fetch remaining pages in batches of
    PAGE_WORKERS (2). Merges results in order and deduplicates VINs.
    """
    widget   = DDC_WIDGET_MAP[condition]
    new_used = DDC_NEW_USED_MAP[condition]

    # Page 1 — get total count
    batch0, total_count = _ddc_fetch_page(base_url, widget, new_used, 0)
    total_pages = (total_count + DDC_PAGE_SIZE - 1) // DDC_PAGE_SIZE
    emit(f"[{label}] {condition.upper()}: {total_count} vehicles, {total_pages} pages")

    page_results = {0: batch0}

    # Remaining pages in parallel batches of PAGE_WORKERS
    remaining = [(p - 1) * DDC_PAGE_SIZE for p in range(2, total_pages + 1)]
    for i in range(0, len(remaining), PAGE_WORKERS):
        batch_starts = remaining[i: i + PAGE_WORKERS]
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
                    emit(f"[{label}] ⚠ page start={s} failed: {e}", "warn")
                    page_results[s] = []
        time.sleep(CRAWL_DELAY)

    # Merge in page order, deduplicate
    results, seen = [], set()
    for s in sorted(page_results):
        for h in page_results[s]:
            if h["vin"] not in seen:
                results.append(h); seen.add(h["vin"])
    return results


# ════════════════════════════════════════════════════════════════════════════
# PLATFORM: DealerOn
# ════════════════════════════════════════════════════════════════════════════

def _dealeron_extract_config(url: str) -> tuple[str, str, str] | None:
    try:
        r = http.get(url, headers={**HEADERS, "Accept": "text/html"},
                     timeout=20, allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        emit(f"DealerOn config fetch failed: {e}", "error")
        return None

    html = r.text
    base = re.match(r"(https?://[^/]+)", url).group(1)
    host = re.match(r"https?://(.+)", base).group(1)

    dealer_id = None
    for pat in [r'"dealerId"\s*:\s*"?(\d+)"?', r'dealerId[=:]\s*"?(\d+)"?',
                r'dealer[_-]?id[=:]\s*"?(\d+)"?', r'/vehicles/(\d+)/\d+']:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            dealer_id = m.group(1); break

    page_id = None
    for pat in [r'"pageId"\s*:\s*"?(\d+)"?', r'pageId[=:]\s*"?(\d+)"?', r'/vehicles/\d+/(\d+)']:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            page_id = m.group(1); break

    if not dealer_id or not page_id:
        emit(f"DealerOn: could not extract dealerId/pageId from {url}", "error")
        return None

    return host, dealer_id, page_id


def _dealeron_normalise_img(raw: str, base_url: str) -> str:
    img = raw.replace("/thumbs/", "/")
    img = re.sub(r"/ip/\d+\.jpg$", "/ip/1.jpg", img)
    if img and not img.startswith("http"):
        img = base_url.rstrip("/") + img
    return img


def _dealeron_fetch_page(base_url: str, dealer_id: str, page_id: str,
                          host: str, page_num: int) -> tuple[list[dict], dict]:
    url = (f"{base_url}/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/{dealer_id}/{page_id}"
           f"?pt={page_num}&host={host}&pn={DEALERON_PAGE_SIZE}"
           f"&baseFilter=e30=&displayCardsShown={DEALERON_PAGE_SIZE}")
    for attempt in range(1, MAX_RETRIES + 1):
        resp = http.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 429:
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            emit(f"429 rate-limited — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})", "warn")
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
        vc = cw.get("VehicleCard", {})
        m  = re.search(r"([A-HJ-NPR-Z0-9]{17})", vc.get("VehicleDetailUrl", ""))
        if not m:
            continue
        vin = m.group(1).upper()
        img = _dealeron_normalise_img(
            vc.get("VehicleImageModel", {}).get("VehiclePhotoSrc") or "", base_url)
        vehicles.append({"vin": vin, "img": img})
    return vehicles, paging


def scrape_dealeron(base_url: str, inv_url: str, condition: str, label: str) -> list[dict]:
    config = _dealeron_extract_config(inv_url)
    if not config:
        return []
    host, dealer_id, page_id = config

    batch0, paging = _dealeron_fetch_page(base_url, dealer_id, page_id, host, 1)
    total_pages = int(paging.get("TotalPages") or 1)
    total_count = int(paging.get("TotalCount") or 0)
    emit(f"[{label}] {condition.upper()}: {total_count} vehicles, {total_pages} pages")

    page_results = {1: batch0}

    remaining = list(range(2, total_pages + 1))
    for i in range(0, len(remaining), PAGE_WORKERS):
        batch_pages = remaining[i: i + PAGE_WORKERS]
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
                    emit(f"[{label}] ⚠ page {pg} failed: {e}", "warn")
                    page_results[pg] = []
        time.sleep(CRAWL_DELAY)

    results, seen = [], set()
    for pg in sorted(page_results):
        for h in page_results[pg]:
            if h["vin"] not in seen:
                results.append(h); seen.add(h["vin"])
    return results


# ════════════════════════════════════════════════════════════════════════════
# CSV HELPERS
# ════════════════════════════════════════════════════════════════════════════

def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _write_csv(hits: list[dict], enterprise: str, rooftop: str,
               condition: str, path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["enterprise_name", "rooftop_name",
                                           "condition", "vin", "first_image_url"])
        w.writeheader()
        for h in hits:
            w.writerow({"enterprise_name": enterprise, "rooftop_name": rooftop,
                        "condition": condition, "vin": h["vin"], "first_image_url": h["img"]})


# ════════════════════════════════════════════════════════════════════════════
# ROOFTOP ORCHESTRATOR
# ════════════════════════════════════════════════════════════════════════════

def scrape_rooftop(row: dict, output_dir: str, index: int, total: int) -> dict:
    enterprise = row["Enterprise_Name"].strip()
    rooftop    = row["Rooftop_Name"].strip()
    new_url    = (row.get("New_url") or "").strip().rstrip("/")
    used_url   = (row.get("used_url") or "").strip().rstrip("/")
    base_url   = (row.get("Website Link") or new_url or used_url or "")
    m          = re.match(r"(https?://[^/]+)", base_url)
    base_url   = m.group(1) if m else ""
    label      = f"{index}/{total} {rooftop}"

    status = {
        "enterprise": enterprise, "rooftop": rooftop, "platform": "unknown",
        "new_url": new_url,  "new_status": "skipped",  "new_vin_count": 0,
        "new_error": "",     "new_output_file": "",
        "used_url": used_url, "used_status": "skipped", "used_vin_count": 0,
        "used_error": "",    "used_output_file": "",
        "run_time_s": 0,
    }

    t0 = time.time()
    emit(f"▶ Starting {rooftop}")

    detect_url = new_url or used_url
    if not detect_url:
        status["new_status"] = status["used_status"] = "no_url"
        status["new_error"]  = status["used_error"]  = "No URL in CSV"
        emit(f"[{label}] ✗ No URLs", "error")
        status["run_time_s"] = round(time.time() - t0, 1)
        return status

    platform = detect_platform(detect_url)
    if not platform:
        err = "Platform not detected (tried HTML + DDC/DealerOn API probes)"
        status["new_status"] = status["used_status"] = "unknown_platform"
        status["new_error"]  = status["used_error"]  = err
        emit(f"[{label}] ✗ {err}", "error")
        status["run_time_s"] = round(time.time() - t0, 1)
        return status

    status["platform"] = platform
    emit(f"[{label}] ✓ Platform: {platform.upper()}")
    rooftop_slug = _slug(rooftop)

    # ── Run NEW + USED concurrently within each rooftop ──────────────────
    def do_condition(condition: str, url: str):
        if not url:
            return "skipped", 0, "", ""
        try:
            if platform == "ddc":
                hits = scrape_ddc(base_url, condition, label)
            elif platform == "dealeron":
                hits = scrape_dealeron(base_url, url, condition, label)
            else:
                raise RuntimeError("Unsupported platform")
            out = str(Path(output_dir) / f"{rooftop_slug}_{condition}.csv")
            _write_csv(hits, enterprise, rooftop, condition, out)
            emit(f"[{label}] ✓ {condition.upper()} — {len(hits)} VINs")
            return "ok", len(hits), "", out
        except Exception as e:
            emit(f"[{label}] ✗ {condition.upper()} failed: {e}", "error")
            return "error", 0, str(e), ""

    with ThreadPoolExecutor(max_workers=2) as ex:
        f_new  = ex.submit(do_condition, "new",  new_url)
        f_used = ex.submit(do_condition, "used", used_url)
        new_st,  new_cnt,  new_err,  new_file  = f_new.result()
        used_st, used_cnt, used_err, used_file = f_used.result()

    status.update({
        "new_status":       new_st,  "new_vin_count":   new_cnt,
        "new_error":        new_err, "new_output_file": new_file,
        "used_status":      used_st, "used_vin_count":  used_cnt,
        "used_error":       used_err,"used_output_file":used_file,
        "run_time_s":       round(time.time() - t0, 1),
    })

    with _state_lock:
        _run_state["summary"].append(status)
    return status


# ════════════════════════════════════════════════════════════════════════════
# API ROUTES
# ════════════════════════════════════════════════════════════════════════════

@app.post("/api/scrape")
async def start_scrape(file: UploadFile = File(...)):
    with _state_lock:
        if _run_state["running"]:
            raise HTTPException(status_code=409, detail="A scrape is already running")

    content = await file.read()
    rows    = list(csv.DictReader(io.StringIO(content.decode("utf-8-sig"))))
    rows    = [{k.strip(): v for k, v in row.items()} for row in rows]
    if not rows:
        raise HTTPException(status_code=400, detail="CSV is empty")

    for f in OUTPUT_DIR.glob("*.csv"):
        f.unlink()

    with _state_lock:
        _run_state.update({"running": True, "log": [], "summary": [],
                           "total": len(rows), "done": 0})

    def run():
        total   = len(rows)
        results = [None] * total
        with ThreadPoolExecutor(max_workers=ROOFTOP_WORKERS) as ex:
            futures = {
                ex.submit(scrape_rooftop, row, str(OUTPUT_DIR), i + 1, total): i
                for i, row in enumerate(rows)
            }
            for fut in as_completed(futures):
                i = futures[fut]
                try:
                    results[i] = fut.result()
                except Exception as e:
                    results[i] = {
                        "enterprise": rows[i].get("Enterprise_Name", "?"),
                        "rooftop": rows[i].get("Rooftop_Name", "?"),
                        "platform": "error",
                        "new_url": rows[i].get("New_url", ""),
                        "new_status": "error", "new_vin_count": 0,
                        "new_error": str(e), "new_output_file": "",
                        "used_url": rows[i].get("used_url", ""),
                        "used_status": "error", "used_vin_count": 0,
                        "used_error": str(e), "used_output_file": "",
                        "run_time_s": 0,
                    }
                with _state_lock:
                    _run_state["done"] += 1

        # Write run_log.csv
        log_fields = [
            "enterprise", "rooftop", "platform",
            "new_url", "new_status", "new_vin_count", "new_output_file", "new_error",
            "used_url", "used_status", "used_vin_count", "used_output_file", "used_error",
            "run_time_s",
        ]
        with open(OUTPUT_DIR / "run_log.csv", "w", newline="", encoding="utf-8") as lf:
            w = csv.DictWriter(lf, fieldnames=log_fields, extrasaction="ignore")
            w.writeheader()
            for s in results:
                if s:
                    w.writerow(s)

        emit("✅ All rooftops complete", "done")
        with _state_lock:
            _run_state["running"] = False

    threading.Thread(target=run, daemon=True).start()
    return {"status": "started", "total": len(rows)}


@app.get("/api/progress")
def stream_progress():
    def event_stream() -> Generator:
        sent = 0
        while True:
            with _state_lock:
                logs    = list(_run_state["log"])
                total   = _run_state.get("total", 0)
                done    = _run_state.get("done", 0)
                running = _run_state["running"]

            while sent < len(logs):
                entry = logs[sent]
                data  = json.dumps({"msg": entry["msg"], "level": entry["level"],
                                    "total": total, "done": done})
                yield f"data: {data}\n\n"
                sent += 1

            if not running and sent >= len(logs):
                yield f"data: {json.dumps({'msg': '__done__', 'level': 'done', 'total': total, 'done': done})}\n\n"
                break
            time.sleep(0.3)

    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/results")
def list_results():
    files = []
    for f in sorted(OUTPUT_DIR.glob("*.csv")):
        try:
            rows = sum(1 for _ in open(f, encoding="utf-8")) - 1
        except Exception:
            rows = 0
        files.append({"name": f.name, "size": f.stat().st_size, "rows": max(rows, 0)})
    return {"files": files}


@app.get("/api/download/{filename}")
def download_file(filename: str):
    path = OUTPUT_DIR / Path(filename).name
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, media_type="text/csv", filename=path.name)


@app.get("/api/download-all")
def download_all():
    files = list(OUTPUT_DIR.glob("*.csv"))
    if not files:
        raise HTTPException(status_code=404, detail="No files to download")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            zf.write(f, f.name)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/zip",
                             headers={"Content-Disposition": "attachment; filename=inventory_results.zip"})


@app.get("/api/status")
def get_status():
    with _state_lock:
        return {"running": _run_state["running"],
                "done":    _run_state.get("done", 0),
                "total":   _run_state.get("total", 0)}


# ── Serve frontend ────────────────────────────────────────────────────────────
_public = Path(__file__).parent.parent / "public"
if _public.exists():
    app.mount("/", StaticFiles(directory=str(_public), html=True), name="static")
