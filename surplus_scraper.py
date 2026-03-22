"""
NJ Surplus Funds Scraper — Supabase Version
Writes directly to Supabase instead of local SQLite.

Setup:
  1. Create a .env file in this folder with:
     SUPABASE_URL=https://xxxxxxxxxxxx.supabase.co
     SUPABASE_KEY=your-anon-key-here

  2. Run: python3 surplus_scraper.py
"""

import requests
from bs4 import BeautifulSoup
import time, re, sys, os
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────
# LOAD ENV
# ─────────────────────────────────────────────────────
def load_env():
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())

load_env()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("✗ Missing SUPABASE_URL or SUPABASE_KEY in .env file")
    sys.exit(1)

SUPA_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}

# ─────────────────────────────────────────────────────
# SUPABASE HELPERS
# ─────────────────────────────────────────────────────
def supa_get(endpoint, params=None):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{endpoint}",
        headers=SUPA_HEADERS,
        params=params,
        timeout=15
    )
    r.raise_for_status()
    return r.json()

def supa_post(endpoint, data):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{endpoint}",
        headers=SUPA_HEADERS,
        json=data,
        timeout=15
    )
    r.raise_for_status()
    return r

def supa_patch(endpoint, data, params=None):
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{endpoint}",
        headers={**SUPA_HEADERS, "Prefer": "return=minimal"},
        json=data,
        params=params,
        timeout=15
    )
    r.raise_for_status()
    return r

def get_existing_sheriff_numbers():
    """Fetch all sheriff numbers already in DB — used to skip duplicates."""
    results = supa_get("leads", params={
        "select": "sheriff_number",
        "limit":  "10000"
    })
    return {r["sheriff_number"] for r in results if r.get("sheriff_number")}

def reset_new_flags():
    """Clear is_new from previous run before marking today's new cases."""
    supa_patch("leads", {"is_new": False}, params={"is_new": "eq.true"})

def save_lead(case, now):
    try:
        supa_post("leads", {
            "case_number":     case.get("Case #") or case.get("Sheriff #", ""),
            "defendant":       case.get("Defendant", ""),
            "plaintiff":       case.get("Plaintiff", ""),
            "sheriff_number":  case.get("Sheriff #", ""),
            "address":         case.get("Address", ""),
            "sale_date":       case.get("Sale Date", ""),
            "approx_judgment": case.get("Approx Judgment", ""),
            "status":          case.get("Status", ""),
            "county":          case.get("County", ""),
            "detail_url":      case.get("_detail_url", ""),
            "first_seen":      now,
            "last_updated":    now,
            "is_new":          True,
        })
        return True
    except Exception as e:
        if "409" in str(e) or "duplicate" in str(e).lower():
            return False
        print(f"    ⚠  Save error: {e}")
        return False

# ─────────────────────────────────────────────────────
# SCRAPER CONFIG
# ─────────────────────────────────────────────────────
COUNTIES = [
    {"name": "Camden",     "id": 1},
    {"name": "Essex",      "id": 2},
    {"name": "Burlington", "id": 3},
    {"name": "Cumberland", "id": 6},
    {"name": "Bergen",     "id": 7},
    {"name": "Monmouth",   "id": 8},
    {"name": "Morris",     "id": 9},
    {"name": "Hudson",     "id": 10},
    {"name": "Union",      "id": 15},
    {"name": "Gloucester", "id": 19},
    {"name": "Cape May",   "id": 52},
    {"name": "Middlesex",  "id": 73},
]

BASE_URL = "https://salesweb.civilview.com"

COMPLETED_STATUSES = [
    "purchased - 3rd party", "purchased-3rd party", "3rd party",
    "sold", "redeemed", "cancelled", "bankruptcy dismissed", "settled",
]

JUDGMENT_LABELS = [
    "approx. judgment*", "approx. judgment", "approx judgment",
    "approx. judgement*", "approx. judgement", "approx judgement",
    "approximate judgment", "upset amount", "upset price",
    "judgment amount", "opening bid", "minimum bid", "judgment",
]

SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

def make_session():
    s = requests.Session()
    s.headers.update(SCRAPE_HEADERS)
    return s

def fetch(session, url, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"    ⚠  Retry {attempt+1}/{retries} — {e}")
            if attempt < retries - 1:
                time.sleep(2)
    return None

def clean(text):
    return re.sub(r"\s+", " ", (text or "").strip())

def is_completed(status):
    return any(k in status.lower() for k in COMPLETED_STATUSES)

def has_search_form(soup):
    for tag in soup.find_all(["select", "input", "form"]):
        if any(k in str(tag).lower() for k in ["sold", "statustype", "saledate"]):
            return True
    return False

# ─────────────────────────────────────────────────────
# LISTING + DETAIL
# ─────────────────────────────────────────────────────
def get_listing(session, county):
    name     = county["name"]
    base_url = f"{BASE_URL}/Sales/SalesSearch?countyId={county['id']}"

    session.headers.update({"Referer": BASE_URL + "/"})
    r = fetch(session, base_url)
    if not r:
        print(f"  ✗ Could not reach {name}")
        return [], base_url

    soup = BeautifulSoup(r.text, "html.parser")

    # For ALL counties use the base URL — this preserves the Status column
    # Form counties were previously filtered via URL but that stripped the Status column
    listing_url = base_url
    if has_search_form(soup):
        # Re-fetch without status filter to get full table with Status column intact
        session.headers.update({"Referer": base_url})

    cases = []
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 6:
            continue
        link = cells[0].find("a", href=True)
        if not link:
            continue

        if len(cells) >= 7:
            status    = clean(cells[2].get_text())
            sale_date = clean(cells[3].get_text())
            plaintiff = clean(cells[4].get_text())
            defendant = clean(cells[5].get_text())
            address   = clean(cells[6].get_text())
        else:
            status    = "See detail"
            sale_date = clean(cells[2].get_text())
            plaintiff = clean(cells[3].get_text())
            defendant = clean(cells[4].get_text())
            address   = clean(cells[5].get_text())

        if listing_url == base_url and not is_completed(status):
            continue

        href = link["href"]
        pid  = re.search(r"PropertyId=(\d+)", href)

        cases.append({
            "Case #":          "",
            "Defendant":       defendant,
            "Plaintiff":       plaintiff,
            "Sheriff #":       clean(cells[1].get_text()),
            "Address":         address,
            "Sale Date":       sale_date,
            "Approx Judgment": "",
            "Status":          status,
            "County":          name,
            "_pid":            pid.group(1) if pid else "",
            "_detail_url":     BASE_URL + href if href.startswith("/") else href,
            "_listing_url":    listing_url,
        })

    return cases, listing_url

def get_detail(session, case):
    if not case["_pid"]:
        return

    session.headers.update({"Referer": case["_listing_url"]})
    r = fetch(session, case["_detail_url"])
    if not r or "SaleDetails" not in r.url:
        return

    soup      = BeautifulSoup(r.text, "html.parser")
    body_text = soup.get_text(separator="|", strip=True)

    fields = {}
    for row in soup.select("table tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) >= 2:
            label = clean(cells[0].get_text()).rstrip(":*").lower()
            value = clean(cells[1].get_text())
            if label and value:
                fields[label] = value
    for dt in soup.find_all("dt"):
        dd = dt.find_next_sibling("dd")
        if dd:
            fields[clean(dt.get_text()).rstrip(":*").lower()] = clean(dd.get_text())

    # Case #
    for label, value in fields.items():
        if any(k in label for k in ["court case", "case #", "case no"]):
            case["Case #"] = value
            break
    if not case["Case #"]:
        case["Case #"] = case["Sheriff #"]

    # Current status — try multiple patterns to extract reliably
    # Pattern 1: "Current Status:Purchased - 3rd Party - 2/5/2025"
    status_match = re.search(
        r'Current Status[:\s|]+([A-Za-z][^\|\d\)][^\|]{2,40}?)(?:\s*-\s*\d{1,2}/\d{1,2}/\d{4})?(?:\||\))',
        body_text, re.IGNORECASE
    )
    if status_match:
        current = status_match.group(1).strip().rstrip("-").strip()
        if current and "status" not in current.lower():
            case["Status"] = current
    # Pattern 2: look in fields table for "status" label
    if not case["Status"] or case["Status"] == "See detail":
        for label, value in fields.items():
            if label == "status" and value:
                case["Status"] = value
                break

    # Approx Judgment
    judg_match = re.search(
        r'Approx\.?\s*Judgment\*?\s*:?\s*\|?\s*(\$[\d,]+\.?\d*)',
        body_text, re.IGNORECASE
    )
    if judg_match:
        case["Approx Judgment"] = judg_match.group(1)
        return

    for label, value in fields.items():
        if any(jl in label for jl in JUDGMENT_LABELS):
            if "$" in value or any(c.isdigit() for c in value):
                case["Approx Judgment"] = value
                break

# ─────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────
def main():
    now = datetime.now(timezone.utc).isoformat()

    print("=" * 62)
    print("  NJ SURPLUS FUNDS SCRAPER — SUPABASE VERSION")
    print("=" * 62)

    # Reset new flags from previous run
    print("\n  Resetting new flags...")
    reset_new_flags()

    # Load existing sheriff numbers to skip
    print("  Loading existing cases from Supabase...")
    existing = get_existing_sheriff_numbers()
    print(f"  → {len(existing)} cases already in database")

    session   = make_session()
    all_cases = []

    print("\n[ Step 1 of 2 ]  Loading county listings...\n")
    for county in COUNTIES:
        print(f"  Loading {county['name']} County...")
        cases, _ = get_listing(session, county)
        all_cases.extend(cases)
        print(f"  ✓ {len(cases)} cases found")
        time.sleep(0.5)

    # Filter to only new cases
    new_cases   = [c for c in all_cases if c["Sheriff #"] not in existing]
    known_count = len(all_cases) - len(new_cases)

    print(f"\n  → {len(all_cases)} total cases on site today")
    print(f"  → {known_count} already in database — skipping")
    print(f"  → {len(new_cases)} NEW cases to process\n")

    if not new_cases:
        print("  ✅ No new cases today. Database is up to date.")
        return

    print(f"[ Step 2 of 2 ]  Fetching detail pages for {len(new_cases)} new cases...\n")

    saved = 0
    for i, case in enumerate(new_cases, 1):
        print(f"  [{i:>3}/{len(new_cases)}]  {case['County']:<12}  {case['Sheriff #']:<16}  {case['Defendant'][:32]}")
        get_detail(session, case)
        if save_lead(case, now):
            saved += 1
        time.sleep(0.5)

    new_3p    = sum(1 for c in new_cases if "3rd party" in c.get("Status", "").lower())
    with_judg = sum(1 for c in new_cases if c.get("Approx Judgment"))

    print("\n" + "=" * 62)
    print(f"  ✅  {saved} new leads saved to Supabase")
    print(f"  🟢  {new_3p} are 'Purchased - 3rd Party'")
    print(f"  💰  {with_judg} have an Approx Judgment amount")
    print(f"  ⏭   {known_count} existing cases skipped")
    print("=" * 62)

if __name__ == "__main__":
    main()

