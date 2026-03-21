"""
One-time script: imports your existing surplus_leads.db into Supabase.
Run once after setting up your .env file.

Usage: python3 import_to_supabase.py
"""

import sqlite3
import requests
import os
import sys
import time
from datetime import datetime, timezone

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

HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "resolution=ignore-duplicates,return=minimal",
}

DB_FILE = "surplus_leads.db"

def main():
    print("=" * 55)
    print("  SQLITE → SUPABASE IMPORTER")
    print("=" * 55)

    if not os.path.exists(DB_FILE):
        print(f"✗ {DB_FILE} not found in this folder")
        sys.exit(1)

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM leads").fetchall()
    conn.close()

    print(f"\n  Found {len(rows)} rows in {DB_FILE}")
    print(f"  Uploading to Supabase in batches...\n")

    now        = datetime.now(timezone.utc).isoformat()
    batch_size = 100
    inserted   = 0
    skipped    = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        records = []
        for row in batch:
            records.append({
                "case_number":     row["case_number"]     or "",
                "defendant":       row["defendant"]       or "",
                "plaintiff":       row["plaintiff"]       or "",
                "sheriff_number":  row["sheriff_number"]  or "",
                "address":         row["address"]         or "",
                "sale_date":       row["sale_date"]       or "",
                "approx_judgment": row["approx_judgment"] or "",
                "status":          row["status"]          or "",
                "county":          row["county"]          or "",
                "detail_url":      row["detail_url"]      or "",
                "first_seen":      now,
                "last_updated":    now,
                "is_new":          False,
            })

        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/leads",
                headers=HEADERS,
                json=records,
                timeout=30
            )
            if r.status_code in (200, 201):
                inserted += len(records)
                print(f"  ✓ Batch {i//batch_size + 1}: {len(records)} rows uploaded")
            else:
                print(f"  ⚠  Batch {i//batch_size + 1} error: {r.status_code} — {r.text[:100]}")
                skipped += len(records)
        except Exception as e:
            print(f"  ✗ Batch error: {e}")
            skipped += len(records)

        time.sleep(0.3)

    print(f"\n{'=' * 55}")
    print(f"  ✅  {inserted} rows uploaded to Supabase")
    print(f"  ⏭   {skipped} rows skipped/errored")
    print(f"{'=' * 55}")
    print("\n  Check your Supabase dashboard → Table Editor → leads")

if __name__ == "__main__":
    main()

