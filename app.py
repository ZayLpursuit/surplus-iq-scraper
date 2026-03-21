"""
Flask backend — reads from Supabase, serves JSON API to frontend.

Setup:
  .env file needs:
    SUPABASE_URL=https://xxxxxxxxxxxx.supabase.co
    SUPABASE_KEY=your-anon-key-here

Run: python3 app.py
"""

from flask import Flask, jsonify, send_from_directory
import requests
import os

app = Flask(__name__, static_folder="static")

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

HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

def supa_get(endpoint, params=None):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{endpoint}",
        headers=HEADERS,
        params=params,
        timeout=15
    )
    r.raise_for_status()
    return r.json()

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/leads")
def get_leads():
    leads = supa_get("leads", params={
        "select":  "id,case_number,defendant,plaintiff,sheriff_number,address,sale_date,approx_judgment,status,county,detail_url,first_seen,is_new",
        "order":   "is_new.desc,first_seen.desc",
        "limit":   "5000",
    })
    return jsonify(leads)

@app.route("/api/stats")
def get_stats():
    all_leads = supa_get("leads", params={
        "select": "status,county,is_new",
        "limit":  "5000",
    })

    total       = len(all_leads)
    third_party = sum(1 for l in all_leads if "3rd party" in (l.get("status") or "").lower())
    new_today   = sum(1 for l in all_leads if l.get("is_new"))

    county_counts = {}
    for l in all_leads:
        c = l.get("county", "Unknown")
        county_counts[c] = county_counts.get(c, 0) + 1
    counties = [{"county": k, "n": v} for k, v in sorted(county_counts.items(), key=lambda x: -x[1])]

    return jsonify({
        "total":       total,
        "third_party": third_party,
        "new_today":   new_today,
        "counties":    counties,
    })

if __name__ == "__main__":
    os.makedirs("static", exist_ok=True)
    print(f"Supabase URL: {SUPABASE_URL[:30]}..." if SUPABASE_URL else "⚠  No SUPABASE_URL set")
    app.run(debug=True, port=5000)

