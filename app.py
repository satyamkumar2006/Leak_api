# app.py
# REST API for Fake Leak Data (Educational & Testing Use Only)

import os
import json
import glob
import sqlite3
from flask import Flask, jsonify, request
from flask_cors import CORS
from typing import Generator, Dict, Any, List

app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
CORS(app)  # Enable CORS for all routes

DB_FILE = "fake_leak.db"
SINGLE_JSON = "fake_leak1.json"
PART_GLOB = "fake_leak_part_*.json"  # pattern for split files

# If SQLite DB is present, we'll use it. Otherwise we'll fall back to streaming JSON parts.
USE_DB = os.path.exists(DB_FILE)

# Helper: open DB connection per-request
def get_db_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

# Helper: stream records from JSON part files (memory-safe)
def stream_records_from_parts() -> Generator[Dict[str, Any], None, None]:
    """
    Yield records from:
      - fake_leak1.json (if present), else
      - all files matching fake_leak_part_*.json (sorted)
    """
    if os.path.exists(SINGLE_JSON):
        with open(SINGLE_JSON, "r", encoding="utf-8") as f:
            text = f.read().strip()
            if text.startswith("[") and text.endswith("]"):
                try:
                    data = json.loads(text)
                    for r in data:
                        yield r
                    return
                except Exception:
                    pass  # fall back to parts
    # Fall back to parts
    files = sorted(glob.glob(PART_GLOB))
    if not files:
        return
    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            try:
                chunk = json.load(f)
            except Exception as e:
                app.logger.error(f"Failed to parse {path}: {e}")
                continue
            for rec in chunk:
                yield rec

@app.route("/")
def home():
    return jsonify({
        "API TYPE": "AADHAR TO TRIP DETAILS.",
        "OWNER": "MORTAL",
    })

# Simple record lookup by numeric id
@app.route("/record/<int:record_id>", methods=["GET"])
def get_record(record_id: int):
    if USE_DB:
        conn = get_db_conn()
        row = conn.execute("SELECT * FROM leaks WHERE id = ?", (record_id,)).fetchone()
        conn.close()
        if not row:
            return jsonify({"error": "Record not found"}), 404
        return jsonify(dict(row))
    else:
        for r in stream_records_from_parts():
            if int(r.get("id", -1)) == record_id:
                return jsonify(r)
        return jsonify({"error": "Record not found"}), 404

# Search by Aadhaar-like number (partial or full)
@app.route("/aadhar/", defaults={"aadhar_query": None}, methods=["GET"])
@app.route("/aadhar/<string:aadhar_query>", methods=["GET"])
def search_by_aadhar(aadhar_query: str):
    """
    If called as /aadhar -> return home content
    If called as /aadhar/<number> -> search for that number
    """
    if not aadhar_query:
        return jsonify({
            "API TYPE": "AADHAR TO TRIP DETAILS.",
            "OWNER": "MORTAL",
        })

    if not aadhar_query.isdigit():
        return jsonify({"error": "Aadhaar Number must be digits only"}), 400

    try:
        limit = int(request.args.get("limit", 10))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit and offset must be integers"}), 400

    if limit < 1 or limit > 1000:
        return jsonify({"error": "limit must be between 1 and 1000"}), 400
    if offset < 0:
        return jsonify({"error": "offset must be >= 0"}), 400

    # DB search
    if USE_DB:
        conn = get_db_conn()
        like_term = f"%{aadhar_query}%"
        cur = conn.execute(
            "SELECT * FROM leaks WHERE aadhar_card LIKE ? ORDER BY id LIMIT ? OFFSET ?",
            (like_term, limit, offset)
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        if not rows:
            return jsonify({"message": "No records found for given Aadhaar"}), 404
        return jsonify(rows)

    # JSON parts search
    start = offset
    end = offset + limit
    matches: List[Dict[str, Any]] = []
    idx = 0
    for rec in stream_records_from_parts():
        a = str(rec.get("aadhar_card", ""))
        if aadhar_query in a:
            if idx >= start and idx < end:
                matches.append(rec)
            idx += 1
            if len(matches) >= limit:
                break

    if not matches:
        return jsonify({"message": "No records found for given Aadhaar"}), 404
    return jsonify(matches)

# Search by email (exact)
@app.route("/search", methods=["GET"])
def search_by_email():
    email = request.args.get("email")
    if not email:
        return jsonify({"error": "Please provide ?email=<value>"}), 400

    if USE_DB:
        conn = get_db_conn()
        row = conn.execute("SELECT * FROM leaks WHERE LOWER(email) = LOWER(?)", (email,)).fetchone()
        conn.close()
        if row:
            return jsonify(dict(row))
        return jsonify({"message": "Not found"}), 404

    for rec in stream_records_from_parts():
        if str(rec.get("email", "")).lower() == email.lower():
            return jsonify(rec)
    return jsonify({"message": "Not found"}), 404

if __name__ == "__main__":
    # Startup info
    if USE_DB:
        app.logger.info(f"Using SQLite DB: {DB_FILE}")
    else:
        parts = sorted(glob.glob(PART_GLOB))
        if not parts and not os.path.exists(SINGLE_JSON):
            app.logger.warning("No data files found: place fake_leak.db or fake_leak1.json or fake_leak_part_*.json in the app folder.")
        else:
            app.logger.info(f"Using JSON parts: {len(parts)} files found")
    app.run(host="0.0.0.0", port=5000, debug=True)
