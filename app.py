# app.py
# REST API for Fake Leak Data (Educational & Testing Use Only)

import json
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
CORS(app)  # Enable CORS for all routes

# Load dataset
with open("fake_leak1.json", "r", encoding="utf-8") as f:
    data = json.load(f)


@app.route("/")
def home():
    return jsonify({
        "API TYPE": "AADHAR NUMBER TO TRIP DETAILS.",
        "OWNER": "MORTAL"
    })


@app.route("/aadhar/<string:aadhar_number>", methods=["GET"])
def search_by_aadhar(aadhar_number):
    """
    Search by Aadhaar-like number (exact or partial digits)
    Example: /aadhar/123456
    """
    # basic input validation (digits only)
    if not aadhar_number.isdigit():
        return jsonify({"error": "Aadhaar number must be digits only"}), 400

    results = [u for u in data if aadhar_number in u["aadhar_card"]]

    if not results:
        return jsonify({"message": "No records found for given Aadhaar"}), 404

    return jsonify(results)


if __name__ == "__main__":
    # Run the Flask app
    app.run(host="0.0.0.0", port=5000, debug=True)
