from dotenv import load_dotenv
import os
import io
import json
import tempfile
import logging
from flask import Flask, jsonify, request, render_template, send_file
import pyreadr
import pandas as pd
import numpy as np
import math
from google.cloud import storage
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)

# ── GCS configuration ──────────────────────────────────────────────────────────
GCS_BUCKET = os.environ.get("GCS_BUCKET_NAME")

def get_gcs_client():
    """Return a GCS client using env-var credentials."""
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        creds_info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(creds_info)
        return storage.Client(credentials=creds)
    # Fallback: use GOOGLE_APPLICATION_CREDENTIALS file path or ADC
    return storage.Client()

# ── Data schema constants ──────────────────────────────────────────────────────

DISABILITY_TYPES = [
    {"value": "disability",        "label": "Any Disability"},
    {"value": "hearing",           "label": "Hearing Difficulty"},
    {"value": "seeing",            "label": "Vision Difficulty"},
    {"value": "mobility",          "label": "Ambulatory Difficulty"},
    {"value": "remembering",       "label": "Cognitive Difficulty"},
    {"value": "independentliving", "label": "Independent Living Difficulty"},
    {"value": "selfcare",          "label": "Self-Care Difficulty"},
]

# US/State measures
STATE_MEASURES = [
    {"value": "AP",        "label": "All People",                          "group": "Population"},
    {"value": "APwD",      "label": "All People with Disabilities",        "group": "Population"},
    {"value": "EMP",       "label": "Employment-to-Population Ratio",      "group": "Economic"},
    {"value": "EMP_age",   "label": "Employment Ratio by Age Group",       "group": "Economic"},
    {"value": "POVERTY",   "label": "Percent in Poverty",                  "group": "Economic"},
    {"value": "EARNINGS",  "label": "Full-Time Workers' Earnings",         "group": "Economic"},
    {"value": "VET",       "label": "Veteran Status",                      "group": "Social"},
    {"value": "INSUR",     "label": "Health Insurance Coverage",           "group": "Social"},
    {"value": "LESSHS",    "label": "Less Than High School",               "group": "Education"},
    {"value": "HSGED",     "label": "High School or Equivalent",           "group": "Education"},
    {"value": "SOMECOL",   "label": "Some College",                        "group": "Education"},
    {"value": "COLLD",     "label": "College Degree",                      "group": "Education"},
    {"value": "MOREC",     "label": "Graduate Degree",                     "group": "Education"},
    {"value": "COLLDMORE", "label": "College Degree or More",              "group": "Education"},
]

# County measures
COUNTY_MEASURES = [
    {"value": "PREV",       "label": "Disability Prevalence",              "group": "Population"},
    {"value": "POVERTY",    "label": "Percent in Poverty",                 "group": "Economic"},
    {"value": "E2PR",       "label": "Employment-to-Population Ratio",     "group": "Economic"},
    {"value": "UNEMP",      "label": "Unemployment Rate",                  "group": "Economic"},
    {"value": "EDUC",       "label": "Educational Attainment",             "group": "Education"},
    {"value": "INSURANCE",  "label": "Health Insurance (Overall)",         "group": "Social"},
    {"value": "INSURANCE1", "label": "Health Insurance (Type 1)",          "group": "Social"},
    {"value": "INSURANCE2", "label": "Health Insurance (Type 2)",          "group": "Social"},
]

STATE_YEAR_RANGE  = list(range(2017, 2025))  # 2017–2024
COUNTY_YEAR_RANGE = list(range(2012, 2025))  # 2012–2024

# Filter combinations (i index) for US/State — 8 combinations
STATE_FILTERS = [
    {"i": 1, "label": "Gender: All  |  Race/Ethnicity: All  |  Age Group: All"},
    {"i": 2, "label": "Gender: All  |  Race/Ethnicity: All  |  Age Group: Any"},
    {"i": 3, "label": "Gender: All  |  Race/Ethnicity: Any  |  Age Group: All"},
    {"i": 4, "label": "Gender: All  |  Race/Ethnicity: Any  |  Age Group: Any"},
    {"i": 5, "label": "Gender: Any  |  Race/Ethnicity: All  |  Age Group: All"},
    {"i": 6, "label": "Gender: Any  |  Race/Ethnicity: All  |  Age Group: Any"},
    {"i": 7, "label": "Gender: Any  |  Race/Ethnicity: Any  |  Age Group: All"},
    {"i": 8, "label": "Gender: Any  |  Race/Ethnicity: Any  |  Age Group: Any"},
]

# County filter combinations vary by measure
COUNTY_FILTERS_PREV = [
    {"i": 1, "label": "Gender: All  |  Age Group: All"},
    {"i": 2, "label": "Gender: All  |  Age Group: Any"},
    {"i": 3, "label": "Gender: Any  |  Age Group: All"},
    {"i": 4, "label": "Gender: Any  |  Age Group: Any"},
]
COUNTY_FILTERS_TWO = [
    {"i": 1, "label": "Age Group: All"},
    {"i": 2, "label": "Age Group: Any"},
]
COUNTY_FILTERS_ONE = [
    {"i": 1, "label": "Age Group: All"},
]

US_STATES = [
    "United States", "Alabama", "Alaska", "Arizona", "Arkansas", "California",
    "Colorado", "Connecticut", "Delaware", "District of Columbia", "Florida",
    "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
    "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
    "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico",
    "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas",
    "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin",
    "Wyoming",
]

# ── Helper functions ───────────────────────────────────────────────────────────

def get_county_filters(measure):
    if measure == "PREV":
        return COUNTY_FILTERS_PREV
    elif measure in ("POVERTY", "INSURANCE", "INSURANCE1", "INSURANCE2"):
        return COUNTY_FILTERS_TWO
    else:  # EDUC, E2PR, UNEMP
        return COUNTY_FILTERS_ONE

def build_filename(year, i, disability, measure, geo_level):
    suffix = "_COUNTY" if geo_level == "county" else ""
    return f"acs{year}_{i}_{disability}_{measure}{suffix}.rds"

def fetch_rds_from_gcs(blob_name):
    """Download an RDS file from GCS and return a pandas DataFrame."""
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_name)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".rds", delete=False) as tmp:
            tmp_path = tmp.name

        # Step 1: download
        logger.info(f"[GCS] Downloading gs://{GCS_BUCKET}/{blob_name} -> {tmp_path}")
        blob.download_to_filename(tmp_path)
        file_size = os.path.getsize(tmp_path)
        logger.info(f"[GCS] Download complete. File size: {file_size} bytes")

        if file_size == 0:
            raise ValueError(f"Downloaded file is empty (0 bytes): {blob_name}")

        # Step 2: read with pyreadr
        logger.info(f"[pyreadr] Reading {tmp_path}")
        result = pyreadr.read_r(tmp_path)
        logger.info(f"[pyreadr] Keys in result: {list(result.keys())}")

        df = list(result.values())[0]
        logger.info(f"[pyreadr] DataFrame shape: {df.shape}, columns: {list(df.columns)}")
        return df

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

def blob_exists(blob_name):
    try:
        client = get_gcs_client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception:
        return False

# ── API routes ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/schema")
def api_schema():
    """Return full schema for the frontend to build dropdowns."""
    return jsonify({
        "disability_types": DISABILITY_TYPES,
        "state_measures":   STATE_MEASURES,
        "county_measures":  COUNTY_MEASURES,
        "state_years":      STATE_YEAR_RANGE,
        "county_years":     COUNTY_YEAR_RANGE,
        "state_filters":    STATE_FILTERS,
        "us_states":        US_STATES,
    })

@app.route("/api/county_filters")
def api_county_filters():
    measure = request.args.get("measure", "")
    return jsonify({"filters": get_county_filters(measure)})

@app.route("/api/data", methods=["POST"])
def api_data():
    """
    Fetch and return data for the selected combination.
    Body JSON:
      geo_level   : "state" | "county"
      geographies : list of geography name strings
      measure     : e.g. "EMP"
      disability  : e.g. "disability"
      years       : list of ints  OR  [year]  for single year
      i           : int (filter index)
    """
    body       = request.get_json()
    geo_level  = body.get("geo_level")
    geos       = body.get("geographies", [])
    measure    = body.get("measure")
    disability = body.get("disability")
    years      = body.get("years", [])
    i          = body.get("i", 1)

    frames = []
    total  = len(years)

    for idx, year in enumerate(years):
        fname = build_filename(year, i, disability, measure, geo_level)
        logger.info(f"Fetching {fname} ({idx+1}/{total})")
        try:
            df = fetch_rds_from_gcs(fname)
            df["_year"] = year
            frames.append(df)
        except Exception as e:
            logger.warning(f"Could not load {fname}: {e}")

    if not frames:
        return jsonify({"error": "No data files could be loaded for the selected combination."}), 404

    combined = pd.concat(frames, ignore_index=True)

    # Detect geography column (first string column that looks like state/county names)
    geo_col = None
    for col in combined.columns:
        if col.lower() in ("state", "county", "geography", "geo", "name", "geoid", "fips"):
            geo_col = col
            break

    # Filter to selected geographies if a geo column was found and geos requested
    if geo_col and geos and "All" not in geos:
        combined = combined[combined[geo_col].isin(geos)]

    # Robustly replace all NaN/Inf variants with None for JSON serialisation.
    # pd.notnull misses some R nan values that survive as float('nan'),
    # so we sanitize every value explicitly.
    def sanitize(v):
        if v is None:
            return None
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        if isinstance(v, (np.floating,)) and (np.isnan(v) or np.isinf(v)):
            return None
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.bool_,)):
            return bool(v)
        return v

    rows = [
        {k: sanitize(v) for k, v in row.items()}
        for row in combined.to_dict(orient="records")
    ]

    return jsonify({
        "columns":  list(combined.columns),
        "rows":     combined.to_dict(orient="records"),
        "geo_col":  geo_col,
        "total_files": total,
        "loaded_files": len(frames),
    })

@app.route("/api/check_files", methods=["POST"])
def api_check_files():
    """Return which files exist in the bucket for the current selection."""
    body       = request.get_json()
    geo_level  = body.get("geo_level")
    measure    = body.get("measure")
    disability = body.get("disability")
    years      = body.get("years", [])
    i          = body.get("i", 1)

    results = {}
    for year in years:
        fname = build_filename(year, i, disability, measure, geo_level)
        results[year] = blob_exists(fname)
    return jsonify(results)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)