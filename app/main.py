import os
import json
import time
import random
import tempfile
import logging
import math
from dotenv import load_dotenv
load_dotenv()
from flask import Flask, jsonify, request, render_template
import pyreadr
import pandas as pd
import numpy as np
from google.cloud import storage
from google.oauth2 import service_account
import anthropic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# ── Anthropic singleton client ─────────────────────────────────────────────────
# Created once at module level so we don't pay HTTP connection setup per request.
# max_retries=0 disables the SDK's built-in retry logic — we handle retries
# ourselves with exponential backoff so we don't multiply requests on 529s.
_anthropic_client: anthropic.Anthropic | None = None

def get_anthropic_client() -> anthropic.Anthropic:
    global _anthropic_client
    if _anthropic_client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY not configured")
        _anthropic_client = anthropic.Anthropic(api_key=api_key, max_retries=0)
        logger.info("Anthropic client initialised (max_retries=0, backoff managed by app)")
    return _anthropic_client

# ── Anthropic API retry constants ──────────────────────────────────────────────
ANTHROPIC_MAX_RETRIES = 5
ANTHROPIC_BASE_DELAY  = 1.0  # seconds; doubles each attempt + jitter

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

# Age group options per measure group
AGE_GROUPS = {
    "population": [
        {"value": "All",              "label": "All"},
        {"value": "Under5",           "label": "Under 5 Years"},
        {"value": "5to17",            "label": "5 to 17 Years"},
        {"value": "18to64",           "label": "18 to 64 Years"},
        {"value": "65plus",           "label": "65 Years and Over"},
    ],
    "employment": [
        {"value": "All",              "label": "All"},
        {"value": "Under18",          "label": "Under 18 Years"},
        {"value": "18to64",           "label": "18 to 64 Years"},
        {"value": "18to24",           "label": "18 to 24 Years"},
        {"value": "25to34",           "label": "25 to 34 Years"},
        {"value": "35to44",           "label": "35 to 44 Years"},
        {"value": "45to54",           "label": "45 to 54 Years"},
        {"value": "55to64",           "label": "55 to 64 Years"},
        {"value": "65plus",           "label": "65 Years and Over"},
    ],
    "earnings_poverty": [
        {"value": "All",              "label": "All"},
        {"value": "Under18",          "label": "Under 18 Years"},
        {"value": "18to24",           "label": "18 to 24 Years"},
        {"value": "25to34",           "label": "25 to 34 Years"},
        {"value": "35to44",           "label": "35 to 44 Years"},
        {"value": "45to54",           "label": "45 to 54 Years"},
        {"value": "55to64",           "label": "55 to 64 Years"},
        {"value": "65plus",           "label": "65 Years and Over"},
    ],
    "vet_insur": [
        {"value": "All",              "label": "All"},
        {"value": "Under5",           "label": "Under 5 Years"},
        {"value": "5to17",            "label": "5 to 17 Years"},
        {"value": "18to64",           "label": "18 to 64 Years"},
        {"value": "65plus",           "label": "65 Years and Over"},
    ],
    "education": [
        {"value": "All",              "label": "All"},
        {"value": "25to34",           "label": "25 to 34 Years"},
        {"value": "35to44",           "label": "35 to 44 Years"},
        {"value": "45to54",           "label": "45 to 54 Years"},
        {"value": "55to64",           "label": "55 to 64 Years"},
        {"value": "65plus",           "label": "65 Years and Over"},
    ],
}

# Map each measure to its age group set
MEASURE_AGE_GROUP = {
    "AP":        "population",
    "APwD":      "population",
    "EMP":       "employment",
    "POVERTY":   "earnings_poverty",
    "EARNINGS":  "earnings_poverty",
    "VET":       "vet_insur",
    "INSUR":     "vet_insur",
    "LESSHS":    "education",
    "HSGED":     "education",
    "SOMECOL":   "education",
    "COLLD":     "education",
    "MOREC":     "education",
    "COLLDMORE": "education",
}

GENDER_OPTIONS = [
    {"value": "All",    "label": "All"},
    {"value": "Female", "label": "Female"},
    {"value": "Male",   "label": "Male"},
]

RACE_OPTIONS = [
    {"value": "All",                "label": "All"},
    {"value": "Hispanic",           "label": "Hispanic"},
    {"value": "NonHispanicAsian",   "label": "Non-Hispanic Asian"},
    {"value": "NonHispanicBlack",   "label": "Non-Hispanic Black"},
    {"value": "NonHispanicOther",   "label": "Non-Hispanic Other"},
    {"value": "NonHispanicWhite",   "label": "Non-Hispanic White"},
]

# i-index lookup: (gender_is_any, race_is_any, age_is_any) -> i
# Row order from spec (1-based), row 8 (Any/Any/Any) is excluded/invalid
I_LOOKUP = {
    (False, False, False): 1,
    (False, False, True):  2,
    (False, True,  False): 3,
    (False, True,  True):  4,
    (True,  False, False): 5,
    (True,  False, True):  6,
    (True,  True,  False): 7,
    # (True, True, True) = 8 is excluded
}

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
    "U.S.", "Alabama", "Alaska", "Arizona", "Arkansas", "California",
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

US_COUNTIES = pd.read_csv('counties.csv')['x'].to_list()


# ── Helper functions ───────────────────────────────────────────────────────────

def get_county_filters(measure):
    if measure == "PREV":
        return COUNTY_FILTERS_PREV
    elif measure in ("POVERTY", "INSURANCE", "INSURANCE1", "INSURANCE2"):
        return COUNTY_FILTERS_TWO
    else:  # EDUC, E2PR, UNEMP
        return COUNTY_FILTERS_ONE

def compute_i(gender, race, age):
    """Compute i index from filter selections. Returns None if Any/Any/Any."""
    g_any = gender != "All"
    r_any = race   != "All"
    a_any = age    != "All"
    return I_LOOKUP.get((g_any, r_any, a_any))  # None if (True,True,True)

def resolve_measure_and_suffix(measure, age):
    """
    For EMP: if age == "18to64", use EMP_age filename suffix.
    Returns actual measure string to use in filename.
    """
    if measure == "EMP" and age == "18to64":
        return "EMP_age"
    return measure


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

# ── API routes ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/schema")
def api_schema():
    """Return full schema for the frontend to build dropdowns."""
    return jsonify({
        "disability_types":  DISABILITY_TYPES,
        "state_measures":    STATE_MEASURES,
        "county_measures":   COUNTY_MEASURES,
        "state_years":       STATE_YEAR_RANGE,
        "county_years":      COUNTY_YEAR_RANGE,
        "gender_options":    GENDER_OPTIONS,
        "race_options":      RACE_OPTIONS,
        "age_groups":        AGE_GROUPS,
        "measure_age_group": MEASURE_AGE_GROUP,
        "us_states":         US_STATES,
        "us_counties":       US_COUNTIES
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
    gender     = body.get("gender", "All")
    race       = body.get("race",   "All")
    age        = body.get("age",    "All")
    i          = body.get("i")          # county only

    if not measure:
        return jsonify({"error": "No measure selected."}), 400
    if not disability:
        return jsonify({"error": "No disability type selected."}), 400

    # At county level, only PREV (Disability Prevalence) supports all disability
    # types. All other county measures only have data for the base "disability" type.
    if geo_level == "county" and measure != "PREV":
        disability = "disability"

    if geo_level == "state":
        i = compute_i(gender, race, age)
        if i is None:
            return jsonify({"error": "Invalid filter combination: all three filters cannot be specific values simultaneously."}), 400
        actual_measure = resolve_measure_and_suffix(measure, age)
    else:
        actual_measure = measure

    frames = []
    total  = len(years)

    for idx, year in enumerate(years):
        fname = build_filename(year, i, disability, actual_measure, geo_level)
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
        if col in ("ST_text", "Geographic.Area.Name"):
            geo_col = col
            break

    print(combined)
    print(geo_col,geos)
    # Filter to selected geographies if a geo column was found and geos requested
    if geo_col and geos and "All" not in geos:
        pattern = '|'.join(geos)
        combined = combined[combined[geo_col].str.contains(pattern)]

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
    gender     = body.get("gender", "All")
    race       = body.get("race",   "All")
    age        = body.get("age",    "All")
    i          = body.get("i")          # county only
    results = {}
    if not measure or not disability:
        return jsonify({"error": "measure and disability are required."}), 400

    # At county level, only PREV (Disability Prevalence) supports all disability
    # types. All other county measures only have data for the base "disability" type.
    if geo_level == "county" and measure != "PREV":
        disability = "disability"

    if geo_level == "state":
        i = compute_i(gender, race, age)
        if i is None:
            return jsonify({"error": "Invalid filter combination: all three filters cannot be specific values simultaneously."}), 400
        actual_measure = resolve_measure_and_suffix(measure, age)
    else:
        actual_measure = measure
    for year in years:
        fname = build_filename(year, i, disability, actual_measure, geo_level)
        results[year] = blob_exists(fname)
    return jsonify(results)

# ── Chat endpoint ──────────────────────────────────────────────────────────────
# Build the system prompt ONCE at startup, embedding the full schema as static
# context.  This string is sent with cache_control="ephemeral" so Anthropic
# caches it server-side (up to 5 min) and reuses it across requests — meaning
# each chat turn only pays tokens for the small dynamic "current state" block,
# not the entire schema repeated on every call.

def _build_system_prompt() -> str:
    lines = []
    lines.append(
        "You are a filter selection assistant embedded in an ACS (American Community Survey) "
        "disability statistics explorer application. Your ONLY job is to help users update the "
        "sidebar filter selections by interpreting their natural language requests."
    )
    lines.append("")
    lines.append("RULES:")
    lines.append("1. You may only update filter values that exist in the schema below. Never invent values.")
    lines.append("2. If the user\'s intent is ambiguous or a required piece of information is missing, ask ONE clear clarifying question. Do not guess.")
    lines.append("3. The \"reply\" field value must always end with exactly: \"Shall I load the data now?\" — this text goes INSIDE the JSON reply field, NOT after the closing brace.")
    lines.append("4. If the user says yes/yep/sure/load/go/do it (or any clear affirmative), set \"trigger_fetch\": true and do NOT set any updates.")
    lines.append("5. If nothing needs to change, explain your limitations politely. Do not set trigger_fetch or updates.")
    lines.append("6. Your ENTIRE response must be a single JSON object and nothing else — no text before the opening brace, no text after the closing brace, no markdown fences.")
    lines.append('''
{
  "reply": "<conversational response to show the user>",
  "updates": {
    "geo_level":   "state" | "county",
    "disability":  "<disability value>",
    "measure":     "<measure value>",
    "geographies": ["<geo name>", ...],
    "year_mode":   "single" | "all",
    "year":        <integer>,
    "gender":      "<gender value>",
    "race":        "<race value>",
    "age":         "<age value>"
  },
  "trigger_fetch": false
}''')

    lines.append("")
    lines.append("=" * 60)
    lines.append("COMPLETE DATA SCHEMA (static — valid values only)")
    lines.append("=" * 60)

    lines.append("\nDISABILITY TYPES:")
    for d in DISABILITY_TYPES:
        lines.append(f"  value={d['value']!r:30s}  label={d['label']}")

    lines.append("\nCOUNTY DISABILITY RULE: At county level, only the PREV measure")
    lines.append("supports all disability types. All other county measures are")
    lines.append("restricted to disability=\"disability\" (Any Disability) only.")

    lines.append("\nUS/STATE MEASURES:")
    for m in STATE_MEASURES:
        lines.append(f"  value={m['value']!r:15s}  label={m['label']!r:45s}  group={m['group']}")

    lines.append("\nCOUNTY MEASURES:")
    for m in COUNTY_MEASURES:
        lines.append(f"  value={m['value']!r:15s}  label={m['label']!r:45s}  group={m['group']}")

    lines.append(f"\nUS/STATE YEAR RANGE: {STATE_YEAR_RANGE[0]} – {STATE_YEAR_RANGE[-1]}")
    lines.append(f"COUNTY YEAR RANGE:   {COUNTY_YEAR_RANGE[0]} – {COUNTY_YEAR_RANGE[-1]}")

    lines.append("\nGENDER OPTIONS:")
    for g in GENDER_OPTIONS:
        lines.append(f"  value={g['value']!r:10s}  label={g['label']}")

    lines.append("\nRACE/ETHNICITY OPTIONS:")
    for r in RACE_OPTIONS:
        lines.append(f"  value={r['value']!r:25s}  label={r['label']}")

    lines.append("\nAGE GROUP SETS (options vary by measure):")
    for group_key, options in AGE_GROUPS.items():
        vals = ", ".join(o["value"] for o in options)
        lines.append(f"  {group_key}: {vals}")

    lines.append("\nMEASURE → AGE GROUP MAPPING:")
    for measure, ag in MEASURE_AGE_GROUP.items():
        lines.append(f"  {measure} -> {ag}")

    lines.append("\nUS STATES (exact strings for geographies):")
    lines.append("  " + ", ".join(US_STATES))

    lines.append("\nCOUNTY FORMAT: \"County Name, ST\" — must be an exact match from the counties list.")
    lines.append("(The full county list is too large to embed here; use the exact format above.)")

    return "\n".join(lines)


# Built once at module load — all subsequent requests reuse this string.
CHAT_SYSTEM_PROMPT: str = _build_system_prompt()
logger.info(f"Chat system prompt built: {len(CHAT_SYSTEM_PROMPT):,} chars")

def build_chat_context(current_state: dict) -> str:
    """
    Build the small per-request context block containing ONLY the user's
    current sidebar selections.  The full schema is already embedded in
    CHAT_SYSTEM_PROMPT (built once at startup) so we never repeat it here.
    """
    geos = current_state.get("geographies", [])
    geo_str = ", ".join(geos) if geos else "none selected"
    lines = [
        "=== CURRENT SIDEBAR SELECTIONS ===",
        f"Geographic level : {current_state.get('geo_level', 'state')}",
        f"Disability type  : {current_state.get('disability', 'not set')}",
        f"Measure          : {current_state.get('measure', 'not set')}",
        f"Geographies      : {geo_str}",
        f"Year mode        : {current_state.get('year_mode', 'single')}",
        f"Year             : {current_state.get('year', 'not set')}",
        f"Gender           : {current_state.get('gender', 'All')}",
        f"Race/ethnicity   : {current_state.get('race', 'All')}",
        f"Age group        : {current_state.get('age', 'All')}",
    ]
    return "\n".join(lines)

# Pre-compute validation sets once at module load (same lifetime as schema constants).
_VALID_DISABILITIES = {d["value"] for d in DISABILITY_TYPES}
_VALID_STATE_MEASURES  = {m["value"] for m in STATE_MEASURES}
_VALID_COUNTY_MEASURES = {m["value"] for m in COUNTY_MEASURES}
_VALID_MEASURES  = _VALID_STATE_MEASURES | _VALID_COUNTY_MEASURES
_VALID_GEOS      = set(US_STATES)  # counties added after US_COUNTIES is loaded below
_VALID_GENDERS   = {g["value"] for g in GENDER_OPTIONS}
_VALID_RACES     = {r["value"] for r in RACE_OPTIONS}
_VALID_AGES      = {o["value"] for opts in AGE_GROUPS.values() for o in opts}
_ALL_YEARS       = set(STATE_YEAR_RANGE) | set(COUNTY_YEAR_RANGE)
# Seed counties into _VALID_GEOS now that US_COUNTIES is available
_VALID_GEOS      |= set(US_COUNTIES)




def validate_updates(updates: dict) -> dict:
    """
    Validate and sanitize LLM-proposed updates against the schema constants.
    Returns a cleaned updates dict with only valid values.
    Schema is read from module-level constants — no per-request dict needed.
    """
    if not updates:
        return {}

    clean = {}

    if "geo_level" in updates and updates["geo_level"] in ("state", "county"):
        clean["geo_level"] = updates["geo_level"]

    if "disability" in updates and updates["disability"] in _VALID_DISABILITIES:
        clean["disability"] = updates["disability"]

    if "measure" in updates and updates["measure"] in _VALID_MEASURES:
        clean["measure"] = updates["measure"]

    if "geographies" in updates and isinstance(updates["geographies"], list):
        validated_geos = [g for g in updates["geographies"] if g in _VALID_GEOS]
        if validated_geos:
            clean["geographies"] = validated_geos

    if "year_mode" in updates and updates["year_mode"] in ("single", "all"):
        clean["year_mode"] = updates["year_mode"]

    if "year" in updates:
        try:
            yr = int(updates["year"])
            if yr in _ALL_YEARS:
                clean["year"] = yr
        except (TypeError, ValueError):
            pass

    if "gender" in updates and updates["gender"] in _VALID_GENDERS:
        clean["gender"] = updates["gender"]

    if "race" in updates and updates["race"] in _VALID_RACES:
        clean["race"] = updates["race"]

    if "age" in updates and updates["age"] in _VALID_AGES:
        clean["age"] = updates["age"]

    return clean


@app.route("/api/chat", methods=["POST"])
def api_chat():
    """
    Process a chat message and return filter update instructions.
    Body JSON:
      message       : str   — user's message
      history       : list  — [{role, content}, ...] previous turns
      current_state : dict  — current sidebar selections
    """
    body          = request.get_json()
    user_message  = body.get("message", "").strip()
    history       = body.get("history", [])
    current_state = body.get("current_state", {})

    if not user_message:
        return jsonify({"error": "Empty message."}), 400

    # Resolve the singleton client (raises RuntimeError if key missing)
    try:
        client = get_anthropic_client()
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 500

    # Build the small per-request context block (current state only — schema is
    # already embedded in the cached system prompt built at startup).
    context_block  = build_chat_context(current_state)
    context_prefix = f"{context_block}\n\n=== USER REQUEST ===\n"

    messages = []
    for i, turn in enumerate(history):
        messages.append({"role": turn["role"], "content": turn["content"]})
    messages.append({"role": "user", "content": context_prefix + user_message})

    # ── Call Anthropic API with exponential backoff for 529 overload errors ────
    # The system prompt is sent with cache_control="ephemeral" so Anthropic
    # caches the large static schema block server-side (TTL ~5 min), billing
    # only cache-read tokens on subsequent hits instead of full input tokens.
    raw_text = None
    last_exc = None

    for attempt in range(ANTHROPIC_MAX_RETRIES):
        try:
            response = client.messages.create(
                model      = "claude-haiku-4-5-20251001",
                max_tokens = 2048,
                system     = [
                    {
                        "type": "text",
                        "text": CHAT_SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages   = messages,
            )
            raw_text = response.content[0].text.strip()
            last_exc = None
            break  # success — exit retry loop

        except anthropic.APIStatusError as e:
            last_exc = e
            if e.status_code == 529:
                # Exponential backoff with full jitter
                delay = ANTHROPIC_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
                request_id = getattr(e.response, "headers", {}).get("request-id", "unknown")
                logger.warning(
                    f"Anthropic 529 overloaded (attempt {attempt + 1}/{ANTHROPIC_MAX_RETRIES}), "
                    f"request_id={request_id}, body={e.body}. "
                    f"Retrying in {delay:.2f}s..."
                )
                time.sleep(delay)
                continue
            # Non-529 API error — don't retry
            logger.error(f"Anthropic API error {e.status_code}: {e}")
            return jsonify({"error": f"API error: {str(e)}"}), 500

        except Exception as e:
            last_exc = e
            logger.error(f"Unexpected error calling Anthropic: {e}")
            return jsonify({"error": f"LLM error: {str(e)}"}), 500

    if last_exc is not None:
        # All retries exhausted — return a user-friendly 503 instead of crashing
        logger.error(f"Anthropic still overloaded after {ANTHROPIC_MAX_RETRIES} attempts: {last_exc}")
        return jsonify({
            "error": "The AI assistant is temporarily unavailable due to high demand. Please try again in a moment."
        }), 503

    # ── Parse JSON response from LLM ───────────────────────────────────────────
    # Extract the outermost { ... } from the response, tolerating any text the
    # model may have leaked before the opening brace or after the closing brace.
    def extract_json(text: str) -> dict | None:
        start = text.find("{")
        end   = text.rfind("}")
        if start == -1 or end == -1 or end < start:
            return None
        candidate = text[start:end + 1]
        # Also strip markdown fences in case they appear inside the slice
        candidate = candidate.replace("```json", "").replace("```", "").strip()
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            return None

    parsed = extract_json(raw_text)
    if parsed is None:
        logger.error(f"LLM returned unparseable response: {raw_text!r}")
        return jsonify({
            "reply":         "Sorry, I couldn't understand the assistant's response. Please try again.",
            "updates":       {},
            "trigger_fetch": False,
        })

    reply         = parsed.get("reply", "")
    raw_updates   = parsed.get("updates", {})
    trigger_fetch = bool(parsed.get("trigger_fetch", False))

    # Validate updates against schema before sending to frontend
    clean_updates = validate_updates(raw_updates)

    # Build full state: start from current selections, overlay validated changes.
    # This ensures the frontend always receives every filter key — not just the
    # ones that changed — so it can do a full replacement rather than a partial merge.
    full_state = {
        "geo_level":   current_state.get("geo_level",   "state"),
        "disability":  current_state.get("disability",  "disability"),
        "measure":     current_state.get("measure",     None),
        "geographies": current_state.get("geographies", []),
        "year_mode":   current_state.get("year_mode",   "single"),
        "year":        current_state.get("year",        2023),
        "gender":      current_state.get("gender",      "All"),
        "race":        current_state.get("race",        "All"),
        "age":         current_state.get("age",         "All"),
    }
    full_state.update(clean_updates)

    # If geo_level changed but measure was not explicitly set by the LLM,
    # translate the measure to its equivalent in the new geo level.
    # This mirrors the frontend translateMeasure() logic.
    MEASURE_TRANSLATE = {
        ("state", "county"): {"EMP": "E2PR"},
        ("county", "state"): {"E2PR": "EMP"},
    }
    prev_geo  = current_state.get("geo_level", "state")
    final_geo = full_state.get("geo_level", "state")
    if prev_geo != final_geo and "measure" not in clean_updates:
        translation_map = MEASURE_TRANSLATE.get((prev_geo, final_geo), {})
        current_measure = full_state.get("measure")
        if current_measure in translation_map:
            full_state["measure"] = translation_map[current_measure]
        else:
            # Measure doesn't exist in target geo level — clear it
            valid_measures = (
                {m["value"] for m in STATE_MEASURES}
                if final_geo == "state"
                else {m["value"] for m in COUNTY_MEASURES}
            )
            if current_measure not in valid_measures:
                full_state["measure"] = None

    return jsonify({
        "reply":         reply,
        "updates":       full_state,   # always the complete set of filter keys
        "trigger_fetch": trigger_fetch,
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)