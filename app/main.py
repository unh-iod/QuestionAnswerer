import os
import json
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

CHAT_SYSTEM_PROMPT = """You are a filter selection assistant embedded in an ACS (American Community Survey) \
disability statistics explorer application. Your ONLY job is to help users update the sidebar filter \
selections by interpreting their natural language requests.

You have access to the full data schema and the user's current selections (provided in each message).

RULES:
1. You may only update filter values that exist in the schema provided. Never invent values.
2. If the user's intent is ambiguous or a required piece of information is missing, ask ONE clear \
   clarifying question. Do not guess.
3. After applying updates, always end your reply with exactly: "Shall I load the data now?"
4. If the user says yes/yep/sure/load/go/do it (or any clear affirmative) in response to that question, \
   set "trigger_fetch": true in your response and do NOT set any updates.
5. If nothing needs to change (e.g. the user asks a question you can't answer via filters), \
   explain your limitations politely. Do not set trigger_fetch or updates.
6. Respond in JSON only — no markdown fences, no preamble. Schema:
{
  "reply": "<conversational response to show the user>",
  "updates": {
    // Include ONLY fields that should change. Omit fields that stay the same.
    // Valid keys and value types:
    "geo_level":   "state" | "county",
    "disability":  "<disability value from schema>",
    "measure":     "<measure value from schema>",
    "geographies": ["<geo name>", ...],   // full replacement list
    "year_mode":   "single" | "all",
    "year":        <integer>,
    "gender":      "<gender value from schema>",
    "race":        "<race value from schema>",
    "age":         "<age value from schema>"
  },
  "trigger_fetch": false   // set true ONLY when user explicitly confirms they want to load data
}
"""

def build_chat_context(current_state, schema):
    """Build a context block describing current selections and full schema."""
    lines = ["=== CURRENT SELECTIONS ==="]
    lines.append(f"Geographic level: {current_state.get('geo_level', 'state')}")
    lines.append(f"Disability type:  {current_state.get('disability', 'not set')}")
    lines.append(f"Measure:          {current_state.get('measure', 'not set')}")
    lines.append(f"Geographies:      {', '.join(current_state.get('geographies', [])) or 'none selected'}")
    lines.append(f"Year mode:        {current_state.get('year_mode', 'single')}")
    lines.append(f"Year:             {current_state.get('year', 'not set')}")
    lines.append(f"Gender:           {current_state.get('gender', 'All')}")
    lines.append(f"Race/ethnicity:   {current_state.get('race', 'All')}")
    lines.append(f"Age group:        {current_state.get('age', 'All')}")

    lines.append("\n=== AVAILABLE SCHEMA ===")

    lines.append("\nDisability types:")
    for d in schema.get("disability_types", []):
        lines.append(f"  value={d['value']}  label={d['label']}")

    lines.append("\nUS/State measures:")
    for m in schema.get("state_measures", []):
        lines.append(f"  value={m['value']}  label={m['label']}  group={m['group']}")

    lines.append("\nCounty measures:")
    for m in schema.get("county_measures", []):
        lines.append(f"  value={m['value']}  label={m['label']}  group={m['group']}")

    lines.append(f"\nUS/State year range: {schema.get('state_years', [])[0]} - {schema.get('state_years', [])[-1]}")
    lines.append(f"County year range:   {schema.get('county_years', [])[0]} - {schema.get('county_years', [])[-1]}")

    lines.append("\nGender options:")
    for g in schema.get("gender_options", []):
        lines.append(f"  value={g['value']}  label={g['label']}")

    lines.append("\nRace/ethnicity options:")
    for r in schema.get("race_options", []):
        lines.append(f"  value={r['value']}  label={r['label']}")

    lines.append("\nAge group sets (vary by measure):")
    for group_key, options in schema.get("age_groups", {}).items():
        labels = ", ".join(o["value"] for o in options)
        lines.append(f"  {group_key}: {labels}")

    lines.append("\nMeasure -> age group mapping:")
    for measure, ag in schema.get("measure_age_group", {}).items():
        lines.append(f"  {measure} -> {ag}")

    lines.append("\nNote: geographies must be exact strings from us_states or us_counties lists.")
    lines.append("Geography names are formatted as 'County Name, ST' for counties.")

    return "\n".join(lines)

def validate_updates(updates, schema):
    """
    Validate and sanitize LLM-proposed updates against the schema.
    Returns a cleaned updates dict with only valid values.
    """
    if not updates:
        return {}

    clean = {}

    # geo_level
    if "geo_level" in updates and updates["geo_level"] in ("state", "county"):
        clean["geo_level"] = updates["geo_level"]

    # disability
    valid_disabilities = {d["value"] for d in schema["disability_types"]}
    if "disability" in updates and updates["disability"] in valid_disabilities:
        clean["disability"] = updates["disability"]

    # measure — valid set depends on geo_level in updates or current
    valid_state   = {m["value"] for m in schema["state_measures"]}
    valid_county  = {m["value"] for m in schema["county_measures"]}
    valid_measures = valid_state | valid_county
    if "measure" in updates and updates["measure"] in valid_measures:
        clean["measure"] = updates["measure"]

    # geographies — validate each against known lists
    if "geographies" in updates and isinstance(updates["geographies"], list):
        valid_geos = set(schema.get("us_states", [])) | set(schema.get("us_counties", []))
        validated_geos = [g for g in updates["geographies"] if g in valid_geos]
        if validated_geos:
            clean["geographies"] = validated_geos

    # year_mode
    if "year_mode" in updates and updates["year_mode"] in ("single", "all"):
        clean["year_mode"] = updates["year_mode"]

    # year — must be integer within either year range
    if "year" in updates:
        try:
            yr = int(updates["year"])
            all_years = set(schema["state_years"]) | set(schema["county_years"])
            if yr in all_years:
                clean["year"] = yr
        except (TypeError, ValueError):
            pass

    # gender
    valid_genders = {g["value"] for g in schema["gender_options"]}
    if "gender" in updates and updates["gender"] in valid_genders:
        clean["gender"] = updates["gender"]

    # race
    valid_races = {r["value"] for r in schema["race_options"]}
    if "race" in updates and updates["race"] in valid_races:
        clean["race"] = updates["race"]

    # age — valid values are union of all age group sets
    valid_ages = set()
    for group in schema.get("age_groups", {}).values():
        for o in group:
            valid_ages.add(o["value"])
    if "age" in updates and updates["age"] in valid_ages:
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

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured."}), 500

    # Build schema dict for context (reuse existing constants)
    schema = {
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
        "us_counties":       getattr(__import__("__main__"), "US_COUNTIES", []),
    }

    context_block = build_chat_context(current_state, schema)

    # Compose messages: inject context into the first user turn of the window
    context_prefix = f"{context_block}\n\n=== USER REQUEST ===\n"
    messages = []
    for i, turn in enumerate(history):
        messages.append({"role": turn["role"], "content": turn["content"]})
    messages.append({"role": "user", "content": context_prefix + user_message})

    try:
        client   = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model      = "claude-sonnet-4-20250514",
            max_tokens = 1024,
            system     = CHAT_SYSTEM_PROMPT,
            messages   = messages,
        )
        raw_text = response.content[0].text.strip()
    except Exception as e:
        logger.error(f"Anthropic API error: {e}")
        return jsonify({"error": f"LLM error: {str(e)}"}), 500

    # Parse JSON response from LLM
    try:
        # Strip markdown fences if model adds them despite instructions
        clean_text = raw_text.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(clean_text)
    except json.JSONDecodeError as e:
        logger.error(f"LLM returned non-JSON: {raw_text}")
        return jsonify({
            "reply":         raw_text,
            "updates":       {},
            "trigger_fetch": False,
        })

    reply         = parsed.get("reply", "")
    raw_updates   = parsed.get("updates", {})
    trigger_fetch = bool(parsed.get("trigger_fetch", False))

    # Validate updates against schema before sending to frontend
    clean_updates = validate_updates(raw_updates, schema)

    return jsonify({
        "reply":         reply,
        "updates":       clean_updates,
        "trigger_fetch": trigger_fetch,
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)