# ════════════════════════════════════════════════════════════════════════════════
# DISABILITY STATISTICS EXPLORER - ACS DATA API
# ════════════════════════════════════════════════════════════════════════════════
# Purpose: Flask web app that lets users explore American Community Survey (ACS)
# disability statistics by filtering by geography, demographics, year, and measures.
# The app has a chat feature where an LLM helps users select filters in natural language.
#
# Architecture:
#   - Frontend: HTML/JS (renders the UI and filter selections)
#   - Backend (this file): Flask API that manages filters and fetches data
#   - Data: RDS files stored in Google Cloud Storage (GCS)
#   - LLM: OpenAI-compatible API for chat-based filter selection help
# ════════════════════════════════════════════════════════════════════════════════

import os
import json
import time
import random
import tempfile
import logging
import math
import requests

from flask import Flask, jsonify, request, render_template
import pyreadr  # Library to read R .rds files into pandas DataFrames
import pandas as pd
import numpy as np
from google.cloud import storage  # Google Cloud Storage client
from google.oauth2 import service_account  # For GCS authentication
import openai  # OpenAI API client (configured for DeepThought endpoint)

import socket
import os
from dotenv import load_dotenv  # Load environment variables from .env file

# ── ENVIRONMENT SETUP ──────────────────────────────────────────────────────────
# Load environment variables from .env file ONLY when running locally (hostname MSI).
# In cloud deployments, env vars come from the deployment environment.
if socket.gethostname() == "MSI":
    load_dotenv()
else:
    pass

# Import health check routes (for monitoring / liveness probes)
from health_check import register_health_routes

# ── LOGGING SETUP ──────────────────────────────────────────────────────────────
# Configure logging so we can see what's happening during development and debugging.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── FLASK APP INITIALIZATION ──────────────────────────────────────────────────────
app = Flask(__name__)
register_health_routes(app)  # Add /health endpoint for monitoring

# Get the GCS bucket name from environment variables
GCS_BUCKET = os.environ.get("GCS_BUCKET_NAME")

# ════════════════════════════════════════════════════════════════════════════════
# GOOGLE CLOUD STORAGE (GCS) SETUP
# ════════════════════════════════════════════════════════════════════════════════
# The app stores all ACS data files in a GCS bucket as RDS (R data) files.
# These functions handle authentication and file operations.

def get_gcs_client():
    """
    Return an authenticated Google Cloud Storage client.
    
    Authentication flow:
    1. First, tries to load credentials from the GOOGLE_APPLICATION_CREDENTIALS_JSON
       environment variable (useful in cloud deployments where we can't use files).
    2. Falls back to the GOOGLE_APPLICATION_CREDENTIALS file path or Application Default
       Credentials (ADC). ADC tries credentials in this order: env var, .gcloud files, 
       attached service account key.
    
    Why separate auth? Using JSON credentials in env vars avoids storing sensitive
    files in the codebase or container.
    """
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        # Parse JSON from env var and create credentials object
        creds_info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(creds_info)
        return storage.Client(credentials=creds)
    # Fallback: use file path or Application Default Credentials
    return storage.Client()

# ════════════════════════════════════════════════════════════════════════════════
# OPENAI-COMPATIBLE API CLIENT (DEEPTHOUGHT ENDPOINT)
# ════════════════════════════════════════════════════════════════════════════════
# This app uses a custom "DeepThought" API endpoint that's compatible with the
# OpenAI SDK (same API structure, different backend). The client is initialized
# once at module load time (singleton pattern) to avoid recreating the HTTP
# connection for every request.

_openai_client: openai.OpenAI | None = None

def get_openai_client() -> openai.OpenAI:
    """
    Get or initialize the OpenAI-compatible client.
    
    Why a singleton? Creating a new client (and thus a new HTTP connection pool)
    for every request is slow. By initializing once at startup, we reuse the same
    connection pool for all subsequent API calls.
    
    Configuration notes:
    - max_retries=3: Let the OpenAI SDK retry on transient failures
    - timeout=30.0: Wait max 30 seconds for a response
    
    The app also implements its own retry logic with exponential backoff for
    rate limit (429) errors, since those aren't idempotent.
    """
    global _openai_client
    if _openai_client is None:
        api_key = os.environ.get("DEEPTHOUGHT_API_KEY")
        
        # Log for debugging: is the API key set, and what's its length/prefix?
        logger.info(f"DEEPTHOUGHT_API_KEY present: {bool(api_key)}, length: {len(api_key) if api_key else 0}, prefix: {api_key[:8] if api_key else 'None'}")
        
        if not api_key:
            raise RuntimeError("DEEPTHOUGHT_API_KEY not configured")
        
        # DeepThought is an OpenAI-compatible endpoint at this URL
        base_url = "https://dtcontroller.sr.unh.edu:4242/openai/v1"
        _openai_client = openai.OpenAI(
            api_key=api_key,
            base_url=base_url,
            max_retries=3,
            timeout=30.0
        )
        logger.info("OpenAI-compatible client initialised (max_retries=3, backoff managed by app)")
    return _openai_client

# ── LLM RETRY CONFIGURATION ────────────────────────────────────────────────────
# When the LLM API returns a 429 (rate limit), we retry with exponential backoff.
LLM_MAX_RETRIES = 5  # Maximum number of retry attempts
LLM_BASE_DELAY  = 1.0  # Starting delay: 1 second. Then 2s, 4s, 8s, 16s with jitter.

# ════════════════════════════════════════════════════════════════════════════════
# DATA SCHEMA CONSTANTS
# ════════════════════════════════════════════════════════════════════════════════
# These constants define all the valid filter options for the UI. They're sent to
# the frontend in the /api/schema endpoint so the UI can build dropdown menus.
# The LLM's system prompt also includes these so it knows what values it can suggest.

# Disability types (from ACS survey questions)
DISABILITY_TYPES = [
    {"value": "disability",        "label": "Any Disability"},
    {"value": "hearing",           "label": "Hearing Difficulty"},
    {"value": "seeing",            "label": "Vision Difficulty"},
    {"value": "mobility",          "label": "Ambulatory Difficulty"},
    {"value": "remembering",       "label": "Cognitive Difficulty"},
    {"value": "independentliving", "label": "Independent Living Difficulty"},
    {"value": "selfcare",          "label": "Self-Care Difficulty"},
]

# Measures available at the US/State level
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

# Measures available at the County level (subset of state measures)
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

# Data is only available for certain years; older data in counties, newer in states
STATE_YEAR_RANGE  = list(range(2017, 2025))
COUNTY_YEAR_RANGE = list(range(2012, 2025))

# Age group options depend on the measure (not all age groups apply to all measures)
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

# Map each state-level measure to its allowed age groups
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

# Gender options (applies to state-level data only)
GENDER_OPTIONS = [
    {"value": "All",    "label": "All"},
    {"value": "Female", "label": "Female"},
    {"value": "Male",   "label": "Male"},
]

# Race/ethnicity options (applies to state-level data only)
RACE_OPTIONS = [
    {"value": "All",                "label": "All"},
    {"value": "Hispanic",           "label": "Hispanic"},
    {"value": "NonHispanicAsian",   "label": "Non-Hispanic Asian"},
    {"value": "NonHispanicBlack",   "label": "Non-Hispanic Black"},
    {"value": "NonHispanicOther",   "label": "Non-Hispanic Other"},
    {"value": "NonHispanicWhite",   "label": "Non-Hispanic White"},
]

# The "i" index is used in filenames to specify demographic filters (state level only).
# It maps combinations of whether gender, race, and age are "specific" (not "All") to an integer.
# This integer goes into the RDS filename: acs2023_3_disability_EMP.rds (i=3)
# "All" for all three filters (i=8) is excluded because that's redundant with i=4.
I_LOOKUP = {
    (False, False, False): 1,  # Gender: All, Race: All,  Age: All
    (False, False, True):  2,  # Gender: All, Race: All,  Age: Specific
    (False, True,  False): 3,  # Gender: All, Race: Spec, Age: All
    (False, True,  True):  4,  # Gender: All, Race: Spec, Age: Spec
    (True,  False, False): 5,  # Gender: Spec, Race: All, Age: All
    (True,  False, True):  6,  # Gender: Spec, Race: All, Age: Spec
    (True,  True,  False): 7,  # Gender: Spec, Race: Spec, Age: All
}

# At county level, the "filters" work differently. Not all combinations of gender/age/race
# are available. These are the valid filter combinations for different county measures.
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

# List of all US states and territories (used to validate geography selections)
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

# List of all US counties loaded from a CSV file (too large to hardcode)
US_COUNTIES = pd.read_csv('counties.csv')['x'].to_list()

# ════════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════════════════════════

def get_county_filters(measure):
    """
    Return the valid filter options for a given county-level measure.
    
    Different county measures support different demographic breakdowns:
    - PREV (prevalence): Full gender + age combinations (4 options)
    - POVERTY, INSURANCE variants: Age only (2 options)
    - Others: Age only, but simpler (1 option)
    
    Args:
        measure: The measure code (e.g., "PREV", "POVERTY")
    
    Returns:
        List of dicts with "i" (index) and "label" (display text)
    """
    if measure == "PREV":
        return COUNTY_FILTERS_PREV
    elif measure in ("POVERTY", "INSURANCE", "INSURANCE1", "INSURANCE2"):
        return COUNTY_FILTERS_TWO
    else:
        return COUNTY_FILTERS_ONE

def compute_i(gender, race, age):
    """
    Compute the "i" index from demographic filter selections.
    
    The "i" index is used in RDS filenames to specify which demographic
    breakdown is in the file. For example:
    - acs2023_1_disability_EMP.rds -> All genders, all races, all ages
    - acs2023_6_disability_EMP.rds -> Specific gender, all races, specific age
    
    Args:
        gender: "All" or a specific gender value
        race:   "All" or a specific race value
        age:    "All" or a specific age value
    
    Returns:
        Integer 1-7 if valid, or None if the combination is invalid.
        (All three being "specific" (non-All) is invalid because that's too granular.)
    """
    # Convert "All" to False (not a specific value), anything else to True (is specific)
    g_any = gender != "All"
    r_any = race   != "All"
    a_any = age    != "All"
    
    # Look up the i index; returns None if not in the dictionary
    return I_LOOKUP.get((g_any, r_any, a_any))

def resolve_measure_and_suffix(measure, age):
    """
    Translate measure codes to their filenames.
    
    Some measures have age-specific variants. For example, the "EMP" (employment)
    measure has a special dataset for the "18to64" age group.
    
    Args:
        measure: Measure code (e.g., "EMP", "POVERTY")
        age:     Age group value (e.g., "18to64", "All")
    
    Returns:
        The filename suffix to use (e.g., "EMP" or "EMP_age")
    """
    if measure == "EMP" and age == "18to64":
        return "EMP_age"
    return measure

def build_filename(year, i, disability, measure, geo_level):
    """
    Construct the GCS blob (filename) for an RDS file.
    
    Filenames follow the pattern: acs{year}_{i}_{disability}_{measure}[_COUNTY].rds
    
    Example filenames:
    - acs2023_1_disability_EMP.rds         (state level)
    - acs2023_1_disability_PREV_COUNTY.rds (county level)
    
    Args:
        year:      The year of the data (e.g., 2023)
        i:         The demographic index (1-7)
        disability: The disability type (e.g., "disability", "hearing")
        measure:   The measure code (e.g., "EMP", "POVERTY")
        geo_level: Either "state" or "county"
    
    Returns:
        The filename string
    """
    suffix = "_COUNTY" if geo_level == "county" else ""
    return f"acs{year}_{i}_{disability}_{measure}{suffix}.rds"

def fetch_rds_from_gcs(blob_name):
    """
    Download an RDS file from GCS and return it as a pandas DataFrame.
    
    RDS is R's native data format. This function:
    1. Downloads the file to a temporary location
    2. Reads it with pyreadr (R data reader)
    3. Extracts the first (and only) data frame
    4. Returns it as a pandas DataFrame
    5. Cleans up the temp file
    
    Args:
        blob_name: The filename in GCS (e.g., "acs2023_1_disability_EMP.rds")
    
    Returns:
        A pandas DataFrame with the data
    
    Raises:
        Various exceptions if the file doesn't exist, is empty, or can't be read
    """
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_name)
    tmp_path = None
    
    try:
        # Create a temporary file to download into
        with tempfile.NamedTemporaryFile(suffix=".rds", delete=False) as tmp:
            tmp_path = tmp.name

        # Download from GCS
        logger.info(f"[GCS] Downloading gs://{GCS_BUCKET}/{blob_name} -> {tmp_path}")
        blob.download_to_filename(tmp_path)
        file_size = os.path.getsize(tmp_path)
        logger.info(f"[GCS] Download complete. File size: {file_size} bytes")

        # Sanity check: file shouldn't be empty
        if file_size == 0:
            raise ValueError(f"Downloaded file is empty (0 bytes): {blob_name}")

        # Read the RDS file using pyreadr
        logger.info(f"[pyreadr] Reading {tmp_path}")
        result = pyreadr.read_r(tmp_path)  # Returns a dict of dataframes
        logger.info(f"[pyreadr] Keys in result: {list(result.keys())}")

        # Extract the first (and usually only) dataframe
        df = list(result.values())[0]
        logger.info(f"[pyreadr] DataFrame shape: {df.shape}, columns: {list(df.columns)}")
        return df

    finally:
        # Always clean up the temp file, even if an error occurred
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

def blob_exists(blob_name):
    """
    Check if a file exists in the GCS bucket without downloading it.
    
    This is useful for the /api/check_files endpoint which shows the user
    which data files are available for their selections before they try to load.
    
    Args:
        blob_name: The filename in GCS
    
    Returns:
        True if the file exists, False otherwise
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception:
        # If there's any error (auth, network, etc.), assume it doesn't exist
        return False

def sanitize(v):
    """
    Convert pandas/numpy data types to Python native types and handle special values.
    
    When we return data to the frontend as JSON, some numpy/pandas types don't
    serialize well. This function converts them to native Python types and removes
    invalid values (NaN, infinity).
    
    Args:
        v: A value (could be None, float, numpy type, etc.)
    
    Returns:
        The value converted to a JSON-serializable type, or None if it's NaN/inf
    """
    if v is None:
        return None
    # Check for NaN/inf in standard float
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    # Check for NaN/inf in numpy float
    if isinstance(v, (np.floating,)) and (np.isnan(v) or np.isinf(v)):
        return None
    # Convert numpy int to Python int
    if isinstance(v, (np.integer,)):
        return int(v)
    # Convert numpy bool to Python bool
    if isinstance(v, (np.bool_,)):
        return bool(v)
    # Return as-is for other types (str, int, etc.)
    return v

# ════════════════════════════════════════════════════════════════════════════════
# API ROUTES
# ════════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    """
    Serve the main HTML page.
    
    Returns the frontend UI from templates/index.html
    """
    return render_template("index.html")

@app.route("/api/schema")
def api_schema():
    """
    Return the complete data schema for the frontend.
    
    The frontend calls this on page load to get all valid dropdown options,
    so it can build the filter UI dynamically without hardcoding values.
    This also ensures the backend and frontend are always in sync.
    
    Returns:
        JSON with all disability types, measures, years, gender/race/age options, etc.
    """
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
    """
    Return the valid filter combinations for a county-level measure.
    
    County data has different demographic breakdowns available depending on
    the measure. This endpoint lets the frontend show only the valid options.
    
    Query parameters:
        measure: The county measure code (e.g., "PREV", "POVERTY")
    
    Returns:
        JSON list of valid filter options with their indices and labels
    """
    measure = request.args.get("measure", "")
    return jsonify({"filters": get_county_filters(measure)})

@app.route("/api/data", methods=["POST"])
def api_data():
    """
    Fetch and combine data for the selected filters, return as JSON.
    
    This is the main endpoint for loading data. It:
    1. Accepts filter selections from the frontend
    2. Builds filenames for each year
    3. Downloads RDS files from GCS
    4. Combines them into one DataFrame
    5. Optionally filters by geography
    6. Returns the data as JSON
    
    POST body:
    {
        "geo_level": "state" or "county",
        "geographies": ["State1", "State2", ...],
        "measure": "EMP",
        "disability": "disability",
        "years": [2021, 2022, 2023],
        "gender": "All",
        "race": "All",
        "age": "All",
        "i": 1  // demographic index (state only)
    }
    
    Returns:
        JSON with columns, rows (the data), geography column name, and file counts
    """
    # Extract the filter selections from the request
    body       = request.get_json()
    geo_level  = body.get("geo_level")
    geos       = body.get("geographies", [])
    measure    = body.get("measure")
    disability = body.get("disability")
    years      = body.get("years", [])
    gender     = body.get("gender", "All")
    race       = body.get("race",   "All")
    age        = body.get("age",    "All")
    i          = body.get("i")

    # Validate required fields
    if not measure:
        return jsonify({"error": "No measure selected."}), 400
    if not disability:
        return jsonify({"error": "No disability type selected."}), 400

    # At county level (except PREV), only "disability" (any disability) is available
    if geo_level == "county" and measure != "PREV":
        disability = "disability"

    # For state level, compute the "i" index from demographic filters
    if geo_level == "state":
        i = compute_i(gender, race, age)
        if i is None:
            return jsonify({"error": "Invalid filter combination: all three filters cannot be specific values simultaneously."}), 400
        actual_measure = resolve_measure_and_suffix(measure, age)
    else:
        actual_measure = measure

    # Download and combine data for each year
    frames = []
    total  = len(years)

    for idx, year in enumerate(years):
        fname = build_filename(year, i, disability, actual_measure, geo_level)
        logger.info(f"Fetching {fname} ({idx+1}/{total})")
        try:
            df = fetch_rds_from_gcs(fname)
            df["_year"] = year  # Add a column to track which year this row came from
            frames.append(df)
        except Exception as e:
            logger.warning(f"Could not load {fname}: {e}")

    # If no files could be loaded, return an error
    if not frames:
        return jsonify({"error": "No data files could be loaded for the selected combination."}), 404

    # Combine all dataframes into one
    combined = pd.concat(frames, ignore_index=True)

    # Find the geography column (name varies between state and county data)
    geo_col = None
    for col in combined.columns:
        if col in ("ST_text", "Geographic.Area.Name"):
            geo_col = col
            break

    # If specific geographies were selected, filter to only those
    if geo_col and geos and "All" not in geos:
        pattern = '|'.join(geos)  # Create a regex: "State1|State2|State3"
        combined = combined[combined[geo_col].str.contains(pattern)]

    # Convert dataframe to list of dicts, cleaning up numpy/pandas types
    rows = [
        {k: sanitize(v) for k, v in row.items()}
        for row in combined.to_dict(orient="records")
    ]

    return jsonify({
        "columns":      list(combined.columns),
        "rows":         rows,
        "geo_col":      geo_col,
        "total_files":  total,
        "loaded_files": len(frames),
    })

@app.route("/api/check_files", methods=["POST"])
def api_check_files():
    """
    Check which data files exist in GCS for the current selection.
    
    This is a lightweight endpoint that doesn't download anything—it just checks
    if files exist. The frontend uses this to show the user which years/selections
    have available data before they try to load.
    
    POST body: Same as /api/data
    
    Returns:
        JSON dict mapping each year to true/false (file exists or not)
    """
    body       = request.get_json()
    geo_level  = body.get("geo_level")
    measure    = body.get("measure")
    disability = body.get("disability")
    years      = body.get("years", [])
    gender     = body.get("gender", "All")
    race       = body.get("race",   "All")
    age        = body.get("age",    "All")
    i          = body.get("i")
    results = {}
    
    if not measure or not disability:
        return jsonify({"error": "measure and disability are required."}), 400

    # Same logic as /api/data
    if geo_level == "county" and measure != "PREV":
        disability = "disability"

    if geo_level == "state":
        i = compute_i(gender, race, age)
        if i is None:
            return jsonify({"error": "Invalid filter combination: all three filters cannot be specific values simultaneously."}), 400
        actual_measure = resolve_measure_and_suffix(measure, age)
    else:
        actual_measure = measure
    
    # Check each year
    for year in years:
        fname = build_filename(year, i, disability, actual_measure, geo_level)
        results[year] = blob_exists(fname)
    
    return jsonify(results)

# ════════════════════════════════════════════════════════════════════════════════
# CHAT ENDPOINT WITH LLM
# ════════════════════════════════════════════════════════════════════════════════
# This section handles the chat feature where an LLM helps users select filters
# using natural language. For example: "Show me employment data for all states."

def _build_system_prompt() -> str:
    """
    Build the system prompt for the LLM.
    
    The system prompt tells the LLM:
    - Its job (helping users pick filters)
    - The rules it must follow (only valid values, JSON output format)
    - The complete list of valid filter options (so it doesn't invent values)
    
    Why embed the schema? Including all valid values in the prompt means the
    LLM can't suggest invalid options. This prompt is large (1000+ tokens) but
    it's cached by the API provider, so the cost is amortized across many requests.
    
    Returns:
        The full system prompt as a string
    """
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
    lines.append("7. SEQUENTIAL VALIDATION: Updates must follow this order: (1) geo_level, (2) measure, (3) geographies, (4) year/demographics. Do not suggest updates that skip steps.")
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

# Build the system prompt once at module startup, then reuse it for all requests.
# Large prompts (>1,024 tokens) are cached by the API provider.
CHAT_SYSTEM_PROMPT: str = _build_system_prompt()
logger.info(f"Chat system prompt built: {len(CHAT_SYSTEM_PROMPT):,} chars")

def build_chat_context(current_state: dict) -> str:
    """
    Build the per-request context showing the user's current filter selections.
    
    This is prepended to each user message so the LLM knows what filters are
    currently set. This helps the LLM understand requests like "add California"
    without knowing what state is already selected.
    
    Args:
        current_state: Dict with current filter values
    
    Returns:
        Formatted text block showing current selections
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

# Build lookup sets of valid values for validation
_VALID_DISABILITIES = {d["value"] for d in DISABILITY_TYPES}
_VALID_STATE_MEASURES  = {m["value"] for m in STATE_MEASURES}
_VALID_COUNTY_MEASURES = {m["value"] for m in COUNTY_MEASURES}
_VALID_MEASURES  = _VALID_STATE_MEASURES | _VALID_COUNTY_MEASURES
_VALID_GEOS      = set(US_STATES)
_VALID_GENDERS   = {g["value"] for g in GENDER_OPTIONS}
_VALID_RACES     = {r["value"] for r in RACE_OPTIONS}
_VALID_AGES      = {o["value"] for opts in AGE_GROUPS.values() for o in opts}
_ALL_YEARS       = set(STATE_YEAR_RANGE) | set(COUNTY_YEAR_RANGE)
_VALID_GEOS      |= set(US_COUNTIES)  # Add counties to valid geos

def validate_updates(updates: dict, current_state: dict) -> dict:
    """
    Validate and sanitize LLM-proposed filter updates.
    
    This enforces "sequential selection" — you can't jump steps. For example:
    - Step 1: Choose geo_level (state or county)
    - Step 2: Choose a measure (which depends on geo_level)
    - Step 3: Choose geographies (which depends on the measure)
    - Step 4: Choose year and demographics (which depend on geographies)
    
    This prevents the LLM from suggesting invalid combinations.
    
    Args:
        updates: Dict of proposed updates from the LLM
        current_state: Dict of current filter values
    
    Returns:
        Dict of validated updates (invalid keys removed)
    """
    if not updates:
        return {}

    clean = {}
    
    # STEP 1: Geo level (no dependencies)
    if "geo_level" in updates and updates["geo_level"] in ("state", "county"):
        clean["geo_level"] = updates["geo_level"]

    # Determine which geo level we're working with
    target_geo = clean.get("geo_level", current_state.get("geo_level", "state"))

    # STEP 2: Measure (requires geo_level to exist)
    if "measure" in updates:
        target_measures = _VALID_STATE_MEASURES if target_geo == "state" else _VALID_COUNTY_MEASURES
        if updates["measure"] in target_measures:
            clean["measure"] = updates["measure"]
            logger.info(f"Measure accepted: {updates['measure']} for {target_geo} level")
        else:
            logger.warning(f"Measure rejected: {updates['measure']} not in {target_geo} measures")
    elif current_state.get("measure"):
        # Preserve existing measure if not being updated
        clean["measure"] = current_state.get("measure")

    # STEP 3: Geographies (requires measure to exist)
    has_measure = clean.get("measure") or current_state.get("measure")
    if "geographies" in updates and has_measure:
        if isinstance(updates["geographies"], list):
            # Filter to only valid geographies
            validated_geos = [g for g in updates["geographies"] if g in _VALID_GEOS]
            if validated_geos:
                clean["geographies"] = validated_geos

    # STEP 4: Year and demographics (requires geographies to exist)
    has_geographies = clean.get("geographies") or current_state.get("geographies")
    
    if has_geographies:
        if "year_mode" in updates and updates["year_mode"] in ("single", "all"):
            clean["year_mode"] = updates["year_mode"]

        if "year" in updates:
            try:
                yr = int(updates["year"])
                if yr in _ALL_YEARS:
                    clean["year"] = yr
            except (TypeError, ValueError):
                pass

        # Demographics only apply at state level, not county
        if target_geo == "state":
            if "gender" in updates and updates["gender"] in _VALID_GENDERS:
                clean["gender"] = updates["gender"]

            if "race" in updates and updates["race"] in _VALID_RACES:
                clean["race"] = updates["race"]

            if "age" in updates and updates["age"] in _VALID_AGES:
                clean["age"] = updates["age"]

    # Disability can be updated anytime (we validate against the measure later)
    if "disability" in updates and updates["disability"] in _VALID_DISABILITIES:
        clean["disability"] = updates["disability"]

    return clean

@app.route("/api/chat", methods=["POST"])
def api_chat():
    """
    Process a chat message using the LLM and return filter update instructions.
    
    The flow is:
    1. Receive the user's message and chat history
    2. Call the LLM with the system prompt + context + message
    3. Parse the JSON response
    4. Validate the proposed updates against the schema
    5. Return the validated updates + the LLM's reply
    
    POST body:
    {
        "message": "Show me employment data for California",
        "history": [  // Previous messages in this conversation
            {"role": "user", "content": "..."},
            {"role": "assistant", "content": "..."}
        ],
        "current_state": {  // Current filter selections
            "geo_level": "state",
            "measure": "EMP",
            ...
        }
    }
    
    Returns:
        JSON with:
        - "reply": What the LLM said to show the user
        - "updates": The validated filter updates
        - "trigger_fetch": Whether to fetch data now (if user said "go")
    """
    body          = request.get_json()
    user_message  = body.get("message", "").strip()
    history       = body.get("history", [])
    current_state = body.get("current_state", {})

    if not user_message:
        return jsonify({"error": "Empty message."}), 400

    # Get the LLM client
    try:
        client = get_openai_client()
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 500

    # Build the context string showing current selections
    context_block  = build_chat_context(current_state)
    context_prefix = f"{context_block}\n\n=== USER REQUEST ===\n"

    # Build the messages array with chat history
    messages = []
    for i, turn in enumerate(history):
        messages.append({"role": turn["role"], "content": turn["content"]})
    # Append the new user message with context prepended
    messages.append({"role": "user", "content": context_prefix + user_message})
    
    # ── Call the LLM with exponential backoff retry logic ──────────────────────
    # We implement our own retry loop to handle rate limits (429) with exponential
    # backoff. This avoids the SDK's default retry behavior which could multiply
    # requests on 429s.
    raw_text = None
    last_exc = None

    for attempt in range(LLM_MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model      = "ets:aws:us.anthropic.claude-haiku-4-5-20251001-v1:0",
                max_tokens = 2048,
                messages   = [{"role": "system", "content": CHAT_SYSTEM_PROMPT}] + messages,
            )
            raw_text = response.choices[0].message.content.strip()
            last_exc = None
            break  # Success — exit the retry loop

        except openai.APIStatusError as e:
            last_exc = e
            if e.status_code == 429:
                # Rate limited. Wait with exponential backoff: 1s, 2s, 4s, 8s, 16s
                delay = LLM_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
                request_id = getattr(e.response, "headers", {}).get("request-id", "unknown")
                logger.warning(
                    f"LLM 429 overloaded (attempt {attempt + 1}/{LLM_MAX_RETRIES}), "
                    f"request_id={request_id}. Retrying in {delay:.2f}s..."
                )
                time.sleep(delay)
                continue
            # Other API error (not 429) — don't retry
            logger.error(f"LLM API error {e.status_code}: {e}")
            return jsonify({"error": f"API error: {str(e)}"}), 500

        except Exception as e:
            # Unexpected error (network, parsing, etc.)
            last_exc = e
            logger.error(f"Unexpected error calling LLM: {type(e).__name__}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return jsonify({"error": f"LLM error: {str(e)}"}), 500

    # If we exhausted all retries, return a user-friendly error
    if last_exc is not None:
        logger.error(f"LLM still overloaded after {LLM_MAX_RETRIES} attempts: {last_exc}")
        return jsonify({
            "error": "The AI assistant is temporarily unavailable due to high demand. Please try again in a moment."
        }), 503

    # ── Parse JSON from the LLM response ───────────────────────────────────────
    # The LLM should return a JSON object, but it might leak text before or after.
    # This function extracts the outermost { ... } and tries to parse it.
    def extract_json(text: str) -> dict | None:
        """Extract and parse JSON from text that might have extra content."""
        start = text.find("{")
        end   = text.rfind("}")
        if start == -1 or end == -1 or end < start:
            return None
        candidate = text[start:end + 1]
        # Remove any markdown code fences
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

    # Extract the fields from the parsed JSON
    reply         = parsed.get("reply", "")
    raw_updates   = parsed.get("updates", {})
    trigger_fetch = bool(parsed.get("trigger_fetch", False))

    # Validate the proposed updates against the schema
    clean_updates = validate_updates(raw_updates, current_state)

    # Build the full state for the response
    # Start with current selections, then overlay validated changes
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

    # If geographies are selected but measure is None, assign a default
    if full_state.get("geographies") and not full_state.get("measure"):
        final_geo = full_state.get("geo_level", "state")
        default_measures = STATE_MEASURES if final_geo == "state" else COUNTY_MEASURES
        full_state["measure"] = default_measures[0]["value"]
        logger.info(f"Assigned default measure: {full_state['measure']} (geographies selected but measure was null)")

    # If geography level changed, some measures might not be valid at the new level
    # (e.g., EMP at state level, E2PR at county level). Translate if needed.
    MEASURE_TRANSLATE = {
        ("state", "county"): {"EMP": "E2PR"},
        ("county", "state"): {"E2PR": "EMP"},
    }
    prev_geo  = current_state.get("geo_level", "state")
    final_geo = full_state.get("geo_level", "state")
    if prev_geo != final_geo and "measure" not in clean_updates:
        # Geography level changed, but measure wasn't explicitly updated
        # Check if we need to translate the current measure
        translation_map = MEASURE_TRANSLATE.get((prev_geo, final_geo), {})
        current_measure = full_state.get("measure")
        if current_measure in translation_map:
            full_state["measure"] = translation_map[current_measure]
        else:
            # Check if the current measure is valid at the new level
            valid_measures = (
                {m["value"] for m in STATE_MEASURES}
                if final_geo == "state"
                else {m["value"] for m in COUNTY_MEASURES}
            )
            if current_measure not in valid_measures:
                full_state["measure"] = None

    return jsonify({
        "reply":         reply,
        "updates":       full_state,
        "trigger_fetch": trigger_fetch,
    })

@app.route("/checkip")
def checkip():
    """
    Utility endpoint that returns the server's IP address.
    
    Useful for debugging network issues or verifying the server is accessible.
    """
    response = requests.get('https://api.ipify.org?format=json')
    return response.json()

# ════════════════════════════════════════════════════════════════════════════════
# SERVER STARTUP
# ════════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Start the Flask development server
    # Listen on 0.0.0.0 (all network interfaces) so it's accessible from other machines
    # Port can be overridden with the PORT environment variable (default 8080)
    # debug=False for production; if developing locally, set to True for auto-reload
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)