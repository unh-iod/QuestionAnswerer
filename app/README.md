# ACS Disability Statistics Explorer

Build using Claude 

A Flask web application for exploring ACS disability statistics stored as RDS files in Google Cloud Storage.

## Project Structure

```
asc_app/
├── app.py                  # Flask backend + API routes
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .env.example            # Copy to .env and fill in values
├── templates/
│   └── index.html          # Single-page app shell
└── static/
    ├── css/style.css
    └── js/app.js           # All frontend logic
```

## File Naming Convention (enforced by the app)

| Geography | Pattern |
|-----------|---------|
| US / State | `acs{year}_{i}_{disability}_{measure}.rds` |
| County     | `acs{year}_{i}_{disability}_{measure}_COUNTY.rds` |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GCS_BUCKET_NAME` | GCS bucket name (no `gs://` prefix) |
| `GOOGLE_APPLICATION_CREDENTIALS_JSON` | Full service account JSON as a single-line string |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to a mounted SA key file (alternative) |
| `PORT` | Port to listen on (default: 8080) |

## Local Development

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 2. Install dependencies (requires R to be installed for pyreadr)
pip install -r requirements.txt

# 3. Set environment variables
cp .env.example .env
# Edit .env with your bucket name and credentials

export $(cat .env | grep -v '#' | xargs)

# 4. Run
python main.py
# Visit http://localhost:8080
```

## Docker (local)

```bash
cp .env.example .env   # fill in values
docker compose up --build
# Visit http://localhost:8080
```

## Deploy to Cloud Run (GCP)

```bash
# 1. Build and push to Artifact Registry
PROJECT_ID=your-gcp-project
REGION=us-central1
IMAGE=gcr.io/$PROJECT_ID/asc-disability-explorer

docker build -t $IMAGE .
docker push $IMAGE

# 2. Deploy
gcloud run deploy asc-disability-explorer \
  --image $IMAGE \
  --region $REGION \
  --platform managed \
  --allow-unauthenticated \
  --set-env-vars GCS_BUCKET_NAME=your-bucket-name \
  --set-secrets GOOGLE_APPLICATION_CREDENTIALS_JSON=asc-sa-key:latest
```

For the secret, store the service account JSON in **Secret Manager** and grant the Cloud Run service account `secretmanager.secretAccessor` role.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Serves the app |
| `GET` | `/api/schema` | Returns full schema (measures, years, filters, states) |
| `GET` | `/api/county_filters?measure=X` | Returns filter combinations for a county measure |
| `POST` | `/api/data` | Fetches and returns data for the selected combination |
| `POST` | `/api/check_files` | Checks which files exist in the bucket |

### POST /api/data body

```json
{
  "geo_level":   "state",
  "geographies": ["California", "Texas"],
  "measure":     "EMP",
  "disability":  "disability",
  "years":       [2022, 2023],
  "i":           1
}
```
