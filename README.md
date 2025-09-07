# Compliance Checks API

API for capturing and analyzing Google Cloud IAM policies with database storage support.

## Features

- **IAM Policy Collection**: Collect VM instance and Cloud Storage bucket IAM policies from Google Cloud projects
- **Database Storage**: Store compliance data in either Firestore (production) or local TinyDB (testing)
- **Flexible Data Retrieval**: Query stored compliance data by project, folder, or organization ID
- **RESTful API**: Complete CRUD operations for compliance data management

## API Endpoints

### Data Collection

- `POST /compliance-data/collect` - Collect and store compliance data from project/folder/org
- `POST /projects/{project_id}/save-iam-data` - Legacy endpoint that also saves to database

### Data Retrieval

- `GET /compliance-data` - List stored compliance data with optional filters
- `GET /compliance-data/{doc_id}` - Get specific compliance data by document ID
- `DELETE /compliance-data/{doc_id}` - Delete compliance data

### Legacy Endpoints

- `GET /vm-iam-policies-asset-api/{project_id}` - Get VM IAM policies
- `GET /bucket-iam-policies-asset-api/{project_id}` - Get bucket IAM policies
- `GET /health` - Health check

## Database Configuration

The API supports two database backends:

### Firestore (Production)
Set `USE_FIRESTORE=true` in your environment to use Google Cloud Firestore.

### TinyDB (Local Testing)
Set `USE_FIRESTORE=false` (default) to use a local JSON-based database for testing.

## Setup

1. Install dependencies:
```bash
pip install -e .
```

2. Configure environment (copy `.env.example` to `.env`):
```bash
cp .env.example .env
```

3. Set up Google Cloud authentication:
```bash
gcloud auth application-default login
```

4. Run the API:
```bash
uvicorn app.main:app --reload
```

## Usage Examples

### Collect compliance data for a project:
```bash
curl -X POST "http://localhost:8000/compliance-data/collect" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "your-project-id",
    "include_vm_policies": true,
    "include_bucket_policies": true
  }'
```

### List all stored compliance data:
```bash
curl "http://localhost:8000/compliance-data"
```

### Filter by project ID:
```bash
curl "http://localhost:8000/compliance-data?project_id=your-project-id"
```

### Get specific compliance data:
```bash
curl "http://localhost:8000/compliance-data/{doc_id}"
```

## Requirements

- Python 3.10+
- Google Cloud SDK (for authentication)
- Required IAM permissions:
  - `cloudasset.assets.listIamPolicy`
  - `compute.instances.list`
  - `storage.buckets.list`

## Development

The API uses:
- **FastAPI** for the web framework
- **Google Cloud Asset API** for IAM policy collection
- **Firestore** for production database
- **TinyDB** for local testing database
- **Structlog** for structured logging