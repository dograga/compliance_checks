# Compliance Checks API

A FastAPI application for capturing and analyzing Google Cloud IAM policies across projects.

## Features

- **IAM Policy Capture**: Retrieve IAM policies for all resources in a Google Cloud project
- **Multiple Asset Types**: Support for various GCP resources (projects, buckets, instances, etc.)
- **Error Handling**: Graceful handling of permission errors and resource access issues
- **RESTful API**: Clean REST endpoints with comprehensive documentation

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -e .
   ```

2. **Google Cloud Authentication**:
   - Set up Application Default Credentials:
     ```bash
     gcloud auth application-default login
     ```
   - Or set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your service account key file

3. **Required IAM Permissions**:
   Your account/service account needs these permissions:
   - `cloudasset.assets.searchAllResources`
   - `cloudasset.assets.searchAllIamPolicies`
   - IAM policy read permissions for target resources

## Usage

1. **Start the Server**:
   ```bash
   python app/main.py
   ```
   Or using uvicorn directly:
   ```bash
   uvicorn app.main:app --reload
   ```

2. **API Endpoints**:
   - `GET /` - API information
   - `GET /iam-policies/{project_id}` - Get IAM policies for a project
   - `GET /health` - Health check
   - `GET /docs` - Interactive API documentation

3. **Example Request**:
   ```bash
   curl "http://localhost:8000/iam-policies/your-project-id"
   ```

4. **Filter by Asset Types**:
   ```bash
   curl "http://localhost:8000/iam-policies/your-project-id?asset_types=storage.googleapis.com/Bucket&asset_types=compute.googleapis.com/Instance"
   ```

## Response Format

The API returns a structured response containing:
- Project ID
- List of policies for each resource
- Total policy count
- Any errors encountered

```json
{
  "project_id": "your-project-id",
  "policies": [
    {
      "project_id": "your-project-id",
      "resource_name": "//storage.googleapis.com/projects/_/buckets/my-bucket",
      "asset_type": "storage.googleapis.com/Bucket",
      "policy": {
        "bindings": [
          {
            "role": "roles/storage.admin",
            "members": ["user:admin@example.com"]
          }
        ]
      }
    }
  ],
  "total_policies": 1,
  "errors": []
}
```

## Supported Asset Types

Default asset types include:
- Projects (`cloudresourcemanager.googleapis.com/Project`)
- Storage Buckets (`storage.googleapis.com/Bucket`)
- Compute Instances (`compute.googleapis.com/Instance`)
- Compute Disks (`compute.googleapis.com/Disk`)
- Cloud SQL Instances (`cloudsql.googleapis.com/Instance`)
- GKE Clusters (`container.googleapis.com/Cluster`)
- Pub/Sub Topics (`pubsub.googleapis.com/Topic`)
- Pub/Sub Subscriptions (`pubsub.googleapis.com/Subscription`)

## Development

- **Interactive Documentation**: Visit `http://localhost:8000/docs` when the server is running
- **Health Check**: `http://localhost:8000/health`
- **Logging**: Configured at INFO level, check console output for request details