import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import structlog
from .dataclass import ProjectPoliciesResponse
from .gcp_helper import (
    get_bucket_policies, fetch_vm_iam_policies_asset_api,
)

# Create data directory for JSON dumps
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Configure structlog
logger = structlog.get_logger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Compliance Checks API",
    description="API for capturing and analyzing Google Cloud IAM policies",
    version="0.1.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/projects/{project_id}/save-iam-data")
async def save_iam_data(project_id: str) -> Dict[str, Any]:
    """Save VM instance and bucket IAM data to JSON file"""
    try:
        logger.info("Saving IAM data for project", project_id=project_id)

        # Fetch VM instance IAM policies
        vm_policies_response = await fetch_vm_iam_policies_asset_api(project_id)
        vm_policies = [p.dict() for p in vm_policies_response.policies]

        # Fetch bucket IAM policies
        bucket_policies_response = await get_bucket_policies(project_id)
        bucket_policies = [p.dict() for p in bucket_policies_response.policies]

        # Prepare JSON structure
        iam_data = {
            "project_id": project_id,
            "timestamp": datetime.now().isoformat(),
            "data": {
                "vm_instances": {
                    "policies": vm_policies,
                    "count": len(vm_policies)
                },
                "buckets": {
                    "policies": bucket_policies,
                    "count": len(bucket_policies)
                }
            }
        }

        # Save to JSON file
        file_path = DATA_DIR / f"{project_id}_iam_data.json"
        with open(file_path, 'w') as f:
            json.dump(iam_data, f, indent=2, default=str)

        logger.info("IAM data saved", file_path=str(file_path))

        return {
            "message": "IAM data saved successfully",
            "file_path": str(file_path),
            "timestamp": iam_data["timestamp"],
            "summary": {
                "vm_count": len(vm_policies),
                "bucket_count": len(bucket_policies)
            }
        }

    except Exception as e:
        logger.error("Error saving IAM data", project_id=project_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to save IAM data: {str(e)}")

@app.get("/vm-iam-policies-asset-api/{project_id}", response_model=ProjectPoliciesResponse)
async def get_vm_iam_policies_asset_api(project_id: str):
    logger.info("Fetching VM instance IAM policies via Asset API", project_id=project_id)
    
    if not project_id:
        raise HTTPException(status_code=400, detail="Project ID is required")
    
    try:
        result = await fetch_vm_iam_policies_asset_api(project_id)
        logger.info("Successfully fetched VM instance policies via Asset API", 
                   project_id=project_id, total_policies=result.total_policies)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Unexpected error fetching VM instance policies via Asset API", 
                    project_id=project_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/bucket-iam-policies-asset-api/{project_id}", response_model=ProjectPoliciesResponse)
async def get_bucket_iam_policies_asset_api(project_id: str):
    logger.info("Fetching bucket IAM policies via Asset API", project_id=project_id)
    
    if not project_id:
        raise HTTPException(status_code=400, detail="Project ID is required")
    
    try:
        result = await get_bucket_policies(project_id)
        logger.info("Successfully fetched bucket policies via Asset API", 
                   project_id=project_id, total_policies=result.total_policies)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Unexpected error fetching bucket policies via Asset API", 
                    project_id=project_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "compliance-checks"}
