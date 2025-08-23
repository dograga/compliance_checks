import logging
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from google.auth import default
import app.buckets as buckets
from .dataclass import (
    ProjectPoliciesResponse, ComplianceAnalysisResponse
)
from .helper import (
    fetch_iam_policies_for_project, fetch_iam_policies_asset_api, 
    fetch_vm_iam_policies_asset_api, fetch_bucket_iam_policies_asset_api,
    analyze_compliance_issues
)

# Create data directory for JSON dumps
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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


@app.get("/iam-policies/{project_id}", response_model=ProjectPoliciesResponse)
async def get_iam_policies(
    project_id: str,
    zones: Optional[List[str]] = Query(None, description="List of zones to filter")
):
    logger.info(f"Fetching VM instance IAM policies for project: {project_id}")
    
    if not project_id:
        raise HTTPException(status_code=400, detail="Project ID is required")
    
    try:
        result = await fetch_iam_policies_for_project(project_id, zones)
        logger.info(f"Successfully fetched {result.total_policies} VM instance policies for project {project_id}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching policies for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/projects/{project_id}/save-iam-data")
async def save_iam_data(project_id: str) -> Dict[str, Any]:
    """Save all IAM data to JSON file for offline use"""
    try:
        logger.info(f"Saving IAM data for project: {project_id}")
        
        # Fetch all IAM policies (using VM instance method by default)
        policies_response = await fetch_iam_policies_for_project(project_id)
        
        # Perform compliance analysis
        compliance_analysis = analyze_compliance_issues(project_id, policies_response.policies)
        
        # Create comprehensive data structure
        iam_data = {
            "project_id": project_id,
            "timestamp": datetime.now().isoformat(),
            "data": {
                "policies": {
                    "data": [policy.dict() for policy in policies_response.policies],
                    "count": policies_response.total_policies
                },
                "compliance_analysis": {
                    "data": compliance_analysis.dict(),
                    "issues_count": len(compliance_analysis.issues_found)
                },
                "errors": {
                    "data": policies_response.errors,
                    "count": len(policies_response.errors)
                }
            },
            "summary": {
                "total_resources": policies_response.total_policies,
                "resources_with_policies": len([p for p in policies_response.policies if p.policy and not p.error]),
                "compliance_issues": len(compliance_analysis.issues_found),
                "public_access_issues": compliance_analysis.summary.get("public_access", 0),
                "cross_project_issues": compliance_analysis.summary.get("cross_project_access", 0),
                "high_severity_issues": compliance_analysis.summary.get("high_severity", 0)
            }
        }
        
        # Save to JSON file
        file_path = DATA_DIR / f"{project_id}_iam_data.json"
        with open(file_path, 'w') as f:
            json.dump(iam_data, f, indent=2, default=str)
        
        logger.info(f"IAM data saved to {file_path}")
        
        return {
            "message": "IAM data saved successfully",
            "file_path": str(file_path),
            "timestamp": iam_data["timestamp"],
            "summary": iam_data["summary"]
        }
    except Exception as e:
        logger.error(f"Error saving IAM data for project {project_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to save IAM data: {str(e)}")


@app.get("/iam-policies-asset-api/{project_id}", response_model=ProjectPoliciesResponse)
async def get_iam_policies_asset_api(
    project_id: str,
    asset_types: Optional[List[str]] = Query(None, description="List of asset types to filter")
):
    logger.info(f"Fetching all resource IAM policies via Asset API for project: {project_id}")
    
    if not project_id:
        raise HTTPException(status_code=400, detail="Project ID is required")
    
    try:
        result = await fetch_iam_policies_asset_api(project_id, asset_types)
        logger.info(f"Successfully fetched {result.total_policies} resource policies via Asset API for project {project_id}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching policies via Asset API for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/vm-iam-policies-asset-api/{project_id}", response_model=ProjectPoliciesResponse)
async def get_vm_iam_policies_asset_api(project_id: str):
    logger.info(f"Fetching VM instance IAM policies via Asset API for project: {project_id}")
    
    if not project_id:
        raise HTTPException(status_code=400, detail="Project ID is required")
    
    try:
        result = await fetch_vm_iam_policies_asset_api(project_id)
        logger.info(f"Successfully fetched {result.total_policies} VM instance policies via Asset API for project {project_id}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching VM instance policies via Asset API for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/bucket-iam-policies-asset-api/{project_id}", response_model=ProjectPoliciesResponse)
async def get_bucket_iam_policies_asset_api(project_id: str):
    logger.info(f"Fetching bucket IAM policies via Asset API for project: {project_id}")
    
    if not project_id:
        raise HTTPException(status_code=400, detail="Project ID is required")
    
    try:
        result = await buckets.get_bucket_policies(project_id)
        logger.info(f"Successfully fetched {result.total_policies} bucket policies via Asset API for project {project_id}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching bucket policies via Asset API for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "compliance-checks"}
