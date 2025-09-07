from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

import structlog
from .dataclass import (
    ComplianceDataRequest,
    PolicyCollectionRequest,
    AssetCollectionResponse
)
from app.gcp_helper import (
    fetch_vm_instances_folder_org,
    fetch_buckets_folder_org
)
from .database import get_database

# Configure structlog
logger = structlog.get_logger(__name__)

# Initialize database
db = get_database()

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

@app.post("/compliance/data_collect")
async def collect_compliance_data(request: ComplianceDataRequest) -> AssetCollectionResponse:
    """Collect compliance data from folder or organization and store in separate tables."""
    try:
        logger.info("Collecting compliance data", request=request.dict())
        
        # Determine parent scope
        parent = request.folder_id if request.folder_id else request.org_id
        if not parent.startswith(("folders/", "organizations/")):
            parent = f"folders/{parent}" if request.folder_id else f"organizations/{parent}"
        
        logger.info("Collecting data using Asset API", parent=parent)
        
        buckets = []
        instances = []
        projects_discovered = set()
        errors = []
        
        # Collect VM instances if requested
        if request.include_vm_policies:
            try:
                instances = await fetch_vm_instances_folder_org(parent)
                
                # Save each instance record to database
                for instance in instances:
                    await db.save_instance_record(instance)
                    projects_discovered.add(instance["project_number"])
                
                logger.info("Collected and saved VM instances", parent=parent, count=len(instances))
            except Exception as e:
                error_msg = f"Failed to collect VM instances from {parent}: {str(e)}"
                errors.append(error_msg)
                logger.error("VM instance collection failed", parent=parent, error=str(e))
        
        # Collect buckets if requested
        if request.include_bucket_policies:
            try:
                buckets = await fetch_buckets_folder_org(parent)
                
                # Save each bucket record to database
                for bucket in buckets:
                    await db.save_bucket_record(bucket)
                    projects_discovered.add(bucket["project_number"])
                
                logger.info("Collected and saved buckets", parent=parent, count=len(buckets))
            except Exception as e:
                error_msg = f"Failed to collect buckets from {parent}: {str(e)}"
                errors.append(error_msg)
                logger.error("Bucket collection failed", parent=parent, error=str(e))
        
        # Remove "unknown" from projects if present
        projects_discovered.discard("unknown")
        projects_list = list(projects_discovered)
        
        logger.info("Asset collection completed", 
                   parent=parent, 
                   buckets=len(buckets), 
                   instances=len(instances),
                   projects=len(projects_list))
        
        return AssetCollectionResponse(
            parent_scope=parent,
            buckets=buckets,
            instances=instances,
            total_buckets=len(buckets),
            total_instances=len(instances),
            projects_discovered=projects_list,
            errors=errors
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to collect compliance data", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to collect compliance data: {str(e)}")


@app.get("/buckets-policies")
async def get_buckets(
    folder_id: Optional[str] = Query(None, description="Filter by folder ID"), 
    org_id: Optional[str] = Query(None, description="Filter by organization ID"),
    limit: int = Query(100, description="Maximum number of records to return")
) -> Dict[str, Any]:
    """Get stored bucket records with optional filters."""
    try:
        logger.info("Getting bucket records", folder_id=folder_id, org_id=org_id, limit=limit)
        
        buckets = await db.get_buckets(folder_id=folder_id, org_id=org_id, limit=limit)
        
        return {
            "buckets": buckets,
            "total_count": len(buckets),
            "filters_applied": {
                "folder_id": folder_id,
                "org_id": org_id,
                "limit": limit
            }
        }
        
    except Exception as e:
        logger.error("Failed to get bucket records", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get bucket records: {str(e)}")


@app.get("/instances-policies")
async def get_instances(
    folder_id: Optional[str] = Query(None, description="Filter by folder ID"), 
    org_id: Optional[str] = Query(None, description="Filter by organization ID"),
    limit: int = Query(100, description="Maximum number of records to return")
) -> Dict[str, Any]:
    """Get stored instance records with optional filters."""
    try:
        logger.info("Getting instance records", folder_id=folder_id, org_id=org_id, limit=limit)
        
        instances = await db.get_instances(folder_id=folder_id, org_id=org_id, limit=limit)
        
        return {
            "instances": instances,
            "total_count": len(instances),
            "filters_applied": {
                "folder_id": folder_id,
                "org_id": org_id,
                "limit": limit
            }
        }
        
    except Exception as e:
        logger.error("Failed to get instance records", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get instance records: {str(e)}")


@app.post("/policies/buckets/collect")
async def collect_bucket_policies(request: PolicyCollectionRequest) -> Dict[str, Any]:
    """Collect bucket IAM policies from folder or organization."""
    try:
        # Determine parent scope
        parent = request.folder_id if request.folder_id else request.org_id
        if not parent.startswith(("folders/", "organizations/")):
            parent = f"folders/{parent}" if request.folder_id else f"organizations/{parent}"
        
        logger.info("Collecting bucket policies", parent_scope=parent)
        
        # Collect buckets
        buckets = await fetch_buckets_folder_org(parent)
        
        # Save bucket records
        saved_buckets = []
        for bucket in buckets:
            doc_id = await db.save_bucket_record(bucket)
            bucket["id"] = doc_id
            saved_buckets.append(bucket)
        
        logger.info("Bucket policies collected", 
                   parent_scope=parent, bucket_count=len(saved_buckets))
        
        return {
            "message": "Bucket policies collected successfully",
            "parent_scope": parent,
            "bucket_count": len(saved_buckets),
            "buckets": saved_buckets
        }
        
    except Exception as e:
        logger.error("Failed to collect bucket policies", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to collect bucket policies: {str(e)}")


@app.post("/policies/instances/collect")
async def collect_instance_policies(request: PolicyCollectionRequest) -> Dict[str, Any]:
    """Collect VM instance IAM policies from folder or organization."""
    try:
        # Determine parent scope
        parent = request.folder_id if request.folder_id else request.org_id
        if not parent.startswith(("folders/", "organizations/")):
            parent = f"folders/{parent}" if request.folder_id else f"organizations/{parent}"
        
        logger.info("Collecting instance policies", parent_scope=parent)
        
        # Collect instances
        instances = await fetch_vm_instances_folder_org(parent)
        
        # Save instance records
        saved_instances = []
        for instance in instances:
            doc_id = await db.save_instance_record(instance)
            instance["id"] = doc_id
            saved_instances.append(instance)
        
        logger.info("Instance policies collected", 
                   parent_scope=parent, instance_count=len(saved_instances))
        
        return {
            "message": "Instance policies collected successfully",
            "parent_scope": parent,
            "instance_count": len(saved_instances),
            "instances": saved_instances
        }
        
    except Exception as e:
        logger.error("Failed to collect instance policies", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to collect instance policies: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "compliance-checks"}
