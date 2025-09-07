import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

import structlog
from .dataclass import (
    ComplianceDataRequest, ComplianceDataResponse, 
    AssetCollectionResponse
)
from app.gcp_helper import (
    fetch_vm_iam_policies_asset_api,
    get_bucket_policies,
    fetch_vm_instances_folder_org,
    fetch_buckets_folder_org
)
from .database import get_database

# Create data directory for JSON dumps
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

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

        # Also save to database
        try:
            doc_id = await db.save_compliance_data(iam_data)
            logger.info("IAM data saved to database", doc_id=doc_id)
        except Exception as e:
            logger.warning("Failed to save to database, continuing with file save", error=str(e))
            doc_id = None

        logger.info("IAM data saved", file_path=str(file_path))

        return {
            "message": "IAM data saved successfully",
            "file_path": str(file_path),
            "database_id": doc_id,
            "timestamp": iam_data["timestamp"],
            "summary": {
                "vm_count": len(vm_policies),
                "bucket_count": len(bucket_policies)
            }
        }

    except Exception as e:
        logger.error("Error saving IAM data", project_id=project_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to save IAM data: {str(e)}")

@app.get("/vm-iam-policies-asset-api/{project_id}")
async def get_vm_iam_policies_asset_api(project_id: str) -> Dict[str, Any]:
    logger.info("Fetching VM instance IAM policies via Asset API", project_id=project_id)
    
    if not project_id:
        raise HTTPException(status_code=400, detail="Project ID is required")
    
    try:
        result = await fetch_vm_iam_policies_asset_api(project_id)
        logger.info("Successfully fetched VM instance policies via Asset API", 
                   project_id=project_id, total_policies=result["total_policies"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Unexpected error fetching VM instance policies via Asset API", 
                    project_id=project_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/bucket-iam-policies-asset-api/{project_id}")
async def get_bucket_iam_policies_asset_api(project_id: str) -> Dict[str, Any]:
    logger.info("Fetching bucket IAM policies via Asset API", project_id=project_id)
    
    if not project_id:
        raise HTTPException(status_code=400, detail="Project ID is required")
    
    try:
        result = await get_bucket_policies(project_id)
        logger.info("Successfully fetched bucket policies via Asset API", 
                   project_id=project_id, total_policies=result["total_policies"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Unexpected error fetching bucket policies via Asset API", 
                    project_id=project_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/compliance-data/collect")
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
                    projects_discovered.add(instance["project_id"])
                
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
                    projects_discovered.add(bucket["project_id"])
                
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


@app.get("/compliance-data")
async def list_compliance_data(
    project_id: Optional[str] = Query(None, description="Filter by project ID"),
    folder_id: Optional[str] = Query(None, description="Filter by folder ID"),
    org_id: Optional[str] = Query(None, description="Filter by organization ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
) -> Dict[str, Any]:
    """List stored compliance data with optional filters."""
    try:
        logger.info("Listing compliance data", 
                   project_id=project_id, folder_id=folder_id, org_id=org_id, limit=limit)
        
        # Get data from database
        data_list = await db.list_compliance_data(
            project_id=project_id,
            folder_id=folder_id,
            org_id=org_id,
            limit=limit
        )
        
        # Convert to response models
        items = []
        for item in data_list:
            # Calculate summary from stored data
            data_section = item.get("data", {})
            summary = {
                "vm_count": data_section.get("vm_instances", {}).get("count", 0),
                "bucket_count": data_section.get("buckets", {}).get("count", 0),
                "total_errors": len(item.get("errors", [])),
                "projects_processed": item.get("total_projects_count", 1)
            }
            
            items.append({
                "id": item["id"],
                "project_id": item.get("project_id"),
                "folder_id": item.get("folder_id"),
                "org_id": item.get("org_id"),
                "timestamp": item.get("timestamp"),
                "vm_policies": item.get("vm_policies", []),
                "bucket_policies": item.get("bucket_policies", []),
                "summary": summary
            })
        
        filters_applied = {}
        if project_id:
            filters_applied["project_id"] = project_id
        if folder_id:
            filters_applied["folder_id"] = folder_id
        if org_id:
            filters_applied["org_id"] = org_id
        
        logger.info("Compliance data listed", count=len(items), filters=filters_applied)
        
        return {
            "items": items,
            "total_count": len(items),
            "filters_applied": filters_applied
        }
        
    except Exception as e:
        logger.error("Failed to list compliance data", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to list compliance data: {str(e)}")


@app.get("/compliance-data/{doc_id}")
async def get_compliance_data(doc_id: str) -> Dict[str, Any]:
    """Get specific compliance data by document ID."""
    try:
        logger.info("Getting compliance data", doc_id=doc_id)
        
        # Get data from database
        data = await db.get_compliance_data(doc_id)
        
        if not data:
            raise HTTPException(status_code=404, detail="Compliance data not found")
        
        # Calculate summary from stored data
        data_section = data.get("data", {})
        summary = {
            "vm_count": data_section.get("vm_instances", {}).get("count", 0),
            "bucket_count": data_section.get("buckets", {}).get("count", 0),
            "total_errors": len(data.get("errors", [])),
            "projects_processed": data.get("total_projects_count", 1)
        }
        
        return {
            "id": data["id"],
            "project_id": data.get("project_id"),
            "folder_id": data.get("folder_id"),
            "org_id": data.get("org_id"),
            "timestamp": data.get("timestamp"),
            "vm_policies": data.get("vm_policies", []),
            "bucket_policies": data.get("bucket_policies", []),
            "summary": summary,
            "projects_processed": data.get("projects_processed"),
            "total_projects_count": data.get("total_projects_count")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get compliance data", doc_id=doc_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get compliance data: {str(e)}")


@app.delete("/compliance-data/{doc_id}")
async def delete_compliance_data(doc_id: str) -> Dict[str, Any]:
    """Delete compliance data by document ID."""
    try:
        logger.info("Deleting compliance data", doc_id=doc_id)
        
        success = await db.delete_compliance_data(doc_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Compliance data not found")
        
        return {"message": "Compliance data deleted successfully", "doc_id": doc_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete compliance data", doc_id=doc_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to delete compliance data: {str(e)}")


@app.get("/buckets")
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


@app.get("/instances")
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


@app.delete("/records/{parent_scope}")
async def delete_records_by_scope(parent_scope: str) -> Dict[str, Any]:
    """Delete all bucket and instance records for a given parent scope."""
    try:
        logger.info("Deleting records by scope", parent_scope=parent_scope)
        
        success = await db.delete_records_by_scope(parent_scope)
        
        if success:
            logger.info("Successfully deleted records", parent_scope=parent_scope)
            return {
                "message": f"Successfully deleted all records for scope: {parent_scope}",
                "parent_scope": parent_scope
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to delete records")
            
    except Exception as e:
        logger.error("Failed to delete records by scope", parent_scope=parent_scope, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to delete records: {str(e)}")


@app.post("/database/reset")
async def reset_database() -> Dict[str, Any]:
    """Reset the mock database (only works with mock database implementation)."""
    try:
        logger.info("Resetting database")
        
        # Check if database has reset method (mock database)
        if hasattr(db, 'reset_database'):
            result = await db.reset_database()
            logger.info("Database reset successfully", **result)
            return {
                "message": "Database reset successfully",
                "deleted_counts": result
            }
        else:
            raise HTTPException(
                status_code=400, 
                detail="Database reset is only available for mock database implementation"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to reset database", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to reset database: {str(e)}")


@app.get("/database/mock-data")
async def get_mock_data() -> Dict[str, Any]:
    """Get all mock database data for visualization (only works with mock database)."""
    try:
        logger.info("Getting mock database data")
        
        # Check if database has direct access to data (mock database)
        if hasattr(db, '_buckets') and hasattr(db, '_instances') and hasattr(db, '_compliance_data'):
            return {
                "buckets": dict(db._buckets),
                "instances": dict(db._instances), 
                "compliance_data": dict(db._compliance_data),
                "counts": {
                    "buckets": len(db._buckets),
                    "instances": len(db._instances),
                    "compliance_data": len(db._compliance_data)
                }
            }
        else:
            raise HTTPException(
                status_code=400,
                detail="Mock data visualization is only available for mock database implementation"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get mock data", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get mock data: {str(e)}")


# Separate bucket policy endpoints
@app.post("/policies/buckets/collect")
async def collect_bucket_policies(request: ComplianceDataRequest) -> Dict[str, Any]:
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


@app.get("/list-bucket-policies")
async def list_bucket_policies(
    org_id: Optional[str] = Query(None, description="Filter by organization ID"),
    project_number: Optional[str] = Query(None, description="Filter by project number"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
) -> Dict[str, Any]:
    """List stored bucket policy records with org_id or project_number filter."""
    try:
        logger.info("Listing bucket policies", org_id=org_id, project_number=project_number, limit=limit)
        
        buckets = await db.get_buckets(org_id=org_id, project_number=project_number, limit=limit)
        
        return {
            "buckets": buckets,
            "count": len(buckets),
            "filters": {
                "org_id": org_id,
                "project_number": project_number,
                "limit": limit
            }
        }
        
    except Exception as e:
        logger.error("Failed to list bucket policies", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to list bucket policies: {str(e)}")


# Separate instance policy endpoints  
@app.post("/policies/instances/collect")
async def collect_instance_policies(request: ComplianceDataRequest) -> Dict[str, Any]:
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


@app.get("/list-instance-policies")
async def list_instance_policies(
    org_id: Optional[str] = Query(None, description="Filter by organization ID"),
    project_number: Optional[str] = Query(None, description="Filter by project number"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
) -> Dict[str, Any]:
    """List stored instance policy records with org_id or project_number filter."""
    try:
        logger.info("Listing instance policies", org_id=org_id, project_number=project_number, limit=limit)
        
        instances = await db.get_instances(org_id=org_id, project_number=project_number, limit=limit)
        
        return {
            "instances": instances,
            "count": len(instances),
            "filters": {
                "org_id": org_id,
                "project_number": project_number,
                "limit": limit
            }
        }
        
    except Exception as e:
        logger.error("Failed to list instance policies", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to list instance policies: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "compliance-checks"}
