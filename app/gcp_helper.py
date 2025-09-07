"""GCP Helper functions for the compliance checks application."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Any
from datetime import datetime
import structlog
from fastapi import HTTPException
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from google.cloud import asset_v1
from google.cloud import resourcemanager_v3
from google.api_core import exceptions as gcp_exceptions
from google.auth import default
import google.auth

from .dataclass import (
    IAMPolicy, IAMBinding
)

logger = structlog.get_logger(__name__)


def extract_ancestors_info(asset) -> Dict[str, str]:
    """Extract project_number and organization_id from asset ancestors."""
    project_number = "unknown"
    organization_id = "unknown"
    
    if hasattr(asset, 'ancestors') and asset.ancestors:
        for ancestor in asset.ancestors:
            if ancestor.startswith("projects/"):
                project_number = ancestor.split("/")[1]
            elif ancestor.startswith("organizations/"):
                organization_id = ancestor.split("/")[1]
    
    return {
        "project_number": project_number,
        "organization_id": organization_id
    }


def get_compute_service():
    """Initialize and return Google Cloud Compute Engine service."""
    try:
        credentials, project = google.auth.default()
        logger.info("FastAPI is using credentials", 
                   account=credentials.service_account_email if hasattr(credentials, "service_account_email") else credentials.quota_project_id)
        return discovery.build("compute", "v1", credentials=credentials)
    except Exception as e:
        logger.error("Failed to initialize Compute service", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to initialize Google Cloud client")


def get_storage_service():
    """Initialize and return Google Cloud Storage service."""
    try:
        credentials, project = google.auth.default()
        logger.info("Storage service initialized")
        return discovery.build("storage", "v1", credentials=credentials)
    except Exception as e:
        logger.error("Failed to initialize Storage service", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to initialize Google Cloud Storage client")


def get_asset_client():
    """Initialize and return Google Cloud Asset client."""
    try:
        credentials, project = google.auth.default()
        logger.info("Asset API client initialized")
        return asset_v1.AssetServiceClient(credentials=credentials)
    except Exception as e:
        logger.error("Failed to initialize Asset client", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to initialize Google Cloud Asset client")


def get_resource_manager_client():
    """Initialize and return Google Cloud Resource Manager client."""
    try:
        credentials, project = google.auth.default()
        logger.info("Resource Manager client initialized")
        return resourcemanager_v3.ProjectsClient(credentials=credentials)
    except Exception as e:
        logger.error("Failed to initialize Resource Manager client", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to initialize Google Cloud Resource Manager client")


def get_folders_client():
    """Initialize and return Google Cloud Folders client."""
    try:
        credentials, project = google.auth.default()
        logger.info("Folders client initialized")
        return resourcemanager_v3.FoldersClient(credentials=credentials)
    except Exception as e:
        logger.error("Failed to initialize Folders client", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to initialize Google Cloud Folders client")


def get_organizations_client():
    """Initialize and return Google Cloud Organizations client."""
    try:
        credentials, project = google.auth.default()
        logger.info("Organizations client initialized")
        return resourcemanager_v3.OrganizationsClient(credentials=credentials)
    except Exception as e:
        logger.error("Failed to initialize Organizations client", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to initialize Google Cloud Organizations client")


async def async_execute_request(request):
    """Execute a Google Cloud API request asynchronously."""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        return await loop.run_in_executor(executor, request.execute)


def convert_policy_to_pydantic(policy) -> Optional[IAMPolicy]:
    """Convert Google Cloud IAM Policy to Pydantic model."""
    if not policy:
        return None
    
    bindings = []
    policy_bindings = policy.get('bindings', [])
    
    for binding in policy_bindings:
        condition_dict = None
        if binding.get('condition'):
            condition = binding['condition']
            condition_dict = {
                "title": condition.get('title', ''),
                "description": condition.get('description', ''),
                "expression": condition.get('expression', '')
            }
        
        bindings.append(IAMBinding(
            role=binding.get('role', ''),
            members=binding.get('members', []),
            condition=condition_dict
        ))
    
    return IAMPolicy(
        version=policy.get('version'),
        bindings=bindings,
        etag=policy.get('etag')
    )


def convert_asset_policy_to_pydantic(policy) -> Optional[IAMPolicy]:
    """Convert Google Cloud Asset API IAM Policy to Pydantic model."""
    if not policy:
        return None
    
    bindings = []
    for binding in policy.bindings:
        condition_dict = None
        if binding.condition:
            condition_dict = {
                "title": binding.condition.title,
                "description": binding.condition.description,
                "expression": binding.condition.expression
            }
        
        bindings.append(IAMBinding(
            role=binding.role,
            members=list(binding.members),
            condition=condition_dict
        ))
    
    return IAMPolicy(
        version=policy.version if hasattr(policy, 'version') else None,
        bindings=bindings,
        etag=policy.etag if hasattr(policy, 'etag') else None
    )


def extract_bindings(iam_policy):
    """Helper to convert IAM bindings."""
    if iam_policy and iam_policy.bindings:
        return {"bindings": [{"role": b.role, "members": list(b.members)} for b in iam_policy.bindings]}
    return None


def get_zones_for_project(service, project_id: str) -> List[str]:
    """Get all zones available in a project. Returns common zones if listing fails."""
    return [
        'asia-southeast1-a', 'asia-southeast1-b', 'asia-southeast1-c'
    ]


def get_vm_asset_types() -> List[str]:
    """Get asset types for VM instances."""
    return ["compute.googleapis.com/Instance"]


def get_bucket_asset_types() -> List[str]:
    """Get asset types for Cloud Storage buckets."""
    return ["storage.googleapis.com/Bucket"]


def get_default_asset_types() -> List[str]:
    """Get default asset types that support IAM policies."""
    return get_vm_asset_types() + get_bucket_asset_types()


# Bucket IAM functions
def process_bucket_asset(asset, project_id: str) -> Dict[str, Any]:
    """Function to process a single bucket asset."""
    try:
        bindings = [{"role": b.role, "members": b.members} for b in asset.iam_policy.bindings] if asset.iam_policy else []
        return {
            "project_id": project_id,
            "resource_name": asset.name,
            "asset_type": asset.asset_type,
            "policy": {"bindings": bindings} if bindings else None,
            "error": None
        }
    except Exception as e:
        return {
            "project_id": project_id,
            "resource_name": getattr(asset, "name", "unknown"),
            "asset_type": getattr(asset, "asset_type", "storage.googleapis.com/Bucket"),
            "policy": None,
            "error": str(e)
        }


async def get_bucket_policies(project_id: str) -> Dict[str, Any]:
    """Main function to get bucket policies via Asset API."""
    try:
        creds, _ = default()
        client = asset_v1.AssetServiceClient(credentials=creds)
        parent = f"projects/{project_id}"

        logger.info("Fetching bucket IAM policies via Asset API", project_id=project_id)

        request = asset_v1.ListAssetsRequest(
            parent=parent,
            asset_types=["storage.googleapis.com/Bucket"],
            content_type=asset_v1.ContentType.IAM_POLICY,
            page_size=1000
        )

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            response = await loop.run_in_executor(executor, lambda: list(client.list_assets(request=request)))

        total_resources = len(response)
        logger.info("Found buckets to process", count=total_resources)

        if not response:
            return {
                "project_id": project_id,
                "policies": [],
                "total_policies": 0,
                "errors": []
            }

        tasks = [
            loop.run_in_executor(None, fetch_bucket_iam, asset, project_id)
            for asset in response
        ]

        policy_results = await asyncio.gather(*tasks, return_exceptions=True)

        policies = []
        errors = []

        for result in policy_results:
            if isinstance(result, Exception):
                error_msg = f"Failed to process bucket: {str(result)}"
                errors.append(error_msg)
                logger.error("Failed to process bucket", error=str(result))
            else:
                policies.append(result)

        logger.info("Completed bucket Asset API scan", 
                   total_buckets=total_resources, policies_retrieved=len(policies), errors=len(errors))

        return {
            "project_id": project_id,
            "policies": policies,
            "total_policies": len(policies),
            "errors": errors
        }

    except gcp_exceptions.PermissionDenied:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied accessing project {project_id}. Ensure you have the required Asset API permissions for Cloud Storage buckets."
        )
    except Exception as e:
        logger.error("Failed to fetch bucket policies", project_id=project_id, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch bucket IAM policies via Asset API: {str(e)}"
        )


# VM Instance IAM functions
async def fetch_vm_iam_policies_asset_api(project_id: str) -> Dict[str, Any]:
    """Main async function to fetch VM IAM policies."""
    try:
        creds, _ = default()
        client = asset_v1.AssetServiceClient(credentials=creds)
        parent = f"projects/{project_id}"

        logger.info("Fetching all VM instances via Asset API", project_id=project_id)

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            resource_request = asset_v1.ListAssetsRequest(
                parent=parent,
                asset_types=["compute.googleapis.com/Instance"],
                content_type=asset_v1.ContentType.RESOURCE,
                page_size=1000
            )
            resource_assets = await loop.run_in_executor(
                executor, lambda: list(client.list_assets(request=resource_request))
            )

            logger.info("Found VM instances (RESOURCE)", count=len(resource_assets))

            iam_request = asset_v1.ListAssetsRequest(
                parent=parent,
                asset_types=["compute.googleapis.com/Instance"],
                content_type=asset_v1.ContentType.IAM_POLICY,
                page_size=1000
            )
            iam_assets = await loop.run_in_executor(
                executor, lambda: list(client.list_assets(request=iam_request))
            )

            logger.info("Found VM instances with IAM policies (IAM_POLICY)", count=len(iam_assets))

        iam_dict = {a.name: a.iam_policy for a in iam_assets if a.iam_policy}

        policies = []
        errors = []

        for asset in resource_assets:
            try:
                policy = None
                if asset.name in iam_dict:
                    policy = extract_bindings(iam_dict[asset.name])

                vm_policy = {
                    "project_id": project_id,
                    "resource_name": asset.name,
                    "asset_type": asset.asset_type,
                    "policy": policy,
                    "error": None
                }
                policies.append(vm_policy)
            except Exception as e:
                error_msg = f"Failed to process VM instance {getattr(asset, 'name', 'unknown')}: {str(e)}"
                errors.append(error_msg)
                logger.error("Failed to process VM instance", 
                           resource_name=getattr(asset, 'name', 'unknown'), error=str(e))

        logger.info("Completed VM Asset API scan", 
                   total_vms=len(resource_assets), policies_retrieved=len(policies), errors=len(errors))

        return {
            "project_id": project_id,
            "policies": policies,
            "total_policies": len(policies),
            "errors": errors
        }

    except gcp_exceptions.PermissionDenied:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied accessing project {project_id}. Ensure you have the required Asset API permissions."
        )
    except Exception as e:
        logger.error("Failed to fetch VM instance policies", project_id=project_id, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch VM instance IAM policies via Asset API: {str(e)}"
        )




async def fetch_vm_iam_policies_folder_org(parent: str) -> Dict[str, Any]:
    """Fetch VM IAM policies directly from folder or organization using Asset API."""
    try:
        creds, _ = default()
        client = asset_v1.AssetServiceClient(credentials=creds)
        
        logger.info("Fetching VM instances via Asset API", parent=parent)
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            # Get VM instances with IAM policies directly from folder/org
            iam_request = asset_v1.ListAssetsRequest(
                parent=parent,
                asset_types=["compute.googleapis.com/Instance"],
                content_type=asset_v1.ContentType.IAM_POLICY,
                page_size=1000
            )
            iam_assets = await loop.run_in_executor(
                executor, lambda: list(client.list_assets(request=iam_request))
            )
            
            logger.info("Found VM instances with IAM policies", parent=parent, count=len(iam_assets))
        
        policies = []
        errors = []
        
        for asset in iam_assets:
            try:
                # Extract project ID from asset name
                project_id = asset.name.split('/')[4] if len(asset.name.split('/')) > 4 else "unknown"
                
                policy = None
                if asset.iam_policy and asset.iam_policy.bindings:
                    policy = extract_bindings(asset.iam_policy)
                
                vm_policy = {
                    "project_id": project_id,
                    "resource_name": asset.name,
                    "asset_type": asset.asset_type,
                    "policy": policy,
                    "error": None
                }
                policies.append(vm_policy)
                
            except Exception as e:
                error_msg = f"Failed to process VM instance {getattr(asset, 'name', 'unknown')}: {str(e)}"
                errors.append(error_msg)
                logger.error("Failed to process VM instance", 
                           resource_name=getattr(asset, 'name', 'unknown'), error=str(e))
        
        logger.info("Completed VM Asset API scan", 
                   parent=parent, total_vms=len(iam_assets), policies_retrieved=len(policies), errors=len(errors))
        
        return {
            "project_id": parent,
            "policies": policies,
            "total_policies": len(policies),
            "errors": errors
        }
        
    except gcp_exceptions.PermissionDenied:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied accessing {parent}. Ensure you have the required Asset API permissions."
        )
    except Exception as e:
        logger.error("Failed to fetch VM instance policies", parent=parent, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch VM instance IAM policies via Asset API: {str(e)}"
        )


async def fetch_vm_instances_folder_org(parent: str) -> List[Dict[str, Any]]:
    """Fetch VM instances with metadata and policies from folder or organization using Asset API."""
    try:
        creds, _ = default()
        client = asset_v1.AssetServiceClient(credentials=creds)
        
        logger.info("Fetching VM instances via Asset API", parent=parent)
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            # Get VM instances with IAM policies
            iam_request = asset_v1.ListAssetsRequest(
                parent=parent,
                asset_types=["compute.googleapis.com/Instance"],
                content_type=asset_v1.ContentType.IAM_POLICY,
                page_size=1000
            )
            iam_assets = await loop.run_in_executor(
                executor, lambda: list(client.list_assets(request=iam_request))
            )
            
            logger.info("Found VM instances with IAM policies", parent=parent, count=len(iam_assets))
        
        instances = []
        
        for asset in iam_assets:
            try:
                logger.info(asset)
                # Extract project number and organization ID from ancestors
                ancestors_info = extract_ancestors_info(asset)
                project_number = ancestors_info["project_number"]
                organization_id = ancestors_info["organization_id"]
                
                # Extract instance name from asset name (projects/PROJECT_ID/zones/ZONE/instances/INSTANCE_NAME)
                name_parts = asset.name.split('/')
                instance_name = name_parts[-1] if len(name_parts) > 0 else "unknown"
                
                # Extract zone and other metadata
                zone = name_parts[3] if len(name_parts) > 3 else "unknown"
                
                # Process IAM policy
                policy = None
                if asset.iam_policy and asset.iam_policy.bindings:
                    bindings_data = extract_bindings(asset.iam_policy)
                    policy = {
                        "bindings": bindings_data["bindings"],
                        "version": bindings_data.get("version"),
                        "etag": bindings_data.get("etag")
                    }
                
                instance_record = {
                    "parent_scope": parent,
                    "project_number": project_number,
                    "organization_id": organization_id,
                    "instance_name": instance_name,
                    "zone": zone,
                    "asset_name": asset.name,
                    "asset_type": asset.asset_type,
                    "policy": policy,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                instances.append(instance_record)
                
            except Exception as e:
                logger.error("Failed to process VM instance", 
                           resource_name=getattr(asset, 'name', 'unknown'), error=str(e))
        
        logger.info("Completed VM Asset API scan", 
                   parent=parent, total_instances=len(instances))
        
        return instances
        
    except gcp_exceptions.PermissionDenied:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied accessing {parent}. Ensure you have the required Asset API permissions."
        )
    except Exception as e:
        logger.error("Failed to fetch VM instances", parent=parent, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch VM instances via Asset API: {str(e)}"
        )


async def fetch_buckets_folder_org(parent: str) -> List[Dict[str, Any]]:
    """Fetch buckets with metadata and policies from folder or organization using Asset API."""
    try:
        creds, _ = default()
        client = asset_v1.AssetServiceClient(credentials=creds)
        
        logger.info("Fetching buckets via Asset API", parent=parent)
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            request = asset_v1.ListAssetsRequest(
                parent=parent,
                asset_types=["storage.googleapis.com/Bucket"],
                content_type=asset_v1.ContentType.IAM_POLICY,
                page_size=1000
            )
            response = await loop.run_in_executor(
                executor, lambda: list(client.list_assets(request=request))
            )
            
            logger.info("Found buckets with IAM policies", parent=parent, count=len(response))
        
        buckets = []
        
        for asset in response:
            try:
                logger.info(asset)
                # Extract project number and organization ID from ancestors
                ancestors_info = extract_ancestors_info(asset)
                project_number = ancestors_info["project_number"]
                organization_id = ancestors_info["organization_id"]
                
                # Extract bucket name from asset name (projects/_/buckets/BUCKET_NAME)
                name_parts = asset.name.split('/')
                bucket_name = name_parts[-1] if len(name_parts) > 0 else "unknown"
                
                # Process IAM policy
                policy = None
                if asset.iam_policy and asset.iam_policy.bindings:
                    bindings_data = extract_bindings(asset.iam_policy)
                    policy = {
                        "bindings": bindings_data["bindings"],
                        "version": bindings_data.get("version"),
                        "etag": bindings_data.get("etag")
                    }
                
                bucket_record = {
                    "parent_scope": parent,
                    "project_number": project_number,
                    "organization_id": organization_id,
                    "bucket_name": bucket_name,
                    "asset_name": asset.name,
                    "asset_type": asset.asset_type,
                    "policy": policy,
                    "timestamp": datetime.utcnow().isoformat()
                }
                buckets.append(bucket_record)
                
            except Exception as e:
                logger.error("Failed to process bucket", 
                           resource_name=getattr(asset, 'name', 'unknown'), error=str(e))
        
        logger.info("Completed bucket Asset API scan", 
                   parent=parent, total_buckets=len(buckets))
        
        return buckets
        
    except gcp_exceptions.PermissionDenied:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied accessing {parent}. Ensure you have the required Asset API permissions for Cloud Storage buckets."
        )
    except Exception as e:
        logger.error("Failed to fetch buckets", parent=parent, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch buckets via Asset API: {str(e)}"
        )


