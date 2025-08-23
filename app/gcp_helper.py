"""GCP Helper functions for the compliance checks application."""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict
from fastapi import HTTPException
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from google.cloud import asset_v1
from google.cloud import resourcemanager_v3
from google.api_core import exceptions as gcp_exceptions
from google.auth import default
import google.auth

from .dataclass import (
    IAMPolicy, IAMBinding, PolicyResponse, ProjectPoliciesResponse
)

logger = logging.getLogger(__name__)


def get_compute_service():
    """Initialize and return Google Cloud Compute Engine service."""
    try:
        credentials, project = google.auth.default()
        logger.info("FastAPI is using credentials for:", credentials.service_account_email if hasattr(credentials, "service_account_email") else credentials.quota_project_id)
        return discovery.build("compute", "v1", credentials=credentials)
    except Exception as e:
        logger.error(f"Failed to initialize Compute service: {e}")
        raise HTTPException(status_code=500, detail="Failed to initialize Google Cloud client")


def get_storage_service():
    """Initialize and return Google Cloud Storage service."""
    try:
        credentials, project = google.auth.default()
        logger.info("Storage service initialized")
        return discovery.build("storage", "v1", credentials=credentials)
    except Exception as e:
        logger.error(f"Failed to initialize Storage service: {e}")
        raise HTTPException(status_code=500, detail="Failed to initialize Google Cloud Storage client")


def get_asset_client():
    """Initialize and return Google Cloud Asset client."""
    try:
        credentials, project = google.auth.default()
        logger.info("Asset API client initialized")
        return asset_v1.AssetServiceClient(credentials=credentials)
    except Exception as e:
        logger.error(f"Failed to initialize Asset client: {e}")
        raise HTTPException(status_code=500, detail="Failed to initialize Google Cloud Asset client")


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
def fetch_bucket_iam(asset, project_id):
    """Function to fetch IAM for a single bucket."""
    try:
        bindings = [{"role": b.role, "members": b.members} for b in asset.iam_policy.bindings] if asset.iam_policy else []
        return PolicyResponse(
            project_id=project_id,
            resource_name=asset.name,
            asset_type=asset.asset_type,
            policy=IAMPolicy(bindings=[IAMBinding(role=b["role"], members=b["members"]) for b in bindings]) if bindings else None,
            error=None
        )
    except Exception as e:
        return PolicyResponse(
            project_id=project_id,
            resource_name=getattr(asset, "name", "unknown"),
            asset_type=getattr(asset, "asset_type", "storage.googleapis.com/Bucket"),
            policy=None,
            error=str(e)
        )


async def get_bucket_policies(project_id: str) -> ProjectPoliciesResponse:
    """Main function to get bucket policies via Asset API."""
    try:
        creds, _ = default()
        client = asset_v1.AssetServiceClient(credentials=creds)
        parent = f"projects/{project_id}"

        logger.info(f"Fetching bucket IAM policies via Asset API for project: {project_id}")

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
        logger.info(f"Found {total_resources} buckets to process")

        if not response:
            return ProjectPoliciesResponse(
                project_id=project_id,
                policies=[],
                total_policies=0,
                errors=[]
            )

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
                logger.error(error_msg)
            else:
                policies.append(result)

        logger.info(f"Completed bucket Asset API scan. Total buckets found: {total_resources}, Policies retrieved: {len(policies)}, Errors: {len(errors)}")

        return ProjectPoliciesResponse(
            project_id=project_id,
            policies=policies,
            total_policies=len(policies),
            errors=errors
        )

    except gcp_exceptions.PermissionDenied:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied accessing project {project_id}. Ensure you have the required Asset API permissions for Cloud Storage buckets."
        )
    except Exception as e:
        logger.error(f"Failed to fetch bucket policies for project {project_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch bucket IAM policies via Asset API: {str(e)}"
        )


# VM Instance IAM functions
async def fetch_vm_iam_policies_asset_api(project_id: str) -> ProjectPoliciesResponse:
    """Main async function to fetch VM IAM policies."""
    try:
        creds, _ = default()
        client = asset_v1.AssetServiceClient(credentials=creds)
        parent = f"projects/{project_id}"

        logger.info(f"Fetching all VM instances via Asset API for project: {project_id}")

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

            logger.info(f"Found {len(resource_assets)} VM instances (RESOURCE)")

            iam_request = asset_v1.ListAssetsRequest(
                parent=parent,
                asset_types=["compute.googleapis.com/Instance"],
                content_type=asset_v1.ContentType.IAM_POLICY,
                page_size=1000
            )
            iam_assets = await loop.run_in_executor(
                executor, lambda: list(client.list_assets(request=iam_request))
            )

            logger.info(f"Found {len(iam_assets)} VM instances with IAM policies (IAM_POLICY)")

        iam_dict = {a.name: a.iam_policy for a in iam_assets if a.iam_policy}

        policies = []
        errors = []

        for asset in resource_assets:
            try:
                policy = None
                if asset.name in iam_dict:
                    policy = extract_bindings(iam_dict[asset.name])

                vm_policy = PolicyResponse(
                    project_id=project_id,
                    resource_name=asset.name,
                    asset_type=asset.asset_type,
                    policy=IAMPolicy(bindings=[IAMBinding(role=b["role"], members=b["members"]) for b in policy["bindings"]]) if policy else None,
                    error=None
                )
                policies.append(vm_policy)
            except Exception as e:
                error_msg = f"Failed to process VM instance {getattr(asset, 'name', 'unknown')}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)

        logger.info(f"Completed VM Asset API scan. Total VMs: {len(resource_assets)}, Policies retrieved: {len(policies)}, Errors: {len(errors)}")

        return ProjectPoliciesResponse(
            project_id=project_id,
            policies=policies,
            total_policies=len(policies),
            errors=errors
        )

    except gcp_exceptions.PermissionDenied:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied accessing project {project_id}. Ensure you have the required Asset API permissions."
        )
    except Exception as e:
        logger.error(f"Failed to fetch VM instance policies for project {project_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch VM instance IAM policies via Asset API: {str(e)}"
        )


