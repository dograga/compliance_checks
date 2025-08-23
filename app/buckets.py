import asyncio
from concurrent.futures import ThreadPoolExecutor
from fastapi import HTTPException
from google.cloud import asset_v1
from google.auth import default
from google.api_core import exceptions as gcp_exceptions
import logging
from typing import List, Optional, Dict
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Response models
class BucketPolicy(BaseModel):
    project_id: str
    resource_name: str
    asset_type: str
    policy: Optional[Dict[str, List[Dict]]]  # Single dictionary {"bindings": [...]}
    error: Optional[str]

class ProjectPoliciesResponse(BaseModel):
    project_id: str
    policies: List[BucketPolicy]
    total_policies: int
    errors: List[str]

# Function to fetch IAM for a single bucket
def fetch_bucket_iam(asset, project_id):
    try:
        bindings = [{"role": b.role, "members": b.members} for b in asset.iam_policy.bindings] if asset.iam_policy else []
        return BucketPolicy(
            project_id=project_id,
            resource_name=asset.name,
            asset_type=asset.asset_type,
            policy={"bindings": bindings} if bindings else {},
            error=None
        )
    except Exception as e:
        return BucketPolicy(
            project_id=project_id,
            resource_name=getattr(asset, "name", "unknown"),
            asset_type=getattr(asset, "asset_type", "storage.googleapis.com/Bucket"),
            policy=None,
            error=str(e)
        )

# Main function to get bucket policies via Asset API
async def get_bucket_policies(project_id: str) -> ProjectPoliciesResponse:
    try:
        creds, _ = default()  # Use user ADC
        client = asset_v1.AssetServiceClient(credentials=creds)
        parent = f"projects/{project_id}"

        logger.info(f"Fetching bucket IAM policies via Asset API for project: {project_id}")

        request = asset_v1.ListAssetsRequest(
            parent=parent,
            asset_types=["storage.googleapis.com/Bucket"],
            content_type=asset_v1.ContentType.IAM_POLICY,
            page_size=1000
        )

        # Run list_assets in a thread to avoid blocking
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

        # Process all buckets concurrently
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
