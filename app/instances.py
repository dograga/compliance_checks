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
class VMPolicy(BaseModel):
    project_id: str
    resource_name: str
    asset_type: str
    policy: Optional[Dict[str, List[Dict]]]  # {"bindings": [...]}
    error: Optional[str]

class ProjectPoliciesResponse(BaseModel):
    project_id: str
    policies: List[VMPolicy]
    total_policies: int
    errors: List[str]

# Helper to convert IAM bindings
def extract_bindings(iam_policy):
    if iam_policy and iam_policy.bindings:
        return {"bindings": [{"role": b.role, "members": list(b.members)} for b in iam_policy.bindings]}
    return None

# Main async function
async def fetch_vm_iam_policies_asset_api(project_id: str) -> ProjectPoliciesResponse:
    try:
        creds, _ = default()  # ADC credentials
        client = asset_v1.AssetServiceClient(credentials=creds)
        parent = f"projects/{project_id}"

        logger.info(f"Fetching all VM instances via Asset API for project: {project_id}")

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            # 1. Fetch all VMs (RESOURCE) → contains all VMs
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

            # 2. Fetch IAM policies for VMs (IAM_POLICY) → only VMs with bindings
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

        # Build dict from IAM_POLICY results
        iam_dict = {a.name: a.iam_policy for a in iam_assets if a.iam_policy}

        # Merge RESOURCE and IAM_POLICY results
        policies = []
        errors = []

        for asset in resource_assets:
            try:
                policy = None
                if asset.name in iam_dict:
                    policy = extract_bindings(iam_dict[asset.name])

                vm_policy = VMPolicy(
                    project_id=project_id,
                    resource_name=asset.name,
                    asset_type=asset.asset_type,
                    policy=policy,
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
