"""Helper functions for the compliance checks application."""

import logging
from typing import List, Optional
from fastapi import HTTPException
from google.cloud import asset_v1
import google.auth
from google.api_core import exceptions as gcp_exceptions

from .dataclass import (
    IAMPolicy, IAMBinding, PolicyResponse, ProjectPoliciesResponse,
    ComplianceIssue, ComplianceAnalysisResponse
)

logger = logging.getLogger(__name__)


def get_asset_client():
    """Initialize and return Google Cloud Asset client."""
    try:
        credentials, project = google.auth.default()
        return asset_v1.AssetServiceClient(credentials=credentials)
    except Exception as e:
        logger.error(f"Failed to initialize Asset client: {e}")
        raise HTTPException(status_code=500, detail="Failed to initialize Google Cloud client")


def convert_policy_to_pydantic(policy) -> Optional[IAMPolicy]:
    """Convert Google Cloud IAM Policy to Pydantic model."""
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


def get_default_asset_types() -> List[str]:
    """Get default asset types that support IAM policies."""
    return [
        "cloudresourcemanager.googleapis.com/Project",
        "storage.googleapis.com/Bucket",
        "compute.googleapis.com/Instance",
        "cloudsql.googleapis.com/Instance",
        "container.googleapis.com/Cluster",
        "pubsub.googleapis.com/Topic",
        "pubsub.googleapis.com/Subscription",
        "bigquery.googleapis.com/Dataset",
        "bigquery.googleapis.com/Table",
    ]


def analyze_compliance_issues(project_id: str, policies: List[PolicyResponse]) -> ComplianceAnalysisResponse:
    """Analyze IAM policies for compliance issues."""
    issues = []
    
    for policy_response in policies:
        if not policy_response.policy or policy_response.error:
            continue
            
        for binding in policy_response.policy.bindings:
            # Check for public access (allUsers, allAuthenticatedUsers)
            public_members = [m for m in binding.members if m in ["allUsers", "allAuthenticatedUsers"]]
            if public_members:
                severity = "high" if "allUsers" in public_members else "medium"
                issues.append(ComplianceIssue(
                    resource_name=policy_response.resource_name,
                    asset_type=policy_response.asset_type,
                    issue_type="public_access",
                    severity=severity,
                    description=f"Resource has public access via {', '.join(public_members)}",
                    role=binding.role,
                    members=public_members
                ))
            
            # Check for cross-project access
            cross_project_members = []
            for member in binding.members:
                if member.startswith(("user:", "serviceAccount:", "group:")):
                    # Extract domain/project info
                    if "@" in member:
                        domain_part = member.split("@")[1]
                        # Check if it's a different project service account
                        if ".iam.gserviceaccount.com" in domain_part:
                            sa_project = domain_part.replace(".iam.gserviceaccount.com", "")
                            if sa_project != project_id:
                                cross_project_members.append(member)
                        # Check for external domains (not Google Workspace)
                        elif not domain_part.endswith((".google.com", ".googleusercontent.com")):
                            cross_project_members.append(member)
            
            if cross_project_members:
                issues.append(ComplianceIssue(
                    resource_name=policy_response.resource_name,
                    asset_type=policy_response.asset_type,
                    issue_type="cross_project_access",
                    severity="medium",
                    description="Resource has cross-project or external access",
                    role=binding.role,
                    members=cross_project_members
                ))
    
    # Generate summary
    summary = {
        "public_access": len([i for i in issues if i.issue_type == "public_access"]),
        "cross_project_access": len([i for i in issues if i.issue_type == "cross_project_access"]),
        "high_severity": len([i for i in issues if i.severity == "high"]),
        "medium_severity": len([i for i in issues if i.severity == "medium"])
    }
    
    # Generate recommendations
    recommendations = []
    if summary["public_access"] > 0:
        recommendations.append("Review and restrict public access (allUsers/allAuthenticatedUsers) where not necessary")
    if summary["cross_project_access"] > 0:
        recommendations.append("Audit cross-project access and ensure it follows principle of least privilege")
    if summary["high_severity"] > 0:
        recommendations.append("Prioritize fixing high-severity issues (allUsers access)")
    
    return ComplianceAnalysisResponse(
        project_id=project_id,
        total_resources_analyzed=len([p for p in policies if p.policy and not p.error]),
        issues_found=issues,
        summary=summary,
        recommendations=recommendations
    )


async def fetch_iam_policies_for_project(project_id: str, asset_types: Optional[List[str]] = None) -> ProjectPoliciesResponse:
    """Fetch IAM policies for all resources in a project."""
    client = get_asset_client()
    parent = f"projects/{project_id}"
    
    # Use default asset types if none provided
    if not asset_types:
        asset_types = get_default_asset_types()
    
    policies = []
    errors = []
    
    try:
        # Search for assets with IAM policies
        request = asset_v1.SearchAllResourcesRequest(
            scope=parent,
            asset_types=asset_types,
            page_size=1000
        )
        
        page_result = client.search_all_resources(request=request)
        
        for resource in page_result:
            try:
                # Get IAM policy for each resource
                policy_request = asset_v1.GetIamPolicyRequest(
                    resource=resource.name
                )
                
                try:
                    policy = client.get_iam_policy(request=policy_request)
                    converted_policy = convert_policy_to_pydantic(policy)
                    
                    policies.append(PolicyResponse(
                        project_id=project_id,
                        resource_name=resource.name,
                        asset_type=resource.asset_type,
                        policy=converted_policy
                    ))
                except gcp_exceptions.PermissionDenied:
                    # Skip resources we don't have permission to access
                    policies.append(PolicyResponse(
                        project_id=project_id,
                        resource_name=resource.name,
                        asset_type=resource.asset_type,
                        error="Permission denied"
                    ))
                except Exception as e:
                    error_msg = f"Failed to get policy for {resource.name}: {str(e)}"
                    errors.append(error_msg)
                    policies.append(PolicyResponse(
                        project_id=project_id,
                        resource_name=resource.name,
                        asset_type=resource.asset_type,
                        error=str(e)
                    ))
                    
            except Exception as e:
                error_msg = f"Failed to process resource: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
    
    except gcp_exceptions.PermissionDenied:
        raise HTTPException(
            status_code=403, 
            detail=f"Permission denied accessing project {project_id}. Ensure you have the required IAM permissions."
        )
    except Exception as e:
        logger.error(f"Failed to fetch policies for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch IAM policies: {str(e)}")
    
    return ProjectPoliciesResponse(
        project_id=project_id,
        policies=policies,
        total_policies=len(policies),
        errors=errors
    )
