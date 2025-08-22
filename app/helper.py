"""Helper functions for the compliance checks application."""

import logging
import asyncio
from typing import List, Optional
from fastapi import HTTPException
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from google.cloud import asset_v1
from google.cloud import resourcemanager_v3
from google.api_core import exceptions as gcp_exceptions
import google.auth
from concurrent.futures import ThreadPoolExecutor

from .dataclass import (
    IAMPolicy, IAMBinding, PolicyResponse, ProjectPoliciesResponse,
    ComplianceIssue, ComplianceAnalysisResponse
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


def get_zones_for_project(service, project_id: str) -> List[str]:
    """Get all zones available in a project. Returns common zones if listing fails."""
    return [
            'asia-southeast1-a', 'asia-southeast1-b', 'asia-southeast1-c'
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


async def process_instance_policy(service, project_id: str, zone: str, instance: dict) -> PolicyResponse:
    """Process IAM policy for a single instance asynchronously."""
    instance_name = instance['name']
    resource_name = f"projects/{project_id}/zones/{zone}/instances/{instance_name}"
    
    try:
        # Get IAM policy for the instance
        logger.debug(f"Fetching IAM policy for {resource_name}")
        request = service.instances().getIamPolicy(
            project=project_id,
            zone=zone,
            resource=instance_name
        )
        policy_response = await async_execute_request(request)
        
        logger.debug(f"Successfully retrieved IAM policy for {instance_name}")
        
        converted_policy = convert_policy_to_pydantic(policy_response)
        
        if converted_policy and converted_policy.bindings:
            logger.debug(f"Instance {instance_name} has {len(converted_policy.bindings)} IAM bindings")
        else:
            logger.debug(f"Instance {instance_name} has no IAM bindings")
        
        return PolicyResponse(
            project_id=project_id,
            resource_name=resource_name,
            asset_type="compute.googleapis.com/Instance",
            policy=converted_policy
        )
        
    except Exception as e:
        if "403" in str(e) or "Permission denied" in str(e):
            logger.warning(f"Permission denied for instance {instance_name}: {str(e)}")
            return PolicyResponse(
                project_id=project_id,
                resource_name=resource_name,
                asset_type="compute.googleapis.com/Instance",
                error="Permission denied"
            )
        else:
            logger.error(f"Failed to get policy for {resource_name}: {str(e)}")
            return PolicyResponse(
                project_id=project_id,
                resource_name=resource_name,
                asset_type="compute.googleapis.com/Instance",
                error=str(e)
            )


async def process_zone_instances(service, project_id: str, zone: str) -> tuple[List[PolicyResponse], List[str], int]:
    """Process all instances in a zone concurrently."""
    logger.info(f"Processing zone: {zone}")
    policies = []
    errors = []
    
    try:
        # List all instances in the zone
        logger.debug(f"Listing instances in zone {zone}")
        request = service.instances().list(project=project_id, zone=zone)
        response = await async_execute_request(request)
        
        instances = response.get('items', [])
        logger.info(f"Found {len(instances)} instances in zone {zone}")
        
        if instances:
            # Process all instances in this zone concurrently
            tasks = [
                process_instance_policy(service, project_id, zone, instance)
                for instance in instances
            ]
            
            # Use asyncio.gather to process instances concurrently
            instance_policies = await asyncio.gather(*tasks, return_exceptions=True)
            
            for policy_result in instance_policies:
                if isinstance(policy_result, Exception):
                    error_msg = f"Failed to process instance in zone {zone}: {str(policy_result)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                else:
                    policies.append(policy_result)
        
        return policies, errors, len(instances)
        
    except Exception as e:
        if "404" in str(e) or "not found" in str(e).lower():
            logger.debug(f"Zone {zone} not found or empty: {str(e)}")
            return [], [], 0
        elif "403" in str(e) or "forbidden" in str(e).lower():
            error_msg = f"Permission denied for zone {zone}: {str(e)}"
            logger.warning(error_msg)
            return [], [error_msg], 0
        else:
            error_msg = f"Failed to process zone {zone}: {str(e)}"
            logger.error(error_msg)
            return [], [error_msg], 0


async def fetch_iam_policies_for_project(project_id: str, zones: Optional[List[str]] = None) -> ProjectPoliciesResponse:
    """Fetch IAM policies for all VM instances in a project with concurrent processing."""
    logger.info(f"Starting VM instance IAM policy fetch for project: {project_id}")
    service = get_compute_service()
    
    # Get all zones if none provided
    if not zones:
        logger.info(f"No zones specified, discovering zones for project {project_id}")
        zones = get_zones_for_project(service, project_id)
        logger.info(f"Found {len(zones)} zones to scan: {zones}")
    else:
        logger.info(f"Using specified zones: {zones}")
    
    try:
        # Process all zones concurrently
        zone_tasks = [
            process_zone_instances(service, project_id, zone)
            for zone in zones
        ]
        
        logger.info(f"Processing {len(zones)} zones concurrently...")
        zone_results = await asyncio.gather(*zone_tasks, return_exceptions=True)
        
        # Aggregate results from all zones
        all_policies = []
        all_errors = []
        total_instances_found = 0
        
        for i, result in enumerate(zone_results):
            if isinstance(result, Exception):
                error_msg = f"Failed to process zone {zones[i]}: {str(result)}"
                all_errors.append(error_msg)
                logger.error(error_msg)
            else:
                policies, errors, instance_count = result
                all_policies.extend(policies)
                all_errors.extend(errors)
                total_instances_found += instance_count
        
        logger.info(f"Completed VM instance scan. Total instances found: {total_instances_found}, Policies retrieved: {len(all_policies)}, Errors: {len(all_errors)}")
    
    except Exception as e:
        if "403" in str(e) or "Permission denied" in str(e):
            raise HTTPException(
                status_code=403, 
                detail=f"Permission denied accessing project {project_id}. Ensure you have the required IAM permissions (compute.instances.list, compute.instances.getIamPolicy)."
            )
        else:
            logger.error(f"Failed to fetch policies for project {project_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch IAM policies: {str(e)}")
    
    result = ProjectPoliciesResponse(
        project_id=project_id,
        policies=all_policies,
        total_policies=len(all_policies),
        errors=all_errors
    )
    
    logger.info(f"VM instance IAM policy fetch completed for project {project_id}: {len(all_policies)} policies, {len(all_errors)} errors")
    return result


def get_asset_client():
    """Initialize and return Google Cloud Asset client."""
    try:
        credentials, project = google.auth.default()
        logger.info("Asset API client initialized")
        return asset_v1.AssetServiceClient(credentials=credentials)
    except Exception as e:
        logger.error(f"Failed to initialize Asset client: {e}")
        raise HTTPException(status_code=500, detail="Failed to initialize Google Cloud Asset client")


def get_default_asset_types() -> List[str]:
    """Get default asset types that support IAM policies."""
    return [
        "compute.googleapis.com/Instance",
    ]


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


async def process_resource_policy_asset_api(resource, project_id: str) -> PolicyResponse:
    """Process IAM policy for a single resource using Asset API asynchronously."""
    try:
        logger.debug(f"Fetching IAM policy for {resource.name}")
        
        # Extract resource type and handle accordingly
        if resource.asset_type == "compute.googleapis.com/Instance":
            # For compute instances, we need to use the compute API directly
            # Extract project, zone, instance from resource name
            # Format: //compute.googleapis.com/projects/PROJECT/zones/ZONE/instances/INSTANCE
            parts = resource.name.split('/')
            if len(parts) >= 8:
                project = parts[4]
                zone = parts[6] 
                instance = parts[8]
                
                # Use compute service to get IAM policy
                compute_service = get_compute_service()
                request = compute_service.instances().getIamPolicy(
                    project=project,
                    zone=zone,
                    resource=instance
                )
                policy_response = await async_execute_request(request)
                
                logger.debug(f"Successfully retrieved IAM policy for {resource.name}")
                converted_policy = convert_policy_to_pydantic(policy_response)
            else:
                logger.warning(f"Could not parse resource name: {resource.name}")
                converted_policy = None
        else:
            # For other resource types, skip for now
            logger.debug(f"Skipping unsupported resource type: {resource.asset_type}")
            converted_policy = None
        
        if converted_policy and converted_policy.bindings:
            logger.debug(f"Resource {resource.name} has {len(converted_policy.bindings)} IAM bindings")
        else:
            logger.debug(f"Resource {resource.name} has no IAM bindings")
        
        return PolicyResponse(
            project_id=project_id,
            resource_name=resource.name,
            asset_type=resource.asset_type,
            policy=converted_policy
        )
        
    except gcp_exceptions.PermissionDenied:
        logger.warning(f"Permission denied for resource {resource.name}")
        return PolicyResponse(
            project_id=project_id,
            resource_name=resource.name,
            asset_type=resource.asset_type,
            error="Permission denied"
        )
    except Exception as e:
        logger.error(f"Failed to get policy for {resource.name}: {str(e)}")
        return PolicyResponse(
            project_id=project_id,
            resource_name=resource.name,
            asset_type=resource.asset_type,
            error=str(e)
        )


async def fetch_iam_policies_asset_api(project_id: str, asset_types: Optional[List[str]] = None) -> ProjectPoliciesResponse:
    """Fetch IAM policies for all resources in a project using Asset API with concurrent processing."""
    logger.info(f"Starting Asset API IAM policy fetch for project: {project_id}")
    
    # Use default asset types if none provided
    if not asset_types:
        asset_types = get_default_asset_types()
        logger.info(f"Using default asset types: {asset_types}")
    else:
        logger.info(f"Using specified asset types: {asset_types}")
    
    try:
        # Initialize client and search for assets
        client = get_asset_client()
        parent = f"projects/{project_id}"
        
        logger.info(f"Searching for assets in project {project_id}")
        request = asset_v1.SearchAllResourcesRequest(
            scope=parent,
            asset_types=asset_types,
            page_size=1000
        )
        
        # Execute the search asynchronously
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            page_result = await loop.run_in_executor(
                executor, 
                client.search_all_resources, 
                request
            )
        
        # Convert to list to get all resources
        resources = list(page_result)
        total_resources_found = len(resources)
        logger.info(f"Found {total_resources_found} resources to process")
        
        if not resources:
            logger.info("No resources found")
            return ProjectPoliciesResponse(
                project_id=project_id,
                policies=[],
                total_policies=0,
                errors=[]
            )
        
        # Process all resources concurrently
        logger.info(f"Processing {total_resources_found} resources concurrently...")
        tasks = [
            process_resource_policy_asset_api(resource, project_id)
            for resource in resources
        ]
        
        # Use asyncio.gather to process resources concurrently
        policy_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Aggregate results
        policies = []
        errors = []
        
        for result in policy_results:
            if isinstance(result, Exception):
                error_msg = f"Failed to process resource: {str(result)}"
                errors.append(error_msg)
                logger.error(error_msg)
            else:
                policies.append(result)
        
        logger.info(f"Completed Asset API scan. Total resources found: {total_resources_found}, Policies retrieved: {len(policies)}, Errors: {len(errors)}")
    
    except gcp_exceptions.PermissionDenied:
        raise HTTPException(
            status_code=403, 
            detail=f"Permission denied accessing project {project_id}. Ensure you have the required Asset API permissions."
        )
    except Exception as e:
        logger.error(f"Failed to fetch policies for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch IAM policies via Asset API: {str(e)}")
    
    result = ProjectPoliciesResponse(
        project_id=project_id,
        policies=policies,
        total_policies=len(policies),
        errors=errors
    )
    
    logger.info(f"Asset API IAM policy fetch completed for project {project_id}: {len(policies)} policies, {len(errors)} errors")
    return result
