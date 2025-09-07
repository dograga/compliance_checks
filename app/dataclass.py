"""Data classes for the compliance checks application."""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class IAMBinding(BaseModel):
    """Represents an IAM policy binding."""
    role: str
    members: List[str]
    condition: Optional[Dict[str, Any]] = None


class IAMPolicy(BaseModel):
    """Represents an IAM policy."""
    version: Optional[int] = None
    bindings: List[IAMBinding] = []
    etag: Optional[str] = None


class BucketRecord(BaseModel):
    """Database record for a bucket."""
    parent_scope: str
    project_number: str
    organization_id: str
    bucket_name: str
    asset_name: str
    asset_type: str
    policy: Optional[Dict[str, Any]] = None
    timestamp: str


class InstanceRecord(BaseModel):
    """Database record for a VM instance."""
    parent_scope: str
    project_number: str
    organization_id: str
    instance_name: str
    asset_name: str
    asset_type: str
    zone: str
    policy: Optional[Dict[str, Any]] = None
    timestamp: str


class AssetCollectionResponse(BaseModel):
    """Response model for asset collection."""
    parent_scope: str
    buckets: List[BucketRecord]
    instances: List[InstanceRecord]
    total_buckets: int
    total_instances: int
    projects_discovered: List[str]
    errors: List[str] = []


class ComplianceDataRequest(BaseModel):
    """Request model for compliance data collection."""
    folder_id: Optional[str] = None
    org_id: Optional[str] = None
    include_vm_policies: bool = True
    include_bucket_policies: bool = True
    
    def __init__(self, **data):
        super().__init__(**data)
        if not self.folder_id and not self.org_id:
            raise ValueError("Either folder_id or org_id must be provided")


class ComplianceDataResponse(BaseModel):
    """Response model for stored compliance data."""
    id: str
    folder_id: Optional[str] = None
    org_id: Optional[str] = None
    timestamp: str
    summary: Dict[str, int]
    projects_discovered: List[str]
    total_projects_count: int


class ComplianceDataListResponse(BaseModel):
    """Response model for listing compliance data."""
    items: List[ComplianceDataResponse]
    total_count: int
    filters_applied: Dict[str, Any]