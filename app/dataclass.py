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


class PolicyResponse(BaseModel):
    """Response model for a single policy."""
    project_id: str
    resource_name: str
    asset_type: str
    policy: Optional[IAMPolicy] = None
    error: Optional[str] = None


# Legacy aliases for backward compatibility
BucketPolicy = PolicyResponse
VMPolicy = PolicyResponse


class ProjectPoliciesResponse(BaseModel):
    """Response model for all policies in a project."""
    project_id: str
    policies: List[PolicyResponse]
    total_policies: int
    errors: List[str] = []