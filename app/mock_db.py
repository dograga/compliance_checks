"""Mock database implementation using in-memory Python dictionaries."""

import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import structlog
import threading

logger = structlog.get_logger(__name__)


class MockDatabase:
    """Mock database using in-memory dictionaries for testing."""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._buckets = {}  # bucket_id -> bucket_record
        self._instances = {}  # instance_id -> instance_record
        self._compliance_data = {}  # doc_id -> compliance_data
        self._counter = 0
        
    def _generate_id(self) -> str:
        """Generate a unique ID."""
        with self._lock:
            self._counter += 1
            return f"mock_{self._counter}"
    
    async def save_bucket_record(self, bucket_record: Dict[str, Any]) -> str:
        """Save bucket record and return document ID."""
        doc_id = self._generate_id()
        bucket_record["id"] = doc_id
        bucket_record["timestamp"] = datetime.utcnow().isoformat()
        
        with self._lock:
            self._buckets[doc_id] = bucket_record.copy()
        
        logger.info("Bucket record saved to mock database", doc_id=doc_id, 
                   bucket_name=bucket_record.get("bucket_name"))
        return doc_id
    
    async def save_instance_record(self, instance_record: Dict[str, Any]) -> str:
        """Save instance record and return document ID."""
        doc_id = self._generate_id()
        instance_record["id"] = doc_id
        instance_record["timestamp"] = datetime.utcnow().isoformat()
        
        with self._lock:
            self._instances[doc_id] = instance_record.copy()
        
        logger.info("Instance record saved to mock database", doc_id=doc_id,
                   instance_name=instance_record.get("instance_name"))
        return doc_id
    
    async def get_buckets(self, folder_id: Optional[str] = None, org_id: Optional[str] = None, project_number: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get bucket records with optional filters."""
        with self._lock:
            records = list(self._buckets.values())
        
        # Apply filters
        if folder_id:
            target_scope = f"folders/{folder_id}" if not folder_id.startswith("folders/") else folder_id
            records = [r for r in records if r.get("parent_scope") == target_scope]
        if org_id:
            records = [r for r in records if r.get("organization_id") == org_id]
        if project_number:
            records = [r for r in records if r.get("project_number") == project_number]
        
        # Apply limit
        records = records[:limit]
        
        logger.info("Retrieved bucket records from mock database", 
                   count=len(records), folder_id=folder_id, org_id=org_id, project_number=project_number)
        return records
    
    async def get_instances(self, folder_id: Optional[str] = None, org_id: Optional[str] = None, project_number: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get instance records with optional filters."""
        with self._lock:
            records = list(self._instances.values())
        
        # Apply filters
        if folder_id:
            target_scope = f"folders/{folder_id}" if not folder_id.startswith("folders/") else folder_id
            records = [r for r in records if r.get("parent_scope") == target_scope]
        if org_id:
            records = [r for r in records if r.get("organization_id") == org_id]
        if project_number:
            records = [r for r in records if r.get("project_number") == project_number]
        
        # Apply limit
        records = records[:limit]
        
        logger.info("Retrieved instance records from mock database", 
                   count=len(records), folder_id=folder_id, org_id=org_id, project_number=project_number)
        return records
    
    async def delete_records_by_scope(self, parent_scope: str) -> bool:
        """Delete all records for a given parent scope."""
        deleted_buckets = 0
        deleted_instances = 0
        
        with self._lock:
            # Delete buckets
            bucket_ids_to_delete = [
                doc_id for doc_id, record in self._buckets.items()
                if record.get("parent_scope") == parent_scope
            ]
            for doc_id in bucket_ids_to_delete:
                del self._buckets[doc_id]
                deleted_buckets += 1
            
            # Delete instances
            instance_ids_to_delete = [
                doc_id for doc_id, record in self._instances.items()
                if record.get("parent_scope") == parent_scope
            ]
            for doc_id in instance_ids_to_delete:
                del self._instances[doc_id]
                deleted_instances += 1
        
        logger.info("Deleted records by scope", parent_scope=parent_scope,
                   buckets_deleted=deleted_buckets, instances_deleted=deleted_instances)
        return True
    
    # Legacy compliance data methods for backward compatibility
    async def save_compliance_data(self, data: Dict[str, Any]) -> str:
        """Save compliance data and return document ID."""
        doc_id = self._generate_id()
        data["id"] = doc_id
        data["timestamp"] = datetime.utcnow().isoformat()
        
        with self._lock:
            self._compliance_data[doc_id] = data.copy()
        
        logger.info("Compliance data saved to mock database", doc_id=doc_id)
        return doc_id
    
    async def list_compliance_data(self, project_id: Optional[str] = None, 
                                 folder_id: Optional[str] = None, 
                                 org_id: Optional[str] = None, 
                                 limit: int = 100) -> List[Dict[str, Any]]:
        """List compliance data with optional filters."""
        with self._lock:
            records = list(self._compliance_data.values())
        
        # Apply filters with flexible matching
        if project_id:
            records = [r for r in records if r.get("project_id") == project_id]
        if folder_id:
            target_folder = f"folders/{folder_id}" if not folder_id.startswith("folders/") else folder_id
            records = [r for r in records if r.get("folder_id") == folder_id or r.get("parent_scope") == target_folder]
        if org_id:
            target_org = f"organizations/{org_id}" if not org_id.startswith("organizations/") else org_id
            records = [r for r in records if r.get("org_id") == org_id or r.get("parent_scope") == target_org]
        
        # Apply limit
        records = records[:limit]
        
        logger.info("Listed compliance data from mock database", 
                   count=len(records), project_id=project_id, folder_id=folder_id, org_id=org_id)
        return records
    
    async def get_compliance_data(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Get specific compliance data by document ID."""
        with self._lock:
            data = self._compliance_data.get(doc_id)
        
        if data:
            logger.info("Retrieved compliance data from mock database", doc_id=doc_id)
        else:
            logger.warning("Compliance data not found in mock database", doc_id=doc_id)
        
        return data.copy() if data else None
    
    async def delete_compliance_data(self, doc_id: str) -> bool:
        """Delete specific compliance data by document ID."""
        with self._lock:
            if doc_id in self._compliance_data:
                del self._compliance_data[doc_id]
                logger.info("Deleted compliance data from mock database", doc_id=doc_id)
                return True
        
        logger.warning("Compliance data not found for deletion", doc_id=doc_id)
        return False
    
    async def reset_database(self) -> Dict[str, int]:
        """Reset all data in the mock database."""
        with self._lock:
            bucket_count = len(self._buckets)
            instance_count = len(self._instances)
            compliance_count = len(self._compliance_data)
            
            self._buckets.clear()
            self._instances.clear()
            self._compliance_data.clear()
            self._counter = 0
        
        logger.info("Mock database reset", 
                   buckets_deleted=bucket_count,
                   instances_deleted=instance_count,
                   compliance_data_deleted=compliance_count)
        
        return {
            "buckets_deleted": bucket_count,
            "instances_deleted": instance_count,
            "compliance_data_deleted": compliance_count
        }
