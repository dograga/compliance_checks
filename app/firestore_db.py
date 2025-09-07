"""Firestore database implementation for production use."""

import os
from datetime import datetime
from typing import List, Dict, Any, Optional
import structlog

# Import Firestore
try:
    from google.cloud import firestore
    FIRESTORE_AVAILABLE = True
except ImportError:
    FIRESTORE_AVAILABLE = False

logger = structlog.get_logger(__name__)


class FirestoreDatabase:
    """Firestore database implementation for production use."""
    
    def __init__(self):
        if not FIRESTORE_AVAILABLE:
            raise ImportError("google-cloud-firestore is required for Firestore database")
        
        # Get database name from environment variable, default to project default
        database_name = os.getenv("FIRESTORE_DATABASE_NAME")
        if database_name:
            self.db = firestore.Client(database=database_name)
            logger.info("Using custom Firestore database", database_name=database_name)
        else:
            self.db = firestore.Client()
            logger.info("Using default Firestore database")
            
        self.buckets_collection = "buckets"
        self.instances_collection = "instances"
        self.compliance_collection = "compliance_data"
    
    async def save_bucket_record(self, bucket_record: Dict[str, Any]) -> str:
        """Save bucket record with upsert using bucket_name as doc_id."""
        bucket_record["timestamp"] = datetime.utcnow().isoformat()
        
        # Use bucket_name as document ID for upsert functionality
        bucket_name = bucket_record.get("bucket_name")
        if not bucket_name:
            raise ValueError("bucket_name is required for document ID")
            
        doc_ref = self.db.collection(self.buckets_collection).document(bucket_name)
        doc_ref.set(bucket_record)
        
        logger.info("Bucket record saved to Firestore", doc_id=bucket_name,
                   bucket_name=bucket_name)
        return bucket_name
    
    async def save_instance_record(self, instance_record: Dict[str, Any]) -> str:
        """Save instance record with upsert using project_number-zone-vmname as doc_id."""
        instance_record["timestamp"] = datetime.utcnow().isoformat()
        
        # Use project_number-zone-instance_name as document ID for upsert functionality
        project_number = instance_record.get("project_number")
        instance_name = instance_record.get("instance_name")
        zone = instance_record.get("zone")
        
        if not project_number or not instance_name:
            raise ValueError("project_number and instance_name are required for document ID")
            
        # Include zone in doc_id if available, otherwise use "unknown"
        zone_part = zone if zone and zone != "unknown" else "unknown"
        doc_id = f"{project_number}-{zone_part}-{instance_name}"
        doc_ref = self.db.collection(self.instances_collection).document(doc_id)
        doc_ref.set(instance_record)
        
        logger.info("Instance record saved to Firestore", doc_id=doc_id,
                   instance_name=instance_name, project_number=project_number, zone=zone_part)
        return doc_id
    
    async def get_buckets(self, folder_id: Optional[str] = None, org_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get bucket records with optional filters."""
        query = self.db.collection(self.buckets_collection)
        
        # Apply filters
        if folder_id:
            query = query.where("parent_scope", "==", f"folders/{folder_id}")
        if org_id:
            query = query.where("parent_scope", "==", f"organizations/{org_id}")
        
        # Apply limit and execute query
        query = query.limit(limit)
        docs = query.stream()
        
        records = []
        for doc in docs:
            record = doc.to_dict()
            record["id"] = doc.id
            records.append(record)
        
        logger.info("Retrieved bucket records from Firestore", count=len(records))
        return records
    
    async def get_instances(self, folder_id: Optional[str] = None, org_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get instance records with optional filters."""
        query = self.db.collection(self.instances_collection)
        
        # Apply filters
        if folder_id:
            query = query.where("parent_scope", "==", f"folders/{folder_id}")
        if org_id:
            query = query.where("parent_scope", "==", f"organizations/{org_id}")
        
        # Apply limit and execute query
        query = query.limit(limit)
        docs = query.stream()
        
        records = []
        for doc in docs:
            record = doc.to_dict()
            record["id"] = doc.id
            records.append(record)
        
        logger.info("Retrieved instance records from Firestore", count=len(records))
        return records
    
    async def delete_records_by_scope(self, parent_scope: str) -> bool:
        """Delete all records for a given parent scope."""
        deleted_buckets = 0
        deleted_instances = 0
        
        # Delete buckets
        bucket_query = self.db.collection(self.buckets_collection).where("parent_scope", "==", parent_scope)
        bucket_docs = bucket_query.stream()
        for doc in bucket_docs:
            doc.reference.delete()
            deleted_buckets += 1
        
        # Delete instances
        instance_query = self.db.collection(self.instances_collection).where("parent_scope", "==", parent_scope)
        instance_docs = instance_query.stream()
        for doc in instance_docs:
            doc.reference.delete()
            deleted_instances += 1
        
        logger.info("Deleted records by scope from Firestore", parent_scope=parent_scope,
                   buckets_deleted=deleted_buckets, instances_deleted=deleted_instances)
        return True
    
    # Legacy compliance data methods for backward compatibility
    async def save_compliance_data(self, data: Dict[str, Any]) -> str:
        """Save compliance data and return document ID."""
        data["timestamp"] = datetime.utcnow().isoformat()
        
        doc_ref = self.db.collection(self.compliance_collection).document()
        doc_ref.set(data)
        
        logger.info("Compliance data saved to Firestore", doc_id=doc_ref.id)
        return doc_ref.id
    
    async def list_compliance_data(self, project_id: Optional[str] = None, 
                                 folder_id: Optional[str] = None, 
                                 org_id: Optional[str] = None, 
                                 limit: int = 100) -> List[Dict[str, Any]]:
        """List compliance data with optional filters."""
        query = self.db.collection(self.compliance_collection)
        
        # Apply filters
        if project_id:
            query = query.where("project_id", "==", project_id)
        if folder_id:
            query = query.where("folder_id", "==", folder_id)
        if org_id:
            query = query.where("org_id", "==", org_id)
        
        # Apply limit and execute query
        query = query.limit(limit)
        docs = query.stream()
        
        records = []
        for doc in docs:
            record = doc.to_dict()
            record["id"] = doc.id
            records.append(record)
        
        logger.info("Listed compliance data from Firestore", count=len(records))
        return records
    
    async def get_compliance_data(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Get specific compliance data by document ID."""
        doc_ref = self.db.collection(self.compliance_collection).document(doc_id)
        doc = doc_ref.get()
        
        if doc.exists:
            data = doc.to_dict()
            data["id"] = doc.id
            logger.info("Retrieved compliance data from Firestore", doc_id=doc_id)
            return data
        else:
            logger.warning("Compliance data not found in Firestore", doc_id=doc_id)
            return None
    
    async def delete_compliance_data(self, doc_id: str) -> bool:
        """Delete specific compliance data by document ID."""
        doc_ref = self.db.collection(self.compliance_collection).document(doc_id)
        doc = doc_ref.get()
        
        if doc.exists:
            doc_ref.delete()
            logger.info("Deleted compliance data from Firestore", doc_id=doc_id)
            return True
        else:
            logger.warning("Compliance data not found for deletion", doc_id=doc_id)
            return False
