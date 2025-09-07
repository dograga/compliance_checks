"""Database service module with separate bucket and instance tables."""

import os
import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
import structlog
import threading

# Import Firestore
try:
    from google.cloud import firestore
    FIRESTORE_AVAILABLE = True
except ImportError:
    FIRESTORE_AVAILABLE = False

# Import TinyDB for local mock
try:
    from tinydb import TinyDB, Query
    TINYDB_AVAILABLE = True
except ImportError:
    TINYDB_AVAILABLE = False

logger = structlog.get_logger(__name__)


class DatabaseInterface(ABC):
    """Abstract interface for database operations."""
    
    @abstractmethod
    async def save_bucket_record(self, bucket_record: Dict[str, Any]) -> str:
        """Save bucket record and return document ID."""
        pass
    
    @abstractmethod
    async def save_instance_record(self, instance_record: Dict[str, Any]) -> str:
        """Save instance record and return document ID."""
        pass
    
    @abstractmethod
    async def get_buckets(self, folder_id: Optional[str] = None, org_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get bucket records with optional filters."""
        pass
    
    @abstractmethod
    async def get_instances(self, folder_id: Optional[str] = None, org_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get instance records with optional filters."""
        pass
    
    @abstractmethod
    async def delete_records_by_scope(self, parent_scope: str) -> bool:
        """Delete all records for a given parent scope."""
        pass


class FirestoreDatabase(DatabaseInterface):
    """Firestore implementation with separate collections."""
    
    def __init__(self):
        if not FIRESTORE_AVAILABLE:
            raise ImportError("google-cloud-firestore is not installed")
        
        self.db = firestore.Client()
        self.buckets_collection = "buckets"
        self.instances_collection = "instances"
        logger.info("Firestore database initialized")
    
    async def save_bucket_record(self, bucket_record: Dict[str, Any]) -> str:
        """Save bucket record to Firestore."""
        try:
            doc_ref = self.db.collection(self.buckets_collection).document()
            doc_ref.set(bucket_record)
            logger.info("Bucket record saved", doc_id=doc_ref.id, bucket=bucket_record.get('bucket_name'))
            return doc_ref.id
        except Exception as e:
            logger.error("Failed to save bucket record", error=str(e))
            raise
    
    async def save_instance_record(self, instance_record: Dict[str, Any]) -> str:
        """Save instance record to Firestore."""
        try:
            doc_ref = self.db.collection(self.instances_collection).document()
            doc_ref.set(instance_record)
            logger.info("Instance record saved", doc_id=doc_ref.id, instance=instance_record.get('instance_name'))
            return doc_ref.id
        except Exception as e:
            logger.error("Failed to save instance record", error=str(e))
            raise
    
    async def get_buckets(self, folder_id: Optional[str] = None, org_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get bucket records from Firestore."""
        try:
            query = self.db.collection(self.buckets_collection)
            
            # Apply parent scope filter
            parent_scope = folder_id or org_id
            if parent_scope:
                query = query.where("parent_scope", "==", parent_scope)
            
            query = query.order_by("timestamp", direction=firestore.Query.DESCENDING).limit(limit)
            docs = query.stream()
            
            results = []
            for doc in docs:
                data = doc.to_dict()
                data['id'] = doc.id
                results.append(data)
            
            return results
        except Exception as e:
            logger.error("Failed to get bucket records", error=str(e))
            raise
    
    async def get_instances(self, folder_id: Optional[str] = None, org_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get instance records from Firestore."""
        try:
            query = self.db.collection(self.instances_collection)
            
            # Apply parent scope filter
            parent_scope = folder_id or org_id
            if parent_scope:
                query = query.where("parent_scope", "==", parent_scope)
            
            query = query.order_by("timestamp", direction=firestore.Query.DESCENDING).limit(limit)
            docs = query.stream()
            
            results = []
            for doc in docs:
                data = doc.to_dict()
                data['id'] = doc.id
                results.append(data)
            
            return results
        except Exception as e:
            logger.error("Failed to get instance records", error=str(e))
            raise
    
    async def delete_records_by_scope(self, parent_scope: str) -> bool:
        """Delete all records for a given parent scope."""
        try:
            # Delete buckets
            bucket_query = self.db.collection(self.buckets_collection).where("parent_scope", "==", parent_scope)
            bucket_docs = bucket_query.stream()
            for doc in bucket_docs:
                doc.reference.delete()
            
            # Delete instances
            instance_query = self.db.collection(self.instances_collection).where("parent_scope", "==", parent_scope)
            instance_docs = instance_query.stream()
            for doc in instance_docs:
                doc.reference.delete()
            
            logger.info("Records deleted by scope", parent_scope=parent_scope)
            return True
        except Exception as e:
            logger.error("Failed to delete records by scope", parent_scope=parent_scope, error=str(e))
            return False


class DictDatabase(DatabaseInterface):
    """Python dict-based implementation with separate tables."""
    
    def __init__(self):
        self.buckets: Dict[str, Dict[str, Any]] = {}
        self.instances: Dict[str, Dict[str, Any]] = {}
        self.next_id = 1
        self.lock = threading.Lock()
        logger.info("Dict-based database initialized")
    
    async def save_bucket_record(self, bucket_record: Dict[str, Any]) -> str:
        """Save bucket record to dict."""
        try:
            with self.lock:
                doc_id = str(self.next_id)
                self.next_id += 1
                self.buckets[doc_id] = bucket_record.copy()
                logger.info("Bucket record saved", doc_id=doc_id, bucket=bucket_record.get('bucket_name'))
                return doc_id
        except Exception as e:
            logger.error("Failed to save bucket record", error=str(e))
            raise
    
    async def save_instance_record(self, instance_record: Dict[str, Any]) -> str:
        """Save instance record to dict."""
        try:
            with self.lock:
                doc_id = str(self.next_id)
                self.next_id += 1
                self.instances[doc_id] = instance_record.copy()
                logger.info("Instance record saved", doc_id=doc_id, instance=instance_record.get('instance_name'))
                return doc_id
        except Exception as e:
            logger.error("Failed to save instance record", error=str(e))
            raise
    
    async def get_buckets(self, folder_id: Optional[str] = None, org_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get bucket records from dict."""
        try:
            with self.lock:
                results = []
                parent_scope = folder_id or org_id
                
                for doc_id, data in self.buckets.items():
                    if parent_scope and data.get('parent_scope') != parent_scope:
                        continue
                    
                    result_data = data.copy()
                    result_data['id'] = doc_id
                    results.append(result_data)
                
                # Sort by timestamp (newest first)
                results.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
                return results[:limit]
        except Exception as e:
            logger.error("Failed to get bucket records", error=str(e))
            raise
    
    async def get_instances(self, folder_id: Optional[str] = None, org_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get instance records from dict."""
        try:
            with self.lock:
                results = []
                parent_scope = folder_id or org_id
                
                for doc_id, data in self.instances.items():
                    if parent_scope and data.get('parent_scope') != parent_scope:
                        continue
                    
                    result_data = data.copy()
                    result_data['id'] = doc_id
                    results.append(result_data)
                
                # Sort by timestamp (newest first)
                results.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
                return results[:limit]
        except Exception as e:
            logger.error("Failed to get instance records", error=str(e))
            raise
    
    async def delete_records_by_scope(self, parent_scope: str) -> bool:
        """Delete all records for a given parent scope."""
        try:
            with self.lock:
                # Delete buckets
                bucket_ids_to_delete = [doc_id for doc_id, data in self.buckets.items() 
                                      if data.get('parent_scope') == parent_scope]
                for doc_id in bucket_ids_to_delete:
                    del self.buckets[doc_id]
                
                # Delete instances
                instance_ids_to_delete = [doc_id for doc_id, data in self.instances.items() 
                                        if data.get('parent_scope') == parent_scope]
                for doc_id in instance_ids_to_delete:
                    del self.instances[doc_id]
                
                logger.info("Records deleted by scope", parent_scope=parent_scope, 
                          buckets_deleted=len(bucket_ids_to_delete), 
                          instances_deleted=len(instance_ids_to_delete))
                return True
        except Exception as e:
            logger.error("Failed to delete records by scope", parent_scope=parent_scope, error=str(e))
            return False


def get_database() -> DatabaseInterface:
    """Factory function to get the appropriate database implementation."""
    database_type = os.getenv("DATABASE_TYPE", "dict").lower()
    use_firestore = os.getenv("USE_FIRESTORE", "false").lower() == "true"
    
    if database_type == "firestore" or use_firestore:
        if FIRESTORE_AVAILABLE:
            try:
                return FirestoreDatabase()
            except Exception as e:
                logger.warning("Failed to initialize Firestore, falling back to dict database", error=str(e))
                return DictDatabase()
        else:
            logger.warning("Firestore requested but not available, using dict database")
            return DictDatabase()
    
    else:  # database_type == "dict" or any other value
        logger.info("Using dict-based database")
        return DictDatabase()
