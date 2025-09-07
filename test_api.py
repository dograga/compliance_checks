#!/usr/bin/env python3
"""Test script for the Compliance Checks API."""

import asyncio
import json
from datetime import datetime
from app.database import get_database
from app.dataclass import ComplianceDataRequest

async def test_database():
    """Test the database functionality."""
    print("🔍 Testing database functionality...")
    
    # Get database instance
    db = get_database()
    print(f"✅ Database initialized: {type(db).__name__}")
    
    # Test data
    test_data = {
        "project_id": "test-project-123",
        "timestamp": datetime.now().isoformat(),
        "data": {
            "vm_instances": {
                "policies": [
                    {
                        "project_id": "test-project-123",
                        "resource_name": "//compute.googleapis.com/projects/test-project-123/zones/us-central1-a/instances/test-vm",
                        "asset_type": "compute.googleapis.com/Instance",
                        "policy": {
                            "bindings": [
                                {
                                    "role": "roles/compute.instanceAdmin",
                                    "members": ["user:test@example.com"]
                                }
                            ]
                        }
                    }
                ],
                "count": 1
            },
            "buckets": {
                "policies": [
                    {
                        "project_id": "test-project-123",
                        "resource_name": "//storage.googleapis.com/test-bucket",
                        "asset_type": "storage.googleapis.com/Bucket",
                        "policy": {
                            "bindings": [
                                {
                                    "role": "roles/storage.objectViewer",
                                    "members": ["user:test@example.com"]
                                }
                            ]
                        }
                    }
                ],
                "count": 1
            }
        },
        "errors": [],
        "collection_config": {
            "include_vm_policies": True,
            "include_bucket_policies": True
        }
    }
    
    # Test save
    print("💾 Testing save operation...")
    doc_id = await db.save_compliance_data(test_data)
    print(f"✅ Data saved with ID: {doc_id}")
    
    # Test retrieve
    print("📖 Testing retrieve operation...")
    retrieved_data = await db.get_compliance_data(doc_id)
    if retrieved_data:
        print(f"✅ Data retrieved successfully")
        print(f"   Project ID: {retrieved_data.get('project_id')}")
        print(f"   VM policies: {retrieved_data.get('data', {}).get('vm_instances', {}).get('count', 0)}")
        print(f"   Bucket policies: {retrieved_data.get('data', {}).get('buckets', {}).get('count', 0)}")
    else:
        print("❌ Failed to retrieve data")
    
    # Test list
    print("📋 Testing list operation...")
    data_list = await db.list_compliance_data(project_id="test-project-123", limit=10)
    print(f"✅ Found {len(data_list)} records for project test-project-123")
    
    # Test list all
    print("📋 Testing list all operation...")
    all_data = await db.list_compliance_data(limit=10)
    print(f"✅ Found {len(all_data)} total records")
    
    # Test delete
    print("🗑️ Testing delete operation...")
    success = await db.delete_compliance_data(doc_id)
    if success:
        print("✅ Data deleted successfully")
    else:
        print("❌ Failed to delete data")
    
    print("🎉 Database tests completed!")

def test_data_models():
    """Test the data models."""
    print("\n🔍 Testing data models...")
    
    # Test ComplianceDataRequest
    request = ComplianceDataRequest(
        project_id="test-project-123",
        include_vm_policies=True,
        include_bucket_policies=True
    )
    print(f"✅ ComplianceDataRequest created: {request.dict()}")
    
    # Test with folder_id
    folder_request = ComplianceDataRequest(
        folder_id="folders/123456789",
        include_vm_policies=False,
        include_bucket_policies=True
    )
    print(f"✅ Folder request created: {folder_request.dict()}")
    
    # Test with org_id
    org_request = ComplianceDataRequest(
        org_id="organizations/987654321",
        include_vm_policies=True,
        include_bucket_policies=False
    )
    print(f"✅ Organization request created: {org_request.dict()}")
    
    print("🎉 Data model tests completed!")

async def main():
    """Run all tests."""
    print("🚀 Starting Compliance Checks API Tests\n")
    
    # Test data models
    test_data_models()
    
    # Test database
    await test_database()
    
    print("\n✨ All tests completed successfully!")
    print("\n📝 Next steps:")
    print("1. Start the API server: uvicorn app.main:app --reload")
    print("2. Visit http://localhost:8000/docs for interactive API documentation")
    print("3. Test the new endpoints:")
    print("   - POST /compliance-data/collect")
    print("   - GET /compliance-data")
    print("   - GET /compliance-data/{doc_id}")

if __name__ == "__main__":
    asyncio.run(main())
