#!/usr/bin/env python3
"""Test script for the dict-based database implementation."""

import asyncio
import os
from datetime import datetime

# Set environment to use dict database
os.environ["DATABASE_TYPE"] = "dict"

async def test_dict_database():
    """Test the dict-based database functionality."""
    print("🔍 Testing Dict Database Implementation")
    print("=" * 50)
    
    try:
        from app.database import get_database
        
        # Get database instance
        db = get_database()
        print(f"✅ Database initialized: {type(db).__name__}")
        
        # Test data
        test_data = {
            "project_id": "test-project-123",
            "folder_id": "folders/456789",
            "org_id": "organizations/987654321",
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
            "errors": []
        }
        
        # Test save
        print("\n💾 Testing save operation...")
        doc_id = await db.save_compliance_data(test_data)
        print(f"✅ Data saved with ID: {doc_id}")
        
        # Test retrieve
        print("\n📖 Testing retrieve operation...")
        retrieved_data = await db.get_compliance_data(doc_id)
        if retrieved_data:
            print(f"✅ Data retrieved successfully")
            print(f"   Project ID: {retrieved_data.get('project_id')}")
            print(f"   Folder ID: {retrieved_data.get('folder_id')}")
            print(f"   Org ID: {retrieved_data.get('org_id')}")
            print(f"   VM policies: {retrieved_data.get('data', {}).get('vm_instances', {}).get('count', 0)}")
            print(f"   Bucket policies: {retrieved_data.get('data', {}).get('buckets', {}).get('count', 0)}")
        else:
            print("❌ Failed to retrieve data")
            return False
        
        # Add more test data
        test_data2 = test_data.copy()
        test_data2["project_id"] = "another-project-456"
        test_data2["folder_id"] = "folders/123456"
        doc_id2 = await db.save_compliance_data(test_data2)
        print(f"✅ Second record saved with ID: {doc_id2}")
        
        # Test list all
        print("\n📋 Testing list all operation...")
        all_data = await db.list_compliance_data(limit=10)
        print(f"✅ Found {len(all_data)} total records")
        
        # Test list with project filter
        print("\n📋 Testing list with project filter...")
        project_data = await db.list_compliance_data(project_id="test-project-123", limit=10)
        print(f"✅ Found {len(project_data)} records for project test-project-123")
        
        # Test list with folder filter
        print("\n📋 Testing list with folder filter...")
        folder_data = await db.list_compliance_data(folder_id="folders/456789", limit=10)
        print(f"✅ Found {len(folder_data)} records for folder folders/456789")
        
        # Test list with org filter
        print("\n📋 Testing list with org filter...")
        org_data = await db.list_compliance_data(org_id="organizations/987654321", limit=10)
        print(f"✅ Found {len(org_data)} records for organization organizations/987654321")
        
        # Test delete
        print("\n🗑️ Testing delete operation...")
        success = await db.delete_compliance_data(doc_id)
        if success:
            print("✅ First record deleted successfully")
        else:
            print("❌ Failed to delete first record")
            return False
        
        # Verify deletion
        deleted_data = await db.get_compliance_data(doc_id)
        if deleted_data is None:
            print("✅ Confirmed: deleted record no longer exists")
        else:
            print("❌ Error: deleted record still exists")
            return False
        
        # Clean up second record
        await db.delete_compliance_data(doc_id2)
        
        print("\n🎉 All dict database tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_api_import():
    """Test that the API can be imported without errors."""
    print("\n🔍 Testing API Import")
    print("=" * 30)
    
    try:
        from app.main import app
        print("✅ API imported successfully")
        
        # Check that database is initialized
        from app.main import db
        print(f"✅ Database initialized in main: {type(db).__name__}")
        
        return True
        
    except Exception as e:
        print(f"❌ API import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run all tests."""
    print("🚀 Testing Updated Implementation (No External Dependencies)")
    print("=" * 70)
    
    # Test dict database
    db_success = await test_dict_database()
    
    # Test API import
    api_success = await test_api_import()
    
    print("\n" + "=" * 70)
    print("📊 TEST SUMMARY")
    print("=" * 70)
    
    if db_success and api_success:
        print("🎉 ALL TESTS PASSED!")
        print("\n✨ The implementation is working correctly with no external dependencies.")
        print("\n🚀 You can now start the API server:")
        print("   uvicorn app.main:app --reload")
        print("\n📖 Visit http://localhost:8000/docs for interactive API documentation")
    else:
        print("❌ Some tests failed. Please check the errors above.")

if __name__ == "__main__":
    asyncio.run(main())
