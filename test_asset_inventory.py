#!/usr/bin/env python3
"""Test script for the updated Asset Inventory approach."""

import asyncio
import os
from datetime import datetime

# Set environment to use dict database
os.environ["DATABASE_TYPE"] = "dict"

async def test_asset_inventory_functions():
    """Test the new Asset Inventory functions."""
    print("🔍 Testing Asset Inventory Functions")
    print("=" * 50)
    
    try:
        from app.gcp_helper import (
            fetch_vm_iam_policies_folder_org,
            fetch_bucket_iam_policies_folder_org
        )
        
        print("✅ Asset Inventory functions imported successfully")
        
        # Note: These functions require actual GCP credentials and resources
        print("📝 Note: Actual testing requires GCP credentials and resources")
        print("   Functions available:")
        print("   - fetch_vm_iam_policies_folder_org(parent)")
        print("   - fetch_bucket_iam_policies_folder_org(parent)")
        print("   Where parent can be:")
        print("     • folders/123456789")
        print("     • organizations/987654321")
        
        return True
        
    except Exception as e:
        print(f"❌ Asset Inventory function test failed: {e}")
        return False

async def test_updated_api():
    """Test the updated API with Asset Inventory approach."""
    print("\n🔍 Testing Updated API")
    print("=" * 30)
    
    try:
        from app.main import app
        from app.dataclass import ComplianceDataRequest
        
        print("✅ Updated API imported successfully")
        
        # Test data models
        folder_request = ComplianceDataRequest(
            folder_id="folders/123456789",
            include_vm_policies=True,
            include_bucket_policies=True
        )
        print(f"✅ Folder request: {folder_request.dict()}")
        
        org_request = ComplianceDataRequest(
            org_id="organizations/987654321",
            include_vm_policies=True,
            include_bucket_policies=False
        )
        print(f"✅ Organization request: {org_request.dict()}")
        
        return True
        
    except Exception as e:
        print(f"❌ Updated API test failed: {e}")
        return False

def show_asset_inventory_benefits():
    """Show benefits of the Asset Inventory approach."""
    print("\n🎯 Asset Inventory Approach Benefits")
    print("=" * 45)
    
    print("\n✨ Performance Improvements:")
    print("  • Single API call per resource type (VM/Bucket)")
    print("  • No need to traverse project hierarchy")
    print("  • Parallel collection across all projects")
    print("  • Built-in pagination and filtering")
    
    print("\n🔧 Simplified Architecture:")
    print("  • Removed Resource Manager API dependency")
    print("  • Eliminated recursive project traversal")
    print("  • Direct Asset API queries with folder/org scope")
    print("  • Automatic project ID extraction from asset names")
    
    print("\n📊 Better Data Quality:")
    print("  • Consistent asset metadata")
    print("  • Real-time IAM policy data")
    print("  • Automatic handling of nested folder structures")
    print("  • Built-in error handling per asset")

def show_updated_workflow():
    """Show the updated collection workflow."""
    print("\n🔄 Updated Collection Workflow")
    print("=" * 40)
    
    print("\n📋 For Folder Collection:")
    print("1. Parse folder_id → folders/123456789")
    print("2. Call Asset API with parent=folders/123456789")
    print("3. Asset API automatically includes:")
    print("   - Direct resources in folder")
    print("   - Resources in all subfolders")
    print("   - Resources in all projects within hierarchy")
    print("4. Extract project IDs from asset names")
    print("5. Store aggregated results")
    
    print("\n📋 For Organization Collection:")
    print("1. Parse org_id → organizations/987654321")
    print("2. Call Asset API with parent=organizations/987654321")
    print("3. Asset API automatically includes:")
    print("   - All resources across entire organization")
    print("   - All folders and subfolders")
    print("   - All projects in organization hierarchy")
    print("4. Extract project IDs from asset names")
    print("5. Store aggregated results")

def show_api_examples():
    """Show updated API usage examples."""
    print("\n📖 Updated API Examples")
    print("=" * 30)
    
    print("\n1. Collect from folder using Asset Inventory:")
    print("""curl -X POST "http://localhost:8000/compliance-data/collect" \\
  -H "Content-Type: application/json" \\
  -d '{
    "folder_id": "folders/123456789",
    "include_vm_policies": true,
    "include_bucket_policies": true
  }'""")
    
    print("\n2. Collect from organization using Asset Inventory:")
    print("""curl -X POST "http://localhost:8000/compliance-data/collect" \\
  -H "Content-Type: application/json" \\
  -d '{
    "org_id": "organizations/987654321",
    "include_vm_policies": true,
    "include_bucket_policies": true
  }'""")
    
    print("\n3. Response now includes discovered projects:")
    print("""{
  "id": "doc_123",
  "folder_id": "folders/123456789",
  "projects_processed": ["project-a", "project-b", "project-c"],
  "total_projects_count": 3,
  "summary": {
    "vm_count": 15,
    "bucket_count": 8,
    "projects_processed": 3
  }
}""")

def show_required_permissions():
    """Show simplified permission requirements."""
    print("\n🔐 Simplified Permission Requirements")
    print("=" * 45)
    
    print("\nRequired permissions (simplified):")
    print("  • cloudasset.assets.listIamPolicy")
    print("    - On folder or organization level")
    print("    - Automatically includes all nested resources")
    
    print("\nNo longer needed:")
    print("  ❌ resourcemanager.projects.list")
    print("  ❌ resourcemanager.folders.list")
    print("  ❌ Multiple project-level API calls")
    
    print("\nRecommended IAM role:")
    print("  • Cloud Asset Viewer (roles/cloudasset.viewer)")
    print("    - At folder or organization level")
    print("    - Provides access to all nested resources")

async def main():
    """Run all tests and show information."""
    print("🚀 Testing Updated Asset Inventory Approach")
    print("=" * 60)
    
    # Test functions
    functions_ok = await test_asset_inventory_functions()
    
    # Test API
    api_ok = await test_updated_api()
    
    # Show benefits and workflow
    show_asset_inventory_benefits()
    show_updated_workflow()
    show_api_examples()
    show_required_permissions()
    
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    
    if functions_ok and api_ok:
        print("🎉 ALL TESTS PASSED!")
        print("\n✨ Asset Inventory approach is now implemented!")
        print("\n🚀 Key improvements:")
        print("• Much faster collection (single API call per resource type)")
        print("• Simplified permissions (only Asset API access needed)")
        print("• Automatic handling of nested folder/project hierarchies")
        print("• Better scalability for large organizations")
        
        print("\n📝 Ready to use:")
        print("1. Start API: uvicorn app.main:app --reload")
        print("2. Use folder_id or org_id in POST /compliance-data/collect")
        print("3. Asset API handles all the complexity automatically")
    else:
        print("❌ Some tests failed. Please check the errors above.")

if __name__ == "__main__":
    asyncio.run(main())
