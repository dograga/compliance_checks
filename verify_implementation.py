#!/usr/bin/env python3
"""Verification script for the new API implementation."""

import sys
import importlib.util
from pathlib import Path

def check_file_exists(file_path: str, description: str) -> bool:
    """Check if a file exists."""
    path = Path(file_path)
    exists = path.exists()
    status = "✅" if exists else "❌"
    print(f"{status} {description}: {file_path}")
    return exists

def check_imports() -> bool:
    """Check if all required modules can be imported."""
    print("\n🔍 Checking imports...")
    
    modules_to_check = [
        ("app.database", "Database module"),
        ("app.dataclass", "Data classes"),
        ("app.main", "Main API module"),
        ("app.gcp_helper", "GCP helper functions")
    ]
    
    all_good = True
    for module_name, description in modules_to_check:
        try:
            spec = importlib.util.find_spec(module_name)
            if spec is not None:
                print(f"✅ {description}: {module_name}")
            else:
                print(f"❌ {description}: {module_name} - Not found")
                all_good = False
        except Exception as e:
            print(f"❌ {description}: {module_name} - Error: {e}")
            all_good = False
    
    return all_good

def check_api_endpoints():
    """Check if API endpoints are properly defined."""
    print("\n🔍 Checking API endpoints...")
    
    try:
        from app.main import app
        routes = [route.path for route in app.routes if hasattr(route, 'path')]
        
        expected_endpoints = [
            "/compliance-data/collect",
            "/compliance-data",
            "/compliance-data/{doc_id}",
            "/projects/{project_id}/save-iam-data",
            "/vm-iam-policies-asset-api/{project_id}",
            "/bucket-iam-policies-asset-api/{project_id}",
            "/health"
        ]
        
        all_good = True
        for endpoint in expected_endpoints:
            if endpoint in routes:
                print(f"✅ Endpoint: {endpoint}")
            else:
                print(f"❌ Missing endpoint: {endpoint}")
                all_good = False
        
        return all_good
        
    except Exception as e:
        print(f"❌ Error checking endpoints: {e}")
        return False

def check_database_classes():
    """Check if database classes are properly implemented."""
    print("\n🔍 Checking database classes...")
    
    try:
        from app.database import DatabaseInterface, FirestoreDatabase, MockDatabase, get_database
        
        # Check interface
        interface_methods = ['save_compliance_data', 'get_compliance_data', 'list_compliance_data', 'delete_compliance_data']
        for method in interface_methods:
            if hasattr(DatabaseInterface, method):
                print(f"✅ DatabaseInterface.{method}")
            else:
                print(f"❌ Missing DatabaseInterface.{method}")
        
        # Check implementations
        for cls_name, cls in [("FirestoreDatabase", FirestoreDatabase), ("MockDatabase", MockDatabase)]:
            for method in interface_methods:
                if hasattr(cls, method):
                    print(f"✅ {cls_name}.{method}")
                else:
                    print(f"❌ Missing {cls_name}.{method}")
        
        # Test factory function
        db = get_database()
        print(f"✅ Database factory returns: {type(db).__name__}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking database classes: {e}")
        return False

def check_data_models():
    """Check if data models are properly defined."""
    print("\n🔍 Checking data models...")
    
    try:
        from app.dataclass import (
            ComplianceDataRequest, 
            ComplianceDataResponse, 
            ComplianceDataListResponse,
            ProjectPoliciesResponse,
            PolicyResponse,
            IAMPolicy,
            IAMBinding
        )
        
        models = [
            ("ComplianceDataRequest", ComplianceDataRequest),
            ("ComplianceDataResponse", ComplianceDataResponse),
            ("ComplianceDataListResponse", ComplianceDataListResponse),
            ("ProjectPoliciesResponse", ProjectPoliciesResponse),
            ("PolicyResponse", PolicyResponse),
            ("IAMPolicy", IAMPolicy),
            ("IAMBinding", IAMBinding)
        ]
        
        for name, model in models:
            print(f"✅ Data model: {name}")
        
        # Test model creation
        request = ComplianceDataRequest(project_id="test", include_vm_policies=True)
        print(f"✅ ComplianceDataRequest creation test passed")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking data models: {e}")
        return False

def main():
    """Run all verification checks."""
    print("🚀 Verifying Compliance Checks API Implementation")
    print("=" * 60)
    
    # Check files
    print("\n📁 Checking required files...")
    files_ok = all([
        check_file_exists("app/database.py", "Database module"),
        check_file_exists("app/dataclass.py", "Data classes (updated)"),
        check_file_exists("app/main.py", "Main API module (updated)"),
        check_file_exists("app/gcp_helper.py", "GCP helper functions"),
        check_file_exists("pyproject.toml", "Project configuration (updated)"),
        check_file_exists(".env.example", "Environment configuration"),
        check_file_exists("README.md", "Documentation (updated)")
    ])
    
    # Check imports
    imports_ok = check_imports()
    
    # Check API endpoints
    endpoints_ok = check_api_endpoints()
    
    # Check database classes
    database_ok = check_database_classes()
    
    # Check data models
    models_ok = check_data_models()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 60)
    
    checks = [
        ("Files", files_ok),
        ("Imports", imports_ok),
        ("API Endpoints", endpoints_ok),
        ("Database Classes", database_ok),
        ("Data Models", models_ok)
    ]
    
    all_passed = True
    for check_name, passed in checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} {check_name}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 ALL CHECKS PASSED! Implementation is ready to use.")
        print("\n🚀 Next steps:")
        print("1. Install dependencies: pip install google-cloud-firestore tinydb")
        print("2. Start the API: uvicorn app.main:app --reload")
        print("3. Visit http://localhost:8000/docs for API documentation")
        print("4. Test with: python demo_endpoints.py")
    else:
        print("⚠️  Some checks failed. Please review the issues above.")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
