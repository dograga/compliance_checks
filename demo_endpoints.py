#!/usr/bin/env python3
"""Demo script showing the new API endpoints functionality."""

import asyncio
import json
from datetime import datetime
from app.database import get_database

async def demo_database_operations():
    """Demonstrate database operations with sample data."""
    print("🎯 Compliance Checks API - Database Demo")
    print("=" * 50)
    
    # Initialize database
    db = get_database()
    print(f"📊 Using database: {type(db).__name__}")
    
    # Sample compliance data for different scenarios
    sample_data = [
        {
            "project_id": "web-app-prod-001",
            "folder_id": "folders/123456789",
            "org_id": "organizations/987654321",
            "timestamp": datetime.now().isoformat(),
            "data": {
                "vm_instances": {
                    "policies": [
                        {
                            "project_id": "web-app-prod-001",
                            "resource_name": "//compute.googleapis.com/projects/web-app-prod-001/zones/us-central1-a/instances/web-server-1",
                            "asset_type": "compute.googleapis.com/Instance",
                            "policy": {
                                "bindings": [
                                    {
                                        "role": "roles/compute.instanceAdmin",
                                        "members": ["user:admin@company.com", "serviceAccount:web-app@web-app-prod-001.iam.gserviceaccount.com"]
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
                            "project_id": "web-app-prod-001",
                            "resource_name": "//storage.googleapis.com/web-app-static-assets",
                            "asset_type": "storage.googleapis.com/Bucket",
                            "policy": {
                                "bindings": [
                                    {
                                        "role": "roles/storage.objectViewer",
                                        "members": ["allUsers"]
                                    }
                                ]
                            }
                        }
                    ],
                    "count": 1
                }
            },
            "errors": []
        },
        {
            "project_id": "analytics-dev-002",
            "folder_id": "folders/123456789",
            "org_id": "organizations/987654321",
            "timestamp": datetime.now().isoformat(),
            "data": {
                "vm_instances": {
                    "policies": [],
                    "count": 0
                },
                "buckets": {
                    "policies": [
                        {
                            "project_id": "analytics-dev-002",
                            "resource_name": "//storage.googleapis.com/analytics-data-lake",
                            "asset_type": "storage.googleapis.com/Bucket",
                            "policy": {
                                "bindings": [
                                    {
                                        "role": "roles/storage.admin",
                                        "members": ["user:data-engineer@company.com"]
                                    }
                                ]
                            }
                        }
                    ],
                    "count": 1
                }
            },
            "errors": []
        },
        {
            "project_id": "ml-training-003",
            "folder_id": "folders/987654321",
            "org_id": "organizations/987654321",
            "timestamp": datetime.now().isoformat(),
            "data": {
                "vm_instances": {
                    "policies": [
                        {
                            "project_id": "ml-training-003",
                            "resource_name": "//compute.googleapis.com/projects/ml-training-003/zones/us-west1-b/instances/gpu-trainer-1",
                            "asset_type": "compute.googleapis.com/Instance",
                            "policy": {
                                "bindings": [
                                    {
                                        "role": "roles/compute.instanceAdmin",
                                        "members": ["user:ml-engineer@company.com"]
                                    }
                                ]
                            }
                        }
                    ],
                    "count": 1
                },
                "buckets": {
                    "policies": [],
                    "count": 0
                }
            },
            "errors": []
        }
    ]
    
    # Store sample data
    doc_ids = []
    print("\n💾 Storing sample compliance data...")
    for i, data in enumerate(sample_data, 1):
        doc_id = await db.save_compliance_data(data)
        doc_ids.append(doc_id)
        print(f"   {i}. Stored project '{data['project_id']}' with ID: {doc_id}")
    
    # Demonstrate querying capabilities
    print(f"\n📋 Querying stored data...")
    
    # List all data
    all_data = await db.list_compliance_data(limit=10)
    print(f"   • Total records: {len(all_data)}")
    
    # Filter by project
    project_data = await db.list_compliance_data(project_id="web-app-prod-001")
    print(f"   • Records for 'web-app-prod-001': {len(project_data)}")
    
    # Filter by folder
    folder_data = await db.list_compliance_data(folder_id="folders/123456789")
    print(f"   • Records for folder '123456789': {len(folder_data)}")
    
    # Filter by organization
    org_data = await db.list_compliance_data(org_id="organizations/987654321")
    print(f"   • Records for organization '987654321': {len(org_data)}")
    
    # Demonstrate individual record retrieval
    print(f"\n🔍 Retrieving individual records...")
    for i, doc_id in enumerate(doc_ids[:2], 1):  # Show first 2 records
        record = await db.get_compliance_data(doc_id)
        if record:
            vm_count = record.get('data', {}).get('vm_instances', {}).get('count', 0)
            bucket_count = record.get('data', {}).get('buckets', {}).get('count', 0)
            print(f"   {i}. {record['project_id']}: {vm_count} VMs, {bucket_count} buckets")
    
    print(f"\n✨ Demo completed! Database now contains sample compliance data.")
    print(f"\n🚀 API Endpoints you can now test:")
    print(f"   • GET  /compliance-data - List all stored data")
    print(f"   • GET  /compliance-data?project_id=web-app-prod-001 - Filter by project")
    print(f"   • GET  /compliance-data?folder_id=folders/123456789 - Filter by folder")
    print(f"   • GET  /compliance-data/{doc_ids[0]} - Get specific record")
    print(f"   • POST /compliance-data/collect - Collect new compliance data")
    
    return doc_ids

def show_api_examples():
    """Show example API calls."""
    print(f"\n📖 Example API Usage:")
    print(f"=" * 30)
    
    print(f"\n1. Collect compliance data:")
    print(f"""curl -X POST "http://localhost:8000/compliance-data/collect" \\
  -H "Content-Type: application/json" \\
  -d '{{
    "project_id": "your-project-id",
    "include_vm_policies": true,
    "include_bucket_policies": true
  }}'""")
    
    print(f"\n2. List all compliance data:")
    print(f'curl "http://localhost:8000/compliance-data"')
    
    print(f"\n3. Filter by project:")
    print(f'curl "http://localhost:8000/compliance-data?project_id=web-app-prod-001"')
    
    print(f"\n4. Filter by folder:")
    print(f'curl "http://localhost:8000/compliance-data?folder_id=folders/123456789"')
    
    print(f"\n5. Get specific record:")
    print(f'curl "http://localhost:8000/compliance-data/{{doc_id}}"')

async def main():
    """Run the demo."""
    try:
        doc_ids = await demo_database_operations()
        show_api_examples()
        
        print(f"\n🎯 Next Steps:")
        print(f"1. Start the API server: uvicorn app.main:app --reload")
        print(f"2. Visit http://localhost:8000/docs for interactive API documentation")
        print(f"3. Test the endpoints with the examples above")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        print(f"Make sure you have the required dependencies installed:")
        print(f"   pip install google-cloud-firestore tinydb")

if __name__ == "__main__":
    asyncio.run(main())
