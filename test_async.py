#!/usr/bin/env python3
"""
Test script to verify async performance improvements.
This script simulates multiple concurrent API requests to test the async behavior.
"""

import asyncio
import time
import logging
from app.helper import fetch_iam_policies_for_project

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_concurrent_requests():
    """Test multiple concurrent API requests."""
    # Mock project IDs for testing
    test_projects = ["test-project-1", "test-project-2", "test-project-3"]
    
    logger.info("Testing concurrent API requests...")
    start_time = time.time()
    
    # Create tasks for concurrent execution
    tasks = [
        fetch_iam_policies_for_project(project_id, zones=["asia-southeast1-a"])
        for project_id in test_projects
    ]
    
    try:
        # Execute all requests concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info(f"Completed {len(test_projects)} concurrent requests in {total_time:.2f} seconds")
        
        # Log results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Project {test_projects[i]} failed: {result}")
            else:
                logger.info(f"Project {test_projects[i]}: {result.total_policies} policies retrieved")
                
    except Exception as e:
        logger.error(f"Test failed: {e}")

async def test_single_project_zones():
    """Test concurrent zone processing for a single project."""
    logger.info("Testing concurrent zone processing...")
    start_time = time.time()
    
    try:
        # Test with multiple zones
        result = await fetch_iam_policies_for_project(
            "test-project", 
            zones=["asia-southeast1-a", "asia-southeast1-b", "asia-southeast1-c"]
        )
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info(f"Processed 3 zones concurrently in {total_time:.2f} seconds")
        logger.info(f"Retrieved {result.total_policies} policies, {len(result.errors)} errors")
        
    except Exception as e:
        logger.error(f"Zone test failed: {e}")

if __name__ == "__main__":
    print("🚀 Testing Async Performance Improvements")
    print("=" * 50)
    
    # Run tests
    asyncio.run(test_concurrent_requests())
    print()
    asyncio.run(test_single_project_zones())
    
    print("\n✅ Async tests completed!")
