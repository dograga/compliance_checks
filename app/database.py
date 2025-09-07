"""Database factory and interface for compliance checks application."""

import os
from typing import Any
import structlog

logger = structlog.get_logger(__name__)


def get_database() -> Any:
    """Factory function to get the appropriate database implementation."""
    database_type = os.getenv("DATABASE_TYPE", "dict").lower()
    
    if database_type == "firestore":
        from .firestore_db import FirestoreDatabase
        logger.info("Using Firestore database")
        return FirestoreDatabase()
    elif database_type == "dict":
        from .mock_db import MockDatabase
        logger.info("Using mock database")
        return MockDatabase()
    else:
        # Default to mock database
        from .mock_db import MockDatabase
        logger.warning("Unknown database type, defaulting to mock database", 
                      database_type=database_type)
        return MockDatabase()
