"""Compatibility shim: canonical implementation now lives in services.mongodb_service."""
from __future__ import annotations

from services.mongodb_service import (
    MemoryMongoDBService,
    MongoDBService,
    _as_object_id,
    _fresh_object_id_hex,
    get_mongodb_service,
)

__all__ = ['_as_object_id', '_fresh_object_id_hex', 'MongoDBService', 'MemoryMongoDBService', 'get_mongodb_service']
