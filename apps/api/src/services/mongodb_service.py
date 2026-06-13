"""
DataTransfer.space — MongoDB Service
Handles all MongoDB operations for persistence and data transfer
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from typing import Optional, Any
from datetime import datetime
import json


class MongoDBService:
    """MongoDB service for DataTransfer platform"""
    
    def __init__(self, connection_string: str = "mongodb://localhost:27017/"):
        self.connection_string = connection_string
        self.client: Optional[MongoClient] = None
        self.db_name = "datatransfer"
    
    def connect(self) -> bool:
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(self.connection_string, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            return True
        except ConnectionFailure as e:
            print(f"[ERROR] MongoDB connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.client = None
    
    def get_database(self, db_name: Optional[str] = None):
        """Get database instance"""
        if not self.client:
            self.connect()
        return self.client[db_name or self.db_name]
    
    def test_connection(self) -> dict:
        """Test connection and return server info"""
        try:
            if not self.client:
                self.connect()
            info = self.client.server_info()
            return {
                "connected": True,
                "version": info.get("version"),
                "host": self.connection_string,
            }
        except Exception as e:
            return {
                "connected": False,
                "error": str(e),
                "host": self.connection_string,
            }
    
    # ═══════════════════════════════════════════════════════════════════════
    # CONNECTOR CONFIGURATION STORAGE
    # ═══════════════════════════════════════════════════════════════════════
    
    def save_connector(self, connector_data: dict) -> str:
        """Save a connector configuration"""
        db = self.get_database()
        collection = db["connectors"]
        
        connector_data["created_at"] = datetime.utcnow()
        connector_data["updated_at"] = datetime.utcnow()
        
        result = collection.insert_one(connector_data)
        return str(result.inserted_id)
    
    def get_connector(self, connector_id: str) -> Optional[dict]:
        """Get a connector by ID"""
        from bson import ObjectId
        db = self.get_database()
        collection = db["connectors"]
        
        result = collection.find_one({"_id": ObjectId(connector_id)})
        if result:
            result["_id"] = str(result["_id"])
        return result
    
    def list_connectors(self) -> list[dict]:
        """List all saved connectors"""
        db = self.get_database()
        collection = db["connectors"]
        
        connectors = []
        for doc in collection.find().sort("created_at", -1):
            doc["_id"] = str(doc["_id"])
            connectors.append(doc)
        return connectors
    
    def update_connector(self, connector_id: str, updates: dict) -> bool:
        """Update a connector configuration"""
        from bson import ObjectId
        db = self.get_database()
        collection = db["connectors"]
        
        updates["updated_at"] = datetime.utcnow()
        result = collection.update_one(
            {"_id": ObjectId(connector_id)},
            {"$set": updates}
        )
        return result.modified_count > 0
    
    def delete_connector(self, connector_id: str) -> bool:
        """Delete a connector"""
        from bson import ObjectId
        db = self.get_database()
        collection = db["connectors"]
        
        result = collection.delete_one({"_id": ObjectId(connector_id)})
        return result.deleted_count > 0
    
    # ═══════════════════════════════════════════════════════════════════════
    # DATA TRANSFER OPERATIONS
    # ═══════════════════════════════════════════════════════════════════════
    
    def insert_data(self, database: str, collection: str, data: list[dict], client: Optional["MongoClient"] = None) -> dict:
        """Insert data into a MongoDB collection"""
        try:
            db_client = client or self.client
            if not db_client:
                self.connect()
                db_client = self.client
            db = db_client[database]
            coll = db[collection]
            
            if not data:
                return {"success": False, "error": "No data to insert"}
            
            result = coll.insert_many(data)
            return {
                "success": True,
                "inserted_count": len(result.inserted_ids),
                "database": database,
                "collection": collection,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_client_for_connector(self, connector_id: str):
        """Build MongoClient from saved connector config"""
        from pymongo import MongoClient
        connector = self.get_connector(connector_id)
        if not connector:
            return None, None
        if connector.get("connection_string"):
            conn_str = connector["connection_string"]
        elif connector.get("username") and connector.get("password"):
            conn_str = (
                f"mongodb://{connector['username']}:{connector['password']}"
                f"@{connector['host']}:{connector['port']}/"
            )
        else:
            conn_str = f"mongodb://{connector['host']}:{connector['port']}/"
        return MongoClient(conn_str, serverSelectionTimeoutMS=10000), connector

    def create_collection_from_schema(self, database: str, collection: str, schema: dict, client=None) -> dict:
        """Create a collection with optional schema validation"""
        try:
            db_client = client or self.client
            if not db_client:
                self.connect()
                db_client = self.client
            db = db_client[database]
            
            if collection in db.list_collection_names():
                return {"success": True, "message": "Collection already exists"}
            
            db.create_collection(collection)
            return {
                "success": True,
                "message": f"Collection '{collection}' created in database '{database}'",
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_collection_stats(self, database: str, collection: str) -> dict:
        """Get statistics for a collection"""
        try:
            db = self.client[database]
            coll = db[collection]
            
            count = coll.count_documents({})
            sample = list(coll.find().limit(5))
            
            for doc in sample:
                doc["_id"] = str(doc["_id"])
            
            return {
                "success": True,
                "document_count": count,
                "sample_documents": sample,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    # ═══════════════════════════════════════════════════════════════════════
    # TRANSFER JOB TRACKING
    # ═══════════════════════════════════════════════════════════════════════
    
    def create_transfer_job(self, job_data: dict) -> str:
        """Create a new transfer job record"""
        db = self.get_database()
        collection = db["transfer_jobs"]
        
        job_data["status"] = "pending"
        job_data["created_at"] = datetime.utcnow()
        job_data["started_at"] = None
        job_data["completed_at"] = None
        job_data["records_processed"] = 0
        job_data["errors"] = []
        
        result = collection.insert_one(job_data)
        return str(result.inserted_id)
    
    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        """Update transfer job status"""
        from bson import ObjectId
        db = self.get_database()
        collection = db["transfer_jobs"]
        
        updates = {"status": status, "updated_at": datetime.utcnow()}
        updates.update(kwargs)
        
        if status == "running":
            updates["started_at"] = datetime.utcnow()
        elif status in ("completed", "failed"):
            updates["completed_at"] = datetime.utcnow()
        
        result = collection.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": updates}
        )
        return result.modified_count > 0
    
    def get_job(self, job_id: str) -> Optional[dict]:
        """Get a transfer job by ID"""
        from bson import ObjectId
        db = self.get_database()
        collection = db["transfer_jobs"]
        
        result = collection.find_one({"_id": ObjectId(job_id)})
        if result:
            result["_id"] = str(result["_id"])
        return result
    
    def list_jobs(self, limit: int = 50) -> list[dict]:
        """List recent transfer jobs"""
        db = self.get_database()
        collection = db["transfer_jobs"]
        
        jobs = []
        for doc in collection.find().sort("created_at", -1).limit(limit):
            doc["_id"] = str(doc["_id"])
            for key in ("created_at", "updated_at", "started_at", "completed_at"):
                if doc.get(key) and hasattr(doc[key], "isoformat"):
                    doc[key] = doc[key].isoformat()
            jobs.append(doc)
        return jobs


# Global instance
_mongodb_service: Optional[MongoDBService] = None


def get_mongodb_service() -> MongoDBService:
    """Get or create MongoDB service instance"""
    global _mongodb_service
    if _mongodb_service is None:
        _mongodb_service = MongoDBService()
        _mongodb_service.connect()
    return _mongodb_service
