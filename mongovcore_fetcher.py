import os
import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

COSMOS_DB_CONNECTION_STRING = os.getenv("COSMOS_CONN_STRING")
DATABASE_NAME = "Synthetic_Data_DB"
COLLECTION_NAME = "Synthetic_Data_COL"

def connect_to_cosmosdb() -> MongoClient:
    """Connect to Cosmos DB with MongoDB API."""
    try:
        client = MongoClient(COSMOS_DB_CONNECTION_STRING)
        logger.info("Connected to Cosmos DB successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to Cosmos DB: {e}")
        raise

def fetch_data_from_cosmosdb(client: MongoClient, batch_size: int = 100) -> List[Dict[Any, Any]]:
    """Fetch data from Cosmos DB collection.""" #ADD: fetch documents last updated within the last fetch operation
    try:
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        
        # Get total document count
        total_docs = collection.count_documents({})
        logger.info(f"Total documents in collection: {total_docs}")
        
        # Fetch all documents
        documents = list(collection.find())
        logger.info(f"Retrieved {len(documents)} documents from Cosmos DB")
        
        # Convert ObjectId to string for JSON serialization
        for doc in documents:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
                
            # Ensure timestamps are serializable
            if 'timestamp_day' in doc:
                doc['timestamp_day'] = doc['timestamp_day'].isoformat()
                
            # Handle nested timestamp fields
            if 'events' in doc and isinstance(doc['events'], list):
                for event in doc['events']:
                    if 'timestamp_event' in event:
                        event['timestamp_event'] = event['timestamp_event'].isoformat()
        
        return documents
    except Exception as e:
        logger.error(f"Failed to fetch data from Cosmos DB: {e}")
        raise

if __name__ == "__main__":
    # Test functionality if run directly
    try:
        client = connect_to_cosmosdb()
        documents = fetch_data_from_cosmosdb(client)
        print(f"Successfully fetched {len(documents)} documents")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")