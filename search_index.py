import os
import json
import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
import requests
from datetime import datetime
import logging
from typing import List, Dict, Any


logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()


COSMOS_DB_CONNECTION_STRING = os.getenv("COSMOS_CONN_STRING")
DATABASE_NAME = "Synthetic_Data_DB"
COLLECTION_NAME = "Synthetic_Data_COL"

SEARCH_SERVICE_NAME = os.getenv("SEARCH_SERVICE_NAME")
SEARCH_ADMIN_KEY = os.getenv("SEARCH_ADMIN_KEY")
SEARCH_INDEX_NAME = "synthetic-index"  # name for creation of new search index
SEARCH_API_VERSION = "2023-10-01-Preview" 

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

def create_search_index() -> bool:
    """Create a new search index in Azure AI Search."""
    endpoint = f"https://{SEARCH_SERVICE_NAME}.search.windows.net/indexes/{SEARCH_INDEX_NAME}?api-version={SEARCH_API_VERSION}"
    headers = {
        'Content-Type': 'application/json',
        'api-key': SEARCH_ADMIN_KEY
    }
    
    # Define the index schema based on the document structure
    index_schema = {
        "name": SEARCH_INDEX_NAME,
        "fields": [
            {"name": "id", "type": "Edm.String", "key": True, "searchable": False},
            {"name": "timestamp_day", "type": "Edm.DateTimeOffset", "filterable": True, "sortable": True},
            {"name": "cat", "type": "Edm.String", "searchable": True, "filterable": True, "sortable": True},
            {"name": "owner_email", "type": "Edm.String", "searchable": True},
            {"name": "owner_firstName", "type": "Edm.String", "searchable": True, "sortable": True},
            {"name": "owner_lastName", "type": "Edm.String", "searchable": True, "sortable": True},
            {"name": "events_count", "type": "Edm.Int32", "filterable": True, "sortable": True},
            {"name": "avg_weight", "type": "Edm.Double", "filterable": True, "sortable": True}
        ]
    }
    
    try:
        # Check if index exists and delete if it does
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            logger.info(f"Index {SEARCH_INDEX_NAME} already exists. Deleting...")
            delete_response = requests.delete(endpoint, headers=headers)
            if delete_response.status_code not in (200, 204):
                logger.error(f"Failed to delete existing index: {delete_response.text}")
                return False
            logger.info(f"Successfully deleted existing index {SEARCH_INDEX_NAME}")
        
        # Create the index
        response = requests.put(endpoint, headers=headers, json=index_schema)
        if response.status_code in (201, 204):
            logger.info(f"Successfully created search index {SEARCH_INDEX_NAME}")
            return True
        else:
            logger.error(f"Failed to create search index: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error creating search index: {e}")
        return False

def verify_search_index() -> bool:
    """Verify that the search index exists in Azure AI Search."""
    endpoint = f"https://{SEARCH_SERVICE_NAME}.search.windows.net/indexes/{SEARCH_INDEX_NAME}?api-version={SEARCH_API_VERSION}"
    headers = {
        'Content-Type': 'application/json',
        'api-key': SEARCH_ADMIN_KEY
    }
    
    try:
        # Check if index exists
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            logger.info(f"Index {SEARCH_INDEX_NAME} exists.")
            return True
        else:
            logger.error(f"Index {SEARCH_INDEX_NAME} not found: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error verifying search index: {e}")
        return False

def push_data_to_search(documents: List[Dict[Any, Any]]) -> bool:
    """Push data to Azure AI Search using the push API."""
    endpoint = f"https://{SEARCH_SERVICE_NAME}.search.windows.net/indexes/{SEARCH_INDEX_NAME}/docs/index?api-version={SEARCH_API_VERSION}"
    headers = {
        'Content-Type': 'application/json',
        'api-key': SEARCH_ADMIN_KEY
    }
    
    # Prepare documents for Azure Search
    search_docs = []
    for doc in documents:
        # Calculate average weight from events
        events_count = len(doc.get("events", []))
        avg_weight = 0.0
        if events_count > 0:
            total_weight = sum(event.get("weight", 0) for event in doc.get("events", []))
            avg_weight = total_weight / events_count
        
        # Timestamp
        timestamp_day = doc["timestamp_day"]
        if '.' in timestamp_day:
            parts = timestamp_day.split('.')
            if len(parts) == 2 and len(parts[1]) > 3:
                timestamp_day = f"{parts[0]}.{parts[1][:3]}Z"
            else:
                timestamp_day = f"{timestamp_day}Z"
        else:
            timestamp_day = f"{timestamp_day}Z"
        
        search_doc = {
            "id": doc["_id"],
            "timestamp_day": timestamp_day,
            "cat": doc["cat"],
            "owner_email": doc["owner"]["email"],
            "owner_firstName": doc["owner"]["firstName"],
            "owner_lastName": doc["owner"]["lastName"],
            "events_count": events_count,
            "avg_weight": avg_weight
        }
        search_docs.append(search_doc)
    
    batch_size = 1000
    total_batches = (len(search_docs) + batch_size - 1) // batch_size
    
    try:
        success = True
        for i in range(0, len(search_docs), batch_size):
            batch = search_docs[i:i+batch_size]
            request_body = {
                "value": batch
            }
            
            logger.info(f"Pushing batch {(i // batch_size) + 1}/{total_batches} ({len(batch)} documents)")
            logger.info(f"Sample document: {batch[0]}")
            
            # Log info for debugging
            response = requests.post(endpoint, headers=headers, json=request_body)
            logger.info(f"Response status code: {response.status_code}")
            
            if response.status_code not in (200, 201):
                logger.error(f"Failed to push batch: {response.status_code}, {response.text}")
                success = False
                continue
                
            # Print detailed information about the first response
            if i == 0:
                logger.info(f"First batch response (first 500 chars): {response.text[:500]}...")
            
            try:
                result = response.json()
                logger.info(f"Response keys: {list(result.keys())}")
                
                # Consider the batch successful if the HTTP status was successful, parse JSON response
                logger.info(f"Successfully indexed batch {(i // batch_size) + 1}")
                
                # If 'value' key exists in result, log some details about it
                if 'value' in result:
                    logger.info(f"Response contains {len(result['value'])} value items")
                    if len(result['value']) > 0:
                        logger.info(f"First value item keys: {list(result['value'][0].keys())}")
                
            except Exception as parse_error:
                logger.error(f"Error parsing response JSON: {parse_error}")
                success = False
        
        return success
    except Exception as e:
        logger.error(f"Error pushing data to search: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def main():
    """Main function to execute the data push from Cosmos DB to Azure AI Search."""
    try:
        missing_vars = []
        if not COSMOS_DB_CONNECTION_STRING:
            missing_vars.append("COSMOS_CONN_STRING")
        if not SEARCH_SERVICE_NAME:
            missing_vars.append("SEARCH_SERVICE_NAME")
        if not SEARCH_ADMIN_KEY:
            missing_vars.append("SEARCH_ADMIN_KEY")
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            return

        # connect to cdb and fetch data
        client = connect_to_cosmosdb()
        documents = fetch_data_from_cosmosdb(client)
        
        if not documents:
            logger.warning("No documents found in Cosmos DB collection")
            return
            
        if create_search_index():
            if push_data_to_search(documents):
                logger.info("Successfully pushed data to Azure AI Search")
            else:
                logger.error("Failed to push data to Azure AI Search")
        else:
            logger.error("Failed to create search index")
    
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}")
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

if __name__ == "__main__":
    main()