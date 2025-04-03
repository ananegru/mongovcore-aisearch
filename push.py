import os
import json
import requests
import logging
import traceback
from dotenv import load_dotenv
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

SEARCH_SERVICE_NAME = os.getenv("SEARCH_SERVICE_NAME")
SEARCH_ADMIN_KEY = os.getenv("SEARCH_ADMIN_KEY")
SEARCH_INDEX_NAME = "synthetic-index"
SEARCH_API_VERSION = "2023-10-01-Preview"

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
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    # Test functionality - this would require sample data
    try:
        from mongovcore_fetcher import connect_to_cosmosdb, fetch_data_from_cosmosdb
        from create_index import verify_search_index
        
        # Check if index exists
        if verify_search_index():
            # Get some data to push
            client = connect_to_cosmosdb()
            documents = fetch_data_from_cosmosdb(client)
            
            if documents:
                if push_data_to_search(documents):
                    print(f"Successfully pushed {len(documents)} documents to search")
                else:
                    print("Failed to push data to search")
            else:
                print("No documents to push")
        else:
            print("Search index does not exist. Please create it first")
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
    finally:
        if 'client' in locals():
            client.close()
            print("MongoDB connection closed")