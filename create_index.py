import os
import json
import requests
import logging
from dotenv import load_dotenv
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

SEARCH_SERVICE_NAME = os.getenv("SEARCH_SERVICE_NAME")
SEARCH_ADMIN_KEY = os.getenv("SEARCH_ADMIN_KEY")
SEARCH_INDEX_NAME = "synthetic-index"  # name for creation of new search index
SEARCH_API_VERSION = "2023-10-01-Preview" 

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

if __name__ == "__main__":
    # Test functionality if run directly
    if create_search_index():
        if verify_search_index():
            print("Search index created and verified successfully")
        else:
            print("Failed to verify search index")
    else:
        print("Failed to create search index")