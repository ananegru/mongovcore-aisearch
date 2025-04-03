import os
import logging
import traceback
from dotenv import load_dotenv

# Import functions from other files
from mongovcore_fetcher import connect_to_cosmosdb, fetch_data_from_cosmosdb
from create_index import create_search_index, verify_search_index
from push import push_data_to_search

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

def validate_environment() -> bool:
    """Validate that all required environment variables are set."""
    missing_vars = []
    if not os.getenv("COSMOS_CONN_STRING"):
        missing_vars.append("COSMOS_CONN_STRING")
    if not os.getenv("SEARCH_SERVICE_NAME"):
        missing_vars.append("SEARCH_SERVICE_NAME")
    if not os.getenv("SEARCH_ADMIN_KEY"):
        missing_vars.append("SEARCH_ADMIN_KEY")
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    return True

def main():
    """Main function to execute the data push from Cosmos DB to Azure AI Search."""
    try:
        if not validate_environment():
            return
            
        # Connect to Cosmos DB and fetch data
        logger.info("Connecting to Cosmos DB...")
        client = connect_to_cosmosdb()
        
        logger.info("Fetching data from Cosmos DB...")
        documents = fetch_data_from_cosmosdb(client)
        
        if not documents:
            logger.warning("No documents found in Cosmos DB collection")
            return
            
        # Create the search index
        logger.info("Creating Azure AI Search index...")
        if create_search_index():
            # Verify the index was created successfully
            if verify_search_index():
                # Push data to the search index
                logger.info("Pushing data to Azure AI Search...")
                if push_data_to_search(documents):
                    logger.info(f"Successfully pushed {len(documents)} documents to Azure AI Search")
                else:
                    logger.error("Failed to push data to Azure AI Search")
            else:
                logger.error("Search index verification failed")
        else:
            logger.error("Failed to create search index")
    
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}")
        logger.error(traceback.format_exc())
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

if __name__ == "__main__":
    main()