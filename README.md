# CosmosDB to Azure AI Search Pipeline

This is a customizable template for exporting data from a CosmosDB MongoDB vCore API instance to an Azure AI Search index using the push API.

## Getting Started

### Prerequisites

- Python v3.10+
- A CosmosDB account with MongoDB vCore API
- An Azure AI Search service
- Node.js v22.14.0+


### Setup

1. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Copy the environment template to create your own `.env` file:
   ```
   cp .env.template .env
   ```

3. Edit the `.env` file and fill in your specific configuration values:
   - `COSMOS_CONN_STRING`: Your CosmosDB MongoDB vCore connection string

   - `SEARCH_SERVICE_NAME`: Your Azure AI Search service name
   - `SEARCH_ADMIN_KEY`: Your Azure AI Search admin key
   - `SEARCH_INDEX_NAME`: The name for your search index

## (optional) Generate Synthetic Data

Run the following command to initialize your project and link it to npm:
```
npm init
```

Press Enter to accept all default values except for entry point: (index.js). When the terminal returns entry point: (index.js), enter this text and press Enter:
```
myapp.js
```

Continue to accept all default values and type Yes when prompted.

In the myapp.js file, replace the following placeholder value with your values and save the contents of the file:

 `COSMOS_CONN_STRING`: the connection string for your CosmoSDB for MongoVCore cluster

 `Synthetic_Data_DB`: the name of the database 
 
 `Synthetic_Data_COL`: the name of the collection

## Configuration

### Customizing the Schema

Open `search_index.py` and modify the `get_search_index_schema()` function to match your specific data structure. The schema defines the fields in your Azure AI Search index.

```python
def get_search_index_schema():
    return {
        "name": SEARCH_INDEX_NAME,
        "fields": [
            # Add or modify fields based on your data structure
            {"name": "id", "type": "Edm.String", "key": True, "searchable": False},
            # Add your custom fields here
        ]
    }
```

### Customizing the Document Transformation

Modify the `transform_document()` function to map your source data fields to the search index fields:

```python
def transform_document(doc):
    # Map fields based on your data structure
    search_doc = {
        "id": doc["_id"],  # Required key field
        # Map your fields here
    }
    
    return search_doc
```

## Running the Pipeline

Execute the pipeline to push data from CosmosDB to Azure AI Search:

```
python main.py
```

The script will:
1. Connect to your CosmosDB for MongoDB vCore instance
2. Fetch all documents from the specified collection
3. Create or (if it already exists) recreate, and validate the search index in Azure AI Search
4. Transform and push the data to the Azure AI Search index

## Batch Processing

By default, documents are processed in batches of 1000. You can adjust this in your `.env` file by changing the `BATCH_SIZE` value.

## Logging

The script logs detailed information about each step of the pipeline process. Check the logs for any errors or issues during execution.

## Customizing Further

For more advanced customization:
- Modify the `fetch_data_from_cosmosdb()` function to filter documents or limit the number of documents fetched
- Add custom error handling or retry logic
- Implement incremental data loading based on timestamps