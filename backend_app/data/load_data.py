# load_data.py
from django.conf import settings
import pandas as pd
# import settings as st
import os
from azure.storage.blob import BlobServiceClient, BlobClient
from io import StringIO
from dotenv import load_dotenv
load_dotenv()
CSV_FILE = os.getenv('CSV_FILE', 'tripadvisor_european_restaurants.csv')
# settings.py
AZURE_STORAGE_ACCOUNT_NAME = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
AZURE_STORAGE_ACCOUNT_KEY = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
AZURE_CONTAINER_NAME = os.getenv('AZURE_CONTAINER_NAME')

def load_csv_data():
    file_name=CSV_FILE
    # Accessing settings from Django settings.py
    account_name = AZURE_STORAGE_ACCOUNT_NAME
    account_key = AZURE_STORAGE_ACCOUNT_KEY
    container_name = AZURE_CONTAINER_NAME

    # Create a blob service client
    blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                            credential=account_key)

    # Access specific blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

    # Download blob content
    blob_data = blob_client.download_blob()
    blob_str = blob_data.readall().decode('utf-8')

    # Use pandas to read the CSV data
    data_frame = pd.read_csv(StringIO(blob_str))

    data_frame.dropna(
        subset = ["restaurant_name", "country", "price_level", "meals", "cuisines", "vegetarian_friendly", "vegan_options", "gluten_free", "avg_rating", "service", "value", "atmosphere"], 
        inplace=True
    )
    data_frame['cuisines'] = data_frame['cuisines'].str.split(',').str[0]
    
    return data_frame
