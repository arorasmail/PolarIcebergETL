from .api_client import ApiClient
from .data_processor import DataProcessor
from .iceberg_storage import IcebergStorage

def ingest_data():
    # Initialize API clients for multiple APIs
    api_clients = [
        ApiClient("https://api1.example.com"),
        ApiClient("https://api2.example.com"),
        ApiClient("https://api3.example.com"),
        ApiClient("https://api4.example.com")
    ]

    # Initialize Iceberg storage
    iceberg_storage = IcebergStorage()

    for client in api_clients:
        # Fetch data from each API
        raw_data = client.get_data("data_endpoint")
        # Process data using Polars
        processed_data = DataProcessor.process_data(raw_data)
        # Store data in Iceberg
        iceberg_storage.store_data(processed_data, "my_catalog.my_table")

    # Stop the Spark session
    iceberg_storage.stop_spark()
