# use_cases/use_case_1.py

from data_ingestion.api_client import ApiClient
from data_ingestion.iceberg_storage import IcebergStorage
from data_ingestion.injection_strategies import AppendStrategy
from data_aggregation.data_aggregator import DataAggregator
from data_aggregation.strategies import GroupSumStrategy

def run_use_case():
    # Step 1: Fetch data from API
    api_client = ApiClient("https://fake-json-api.mock.beeceptor.com")
    data = api_client.get_data("companies")

    print(data)
    
  # Convert the fetched data to a Polars DataFrame (assuming the data is in a proper format)
    #import polars as pl
    #polars_df = pl.DataFrame(data)

    import pandas as pd
    pandas_df = pd.DataFrame(data)
    
    # Step 2: Store the data in Iceberg using Append Strategy
    append_strategy = AppendStrategy()
    iceberg_storage = IcebergStorage(injection_strategy=append_strategy)
    iceberg_storage.store(pandas_df, "my_catalog.use_case_1_table")

    # Step 3: Load the data from Iceberg
    loaded_data = iceberg_storage.load("my_catalog.use_case_1_table")

    # Step 4: Aggregate data using Group Sum Strategy
    group_sum_strategy = GroupSumStrategy(group_by_field="category", sum_field="value")
    aggregator = DataAggregator(strategy=group_sum_strategy)
    aggregated_data = aggregator.aggregate(loaded_data)

    # Display the aggregated data
    print("Aggregated Data (Use Case 1):")
    print(aggregated_data)
