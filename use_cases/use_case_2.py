from data_aggregation.data_aggregator import DataAggregator
from data_aggregation.strategies.average_strategy import AverageStrategy
from data_ingestion.iceberg_storage import IcebergStorage
from data_ingestion.injection_strategies.overwrite_strategy import OverwriteStrategy

def run_use_case():
    # Define table for this use case
    table_name = "my_catalog.use_case_2_table"

    # Initialize DataAggregator with the chosen aggregation strategy
    aggregation_strategy = AverageStrategy()
    aggregator = DataAggregator(aggregation_strategy)

    # Load data from Iceberg
    spark_df = aggregator.load_data(table_name)

    # Perform aggregation
    aggregated_df = aggregator.aggregate_data(spark_df)

    # Initialize Iceberg storage and injection strategy
    injection_strategy = OverwriteStrategy()
    iceberg_storage = IcebergStorage()

    # Inject the aggregated data into Iceberg
    injection_strategy.inject(aggregated_df, table_name)

    # Stop the Spark session
    aggregator.stop_spark()
