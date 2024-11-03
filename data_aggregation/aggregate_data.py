from .data_aggregator import DataAggregator

def aggregate_data():
    # Initialize DataAggregator
    aggregator = DataAggregator()

    # Load data from Iceberg
    spark_df = aggregator.load_data("my_catalog.my_table")

    # Perform aggregation
    aggregated_df = aggregator.aggregate_data(spark_df)

    # Convert to Polars DataFrame for further analysis if needed
    polars_df = aggregator.convert_to_polars(aggregated_df)

    # Output the aggregated data
    print(polars_df)

    # Stop the Spark session
    aggregator.stop_spark()
