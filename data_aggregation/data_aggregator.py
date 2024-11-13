from pyspark.sql import SparkSession
import polars as pl
from .strategies.base_strategy import BaseStrategy

class DataAggregator:
    def __init__(self, strategy: BaseStrategy):
        # Initialize the Spark session
        self.spark = SparkSession.builder \
            .appName("Iceberg Data Aggregation") \
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.my_catalog.type", "hadoop") \
            .config("spark.sql.catalog.my_catalog.warehouse", "/warehouse") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0") \
            .getOrCreate()

        # Set the aggregation strategy
        self.strategy = strategy

    def load_data(self, table_name):
        # Read data from the Iceberg table into a PySpark DataFrame
        spark_df = self.spark.read.format("iceberg").load(table_name)
        return spark_df

    def aggregate_data(self, spark_df):
        # Use the aggregation strategy to process the data
        return self.strategy.aggregate(spark_df)

    def convert_to_polars(self, spark_df):
        # Convert PySpark DataFrame to Pandas and then to Polars DataFrame
        pandas_df = spark_df.toPandas()
        polars_df = pl.DataFrame(pandas_df)
        return polars_df

    def stop_spark(self):
        self.spark.stop()
