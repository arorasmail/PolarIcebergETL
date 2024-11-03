from pyspark.sql import SparkSession
import polars as pl

class DataAggregator:
    def __init__(self):
        # Initialize the Spark session
        self.spark = SparkSession.builder \
            .appName("Iceberg Data Aggregation") \
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.my_catalog.type", "hadoop") \
            .config("spark.sql.catalog.my_catalog.warehouse", "path/to/warehouse") \
            .getOrCreate()

    def load_data(self, table_name):
        # Read data from the Iceberg table into a PySpark DataFrame
        spark_df = self.spark.read.format("iceberg").load(table_name)
        return spark_df

    def aggregate_data(self, spark_df):
        # Example aggregation: Group by a column and calculate sum
        aggregated_df = spark_df.groupBy("column_to_group").sum("column_to_sum")
        return aggregated_df

    def convert_to_polars(self, spark_df):
        # Convert PySpark DataFrame to Pandas and then to Polars DataFrame
        pandas_df = spark_df.toPandas()
        polars_df = pl.DataFrame(pandas_df)
        return polars_df

    def stop_spark(self):
        self.spark.stop()
