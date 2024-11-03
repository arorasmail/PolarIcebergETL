from pyspark.sql import SparkSession

class IcebergStorage:
    def __init__(self):
        # Initialize the Spark session
        self.spark = SparkSession.builder \
            .appName("Polars to Iceberg Framework") \
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.my_catalog.type", "hadoop") \
            .config("spark.sql.catalog.my_catalog.warehouse", "path/to/warehouse") \
            .getOrCreate()

    def store_data(self, polars_df, table_name):
        # Convert Polars DataFrame to Pandas DataFrame and then to PySpark DataFrame
        spark_df = self.spark.createDataFrame(polars_df.to_pandas())
        # Write the data to an Iceberg table
        spark_df.write.format("iceberg").mode("append").save(table_name)

    def stop_spark(self):
        self.spark.stop()
