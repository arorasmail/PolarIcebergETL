from pyspark.sql import SparkSession

class IcebergStorage:
    def __init__(self, injection_strategy):

        
        self.spark = SparkSession.builder \
        .appName("Polars to Iceberg Framework") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "file:///Users/admin/Documents/official/PolarIcebergETL/warehouse") \
        .config("saprk.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0") \
        .getOrCreate()

        self.injection_strategy = injection_strategy  # Injection strategy for storing data

    def store(self, polars_df, table_name):
        """
        Store the Polars DataFrame to the Iceberg table using the specified injection strategy
        """
        # Convert Polars DataFrame to Pandas DataFrame and then to PySpark DataFrame
        spark_df = self.spark.createDataFrame(polars_df)

        # Use the injection strategy to store the data
        self.injection_strategy.store(spark_df, table_name)

    def load(self, table_name):
        """
        Load data from the specified Iceberg table
        """
        # Load the data as a Spark DataFrame
        spark_df = self.spark.read.format("iceberg").load(table_name)
        
        # Convert the Spark DataFrame to Pandas and then to Polars DataFrame
        return spark_df.toPandas()

    def stop_spark(self):
        """
        Stop the Spark session
        """
        self.spark.stop()
