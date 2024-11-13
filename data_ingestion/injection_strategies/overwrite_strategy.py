# data_ingestion/injection_strategies.py

from pyspark.sql import SparkSession

class OverwriteStrategy:


    def store(self, spark_df, table_name):
        """
        Overwrite and recreate the Iceberg table with the data schema from the API.
        """
        # Ensure the catalog is fully qualified
        full_table_name = f"my_catalog.{table_name}"

        

        # Write the DataFrame to Iceberg, creating the table with the new schema
        print(f"Creating and overwriting Iceberg table {full_table_name}...")
        spark_df.writeTo(full_table_name).createOrReplace()
        print(f"Data successfully written to Iceberg table {full_table_name}.")
