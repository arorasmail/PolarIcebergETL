# data_ingestion/injection_strategies.py

class OverwriteStrategy:
    def store(self, spark_df, table_name):
        """
        Overwrite data in the specified Iceberg table.
        """
        print(f"Overwriting data in Iceberg table {table_name}...")
        spark_df.write.format("iceberg").mode("overwrite").save(table_name)
