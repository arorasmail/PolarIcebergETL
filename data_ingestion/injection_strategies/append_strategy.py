# data_ingestion/injection_strategies.py

class AppendStrategy:
    def store(self, spark_df, table_name):
        """
        Append data to the specified Iceberg table.
        """
        print(f"Appending data to Iceberg table {table_name}...")
        spark_df.write.format("iceberg").mode("append").save(table_name)
