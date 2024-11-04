from .base_injection_strategy import BaseInjectionStrategy
from pyspark.sql import DataFrame

class AppendStrategy(BaseInjectionStrategy):
    def inject(self, spark_df: DataFrame, table_name: str):
        # Append data to the existing Iceberg table
        spark_df.write.format("iceberg").mode("append").save(table_name)
