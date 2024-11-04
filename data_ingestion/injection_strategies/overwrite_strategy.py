from .base_injection_strategy import BaseInjectionStrategy
from pyspark.sql import DataFrame

class OverwriteStrategy(BaseInjectionStrategy):
    def inject(self, spark_df: DataFrame, table_name: str):
        # Overwrite the existing Iceberg table
        spark_df.write.format("iceberg").mode("overwrite").save(table_name)
