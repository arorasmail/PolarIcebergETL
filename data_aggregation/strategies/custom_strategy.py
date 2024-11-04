from .base_strategy import BaseStrategy
from pyspark.sql import DataFrame

class CustomStrategy(BaseStrategy):
    def aggregate(self, spark_df: DataFrame) -> DataFrame:
        # Custom aggregation logic goes here
        aggregated_df = spark_df.groupBy("custom_column").agg({
            "another_column": "max"
        })
        return aggregated_df
