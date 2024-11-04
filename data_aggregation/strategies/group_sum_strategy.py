from .base_strategy import BaseStrategy
from pyspark.sql import DataFrame

class GroupSumStrategy(BaseStrategy):
    def aggregate(self, spark_df: DataFrame) -> DataFrame:
        # Example aggregation: Group by a column and calculate sum
        aggregated_df = spark_df.groupBy("column_to_group").sum("column_to_sum")
        return aggregated_df
