from .base_strategy import BaseStrategy
from pyspark.sql import DataFrame

class AverageStrategy(BaseStrategy):
    def aggregate(self, spark_df: DataFrame) -> DataFrame:
        # Example aggregation: Group by a column and calculate average
        aggregated_df = spark_df.groupBy("column_to_group").avg("column_to_average")
        return aggregated_df
