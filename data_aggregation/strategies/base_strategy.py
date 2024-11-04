from pyspark.sql import DataFrame

class BaseStrategy:
    def aggregate(self, spark_df: DataFrame) -> DataFrame:
        raise NotImplementedError("Aggregate method must be implemented by the subclass.")
