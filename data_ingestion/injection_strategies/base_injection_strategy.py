from pyspark.sql import DataFrame

class BaseInjectionStrategy:
    def inject(self, spark_df: DataFrame, table_name: str):
        raise NotImplementedError("Injection method must be implemented by the subclass.")
