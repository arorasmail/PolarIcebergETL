import polars as pl

class DataProcessor:
    @staticmethod
    def process_data(data):
        # Convert the JSON response to a Polars DataFrame
        df = pl.DataFrame(data)
        return df
