from data_ingestion.ingest_data import ingest_data
from data_aggregation.aggregate_data import aggregate_data

if __name__ == "__main__":
    # Step 1: Ingest data from APIs and store in Iceberg
    ingest_data()

    # Step 2: Perform data aggregation
    aggregate_data()
