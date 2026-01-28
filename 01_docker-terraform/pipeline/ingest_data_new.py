# pipeline/ingest_data.py

# 3rd party imports
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine, inspect
import click


# Define data types for each column to optimize memory usage
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}
# Columns to parse as dates
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading data')
@click.option('--year', default=2025, type=int, help='Year of the dataset')
@click.option('--month', default=11, type=int, help='Month of the dataset')
@click.option('--file-format', default='parquet', type=click.Choice(['csv', 'parquet']), help='File format (csv or parquet)')


def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize, year, month, file_format):
    """Ingest data into PostgreSQL database."""
    
    # Determine URL based on file format
    if file_format == 'csv':
        prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
        file_name = f'green_tripdata_{year}-{month:02d}.csv.gz'
    else:  # parquet
        prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
        file_name = f'green_tripdata_{year}-{month:02d}.parquet'
    
    url = prefix + file_name
    print(f"Reading data from: {url}")

    # Create the database engine
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Read a small sample of the data to inspect and verify data types
    if file_format == 'csv':
        df = pd.read_csv(
            url,
            nrows=100,
            dtype=dtype,
            parse_dates=parse_dates
        )
    else:  # parquet
        # Read the entire parquet file first (parquet doesn't support nrows like CSV)
        df_full = pd.read_parquet(url)
        df = df_full.head(100)
        print(f"Total rows in parquet file: {len(df_full)}")

    # Inspect the first few rows of the dataframe
    print(df.head())
    print(f"\nData types:\n{df.dtypes}")

    # Check if the table exists before doing the count
    inspector = inspect(engine)
    if inspector.has_table(target_table):
        count_df = pd.read_sql(f"SELECT COUNT(*) FROM {target_table}", con=engine)
        print(f"Table {target_table} exists. Current rows: {count_df.iloc[0,0]}")
    else:
        print(f"Table {target_table} does not exist yet. Creating it...")
    
    # Create the table in the database with the correct schema
    # Use if_exists='replace' to drop the table if it already exists
    df.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')
    count_df = pd.read_sql(f"SELECT COUNT(*) FROM {target_table}", con=engine)
    print(f"Table {target_table} after reset: {count_df}")

    # Ingest data in chunks
    if file_format == 'csv':
        df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize
        )
        chunk_index = 1
        for df_chunk in tqdm(df_iter):
            print(f"Chunk {chunk_index}, rows: {len(df_chunk)}")
            chunk_index = chunk_index + 1
            df_chunk.to_sql(name=target_table, con=engine, if_exists="append")
    else:  # parquet
        # For parquet, read the full file and process in chunks manually
        df_full = pd.read_parquet(url)
        total_rows = len(df_full)
        chunk_index = 1
        
        for start_idx in tqdm(range(0, total_rows, chunksize)):
            end_idx = min(start_idx + chunksize, total_rows)
            df_chunk = df_full.iloc[start_idx:end_idx]
            print(f"Chunk {chunk_index}, rows: {len(df_chunk)} (from {start_idx} to {end_idx})")
            chunk_index = chunk_index + 1
            df_chunk.to_sql(name=target_table, con=engine, if_exists="append")

    count_df = pd.read_sql(f"SELECT COUNT(*) FROM {target_table}", con=engine)
    print(f"Table {target_table} after ingestion: {count_df}")  


if __name__ == '__main__':
    run()