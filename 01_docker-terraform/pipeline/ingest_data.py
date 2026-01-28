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
@click.option('--year', default=2021, type=int, help='Year of the dataset')
@click.option('--month', default=1, type=int, help='Month of the dataset')


def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize, year, month):
    """Ingest data into PostgreSQL database."""
    # App Configuration
    # # PostgreSQL connection parameters
    # pg_user = 'root'
    # pg_password = 'root'
    # pg_host = 'localhost'
    # pg_port = 5432
    # pg_db = 'ny_taxi'
    # target_table = 'yellow_taxi_data'
    #chunksize = 100000

    # Yellow Taxi dataset from web
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    file_name = f'yellow_tripdata_{year}-{month:02d}.csv.gz'
    url = prefix + file_name

    # Create the database engine
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


    # Read a small sample of the data to inspect and verify data types
    df = pd.read_csv(
        url,
        nrows=100,
        dtype=dtype,
        parse_dates=parse_dates
    )

    # Inspect the first few rows of the dataframe
    print(df.head())

    # Controllo se la tabella esiste prima di fare il count
    inspector = inspect(engine)
    if inspector.has_table(target_table):
        count_df = pd.read_sql(f"SELECT COUNT(*) FROM {target_table}", con=engine)
        print(f"Table {target_table} exists. Current rows: {count_df.iloc[0,0]}")
    else:
        print(f"Table {target_table} does not exist yet. Creating it...")

    # count_df = pd.read_sql(f"SELECT COUNT(*) FROM {target_table}", con=engine)
    # print(f"Table {target_table} before ingestion: {count_df}")
    
    # Create the table in the database with the correct schema
    # Use if_exists='replace' to drop the table if it already exists
    df.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')
    count_df = pd.read_sql(f"SELECT COUNT(*) FROM {target_table}", con=engine)
    print(f"Table {target_table} after reset: {count_df}")

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )
    chunk_index = 1
    for df_chunk in tqdm(df_iter):
        print(chunk_index, len(df_chunk))
        chunk_index = chunk_index + 1
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")

    count_df = pd.read_sql(f"SELECT COUNT(*) FROM {target_table}", con=engine)
    print(f"Table {target_table} after ingestion: {count_df}")  


if __name__ == '__main__':
    run()