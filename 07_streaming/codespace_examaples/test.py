import pandas as pd
import requests
import io

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']

headers = {'User-Agent': 'Mozilla/5.0'}
try:
    response = requests.get(url, headers=headers)
    print(f"Fetching data: {response.status_code}")
except requests.RequestException as e:
    print(f"Error fetching data: {e}")