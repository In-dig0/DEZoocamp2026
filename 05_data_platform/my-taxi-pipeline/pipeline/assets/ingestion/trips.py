"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11
requirements:
  - pandas
  - pyarrow
  - tzdata
  - requests

columns:
  - name: pickup_datetime
    type: timestamp
    description: When the meter was engaged
  - name: dropoff_datetime
    type: timestamp
    description: When the meter was disengaged

@bruin"""

import os
import json
import pandas as pd
from typing import List, Tuple
import requests
from datetime import datetime
import tzdata

#os.environ["PYARROW_TZDATA_PATH"] = os.path.join(os.path.dirname(tzdata.__file__), "zoneinfo")



# Generate list of months between start and end dates
# Fetch parquet files from:
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"

def generate_month_to_ingest(start_date, end_date) -> List[Tuple[int, str]]:
    # 1. Inizializzi una lista vuota
    results = [] 
    
    for year in range(start_date.year, end_date.year + 1):
        for month in range(1, 13):
            if (year == start_date.year and month < start_date.month) or \
               (year == end_date.year and month > end_date.month):
                continue
            
            # 2. Formattazione: :02d aggiunge lo zero iniziale se il numero è < 10
            month_str = f"{month:02d}"
            results.append((year, month_str))
            
    # 3. Restituisci la lista completa
    return results

def build_parquet_url(taxi_type: str, year: int, month: int) -> str:
    """Costruisce l'URL specifico per un tipo di taxi, anno e mese."""
    # Il modificatore :02d assicura che il mese sia sempre di due cifre (es. 01)
    return BASE_URL.format(taxi_type=taxi_type, year=year, month=month)

def fetch_trip_data(taxi_types: List[str], year: int, month: int) -> pd.DataFrame:
    """Scarica e unisce i dati per tutti i tipi di taxi specificati per un dato mese."""
    month_dfs = []
    
    for t_type in taxi_types:
        url = build_parquet_url(t_type, year, month)
        try:
            print(f"Fetching data from: {url}")
            # Pandas può leggere file parquet direttamente da un URL
            df = pd.read_parquet(url)
            # Aggiungiamo una colonna per distinguere il tipo di taxi nel dataframe finale
            df['taxi_type'] = t_type
            month_dfs.append(df)
        except Exception as e:
            print(f"Errore nel download di {url}: {e}")
            
    if not month_dfs:
        return pd.DataFrame()
        
    return pd.concat(month_dfs, ignore_index=True)

def materialize() -> pd.DataFrame:
    """Funzione principale per il caricamento dei dati in Bruin."""
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"    
    
    # Bruin passa le date come stringhe (es. "2023-01-01"), dobbiamo convertirle in oggetti datetime
    start_date_str = os.environ.get("BRUIN_START_DATE", "2023-01-01")
    end_date_str = os.environ.get("BRUIN_END_DATE", "2023-01-31")
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # Estrazione variabili da BRUIN_VARS
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])
    
    all_dataframes = []

    # Iterazione sui mesi generati
    for year, month in generate_month_to_ingest(start_date, end_date):
        df_month = fetch_trip_data(taxi_types, year, month)
        if not df_month.empty:
            all_dataframes.append(df_month)

    # Unione di tutti i dati raccolti
    if not all_dataframes:
        print("Nessun dato trovato per il periodo specificato.")
        return pd.DataFrame()

    final_dataframe = pd.concat(all_dataframes, ignore_index=True)
    
    # Converti colonne datetime con timezone in UTC naive
    for col in final_dataframe.select_dtypes(include=["datetimetz"]).columns:
        final_dataframe[col] = final_dataframe[col].dt.tz_convert("UTC").dt.tz_localize(None)
    
    return final_dataframe
