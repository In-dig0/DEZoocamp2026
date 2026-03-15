import pandas as pd
import json
from kafka import KafkaProducer
from time import time

url = "green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime',
           'lpep_dropoff_datetime',
           'PULocationID', 
           'DOLocationID',
           'passenger_count', 
           'trip_distance', 
           'tip_amount',
           'total_amount', 
]
#df = pd.read_parquet(url, columns=columns).head(5)  # Carichiamo solo le prime 5 righe per test
df = pd.read_parquet(url, columns=columns)
print(df.head(5))
print(df.shape)

# 2. Gestione datetime: Convertiamo le colonne temporali in stringhe
# JSON non sa "leggere" i formati datetime di Python nativamente
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype(str)
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)

# 3. Inizializzazione del Producer Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

t0 = time()

# 4. Iterazione sulle righe e invio al topic 'green-trips'

# Sostituisci i valori NaN con None (che in JSON diventa 'null')
#df = df.fillna(value=None).replace({pd.NA: None})
df = df.replace({pd.NA: None, float('nan'): None})

print("Inizio invio messaggi...")
count = 0
for row in df.to_dict(orient='records'):
    producer.send('green-trips', value=row)
    count += 1

# Assicuriamoci che tutti i messaggi siano inviati prima di chiudere
producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
print("")
print(f"Numero di righe processate: {df.shape[0]}")
print(f"Numero di messaggi inviati: {count}")
print("Invio completato!")


