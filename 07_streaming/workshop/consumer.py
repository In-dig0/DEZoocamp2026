import json
from kafka import KafkaConsumer

# 1. Inizializzazione del Consumer
consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', # Legge dall'inizio se non ci sono offset salvati
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    # Questo parametro serve per far chiudere lo script se non arrivano messaggi per 1 secondo
    consumer_timeout_ms=1000 
)

count = 0

print("Analisi dei messaggi in corso...")

# 2. Ciclo di lettura e conteggio
for message in consumer:
    row = message.value
    # Verifichiamo la condizione sulla distanza
    if float(row['trip_distance']) > 5.0:
        count += 1

print(f"--- RISULTATO ---")
print(f"Numero di corse con distanza > 5.0 km: {count}")