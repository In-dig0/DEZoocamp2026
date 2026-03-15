import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def run_aggregation():
    # 1. Inizializzazione Ambiente
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(settings)
    
    # 2. Configurazione Parallelismo (Fondamentale come da istruzioni!)
    table_env.get_config().set("parallelism.default", "1")

    # 3. Sorgente Kafka (Green Trips)
    # Usiamo VARCHAR e TO_TIMESTAMP come richiesto
    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            PULocationID INT,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'flink-worker',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'scan.startup.mode' = 'earliest-offset'
        )
    """
    table_env.execute_sql(source_ddl)

    # 4. Sink PostgreSQL
    # Assicurati di aver creato la tabella 'processed_trips' nel DB prima!
    sink_ddl = """
        CREATE TABLE sink_table (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'processed_trips',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """
    table_env.execute_sql(sink_ddl)

    # 5. Query di Aggregazione (Tumbling Window 5 min)
    aggregation_query = """
        INSERT INTO sink_table
        SELECT 
            TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
            PULocationID,
            COUNT(*) AS num_trips
        FROM green_trips
        GROUP BY 
            TUMBLE(event_timestamp, INTERVAL '5' MINUTE),
            PULocationID
    """
    
    # Questo comando invia il job al cluster Flink
    table_env.execute_sql(aggregation_query)

if __name__ == '__main__':
    run_aggregation()