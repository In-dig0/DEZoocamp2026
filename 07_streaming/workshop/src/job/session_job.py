import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def run_session_aggregation():
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(settings)
    table_env.get_config().set("parallelism.default", "1")

    # Sorgente (identica a prima)
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
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'json.ignore-parse-errors' = 'true'
        )
    """
    table_env.execute_sql(source_ddl)

    # Sink per le sessioni
    sink_ddl = """
        CREATE TABLE sink_session (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'session_events',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """
    table_env.execute_sql(sink_ddl)

    # Query con SESSION window (gap di 5 minuti)
    session_query = """
        INSERT INTO sink_session
        SELECT 
            SESSION_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
            SESSION_END(event_timestamp, INTERVAL '5' MINUTE) AS window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM green_trips
        GROUP BY 
            SESSION(event_timestamp, INTERVAL '5' MINUTE),
            PULocationID
    """
    table_env.execute_sql(session_query)

if __name__ == '__main__':
    run_session_aggregation()