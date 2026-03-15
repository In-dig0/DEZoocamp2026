import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def run_tip_aggregation():
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(settings)
    table_env.get_config().set("parallelism.default", "1")

    # Sorgente Kafka
    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            tip_amount DOUBLE,
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

    # Sink PostgreSQL
    sink_ddl = """
        CREATE TABLE sink_tips (
            window_start TIMESTAMP(3),
            total_tip DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'tip_stats',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """
    table_env.execute_sql(sink_ddl)

    # Query: Tumbling Window di 1 ora, somma delle mance
    tip_query = """
        INSERT INTO sink_tips
        SELECT 
            TUMBLE_START(event_timestamp, INTERVAL '1' HOUR) AS window_start,
            SUM(tip_amount) AS total_tip
        FROM green_trips
        GROUP BY 
            TUMBLE(event_timestamp, INTERVAL '1' HOUR)
    """
    table_env.execute_sql(tip_query)

if __name__ == '__main__':
    run_tip_aggregation()