"""Building a `dlt` pipeline to ingest NYC taxi data from a REST API."""

import dlt
import requests


@dlt.source
def taxi_homework_pipeline_rest_api_source():
    """Define dlt resources from NYC taxi REST API endpoints."""
    base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net"
    endpoint = "/data_engineering_zoomcamp_api"

    @dlt.resource(name="taxi_records")
    def fetch_taxi_records():
        page = 1
        while True:
            resp = requests.get(f"{base_url}{endpoint}", params={"page": page})
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            for item in data:
                yield item
            page += 1

    yield fetch_taxi_records()


pipeline = dlt.pipeline(
    pipeline_name='taxi_homework_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_homework_pipeline_rest_api_source())
    print(load_info)  # noqa: T201
