from __future__ import annotations
from airflow.decorators import dag, task
from airflow.datasets import Dataset
import pendulum

PIPELINE_REQUESTED = Dataset("s3://sparkify/pipeline_requested")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
)
def run_pipeline():
    @task(outlets=[PIPELINE_REQUESTED])
    def request_pipeline_run(*, outlet_events=None):
        """
        Trigger the lakehouse pipeline by emitting the pipeline_requested asset.

        Use outlet_events to attach metadata directly to the asset event so
        downstream DAGs can read data_interval and tables from
        triggering_dataset_events.
        """
        payload = {
            "data_interval": "interval_1",
            "tables": ["logs", "songs"],
        }
        if outlet_events is not None:
            outlet_events[PIPELINE_REQUESTED].extra = payload
        return payload

    request_pipeline_run()


run_pipeline()
