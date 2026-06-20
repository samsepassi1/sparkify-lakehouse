from __future__ import annotations
from airflow.decorators import dag, task
from airflow.datasets import Dataset
import pendulum

PIPELINE_REQUESTED = Dataset("s3://sparkify/pipeline_requested")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    params={
        "data_interval": "interval_1",
        "tables": ["logs", "songs"],
    },
)
def run_pipeline():
    @task(outlets=[PIPELINE_REQUESTED])
    def request_pipeline_run(data_interval, tables, *, outlet_events=None):
        """
        Trigger the lakehouse pipeline by emitting the pipeline_requested asset.

        data_interval and tables come from DAG run params (configurable at
        trigger time) so the same pipeline handles any interval without code edits.
        """
        payload = {
            "data_interval": data_interval,
            "tables": tables,
        }
        if outlet_events is not None:
            outlet_events[PIPELINE_REQUESTED].extra = payload
        return payload

    request_pipeline_run(
        data_interval="{{ params.data_interval }}",
        tables="{{ params.tables }}",
    )


run_pipeline()
