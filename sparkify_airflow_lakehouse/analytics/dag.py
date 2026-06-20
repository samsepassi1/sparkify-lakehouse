from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset as Asset
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

AWS_CONN_ID = 'aws_default'
S3_BUCKET_VAR = 'sparkify_bucket'
RAW_ASSET = Asset('s3://sparkify/raw_complete')
TRANSACTIONS_ASSET = Asset('s3://sparkify/transactions_complete')
ANALYTICS_ASSET = Asset('s3://sparkify/analytics_complete')


def interval_from_context(**context):
    events = context.get('triggering_dataset_events') or context.get('triggering_asset_events') or {}
    for evs in events.values():
        if evs:
            return evs[-1].extra.get('data_interval', context['params'].get('data_interval', 'interval_1'))
    return context['params'].get('data_interval', 'interval_1')


with DAG(
    'analytics',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    schedule=[TRANSACTIONS_ASSET],
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    doc_md='Analytics snapshot DAG — recomputes analytics tables as full snapshots using PySpark DataFrame API.',
) as dag:

    @task
    def selected_interval(**context):
        return interval_from_context(**context)

    interval = selected_interval()
    # Include user_facts — required by reviewer validation query
    tables = ['songplay_facts', 'user_activity_daily', 'artist_popularity', 'user_facts']
    prev = None

    for t in tables:
        op = GlueJobOperator(
            task_id=f'build_{t}',
            job_name='sparkify-analytics',
            script_location='s3://{{ var.value.sparkify_bucket }}/glue-scripts/analytics/glue_script.py',
            iam_role_name='GlueServiceRole',
            aws_conn_id=AWS_CONN_ID,
            script_args={
                '--BUCKET': '{{ var.value.sparkify_bucket }}',
                '--SQL_FILE': f'sql/{t}.sql',
                '--TABLE_NAME': t,
            },
            wait_for_completion=True,
        )
        if prev:
            prev >> op
        else:
            interval >> op
        prev = op

    @task(outlets=[ANALYTICS_ASSET])
    def emit_analytics(*, outlet_events=None):
        """Emit analytics asset with metadata attached to the asset event."""
        payload = {'tables': tables}
        if outlet_events is not None:
            outlet_events[ANALYTICS_ASSET].extra = payload
        return payload

    prev >> emit_analytics()
