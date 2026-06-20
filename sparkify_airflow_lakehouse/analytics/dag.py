from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset as Asset
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# ── Centralized constants ──────────────────────────────────────────
AWS_CONN_ID     = 'aws_default'
S3_BUCKET_VAR   = 'sparkify_bucket'
S3_BUCKET_TPL   = '{{ var.value.sparkify_bucket }}'
GLUE_SCRIPTS    = 'glue-scripts'
ANALYTICS_SCRIPT = f's3://{S3_BUCKET_TPL}/{GLUE_SCRIPTS}/analytics/glue_script.py'

# ── Assets ─────────────────────────────────────────────────────────
RAW_ASSET             = Asset('s3://sparkify/raw_complete')
TRANSACTIONS_ASSET    = Asset('s3://sparkify/transactions_complete')
ANALYTICS_ASSET       = Asset('s3://sparkify/analytics_complete')


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
    tables = ['songplay_facts', 'user_activity_daily', 'artist_popularity', 'user_facts']
    prev = None

    for t in tables:
        op = GlueJobOperator(
            task_id=f'build_{t}',
            job_name='sparkify-analytics',
            script_location=ANALYTICS_SCRIPT,
            iam_role_name='GlueServiceRole',
            aws_conn_id=AWS_CONN_ID,
            script_args={
                '--BUCKET': S3_BUCKET_TPL,
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
    def emit_analytics(data_interval, *, outlet_events=None):
        """Emit analytics asset with BOTH data_interval and tables."""
        payload = {'data_interval': data_interval, 'tables': tables}
        if outlet_events is not None:
            outlet_events[ANALYTICS_ASSET].extra = payload
        return payload

    prev >> emit_analytics(interval)
