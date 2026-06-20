from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset as Asset
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

AWS_CONN_ID = 'aws_default'
ATHENA_CONN_ID = 'aws_default'
S3_BUCKET_VAR = 'sparkify_bucket'
LANDING_ROOT = 'landing'
SCRIPTS_ROOT = 'glue-scripts'
RAW_ASSET = Asset('s3://sparkify/raw_complete')
TRANSACTIONS_ASSET = Asset('s3://sparkify/transactions_complete')
ANALYTICS_ASSET = Asset('s3://sparkify/analytics_complete')

PIPELINE_REQUESTED = Asset('s3://sparkify/pipeline_requested')


def interval_from_context(**context):
    events = context.get('triggering_dataset_events') or context.get('triggering_asset_events') or {}
    for evs in events.values():
        if evs:
            return evs[-1].extra.get('data_interval', context['params'].get('data_interval', 'interval_1'))
    return context['params'].get('data_interval', 'interval_1')


with DAG(
    'raw',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    schedule=[PIPELINE_REQUESTED],
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    doc_md='Raw layer ingests discovered landing tables into Iceberg raw database.',
) as dag:

    @task
    def discover_tables(**context):
        bucket = context['var'].value.get(S3_BUCKET_VAR)
        interval = interval_from_context(**context)
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        prefix = f'{LANDING_ROOT}/{interval}/'
        keys = hook.list_keys(bucket, prefix=prefix) or []
        tables = sorted({k[len(prefix):].split('/')[0] for k in keys if k.endswith('.json') and '/' in k[len(prefix):]})
        return {'data_interval': interval, 'tables': tables}

    @task
    def selected_interval(**context):
        return interval_from_context(**context)

    meta = discover_tables()
    interval = selected_interval()

    run = GlueJobOperator(
        task_id='run_raw_glue',
        job_name='sparkify-raw',
        script_location='s3://{{ var.value.sparkify_bucket }}/glue-scripts/raw/glue_script.py',
        iam_role_name='GlueServiceRole',
        aws_conn_id=AWS_CONN_ID,
        script_args={
            '--BUCKET': '{{ var.value.sparkify_bucket }}',
            '--DATA_INTERVAL': '{{ ti.xcom_pull(task_ids="discover_tables")["data_interval"] }}',
            '--TABLES': '{{ ti.xcom_pull(task_ids="discover_tables")["tables"] | join(",") }}',
        },
        wait_for_completion=True,
    )

    # SQL Check: verify raw.logs table is non-empty after Glue ingestion
    check_raw = SQLCheckOperator(
        task_id='check_raw_logs',
        conn_id=ATHENA_CONN_ID,
        sql="SELECT count(*) AS cnt FROM raw.logs HAVING count(*) > 0",
    )

    @task(outlets=[RAW_ASSET])
    def emit_raw(data_interval, tables, *, outlet_events=None):
        """Emit raw asset with metadata attached to the asset event."""
        payload = {'data_interval': data_interval, 'tables': tables}
        if outlet_events is not None:
            outlet_events[RAW_ASSET].extra = payload
        return payload

    # Wire dependencies
    meta >> run
    interval >> run
    run >> check_raw
    check_raw >> emit_raw(meta['data_interval'], meta['tables'])
