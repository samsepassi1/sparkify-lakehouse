from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset as Asset
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

# ── Centralized constants ──────────────────────────────────────────
AWS_CONN_ID     = 'aws_default'
ATHENA_CONN_ID  = 'athena_default'
S3_BUCKET_VAR   = 'sparkify_bucket'
S3_BUCKET_TPL   = '{{ var.value.sparkify_bucket }}'
GLUE_SCRIPTS    = 'glue-scripts'
LANDING_ROOT    = 'landing'
RAW_SCRIPT      = f's3://{S3_BUCKET_TPL}/{GLUE_SCRIPTS}/raw/glue_script.py'
CATALOG         = 'glue_catalog'
RAW_DB          = 'raw'

# ── Assets ─────────────────────────────────────────────────────────
RAW_ASSET             = Asset('s3://sparkify/raw_complete')
TRANSACTIONS_ASSET    = Asset('s3://sparkify/transactions_complete')
ANALYTICS_ASSET       = Asset('s3://sparkify/analytics_complete')
PIPELINE_REQUESTED    = Asset('s3://sparkify/pipeline_requested')


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

    # ── Build dynamic SQL checks from discovered tables (no hardcoding) ──
    @task
    def build_raw_checks(meta):
        """Generate one SQL check per discovered table — no hardcoded table names."""
        checks = []
        for table in meta['tables']:
            checks.append({
                'table': table,
                'sql': f'SELECT count(*) AS cnt FROM {RAW_DB}.{table} HAVING count(*) > 0',
            })
        return checks

    meta = discover_tables()
    interval = selected_interval()

    run = GlueJobOperator(
        task_id='run_raw_glue',
        job_name='sparkify-raw',
        script_location=RAW_SCRIPT,
        iam_role_name='GlueServiceRole',
        aws_conn_id=AWS_CONN_ID,
        script_args={
            '--BUCKET': S3_BUCKET_TPL,
            '--DATA_INTERVAL': '{{ ti.xcom_pull(task_ids="discover_tables")["data_interval"] }}',
            '--TABLES': '{{ ti.xcom_pull(task_ids="discover_tables")["tables"] | join(",") }}',
        },
        wait_for_completion=True,
    )

    # ── Dynamic validation: one SQLCheckOperator per discovered table ──
    checks = build_raw_checks(meta)

    check_raw = SQLCheckOperator.partial(
        task_id='check_raw_table',
        conn_id=ATHENA_CONN_ID,
    ).expand(
        sql="{{ task.build_raw_checks.output | map(attribute='sql') | list }}",
    )

    @task(outlets=[RAW_ASSET])
    def emit_raw(data_interval, tables, *, outlet_events=None):
        """Emit raw asset with both data_interval and tables in metadata."""
        payload = {'data_interval': data_interval, 'tables': tables}
        if outlet_events is not None:
            outlet_events[RAW_ASSET].extra = payload
        return payload

    # Wire dependencies
    meta >> run
    interval >> run
    run >> check_raw
    check_raw >> emit_raw(meta['data_interval'], meta['tables'])
