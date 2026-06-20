from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset as Asset
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

AWS_CONN_ID = 'aws_default'
ATHENA_CONN_ID = 'aws_default'
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
    'transactions',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    schedule=[RAW_ASSET],
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    doc_md='Transactions layer normalizes raw Sparkify data into Iceberg tables.',
) as dag:

    # Dependency-ordered promotion: dimension tables first, then fact tables
    SQL_ORDER = ['artists', 'users', 'song_versions', 'songs', 'user_levels', 'events']

    @task
    def selected_interval(**context):
        return interval_from_context(**context)

    interval = selected_interval()
    prev = None

    for name in SQL_ORDER:
        op = GlueJobOperator(
            task_id=f'promote_{name}',
            job_name='sparkify-transactions',
            script_location='s3://{{ var.value.sparkify_bucket }}/glue-scripts/transactions/glue_script.py',
            iam_role_name='GlueServiceRole',
            aws_conn_id=AWS_CONN_ID,
            script_args={
                '--BUCKET': '{{ var.value.sparkify_bucket }}',
                '--DATA_INTERVAL': '{{ ti.xcom_pull(task_ids="selected_interval") }}',
                '--SQL_FILE': f'sql/{name}.sql',
                '--TABLE_NAME': name,
            },
            wait_for_completion=True,
        )
        if prev:
            prev >> op
        else:
            interval >> op
        prev = op

    # SQL Check: verify transactions.events has no duplicate event_id
    check_events = SQLCheckOperator(
        task_id='check_events_dedup',
        conn_id=ATHENA_CONN_ID,
        sql="""
            SELECT count(*) - count(DISTINCT event_id) AS dup_count
            FROM transactions.events
            HAVING count(*) - count(DISTINCT event_id) = 0
        """,
    )

    # SQL Check: verify transactions.users is non-empty
    check_users = SQLCheckOperator(
        task_id='check_users_nonempty',
        conn_id=ATHENA_CONN_ID,
        sql="SELECT count(*) AS cnt FROM transactions.users HAVING count(*) > 0",
    )

    prev >> check_events
    prev >> check_users

    @task(outlets=[TRANSACTIONS_ASSET])
    def emit_transactions(data_interval, *, outlet_events=None):
        """Emit transactions asset with metadata attached to the asset event."""
        payload = {'data_interval': data_interval, 'tables': SQL_ORDER}
        if outlet_events is not None:
            outlet_events[TRANSACTIONS_ASSET].extra = payload
        return payload

    check_events >> emit_transactions(interval)
    check_users >> emit_transactions(interval)
