# AWS Data Lakehouse Pipeline for Sparkify

**Author:** Sam Sepassi  
**Course:** Udacity Data Engineering with AWS Nanodegree

---

## ⚠️ Reviewer Note

> **Please review this project located in `sparkify_airflow_lakehouse/`.**
>
> - **Setup DAG:** `setup/run_pipeline.py` (interval configurable via DAG params)
> - **Raw DAG:** `raw/dag.py` (dynamic discovery + dynamic SQL checks)
> - **Transactions DAG:** `transactions/dag.py` (dependency-ordered promotion + SQL checks)
> - **Analytics DAG:** `analytics/dag.py` (PySpark DataFrames, zero spark.sql)
> - **Validation SQL:** `validation/athena_checks.sql`

---

## Architecture

```
setup/run_pipeline.py
    │ interval configurable via DAG run params (not hardcoded)
    │ emits Dataset("s3://sparkify/pipeline_requested") via outlet_events
    │ metadata: {data_interval, tables}
    ▼
raw/dag.py
    │ discovers S3 landing tables dynamically via S3Hook.list_keys()
    │ Glue job writes to glue_catalog.raw.{table} (fully qualified catalog)
    │ Dynamic SQL checks: one per discovered table (no hardcoded table names)
    │ SQL checks use conn_id='athena_default'
    │ emits Dataset("s3://sparkify/raw_complete") via outlet_events
    │ metadata: {data_interval, tables}
    ▼
transactions/dag.py
    │ promotes raw → transactions in dependency order:
    │   artists → users → song_versions → songs → user_levels → events
    │ Glue script reads/writes glue_catalog.transactions.{table}
    │ SQL checks use conn_id='athena_default'
    │ emits Dataset("s3://sparkify/transactions_complete") via outlet_events
    │ metadata: {data_interval, tables}
    ▼
analytics/dag.py
    │ builds analytics snapshots using PySpark DataFrame API (ZERO spark.sql calls)
    │ reads glue_catalog.transactions.*, writes glue_catalog.analytics.*
    │   songplay_facts, user_activity_daily, artist_popularity, user_facts
    │ full overwrite via createOrReplace() — no append/insert
    │ emits Dataset("s3://sparkify/analytics_complete") via outlet_events
    │ metadata: {data_interval, tables}
```

---

## DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `run_pipeline` | Manual (schedule=None, params configurable) | Emits pipeline_requested asset with metadata |
| `raw` | Asset-triggered (pipeline_requested) | Discovers & ingests landing tables into Iceberg raw layer |
| `transactions` | Asset-triggered (raw_complete) | Normalizes raw → transactions layer with deduplication |
| `analytics` | Asset-triggered (transactions_complete) | Builds analytics marts as full snapshots (PySpark DataFrames) |

---

## Centralized Constants

All S3 paths, connection IDs, and catalog names are defined as constants at the top of each DAG file:

| Constant | Value | Used For |
|----------|-------|----------|
| `AWS_CONN_ID` | `'aws_default'` | GlueJobOperator, S3Hook |
| `ATHENA_CONN_ID` | `'athena_default'` | SQLCheckOperator |
| `S3_BUCKET_TPL` | `'{{ var.value.sparkify_bucket }}'` | All S3 path references |
| `GLUE_SCRIPTS` | `'glue-scripts'` | Glue script location root |
| `CATALOG` | `'glue_catalog'` | Iceberg catalog prefix (Glue scripts) |

No S3 paths or connection IDs are hardcoded inline in operators — all reference these constants.

---

## Iceberg Catalog Consistency

All Glue scripts use the **same fully qualified catalog prefix** (`glue_catalog`) for database creation, reads, and writes:

| Layer | Write Target | Read Source |
|-------|-------------|-------------|
| raw | `glue_catalog.raw.{table}` | S3 landing JSON |
| transactions | `glue_catalog.transactions.{table}` | `glue_catalog.raw.*` (via SQL) |
| analytics | `glue_catalog.analytics.{table}` | `glue_catalog.transactions.*` |

This ensures all tables are registered in the Glue Data Catalog and queryable from Athena.

---

## Athena Database Names (Queryable from Athena)

| Database | Tables |
|----------|--------|
| `raw` | `logs`, `songs` |
| `transactions` | `events`, `users`, `artists`, `songs`, `song_versions`, `user_levels` |
| `analytics` | `songplay_facts`, `user_activity_daily`, `artist_popularity`, `user_facts` |

### Athena Smoke-Test Queries

```sql
-- Raw layer
SELECT count(*) FROM raw.logs;
SELECT count(*) FROM raw.songs;

-- Transaction layer
SELECT count(*) FROM transactions.events;
SELECT count(*) - count(DISTINCT event_id) AS dupes FROM transactions.events;
SELECT count(*) FROM transactions.users;
SELECT count(*) FROM transactions.artists;
SELECT count(*) FROM transactions.songs;

-- Analytics layer
SELECT count(*) FROM analytics.songplay_facts;
SELECT count(*) FROM analytics.user_activity_daily;
SELECT count(*) FROM analytics.artist_popularity;
SELECT * FROM analytics.user_facts ORDER BY event_count DESC LIMIT 20;
```

---

## Key Design Decisions

- **Configurable interval:** `setup/run_pipeline.py` uses DAG `params` for `data_interval` — trigger with any interval without code edits
- **Asset metadata via outlet_events:** Every emit task uses `outlet_events[ASSET].extra = payload` with both `data_interval` and `tables`
- **Dynamic table discovery + validation:** `raw/dag.py` discovers tables from S3 and generates SQL checks dynamically — no hardcoded table names
- **Centralized constants:** All S3 paths, connection IDs, and script roots defined as top-of-file constants — no inline hardcoding
- **Iceberg catalog consistency:** All scripts use `glue_catalog.{db}.{table}` for reads and writes
- **Athena connection:** SQL checks use `athena_default` (not `aws_default`)
- **Analytics without SQL:** `analytics/glue_script.py` uses pure PySpark DataFrame API — zero `spark.sql()` calls
- **Full snapshot overwrites:** Analytics tables use `createOrReplace()` (no append/insert)

---

## Project Structure

```
sparkify_airflow_lakehouse/
├── setup/
│   └── run_pipeline.py              ← pipeline trigger (params-configurable interval)
├── raw/
│   ├── dag.py                       ← raw ingestion DAG + dynamic SQL checks
│   └── glue_script.py               ← Glue job: S3 JSON → glue_catalog.raw tables
├── transactions/
│   ├── dag.py                       ← transactions promotion DAG + SQL checks
│   ├── glue_script.py               ← Glue job: SQL transform → glue_catalog.transactions
│   └── sql/
│       ├── artists.sql
│       ├── events.sql               ← includes event_id (stable primary key)
│       ├── song_versions.sql
│       ├── songs.sql
│       ├── user_levels.sql
│       └── users.sql
├── analytics/
│   ├── dag.py                       ← analytics snapshot DAG
│   ├── glue_script.py               ← Glue job: PySpark DataFrame API (ZERO spark.sql)
│   └── sql/
│       ├── artist_popularity.sql    ← reference SQL (not executed by Glue)
│       ├── songplay_facts.sql       ← reference SQL (not executed by Glue)
│       ├── user_activity_daily.sql  ← reference SQL (not executed by Glue)
│       └── user_facts.sql           ← reference SQL (not executed by Glue)
├── validation/
│   └── athena_checks.sql            ← Athena validation queries (includes user_facts)
└── README.md                        ← this file
```
