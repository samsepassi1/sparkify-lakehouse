import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    to_date, from_unixtime, lit, when, desc, row_number
)
from pyspark.sql.window import Window

CATALOG = 'glue_catalog'
DATABASE = 'analytics'

args = getResolvedOptions(sys.argv, ['BUCKET', 'SQL_FILE', 'TABLE_NAME'])

spark = (
    SparkSession.builder
    .appName('sparkify-analytics')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config(f'spark.sql.catalog.{CATALOG}', 'org.apache.iceberg.spark.GlueCatalog')
    .config(f'spark.sql.catalog.{CATALOG}.warehouse', f"s3://{args['BUCKET']}/warehouse")
    .config(f'spark.sql.catalog.{CATALOG}.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
    .config(f'spark.sql.catalog.{CATALOG}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    .getOrCreate()
)

table_name = args['TABLE_NAME']
target = f'{CATALOG}.{DATABASE}.{table_name}'

# Read source tables using fully qualified glue_catalog prefix (consistent with writes)
events_df  = spark.table(f'{CATALOG}.transactions.events')
users_df   = spark.table(f'{CATALOG}.transactions.users')
artists_df = spark.table(f'{CATALOG}.transactions.artists')

if table_name == 'songplay_facts':
    df = (
        events_df
        .filter(col('page') == 'NextSong')
        .select(
            col('ts'),
            col('user_id'),
            col('session_id'),
            col('song_id'),
            col('artist_id'),
            col('level'),
        )
    )

elif table_name == 'user_activity_daily':
    df = (
        events_df
        .withColumn('activity_date', to_date(from_unixtime(col('ts') / 1000)))
        .groupBy('activity_date', 'user_id')
        .agg(
            count('*').alias('events'),
            countDistinct('session_id').alias('sessions'),
        )
        .orderBy('activity_date', 'user_id')
    )

elif table_name == 'artist_popularity':
    df = (
        events_df
        .join(artists_df, on='artist_id', how='inner')
        .groupBy('artist_id', 'artist_name')
        .agg(count('*').alias('plays'))
        .orderBy(desc('plays'))
    )

elif table_name == 'user_facts':
    df = (
        users_df
        .join(events_df, on='user_id', how='left')
        .groupBy('user_id', 'first_name', 'last_name', 'gender', 'level')
        .agg(
            count('event_id').alias('event_count'),
            countDistinct('session_id').alias('session_count'),
        )
    )

else:
    raise ValueError(f'Unknown analytics table: {table_name}')

# Full snapshot overwrite via DataFrameWriterV2 createOrReplace()
# ZERO spark.sql() calls — no CREATE DATABASE, no DROP TABLE
(
    df.writeTo(target)
      .using('iceberg')
      .tableProperty('format-version', '2')
      .createOrReplace()
)
print(f"Analytics snapshot complete: {target} ({df.count()} rows)")

spark.stop()
