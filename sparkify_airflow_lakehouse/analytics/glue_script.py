import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    to_date, from_unixtime, lit, when, desc, row_number
)
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['BUCKET', 'SQL_FILE', 'TABLE_NAME'])

spark = (
    SparkSession.builder
    .appName('sparkify-analytics')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.GlueCatalog')
    .config('spark.sql.catalog.glue_catalog.warehouse', f"s3://{args['BUCKET']}/warehouse")
    .config('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
    .config('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    .getOrCreate()
)

table_name = args['TABLE_NAME']
# Use glue_catalog prefix so createOrReplace manages the table via the Iceberg catalog
# without executing any SQL statements.
target = f'glue_catalog.analytics.{table_name}'

# Read source tables as DataFrames (NO SQL — pure PySpark DataFrame API)
events_df = spark.table('glue_catalog.transactions.events')
users_df = spark.table('glue_catalog.transactions.users')
artists_df = spark.table('glue_catalog.transactions.artists')

if table_name == 'songplay_facts':
    # Filter to NextSong events and select fact columns
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
    # Aggregate user activity by day using DataFrame operations
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
    # Join events with artists and aggregate play counts
    df = (
        events_df
        .join(artists_df, on='artist_id', how='inner')
        .groupBy('artist_id', 'artist_name')
        .agg(count('*').alias('plays'))
        .orderBy(desc('plays'))
    )

elif table_name == 'user_facts':
    # User-level mart: join users with events, aggregate per user
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

# Overwrite full snapshot using DataFrameWriterV2 createOrReplace()
# NO SQL statements (no spark.sql, no DROP TABLE, no CREATE DATABASE)
(
    df.writeTo(target)
      .using('iceberg')
      .tableProperty('format-version', '2')
      .createOrReplace()
)
print(f"Analytics snapshot complete: {target} ({df.count()} rows)")

spark.stop()
