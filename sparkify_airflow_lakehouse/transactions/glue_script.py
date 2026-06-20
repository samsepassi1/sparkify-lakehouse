import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

CATALOG = 'glue_catalog'
DATABASE = 'transactions'

args = getResolvedOptions(sys.argv, ['BUCKET', 'DATA_INTERVAL', 'SQL_FILE', 'TABLE_NAME'])

spark = (
    SparkSession.builder
    .appName('sparkify-transactions')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config(f'spark.sql.catalog.{CATALOG}', 'org.apache.iceberg.spark.GlueCatalog')
    .config(f'spark.sql.catalog.{CATALOG}.warehouse', f"s3://{args['BUCKET']}/warehouse")
    .config(f'spark.sql.catalog.{CATALOG}.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
    .config(f'spark.sql.catalog.{CATALOG}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    .getOrCreate()
)

# Fully qualified catalog.database for Glue Data Catalog registration
spark.sql(f'CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}')

# Read SQL file from S3 and execute it
s3 = boto3.client('s3')
sql = s3.get_object(Bucket=args['BUCKET'], Key=f"transactions/{args['SQL_FILE']}")['Body'].read().decode()
sql = sql.replace('{{ data_interval }}', args['DATA_INTERVAL'])

# The SQL references raw.* and transactions.* — use the catalog-qualified names
# by replacing unqualified db references with catalog-qualified ones
sql = sql.replace('raw.', f'{CATALOG}.raw.')
sql = sql.replace('transactions.', f'{CATALOG}.transactions.')

df = spark.sql(sql)

# Table-specific primary keys for deduplication
TABLE_PRIMARY_KEYS = {
    'events': ['event_id'],
    'users': ['user_id'],
    'songs': ['song_id'],
    'artists': ['artist_id'],
    'song_versions': ['song_id'],
    'user_levels': ['user_id'],
}

pk = TABLE_PRIMARY_KEYS.get(args['TABLE_NAME'])
if pk:
    pk_cols = [c for c in pk if c in df.columns]
    if pk_cols:
        df = df.dropDuplicates(pk_cols)

target = f'{CATALOG}.{DATABASE}.{args["TABLE_NAME"]}'
df.writeTo(target).using('iceberg').tableProperty('format-version', '2').createOrReplace()
print(f"Transactions promotion complete: {target} ({df.count()} rows)")

spark.stop()
