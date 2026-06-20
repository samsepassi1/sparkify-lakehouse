import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

args = getResolvedOptions(sys.argv, ['BUCKET', 'DATA_INTERVAL', 'SQL_FILE', 'TABLE_NAME'])

spark = (
    SparkSession.builder
    .appName('sparkify-transactions')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.GlueCatalog')
    .config('spark.sql.catalog.glue_catalog.warehouse', f"s3://{args['BUCKET']}/warehouse")
    .config('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
    .config('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    .getOrCreate()
)

# Use Athena/Glue database name "transactions" (not "sparkify_transactions")
spark.sql('CREATE DATABASE IF NOT EXISTS transactions')

# Read SQL file from S3 and execute it
s3 = boto3.client('s3')
sql = s3.get_object(Bucket=args['BUCKET'], Key=f"transactions/{args['SQL_FILE']}")['Body'].read().decode()
sql = sql.replace('{{ data_interval }}', args['DATA_INTERVAL'])

df = spark.sql(sql)

# Table-specific primary keys for deduplication (NOT generic _id matching)
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
    # Only deduplicate on the explicit primary key for this table
    pk_cols = [c for c in pk if c in df.columns]
    if pk_cols:
        df = df.dropDuplicates(pk_cols)

target = f"transactions.{args['TABLE_NAME']}"
df.writeTo(target).using('iceberg').tableProperty('format-version', '2').createOrReplace()
print(f"Transactions promotion complete: {target} ({df.count()} rows)")

spark.stop()
