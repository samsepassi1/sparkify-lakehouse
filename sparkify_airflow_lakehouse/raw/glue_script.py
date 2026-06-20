import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

args = getResolvedOptions(sys.argv, ['BUCKET', 'DATA_INTERVAL', 'TABLES'])

CATALOG = 'glue_catalog'
DATABASE = 'raw'

spark = (
    SparkSession.builder
    .appName('sparkify-raw')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config(f'spark.sql.catalog.{CATALOG}', 'org.apache.iceberg.spark.GlueCatalog')
    .config(f'spark.sql.catalog.{CATALOG}.warehouse', f"s3://{args['BUCKET']}/warehouse")
    .config(f'spark.sql.catalog.{CATALOG}.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
    .config(f'spark.sql.catalog.{CATALOG}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    .getOrCreate()
)

bucket = args['BUCKET']
interval = args['DATA_INTERVAL']
tables = [t for t in args['TABLES'].split(',') if t]

# Use fully qualified catalog.database so tables register in Glue Data Catalog
spark.sql(f'CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}')

for table in tables:
    path = f"s3://{bucket}/landing/{interval}/{table}/"
    df = spark.read.json(path)
    df = df.withColumn('data_interval', lit(interval))
    target = f'{CATALOG}.{DATABASE}.{table}'
    df.writeTo(target).using('iceberg').tableProperty('format-version', '2').partitionedBy('data_interval').createOrReplace()
    print(f"Raw ingestion complete: {target} ({df.count()} rows)")

spark.stop()
