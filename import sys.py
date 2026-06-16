import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth,
    count, sum as spark_sum, countDistinct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# PATHS
# ============================================================
S3_RAW       = 's3://ecomm-pipeline-data-bucket-538072680807-us-east-1-an/raw/'
S3_PROCESSED = 's3://ecomm-pipeline-data-bucket-538072680807-us-east-1-an/processed/'
S3_CURATED   = 's3://ecomm-pipeline-data-bucket-538072680807-us-east-1-an/curated/'

# ============================================================
# SCHÉMA EXPLICITE — résout UNABLE_TO_INFER_SCHEMA
# ============================================================
schema = StructType([
    StructField("event_id",   StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id",    StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category",   StringType(), True),
    StructField("country",    StringType(), True),
    StructField("price",      DoubleType(), True),
    StructField("timestamp",  StringType(), True),
])

# ============================================================
# LECTURE RAW — schéma fixe, pas besoin de PERMISSIVE
# puisque les données sont propres
# ============================================================
raw_df = spark.read \
    .option("multiline", "true") \
    .schema(schema) \
    .json(S3_RAW)

print(f"[INFO] Records lus depuis RAW: {raw_df.count()}")

# ============================================================
# NETTOYAGE & TRANSFORMATION (Bronze → Silver)
# price filtré uniquement sur purchases plus bas
# ============================================================
cleaned_df = raw_df \
    .dropDuplicates(["event_id"]) \
    .filter(col("event_type").isNotNull()) \
    .filter(col("timestamp").isNotNull()) \
    .withColumn("event_date", to_date(col("timestamp"))) \
    .withColumn("year",  year(col("event_date"))) \
    .withColumn("month", month(col("event_date"))) \
    .withColumn("day",   dayofmonth(col("event_date")))

print(f"[INFO] Records après nettoyage: {cleaned_df.count()}")

# ============================================================
# ÉCRITURE PROCESSED — Silver partitionné
# ============================================================
cleaned_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day", "event_type") \
    .parquet(S3_PROCESSED)

print("[INFO] Zone PROCESSED écrite.")

# ============================================================
# AGRÉGATIONS BUSINESS — Gold/Curated
# ============================================================

# -- Revenue par catégorie & pays
purchases_df = cleaned_df \
    .filter(col("event_type") == "purchase") \
    .filter(col("price").isNotNull()) \
    .filter(col("price") > 0)

print(f"[INFO] Purchases valides: {purchases_df.count()}")

revenue_df = purchases_df \
    .groupBy("event_date", "category", "country") \
    .agg(
        spark_sum("price").alias("total_revenue"),
        count("event_id").alias("total_transactions"),
        countDistinct("user_id").alias("unique_buyers"),
        countDistinct("product_id").alias("unique_products")
    )

revenue_df.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet(S3_CURATED + "revenue_by_category/")

print("[INFO] Revenue curated écrit.")

# -- Taux de conversion panier → achat
cart_df = cleaned_df \
    .filter(col("event_type") == "add_to_cart") \
    .groupBy("event_date") \
    .agg(count("event_id").alias("cart_adds"))

purchase_count_df = purchases_df \
    .groupBy("event_date") \
    .agg(count("event_id").alias("purchases"))

conversion_df = cart_df \
    .join(purchase_count_df, "event_date", "left") \
    .withColumn("conversion_rate",
        col("purchases") / col("cart_adds") * 100
    )

conversion_df.write \
    .mode("overwrite") \
    .parquet(S3_CURATED + "conversion_rates/")

print("[INFO] Conversion rates curated écrit.")
job.commit()