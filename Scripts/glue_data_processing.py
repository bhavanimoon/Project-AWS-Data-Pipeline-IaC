import sys
import boto3
import logging
import re
import datetime as dt
from datetime import datetime
from awsglue.context import GlueContext # type: ignore
from awsglue.utils import getResolvedOptions # type: ignore
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, when
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType


# Initialize Spark & Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Parameters
args = getResolvedOptions(sys.argv, ["BUCKET_NAME", "DATE_FOLDER"])
bucket_name = args["BUCKET_NAME"]
date_folder = args["DATE_FOLDER"]

input_prefix = f"validated-files/pass/{date_folder}/"
output_prefix = f"glue-job/output/{date_folder}/"
reject_prefix = f"glue-job/reject/{date_folder}/"

# Expected schema headers (Title Case)
expected_headers = [
    "Name", "Address", "Type", "Bedroom Limit",
    "Guest Limit", "Expiration Date", "Location", "X", "Y"
]

# --- Helper Functions (User Defined Function UDFs) ---
def strip_leading_digits(value):
    return re.sub(r'^\d+', '', value).strip().title() if value else value

def parse_date(value, default="30/12/2050"):
    if not value or not value.strip():  # case 1: blank/null
        return default
    formats = [
        "%m/%d/%Y %I:%M:%S %p",   # 05/13/2025 11:59:00 PM
        "%m-%d-%Y %I:%M:%S %p",   # 04-02-2025 11:59:00 PM
        "%d/%m/%Y",               # 13/05/2025
        "%d-%m-%Y",               # 13-05-2025
        "%m/%d/%Y %H:%M",         # 05/13/2025 23:59
        "%m-%d-%Y %H:%M",         # 04-02-2025 23:59
        "%d/%m/%Y %H:%M",         # 13/05/2025 23:59
        "%d-%m-%Y %H:%M"          # 13-05-2025 23:59
    ]
    for fmt in formats:
        try:
            return dt.datetime.strptime(value.strip(), fmt).strftime("%d/%m/%Y")
        except:
            continue
    return None  # case 2: unparsable non-empty string

def split_location(value):
    try:
        lat, lon = value.strip("()").split(",")
        return float(lat), float(lon)
    except:
        return 29.95, -90.07

strip_udf = udf(strip_leading_digits, StringType())
parse_date_udf = udf(lambda v: parse_date(v), StringType())
lat_udf = udf(lambda v: split_location(v)[0], DoubleType())
lon_udf = udf(lambda v: split_location(v)[1], DoubleType())

# --- Main Job ---
def process_file(file_key):
    filename = file_key.split("/")[-1].replace(".csv", "")
    df = spark.read.option("header", True).csv(f"s3://{bucket_name}/{file_key}")

    # Schema validation
    if [c.strip() for c in df.columns] != expected_headers:
        rej = df.withColumn("Reject_Reason", lit("Schema mismatch"))
        rej.write.mode("overwrite").option("header", True).csv(f"s3://{bucket_name}/{reject_prefix}{filename}_fail")
        return False

    # Row-level validation
    df = df.withColumn("Reject_Reason", lit(None))
    df = df.withColumn("Reject_Reason",
        when((col("Name").isNull()) & (col("Address").isNull()), lit("Missing mandatory fields"))
        .when(~col("Bedroom Limit").rlike("^[0-9]*$"), lit("Invalid Bedroom Limit"))
        .when(~col("Guest Limit").rlike("^[0-9]*$"), lit("Invalid Guest Limit"))
        .otherwise(col("Reject_Reason"))
    )

    # Split pass/reject
    pass_df = df.filter(col("Reject_Reason").isNull())
    reject_df = df.filter(col("Reject_Reason").isNotNull())

    # Transformations on pass_df
    pass_df = pass_df.withColumn("Name", strip_udf(col("Name"))) \
                     .withColumn("Address", when(col("Address").isNotNull(), col("Address")).otherwise(col("Name"))) \
                     .withColumn("Type", when(col("Type").isNull(), lit("Default Value - B&B")).otherwise(col("Type"))) \
                     .withColumn("Bedroom Limit", when(col("Bedroom Limit").rlike("^[0-9]+$"),
                                                       col("Bedroom Limit").cast(IntegerType()))
                                                       .otherwise(lit(0))) \
                     .withColumn("Guest Limit", when(col("Guest Limit").rlike("^[0-9]+$"),
                                                     col("Guest Limit").cast(IntegerType()))
                                                     .otherwise(lit(0))) \
                     .withColumn("Expiration Date", parse_date_udf(col("Expiration Date"))) \
                     .withColumn("Loc_Latitude", lat_udf(col("Location"))) \
                     .withColumn("Loc_Longitude", lon_udf(col("Location"))) \
                     .withColumn("GIS_Easting", when(col("X").isNotNull(), col("X").cast(IntegerType()))
                                                .otherwise(lit(3650000))) \
                     .withColumn("GIS_Northing", when(col("Y").isNotNull(), col("Y").cast(IntegerType()))
                                                 .otherwise(lit(500000))) \
                     .withColumn("Data_Lineage_ID", lit(f"{filename}_{datetime.now().strftime('%Y%m%d%H%M%S')}"))

    # Exclude Location, X, Y from final output
    output_columns = [c for c in pass_df.columns if c not in ["Location", "X", "Y"]]
    pass_df = pass_df.select(output_columns)

    # Write outputs
    if pass_df.count() > 0:
        pass_df.write.mode("overwrite").option("header", True).csv(f"s3://{bucket_name}/{output_prefix}{filename}_pass")
    if reject_df.count() > 0:
        reject_df.write.mode("overwrite").option("header", True).csv(f"s3://{bucket_name}/{reject_prefix}{filename}_fail")

    return pass_df.count() > 0

def glue_job_main():
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_prefix)
    files = [obj["Key"] for obj in response.get("Contents", [])]

    if not files:
        logger.warning("No validated files found")
        return {"status": "Fail"}

    any_pass = False
    for f in files:
        logger.info(f"Processing file: {f}")
        if process_file(f):
            any_pass = True

    return {"status": "Pass" if any_pass else "Fail"}

# Entry point
if __name__ == "__main__":
    glue_job_main()