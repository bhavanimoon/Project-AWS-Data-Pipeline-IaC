import sys
import boto3
import logging
import re
import datetime as dt
from datetime import datetime
from awsglue.job import Job # type: ignore
from awsglue.context import GlueContext # type: ignore
from awsglue.utils import getResolvedOptions # type: ignore
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_replace, trim, initcap, to_date, split, concat

# --- 1. INITIALIZATION & ENTRY HANDLING ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "DATE_FOLDER", "FILES"])
except Exception as e:
    logger.error(f"Failed to resolve job arguments during startup: {str(e)}")
    raise e

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket_name = args["BUCKET_NAME"]
date_folder = args["DATE_FOLDER"]
files_arg = args["FILES"]

input_prefix = f"validated-files/pass/{date_folder}/"
output_prefix = f"glue-job/output/{date_folder}/"
reject_prefix = f"glue-job/reject/{date_folder}/"

expected_headers = [
    "Name", "Address", "Type", "Bedroom Limit",
    "Guest Limit", "Expiration Date", "Location", "X", "Y"
]

def normalize_header(header):
    header = header.lstrip("\ufeff")
    return header.strip().title().replace("_", " ")

# --- Core Processing Logic ---
def process_file(file_key):
    filename = file_key.replace(".csv", "")
    df = spark.read.option("header", True).csv(f"s3://{bucket_name}/{input_prefix}/{file_key}")

    # Normalize columns natively
    normalized_headers = [normalize_header(c) for c in df.columns]
    df = df.toDF(*normalized_headers)

    if set(normalized_headers) != set(expected_headers):
        rej = df.withColumn("Reject_Reason", lit("Schema mismatch"))
        rej.write.mode("append").option("header", True).csv(f"s3://{bucket_name}/{reject_prefix}{filename}_fail")
        return False

    # Row-level validation using declarative Spark SQL operations
    df = df.withColumn(
        "Reject_Reason",
        when((col("Name").isNull()) & (col("Address").isNull()), lit("Missing mandatory fields"))
        .otherwise(
            when(~col("Bedroom Limit").rlike("^[0-9]*$"), lit("Invalid Bedroom Limit"))
            .otherwise(
                when(~col("Guest Limit").rlike("^[0-9]*$"), lit("Invalid Guest Limit"))
                .otherwise(lit(None))
            )
        )
    )

    # Cache conditionally to evaluate lineage efficiently without creating thread lock
    df.cache()

    pass_df = df.filter(col("Reject_Reason").isNull())
    reject_df = df.filter(col("Reject_Reason").isNotNull())

    # NATIVE JVM TRANSFORMATIONS (Eliminating Python UDF performance penalties & thread blocks)
    # Strip leading digits and title-case strings natively
    pass_df = pass_df.withColumn("Name", initcap(trim(regexp_replace(col("Name"), r'^\d+', ''))))
    pass_df = pass_df.withColumn("Address", when(col("Address").isNotNull(), col("Address")).otherwise(col("Name")))
    pass_df = pass_df.withColumn("Type", when(col("Type").isNull(), lit("Default Value - B&B")).otherwise(col("Type")))
    
    pass_df = pass_df.withColumn("Bedroom Limit", when(col("Bedroom Limit").rlike("^[0-9]+$"), col("Bedroom Limit").cast("int")).otherwise(lit(0)))
    pass_df = pass_df.withColumn("Guest Limit", when(col("Guest Limit").rlike("^[0-9]+$"), col("Guest Limit").cast("int")).otherwise(lit(0)))

    # Native Multi-Format Date Handling
    pass_df = pass_df.withColumn(
        "Expiration Date",
        when(col("Expiration Date").isNull() | (trim(col("Expiration Date")) == ""), lit("30/12/2050"))
        .otherwise(
            # Coalesce standard formats natively inside the JVM execution pool
            coalesce(
                to_date(col("Expiration Date"), "MM/dd/yyyy hh:mm:ss a"),
                to_date(col("Expiration Date"), "MM-dd-yyyy hh:mm:ss a"),
                to_date(col("Expiration Date"), "dd/MM/yyyy"),
                to_date(col("Expiration Date"), "dd-MM-yyyy"),
                to_date(col("Expiration Date"), "MM/dd/yyyy HH:mm"),
                to_date(col("Expiration Date"), "MM-dd-yyyy HH:mm")
            ).cast("string")
        )
    )
    # Handle Default String Date Fallback if unparsable
    pass_df = pass_df.withColumn("Expiration Date", when(col("Expiration Date").isNull(), lit("30/12/2050")).otherwise(col("Expiration Date")))

    # Native Parsing for Coordinates
    clean_loc = regexp_replace(col("Location"), r'[\(\)]', '')
    loc_split = split(clean_loc, ",")
    pass_df = pass_df.withColumn("Loc_Latitude", when(loc_split.getItem(0).isNotNull(), loc_split.getItem(0).cast("double")).otherwise(lit(29.95)))
    pass_df = pass_df.withColumn("Loc_Longitude", when(loc_split.getItem(1).isNotNull(), loc_split.getItem(1).cast("double")).otherwise(lit(-90.07)))

    pass_df = pass_df.withColumn("GIS_Easting", when(col("X").isNotNull(), col("X").cast("int")).otherwise(lit(3650000)))
    pass_df = pass_df.withColumn("GIS_Northing", when(col("Y").isNotNull(), col("Y").cast("int")).otherwise(lit(500000)))
    pass_df = pass_df.withColumn("Data_Lineage_ID", lit(f"{filename}_{datetime.now().strftime('%Y%m%d%H%M%S')}"))

    output_columns = [c for c in pass_df.columns if c not in ["Location", "X", "Y", "Reject_Reason"]]
    pass_df = pass_df.select(output_columns)

    # --- WRITING DATA DIRECTLY ---
    # Safe empty assessment via underlying RDD metadata partition status instead of invoking .head(1) thread locks
    has_pass_records = not pass_df.rdd.isEmpty()
    if has_pass_records:
        pass_df.write.mode("overwrite").option("header", True).csv(f"s3://{bucket_name}/{output_prefix}{filename}_pass")
    
    has_fail_records = not reject_df.rdd.isEmpty()
    if has_fail_records:
        reject_df.write.mode("overwrite").option("header", True).csv(f"s3://{bucket_name}/{reject_prefix}{filename}_fail")

    df.unpersist()
    return has_pass_records

# Native fallback utility for date evaluation matching coalesce
from pyspark.sql.functions import coalesce

def glue_job_main():
    files_from_lambda = files_arg.split(",") if files_arg else []
    logger.info(f"Files passed from Lambda: {files_from_lambda}")

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_prefix)
    
    files_from_s3 = [
        obj["Key"].split("/")[-1] for obj in response.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]
    logger.info(f"Files discovered in S3: {files_from_s3}")

    all_files = sorted(set(files_from_lambda + files_from_s3))
    if not all_files:
        logger.warning("No validated files found to process.")
        return False

    any_pass = False
    for f in all_files:
        logger.info(f"Processing file: {f}")
        try:
            if process_file(f):
                any_pass = True
        except Exception as e:
            logger.error(f"Error processing {f}: {str(e)}")
            continue
    
    return any_pass

# --- 2. EXIT & CLEANUP HANDLING ---
if __name__ == "__main__":
    job_failed = False
    try:
        success = glue_job_main()
        if success:
            logger.info("Glue job pipeline completed successfully.")
        else:
            logger.warning("Glue job pipeline executed, but no passing records produced.")
            
    except Exception as e:
        logger.critical(f"Glue job execution encountered an unhandled exception: {str(e)}")
        job_failed = True
        
    finally:
        logger.info("Starting teardown and context cleanup...")
        try:
            job.commit()
            logger.info("Glue job object committed successfully.")
        except Exception as commit_error:
            logger.error(f"Error tracking job commit: {str(commit_error)}")
        
        try:
            # Context cleanup triggers safely and instantly because JVM worker daemons are not locked
            spark.stop()
            logger.info("Spark session explicitly stopped.")
        except Exception as spark_error:
            logger.error(f"Error closing Spark context: {str(spark_error)}")
            
        if job_failed:
            sys.exit("Job execution failed due to critical error.")