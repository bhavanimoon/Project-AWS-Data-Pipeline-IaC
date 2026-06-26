import sys
import boto3
import logging
import traceback
import datetime as dt
from datetime import datetime
from awsglue.job import Job # type: ignore
from awsglue.context import GlueContext # type: ignore
from awsglue.utils import getResolvedOptions # type: ignore
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_replace, trim, initcap, to_date, split, coalesce

# --- 1. INITIALIZATION & PARAMETER HANDLING ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "DATE_FOLDER", "FILES"])
except Exception as e:
    logger.error(f"Failed to resolve job arguments during startup: {str(e)}")
    raise e

# Initialize Spark & Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# CRITICAL FIX: Explicitly guarantee the legacy time parser policy is active inside the JVM engine pool
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

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

def clear_s3_prefix(bucket, prefix):
    """Utility to safely wipe out stale files to avoid FileAlreadyExistsException errors."""
    s3 = boto3.resource("s3")
    bucket_obj = s3.Bucket(bucket)
    bucket_obj.objects.filter(Prefix=prefix).delete()

# --- Core Processing Logic ---
def process_file(file_key):
    filename = file_key.replace(".csv", "")
    df = spark.read.option("header", True).csv(f"s3://{bucket_name}/{input_prefix}/{file_key}")

    normalized_headers = [normalize_header(c) for c in df.columns]
    df = df.toDF(*normalized_headers)

    # Check structural integrity
    if set(normalized_headers) != set(expected_headers):
        rej_path = f"{reject_prefix}{filename}_fail"
        clear_s3_prefix(bucket_name, rej_path)
        rej = df.withColumn("Reject_Reason", lit("Schema mismatch"))
        rej.write.mode("overwrite").option("header", True).csv(f"s3://{bucket_name}/{rej_path}")
        return False

    # Declarative row-level validations
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

    pass_df = df.filter(col("Reject_Reason").isNull())
    reject_df = df.filter(col("Reject_Reason").isNotNull())

    # --- JVM Transformations ---
    pass_df = pass_df.withColumn("Name", initcap(trim(regexp_replace(col("Name"), r'^\d+', ''))))
    pass_df = pass_df.withColumn("Address", when(col("Address").isNotNull(), col("Address")).otherwise(col("Name")))
    pass_df = pass_df.withColumn("Type", when(col("Type").isNull(), lit("Default Value - B&B")).otherwise(col("Type")))
    
    pass_df = pass_df.withColumn("Bedroom Limit", when(col("Bedroom Limit").rlike("^[0-9]+$"), col("Bedroom Limit").cast("int")).otherwise(lit(0)))
    pass_df = pass_df.withColumn("Guest Limit", when(col("Guest Limit").rlike("^[0-9]+$"), col("Guest Limit").cast("int")).otherwise(lit(0)))

    # Handle Multi-Format Date Strings safely
    pass_df = pass_df.withColumn(
        "Expiration Date",
        when(col("Expiration Date").isNull() | (trim(col("Expiration Date")) == ""), lit("30/12/2050"))
        .otherwise(
            coalesce(
                to_date(col("Expiration Date"), "MM/dd/yyyy hh:mm:ss a"),
                to_date(col("Expiration Date"), "MM-dd-yyyy hh:mm:ss a"),
                to_date(col("Expiration Date"), "dd/MM/yyyy"),
                to_date(col("Expiration Date"), "dd-MM-yyyy"),
                to_date(col("Expiration Date"), "MM/dd/yyyy HH:mm"),
                to_date(col("Expiration Date"), "MM-dd-yyyy HH:mm"),
                to_date(col("Expiration Date"), "dd/MM/yyyy HH:mm"),
                to_date(col("Expiration Date"), "dd-MM-yyyy HH:mm")
            ).cast("string")
        )
    )
    pass_df = pass_df.withColumn("Expiration Date", when(col("Expiration Date").isNull(), lit("30/12/2050")).otherwise(col("Expiration Date")))

    clean_loc = regexp_replace(col("Location"), r'[\(\)]', '')
    loc_split = split(clean_loc, ",")
    pass_df = pass_df.withColumn("Loc_Latitude", when(loc_split.getItem(0).isNotNull(), loc_split.getItem(0).cast("double")).otherwise(lit(29.95)))
    pass_df = pass_df.withColumn("Loc_Longitude", when(loc_split.getItem(1).isNotNull(), loc_split.getItem(1).cast("double")).otherwise(lit(-90.07)))

    pass_df = pass_df.withColumn("GIS_Easting", when(col("X").isNotNull(), col("X").cast("int")).otherwise(lit(3650000)))
    pass_df = pass_df.withColumn("GIS_Northing", when(col("Y").isNotNull(), col("Y").cast("int")).otherwise(lit(500000)))
    pass_df = pass_df.withColumn("Data_Lineage_ID", lit(f"{filename}_{datetime.now().strftime('%Y%m%d%H%M%S')}"))

    output_columns = [c for c in pass_df.columns if c not in ["Location", "X", "Y", "Reject_Reason"]]
    pass_df = pass_df.select(output_columns)

    # --- Safe Execution Boundaries ---
    pass_path = f"{output_prefix}{filename}_pass"
    fail_path = f"{reject_prefix}{filename}_fail"
    
    # Pre-clear directories to prevent DirectFileOutputCommitter naming blockades
    clear_s3_prefix(bucket_name, pass_path)
    clear_s3_prefix(bucket_name, fail_path)

    # Direct native save executions
    pass_df.write.mode("overwrite").option("header", True).csv(f"s3://{bucket_name}/{pass_path}")
    reject_df.write.mode("overwrite").option("header", True).csv(f"s3://{bucket_name}/{fail_path}")

    return True

def glue_job_main():
    files_from_lambda = files_arg.split(",") if files_arg else []
    
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_prefix)
    
    files_from_s3 = [
        obj["Key"].split("/")[-1] for obj in response.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]

    all_files = sorted(set(files_from_lambda + files_from_s3))
    if not all_files:
        logger.warning("No validated files found to process.")
        return True

    failed_files = []
    for f in all_files:
        logger.info(f"Processing file: {f}")
        try:
            process_file(f)
        except Exception as e:
            # Capture the exact line and failure code block out of PySpark
            err_msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            logger.error(f"CRITICAL: Application layer failed processing {f}:\n{err_msg}")
            failed_files.append(f)
            
    if failed_files:
        raise RuntimeError(f"Pipeline finished with partial failures. Files that crashed: {failed_files}")
    
    return True

if __name__ == "__main__":
    try:
        glue_job_main()
        logger.info("Glue job pipeline completed successfully. Proceeding to natural exit.")
        job.commit()
    except Exception as e:
        logger.critical(f"Glue job run aborted: {str(e)}")
        sys.exit(1) # Bubble immediately up to the Step Function orchestrator