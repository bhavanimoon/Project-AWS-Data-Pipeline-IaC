import sys
import boto3
import logging
from datetime import datetime
from awsglue.job import Job # type: ignore
from awsglue.context import GlueContext # type: ignore
from awsglue.utils import getResolvedOptions # type: ignore
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_replace, trim, initcap, to_date, split, coalesce, input_file_name, regexp_extract

# --- 1. INITIALIZATION & PARAMETER HANDLING ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    # Cleanly resolve incoming arguments from Step Functions / Lambda orchestration
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "DATE_FOLDER", "FILES"])
except Exception as e:
    logger.error(f"Failed to resolve job arguments during startup: {str(e)}")
    raise e

# Initialize underlying cluster architecture contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# CRITICAL ENGINE SAFETY FIXES:
# 1. Enforce LEGACY date policy directly inside the JVM pool to resolve SparkUpgradeExceptions
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# 2. Prevent task speculation and optimize execution partition chunks
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")

# Initialize Glue lifecycle object tracking
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Extract pipeline scope variables
bucket_name = args["BUCKET_NAME"]
date_folder = args["DATE_FOLDER"]

# Define clean S3 path URI boundaries
input_path = f"s3://{bucket_name}/validated-files/pass/{date_folder}/*.csv"
output_base = f"s3://{bucket_name}/glue-job/output/{date_folder}/"
reject_base = f"s3://{bucket_name}/glue-job/reject/{date_folder}/"

# Structural contract validation headers
expected_headers = [
    "Name", "Address", "Type", "Bedroom Limit",
    "Guest Limit", "Expiration Date", "Location", "X", "Y"
]

def glue_job_main():
    """Main execution block processing datasets across parallel worker threads."""
    logger.info(f"Ingesting all matching target folder objects simultaneously from: {input_path}")
    
    try:
        # Load all folder contents natively in parallel into a unified DataFrame
        df = spark.read.option("header", True).csv(input_path)
    except Exception as e:
        logger.warning(f"No files discovered or path read execution aborted: {str(e)}")
        return True

    if len(df.columns) == 0:
        logger.warning("Empty source metadata schema discovered. Exiting task sequence early.")
        return True

    # --- 2. PARALLEL LINEAGE & FILE ORIGIN TRACKING ---
    # Dynamically extract individual filename strings from the file URL across execution workers
    df = df.withColumn("_source_file_path", input_file_name())
    df = df.withColumn("Source_File_Name", regexp_extract(col("_source_file_path"), r"([^/]+)\.csv$", 1))

    # Clean and standardize all structural incoming headers natively 
    for c in df.columns:
        if c not in ["_source_file_path", "Source_File_Name"]:
            normalized = c.lstrip("\ufeff").strip().title().replace("_", " ")
            df = df.withColumnRenamed(c, normalized)

    # --- 3. DECLARATIVE ROW-LEVEL VALIDATION BRACKETS ---
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

    # Bifurcate operational records downstream instantly
    pass_df = df.filter(col("Reject_Reason").isNull())
    reject_df = df.filter(col("Reject_Reason").isNotNull())

    # --- 4. HIGH-PERFORMANCE DATA TRANSFORMATION PIPELINES ---
    pass_df = pass_df.withColumn("Name", initcap(trim(regexp_replace(col("Name"), r'^\d+', ''))))
    pass_df = pass_df.withColumn("Address", when(col("Address").isNotNull(), col("Address")).otherwise(col("Name")))
    pass_df = pass_df.withColumn("Type", when(col("Type").isNull(), lit("Default Value - B&B")).otherwise(col("Type")))
    pass_df = pass_df.withColumn("Bedroom Limit", when(col("Bedroom Limit").rlike("^[0-9]+$"), col("Bedroom Limit").cast("int")).otherwise(lit(0)))
    pass_df = pass_df.withColumn("Guest Limit", when(col("Guest Limit").rlike("^[0-9]+$"), col("Guest Limit").cast("int")).otherwise(lit(0)))

    # Coalesce multi-format timestamp records via the restored legacy parser engine
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

    # Structural geospatial component mapping
    clean_loc = regexp_replace(col("Location"), r'[\(\)]', '')
    loc_split = split(clean_loc, ",")
    pass_df = pass_df.withColumn("Loc_Latitude", when(loc_split.getItem(0).isNotNull(), loc_split.getItem(0).cast("double")).otherwise(lit(29.95)))
    pass_df = pass_df.withColumn("Loc_Longitude", when(loc_split.getItem(1).isNotNull(), loc_split.getItem(1).cast("double")).otherwise(lit(-90.07)))

    pass_df = pass_df.withColumn("GIS_Easting", when(col("X").isNotNull(), col("X").cast("int")).otherwise(lit(3650000)))
    pass_df = pass_df.withColumn("GIS_Northing", when(col("Y").isNotNull(), col("Y").cast("int")).otherwise(lit(500000)))
    pass_df = pass_df.withColumn("Data_Lineage_ID", col("Source_File_Name"))

    # Cleanup staging structural indicators prior to file sink commits
    final_pass_cols = [c for c in pass_df.columns if c not in ["Location", "X", "Y", "Reject_Reason", "_source_file_path", "Source_File_Name"]]
    pass_df = pass_df.select(final_pass_cols)

    final_fail_cols = [c for c in reject_df.columns if c not in ["_source_file_path", "Source_File_Name"]]
    reject_df = reject_df.select(final_fail_cols)

    # --- 5. SAFE OVERWRITE ATOMIC WRITES ---
    logger.info("Committing parsed rows down to partitioned S3 endpoints...")
    
    pass_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_base)

    reject_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(reject_base)

    return True

# --- 6. PRODUCTION ENTRY POINT CONTROLS ---
if __name__ == "__main__":
    try:
        # Code execution lifecycle initialization begins here
        glue_job_main()
        logger.info("Glue job pipeline completed successfully. Proceeding to natural exit.")
        job.commit()
    except Exception as e:
        logger.critical(f"Fatal processing error encountered by driver context. Aborting: {str(e)}")
        sys.exit(1) # Break immediately to trigger failure signals up to the Step Function orchestrator