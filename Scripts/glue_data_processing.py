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

# --- 1. CONTEXT INITIALIZATION & PARAMETER HANDLING ---
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

logger = glueContext.get_logger()
logger.setLevel(logging.INFO)

try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "DATE_FOLDER", "FILES"])
except Exception as e:
    logger.error(f"Failed to resolve job arguments during startup: {str(e)}")
    raise e

# --- CRITICAL CONFIGURATIONS LINKED NATIVELY WITHIN THE ENGINE ---
# This eliminates the need for any complex or invalid --conf setups inside Terraform maps
spark.conf.set("spark.task.maxFailures", "1")
spark.conf.set("spark.speculation", "false")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket_name = args["BUCKET_NAME"]
date_folder = args["DATE_FOLDER"]

# Define S3 input and output targets
input_path = f"s3://{bucket_name}/validated-files/pass/{date_folder}/*.csv"
output_base = f"s3://{bucket_name}/glue-job/output/{date_folder}/"
reject_base = f"s3://{bucket_name}/glue-job/reject/{date_folder}/"

def glue_job_main():
    """Main execution block processing datasets across parallel worker threads."""
    logger.info(f"Ingesting all matching target folder objects simultaneously from: {input_path}")
    
    try:
        df = spark.read.option("header", True).csv(input_path)
        logger.info(f"Columns: {df.columns}")
        logger.info(f"Rows: {df.count()}")
    except Exception as e:
        logger.warning(f"No files discovered or path read execution aborted: {str(e)}")
        # return True
        raise
        
    if len(df.columns) == 0:
        logger.warning("No columns found.")
        raise RuntimeError("Input CSV has no schema.")

    # --- 2. PARALLEL LINEAGE & FILE ORIGIN TRACKING ---
    df = df.withColumn("_source_file_path", input_file_name())
    df = df.withColumn("Source_File_Name", regexp_extract(col("_source_file_path"), r"([^/]+)\.csv$", 1))

    # Clean and standardize headers
    for c in df.columns:
        if c not in ["_source_file_path", "Source_File_Name"]:
            normalized = c.lstrip("\ufeff").strip().title().replace("_", " ")
            df = df.withColumnRenamed(c, normalized)

    # --- 3. DECLARATIVE ROW-LEVEL VALIDATIONS ---
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

    # --- 4. DATA TRANSFORMATION PIPELINES ---
    pass_df = pass_df.withColumn("Name", initcap(trim(regexp_replace(col("Name"), r'^\d+', ''))))
    pass_df = pass_df.withColumn("Address", when(col("Address").isNotNull(), col("Address")).otherwise(col("Name")))
    pass_df = pass_df.withColumn("Type", when(col("Type").isNull(), lit("Default Value - B&B")).otherwise(col("Type")))
    pass_df = pass_df.withColumn("Bedroom Limit", when(col("Bedroom Limit").rlike("^[0-9]+$"), col("Bedroom Limit").cast("int")).otherwise(lit(0)))
    pass_df = pass_df.withColumn("Guest Limit", when(col("Guest Limit").rlike("^[0-9]+$"), col("Guest Limit").cast("int")).otherwise(lit(0)))

    # Multi-Format Date Strings parsing
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

    # Geospatial component mapping
    clean_loc = regexp_replace(col("Location"), r'[\(\)]', '')
    loc_split = split(clean_loc, ",")
    pass_df = pass_df.withColumn("Loc_Latitude", when(loc_split.getItem(0).isNotNull(), loc_split.getItem(0).cast("double")).otherwise(lit(29.95)))
    pass_df = pass_df.withColumn("Loc_Longitude", when(loc_split.getItem(1).isNotNull(), loc_split.getItem(1).cast("double")).otherwise(lit(-90.07)))

    pass_df = pass_df.withColumn("GIS_Easting", when(col("X").isNotNull(), col("X").cast("int")).otherwise(lit(3650000)))
    pass_df = pass_df.withColumn("GIS_Northing", when(col("Y").isNotNull(), col("Y").cast("int")).otherwise(lit(500000)))
    pass_df = pass_df.withColumn("Data_Lineage_ID", col("Source_File_Name"))

    # Select output columns
    final_pass_cols = [c for c in pass_df.columns if c not in ["Location", "X", "Y", "Reject_Reason", "_source_file_path", "Source_File_Name"]]
    pass_df = pass_df.select(final_pass_cols)

    final_fail_cols = [c for c in reject_df.columns if c not in ["_source_file_path", "Source_File_Name"]]
    reject_df = reject_df.select(final_fail_cols)

    # --- 5. SAFE OVERWRITE WRITES ---
    logger.info("Committing parsed rows down to S3...")
        
    if pass_df.head(1):
        pass_df.write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(output_base)
        logger.info(f"Valid rows: {pass_df.count()}")
    else:
        logger.info("No valid records found. Skipping pass output.")

    if reject_df.head(1):
        reject_df.write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(reject_base)
        logger.info(f"Rejected rows: {reject_df.count()}")
    else:
        logger.info("No rejected records found.")

    return True

# --- 6. ENTRY POINT ---
if __name__ == "__main__":
    try:
        glue_job_main()
        logger.info("Glue job pipeline completed successfully. Proceeding to natural exit.")
        job.commit()
    except Exception as e:
        logger.exception(f"Fatal processing error encountered by driver context. Aborting: {str(e)}")
        raise
    finally:
        spark.stop()