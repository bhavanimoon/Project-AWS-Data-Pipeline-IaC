import sys
import boto3
from datetime import datetime
from awsglue.job import Job # type: ignore
from awsglue.context import GlueContext # type: ignore
from awsglue.utils import getResolvedOptions # type: ignore
from pyspark import SparkContext
from pyspark.sql.functions import col, lit, when, regexp_replace, trim, initcap, to_date, split, coalesce, input_file_name, regexp_extract

# --- 1. CONTEXT INITIALIZATION & PARAMETER HANDLING ---
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

logger = glueContext.get_logger()

try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "DATE_FOLDER", "FILES", "VALIDATED_PREFIX"])
except Exception as e:
    logger.error(f"Failed to resolve job arguments during startup: {str(e)}")
    raise e

# --- CRITICAL CONFIGURATIONS LINKED NATIVELY WITHIN THE ENGINE ---
# This eliminates the need for any complex or invalid --conf setups inside Terraform maps
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket_name = args["BUCKET_NAME"]
date_folder = args["DATE_FOLDER"]
validated_prefix = args["VALIDATED_PREFIX"]
files_arg = args["FILES"]

s3 = boto3.client("s3")

# Define S3 input and output targets
input_path = f"s3://{bucket_name}/{validated_prefix}{date_folder}/*.csv"
output_base = f"s3://{bucket_name}/glue-job/output/{date_folder}/"
reject_base = f"s3://{bucket_name}/glue-job/reject/{date_folder}/"

# Expected schema headers (Title Case)
expected_headers = [
    "Name", "Address", "Type", "Bedroom Limit",
    "Guest Limit", "Expiration Date", "Location", "X", "Y"
]

# Validate files from Lambda & S3 bucket
def validate_input_files(bucket_name, validated_prefix, date_folder, files_arg):
    """
    Compare Lambda validated files with files currently available in S3.
    This is only for validation and logging.
    Spark will still process every file in the folder.
    """

    prefix = f"{validated_prefix}{date_folder}/"

    try:
        response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
        )
    except Exception as e:
        raise RuntimeError(f"Unable to read validated folder: {e}")

    s3_files = {
        obj["Key"].split("/")[-1]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".csv")
    }

    lambda_files = {
        f.strip()
        for f in files_arg.split(",")
        if f.strip()
    }

    if not lambda_files:
        logger.info("No files received from Lambda.")

    missing_files = lambda_files - s3_files
    extra_files = s3_files - lambda_files

    logger.info(f"Lambda files : {sorted(lambda_files)}")
    logger.info(f"S3 files     : {sorted(s3_files)}")

    if missing_files:
        logger.warn(
            f"Files expected from Lambda but missing in S3: "
            f"{sorted(missing_files)}"
        )

    if extra_files:
        logger.info(
            f"Additional files found in S3: "
            f"{sorted(extra_files)}"
        )

    if not s3_files:
        raise RuntimeError(
            "No CSV files found in validated folder."
        )

    return {
        "lambda_count": len(lambda_files),
        "s3_count": len(s3_files),
        "missing": sorted(missing_files),
        "extra": sorted(extra_files)
    }

def glue_job_main():
    """Main execution block processing datasets across parallel worker threads."""
    file_validations = validate_input_files(
        bucket_name,
        validated_prefix,
        date_folder,
        args["FILES"]
    )
    logger.info(f"Input files validation summary: {file_validations}")

    logger.info(f"Ingesting all matching target folder objects simultaneously from: {input_path}")    
    try:
        df = (
            spark.read
                .option("header", True)
                .option("inferSchema", False)
                .csv(input_path)
        )
        logger.info(f"Columns: {df.columns}")
        # logger.info(f"Rows: {df.count()}")
    except Exception as e:
        logger.warn(f"No files discovered or path read execution aborted: {str(e)}")
        raise
        
    if len(df.columns) == 0:
        logger.warn("No columns found.")
        raise RuntimeError("Input CSV has no schema.")

    # --- 2. PARALLEL LINEAGE & FILE ORIGIN TRACKING ---
    df = df.withColumn("_source_file_path", input_file_name())
    df = df.withColumn("Source_File_Name", regexp_extract(col("_source_file_path"), r"([^/]+)\.csv$", 1))

    # Clean and standardize headers
    normalized_headers = []
    for c in df.columns:
        if c not in ["_source_file_path", "Source_File_Name"]:
            normalized = c.lstrip("\ufeff").strip().title().replace("_", " ")
            normalized_headers.append(normalized)
            df = df.withColumnRenamed(c, normalized)

    if set(normalized_headers) != set(expected_headers):
        raise RuntimeError(
            f"Schema mismatch detected. Normalized headers: {normalized_headers}, Expected headers: {expected_headers}"
        )

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
        when(col("Expiration Date").isNull() | (trim(col("Expiration Date")) == ""), lit(datetime(2050,12,30).date()))
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
    pass_df = pass_df.withColumn("Expiration Date", when(col("Expiration Date").isNull(), lit(datetime(2050,12,30).date())).otherwise(col("Expiration Date")))

    # Geospatial component mapping
    clean_loc = regexp_replace(col("Location"), r'[\(\)]', '')
    loc_split = split(clean_loc, ",")
    pass_df = pass_df.withColumn("Loc_Latitude", coalesce(loc_split.getItem(0).cast("double"),lit(29.95)))
    pass_df = pass_df.withColumn("Loc_Longitude", coalesce(loc_split.getItem(1).cast("double"),lit(-90.07)))

    pass_df = pass_df.withColumn("GIS_Easting", coalesce(col("X").cast("int"), lit(3650000)))
    pass_df = pass_df.withColumn("GIS_Northing", coalesce(col("Y").cast("int"), lit(500000)))
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
        # logger.info(f"Valid rows: {pass_df.count()}")
    else:
        logger.info("No valid records found. Skipping pass output.")

    if reject_df.head(1):
        reject_df.write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(reject_base)
        # logger.info(f"Rejected rows: {reject_df.count()}")
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
        logger.info(f"Bucket={bucket_name}, Date={date_folder}, Input={input_path}, Output={output_base}")
        logger.error(f"Fatal processing error encountered by driver context. Aborting: {str(e)}")
        raise