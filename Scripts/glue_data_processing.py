import sys
import boto3
import logging
from datetime import datetime
from awsglue.context import GlueContext # type: ignore
from awsglue.utils import getResolvedOptions # type: ignore
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, IntegerType, DateType
import re
import datetime as dt

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

# --- Helper Functions ---
def title_case(value):
    return value.title() if value else value

def strip_leading_digits(value):
    return re.sub(r'^\d+', '', value).strip().title() if value else value

def cast_to_int(value, default=0):
    try:
        return int(value)
    except:
        return default

def parse_date(value, default="30/12/2050"):
    try:
        return dt.datetime.strptime(value, "%d/%m/%Y").date()
    except:
        return dt.datetime.strptime(default, "%d/%m/%Y").date()

def split_location(value):
    try:
        lat, lon = value.strip("()").split(",")
        return float(lat), float(lon)
    except:
        return 29.95, -90.07

# --- Main Job ---
def process_file(file_path):
    filename = file_path.split("/")[-1].replace(".csv", "")

    # Read CSV
    df = spark.read.option("header", True).csv(f"s3://{bucket_name}/{file_path}")

    pass_records = []
    reject_records = []

    # Step 2: Duplicate detection
    dup_df = df.groupBy(df.columns).count().filter(col("count") > 1).drop("count")
    if dup_df.count() > 0:
        dup_df = dup_df.withColumn("Reject_Reason", lit("Duplicate record"))
        reject_records.append(dup_df)
        df = df.subtract(dup_df.drop("Reject_Reason"))

    # Step 3: Schema validation
    if [c.strip() for c in df.columns] != expected_headers:
        rej = df.withColumn("Reject_Reason", lit("Schema mismatch"))
        reject_records.append(rej)
        return None, rej

    # Step 4: Row-level validation
    def validate_row(row):
        reasons = []
        if len(row) != len(expected_headers):
            reasons.append("Incomplete record - missing fields")
        if not row["Name"] and not row["Address"]:
            reasons.append("Missing mandatory fields")
        try:
            if row["Bedroom Limit"] and not row["Bedroom Limit"].isdigit():
                reasons.append("Invalid Bedroom/Guest Limit")
        except:
            reasons.append("Invalid Bedroom/Guest Limit")
        try:
            if row["Expiration Date"]:
                dt.datetime.strptime(row["Expiration Date"], "%d/%m/%Y")
        except:
            reasons.append("Invalid Expiration Date")
        return reasons

    validated = []
    rejected = []
    for r in df.collect():
        reasons = validate_row(r.asDict())
        if reasons:
            r_dict = r.asDict()
            r_dict["Reject_Reason"] = "; ".join(reasons)
            rejected.append(r_dict)
        else:
            validated.append(r.asDict())

    # Step 5: Transformations
    transformed = []
    for r in validated:
        r["Address"] = title_case(r["Address"]) if r["Address"] else r["Name"]
        r["Name"] = strip_leading_digits(r["Name"]) if r["Name"] else strip_leading_digits(r["Address"])
        r["Type"] = title_case(r["Type"]) if r["Type"] else "Default Value - B&B"
        r["Bedroom Limit"] = cast_to_int(r["Bedroom Limit"], 0)
        r["Guest Limit"] = cast_to_int(r["Guest Limit"], 0)
        r["Expiration Date"] = parse_date(r["Expiration Date"])
        lat, lon = split_location(r["Location"])
        r["Loc_Latitude"] = lat
        r["Loc_Longitude"] = lon
        r["GIS_Easting"] = int(r["X"]) if r["X"] else 3650000
        r["GIS_Northing"] = int(r["Y"]) if r["Y"] else 500000
        r["Data_Lineage_ID"] = f"{filename}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        transformed.append(r)

    # Convert to DataFrames
    pass_df = spark.createDataFrame(transformed)
    reject_df = spark.createDataFrame(rejected)

    # Step 7: Write outputs
    pass_df.write.mode("overwrite").option("header", True).csv(f"s3://{bucket_name}/{output_prefix}{filename}_pass.csv")
    reject_df.write.mode("overwrite").option("header", True).csv(f"s3://{bucket_name}/{reject_prefix}{filename}_fail.csv")

    return pass_df, reject_df

# --- Driver ---
def glue_job_main():
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_prefix)
    files = [obj["Key"] for obj in response.get("Contents", [])]

    if not files:
        logger.warning("No validated files found")
        return {"status": "Fail"}

    any_pass = False
    for f in files:
        pass_df, reject_df = process_file(f)
        if pass_df and pass_df.count() > 0:
            any_pass = True

    if any_pass:
        return {"status": "Pass"}
    else:
        return {"status": "Fail"}

# Entry point
if __name__ == "__main__":
    glue_job_main()