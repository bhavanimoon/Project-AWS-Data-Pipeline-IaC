import boto3
import csv
import io
import logging
from datetime import datetime

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variable for bucket name (set in Lambda configuration)
# Not used now. bucket name is passed from Event Bridge input via Step Functions
# BUCKET_NAME = os.environ.get("BUCKET_NAME")

# Prefixes
INPUT_PREFIX = "input/"
VALIDATED_PASS_PREFIX = "validated-files/pass/"
VALIDATED_FAIL_PREFIX = "validated-files/fail/"

# Expected schema headers (Title Case)
EXPECTED_HEADERS = [
    "Name", "Address", "Type", "Bedroom Limit",
    "Guest Limit", "Expiration Date", "Location", "X", "Y"
]

def normalize_header(header):
    """Normalize header text for comparison."""
    header = header.lstrip("\ufeff")
    normalized = header.strip().title().replace("_", " ")
    return normalized

def validate_headers(headers):
    """Validate headers after normalization."""
    normalized = [normalize_header(h) for h in headers]
    logger.info(f"Normalized headers: {normalized}")
    logger.info(f"Expected headers: {EXPECTED_HEADERS}")
    return set(EXPECTED_HEADERS) == set(normalized)
    # return set(EXPECTED_HEADERS).issubset(set(normalized))

def move_file(s3, bucket_name, file_key, destination_prefix, date_folder):
    """Move file to pass/fail folder with date subfolder."""
    dest_key = f"{destination_prefix}{date_folder}/{file_key.split('/')[-1]}"
    s3.copy_object(Bucket=bucket_name,
                   CopySource={'Bucket': bucket_name, 'Key': file_key},
                   Key=dest_key)
    s3.delete_object(Bucket=bucket_name, Key=file_key)
    logger.info(f"Moved {file_key} → {dest_key}")

def lambda_handler(event, context):
    """Lambda entry point for preliminary checks."""
    s3 = boto3.client('s3')    
    bucket_name = event.get("bucket")   # Get bucket name from Event Bridge input passed via Step Functions

    try:
        # Step 0: Get today's date folder (ddmmyyyy)
        date_folder = datetime.now().strftime("%d%m%Y")

        # Step 1: List all files in today's input folder
        input_prefix_today = f"{INPUT_PREFIX}{date_folder}/"
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_prefix_today)
        files = [
            obj['Key'] for obj in response.get('Contents', [])
            if not obj['Key'].endswith('/')
        ]

        # Defensive patch
        if not bucket_name:
            return {
                "status": "Fail",
                "reason": "Bucket name missing in event"
            }

        if not files:
            logger.warning(f"No files found in {input_prefix_today}")
            return {
                "status": "Fail",
                "bucket": bucket_name,
                "files": [],
                "date_folder": date_folder,
                "validated_prefix": VALIDATED_FAIL_PREFIX,
                "reason": "No input files"
            }

        passed_files = []

        for file_key in files:
            logger.info(f"Validating file: {file_key}")

            # Step 2: Basic checks
            if not file_key.endswith(".csv"):
                move_file(s3, bucket_name, file_key, VALIDATED_FAIL_PREFIX, date_folder)
                logger.error("Invalid file format")
                continue

            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            file_content = response['Body'].read().decode('utf-8')
            reader = csv.reader(io.StringIO(file_content))
            rows = list(reader)

            if not rows or len(rows) < 2:
                move_file(s3, bucket_name, file_key, VALIDATED_FAIL_PREFIX, date_folder)
                logger.error("Empty file or no data rows")
                continue

            headers = rows[0]
            if not validate_headers(headers):
                move_file(s3, bucket_name, file_key, VALIDATED_FAIL_PREFIX, date_folder)
                logger.error("Header mismatch")
                continue

            # Step 3: Mandatory fields check
            try:
                normalized_headers = [normalize_header(h) for h in headers]
                name_idx = normalized_headers.index("Name")
                address_idx = normalized_headers.index("Address")
            except ValueError:
                move_file(s3, bucket_name, file_key, VALIDATED_FAIL_PREFIX, date_folder)
                logger.error("Mandatory headers missing")
                continue

            valid_rows = [
                r for r in rows[1:]
                if len(r) > max(name_idx, address_idx)
                and (r[name_idx].strip() or r[address_idx].strip())
            ]

            if not valid_rows:
                move_file(s3, bucket_name, file_key, VALIDATED_FAIL_PREFIX, date_folder)
                logger.error("Missing mandatory fields")
                continue

            # Step 4: Passed preliminary checks
            move_file(s3, bucket_name, file_key, VALIDATED_PASS_PREFIX, date_folder)
            temp_file_key = f"{file_key.split('/')[-1]}"
            passed_files.append(temp_file_key)
            logger.info("Validation passed")
            logger.info(f"Passed Files -> {passed_files}")

        # Step 5: After all files processed
        if passed_files:
            return {
                "status": "Pass",
                "bucket": bucket_name,
                "files": ",".join(passed_files),
                "date_folder": date_folder,
                "validated_prefix": VALIDATED_PASS_PREFIX
            }
        else:
            return {
                "status": "Fail",
                "bucket": bucket_name,
                "files": [],
                "date_folder": date_folder,
                "validated_prefix": VALIDATED_FAIL_PREFIX,
                "reason": "No files passed validation"
            }

    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        return {
            "status": "Fail",
            "bucket": bucket_name,
            "files": [],
            "date_folder": date_folder,
            "validated_prefix": VALIDATED_FAIL_PREFIX,
            "reason": str(e)
        }