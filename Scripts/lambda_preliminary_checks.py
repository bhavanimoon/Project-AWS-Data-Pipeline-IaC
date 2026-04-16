import boto3
import csv
import io
import logging
from datetime import datetime

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variable for bucket name
BUCKET_NAME = "your-bucket-name"

# Prefixes
INPUT_PREFIX = "input/"
VALIDATED_PASS_PREFIX = "validated-files/pass/"
VALIDATED_FAIL_PREFIX = "validated-files/fail/"

# Expected raw schema (normalized)
EXPECTED_HEADERS = [
    "name", "address", "type", "bedroom limit",
    "guest limit", "expiration date", "location", "x", "y"
]

def normalize_header(header):
    """Normalize header text for comparison."""
    return header.strip().lower().replace("_", " ")

def validate_headers(headers):
    """Validate headers after normalization."""
    normalized = [normalize_header(h) for h in headers]
    return normalized == EXPECTED_HEADERS

def move_file(s3, file_key, destination_prefix, date_folder):
    """Move file to pass/fail folder with date subfolder."""
    dest_key = f"{destination_prefix}{date_folder}/{file_key.split('/')[-1]}"
    s3.copy_object(Bucket=BUCKET_NAME,
                   CopySource={'Bucket': BUCKET_NAME, 'Key': file_key},
                   Key=dest_key)
    s3.delete_object(Bucket=BUCKET_NAME, Key=file_key)
    logger.info(f"Moved {file_key} → {dest_key}")

def lambda_handler(event, context):
    """Lambda entry point for preliminary checks."""
    s3 = boto3.client('s3')

    try:
        # Step 0: Get today's date folder (ddmmyyyy)
        date_folder = datetime.now().strftime("%d%m%Y")

        # Step 1: List all files in today's input folder
        input_prefix_today = f"{INPUT_PREFIX}{date_folder}/"
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=input_prefix_today)
        files = [obj['Key'] for obj in response.get('Contents', [])]

        if not files:
            logger.warning(f"No files found in {input_prefix_today}")
            return {"status": "Fail", "reason": "No input files"}

        passed_files = []

        for file_key in files:
            logger.info(f"Validating file: {file_key}")

            # Step 2: Basic checks
            if not file_key.endswith(".csv"):
                move_file(s3, file_key, VALIDATED_FAIL_PREFIX, date_folder)
                logger.error("Invalid file format")
                continue

            response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
            file_content = response['Body'].read().decode('utf-8')
            reader = csv.reader(io.StringIO(file_content))
            rows = list(reader)

            if not rows or len(rows) < 2:
                move_file(s3, file_key, VALIDATED_FAIL_PREFIX, date_folder)
                logger.error("Empty file or no data rows")
                continue

            headers = rows[0]
            if not validate_headers(headers):
                move_file(s3, file_key, VALIDATED_FAIL_PREFIX, date_folder)
                logger.error("Header mismatch")
                continue

            # Step 3: Mandatory fields check
            name_idx = headers.index("Name")
            address_idx = headers.index("Address")
            valid_rows = [
                r for r in rows[1:]
                if len(r) > max(name_idx, address_idx)
                and (r[name_idx].strip() or r[address_idx].strip())
            ]

            if not valid_rows:
                move_file(s3, file_key, VALIDATED_FAIL_PREFIX, date_folder)
                logger.error("Missing mandatory fields")
                continue

            # Step 4: Passed preliminary checks
            move_file(s3, file_key, VALIDATED_PASS_PREFIX, date_folder)
            passed_files.append(file_key)
            logger.info("Validation passed")

        # Step 5: After all files processed
        if passed_files:
            return {"status": "Pass", "files": passed_files, "date_folder": date_folder}
        else:
            return {"status": "Fail", "reason": "No files passed validation", "date_folder": date_folder}

    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        return {"status": "Fail", "reason": str(e)}


# """ 
# Commented - Version 2 of Lambda preliminary checks function: 
# import boto3
# import csv
# import io
# import logging

# # Initialize logger
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# # Environment variable for bucket name
# # (Set this in Lambda configuration)
# BUCKET_NAME = "your-bucket-name"
# INPUT_PREFIX = "input/"
# VALIDATED_PASS_PREFIX = "validated-files/pass/"
# VALIDATED_FAIL_PREFIX = "validated-files/fail/"

# # Expected raw schema (normalized)
# EXPECTED_HEADERS = [
#     "name", "address", "type", "bedroom limit",
#     "guest limit", "expiration date", "location", "x", "y"
# ]

# def normalize_header(header):
#     """Normalize header text for comparison."""
#     return header.strip().lower().replace("_", " ")

# def validate_headers(headers):
#     """Validate headers after normalization."""
#     normalized = [normalize_header(h) for h in headers]
#     return normalized == EXPECTED_HEADERS

# def move_file(s3, file_key, destination_prefix):
#     """Move file to pass/fail folder."""
#     dest_key = destination_prefix + file_key.split("/")[-1]
#     s3.copy_object(Bucket=BUCKET_NAME,
#                    CopySource={'Bucket': BUCKET_NAME, 'Key': file_key},
#                    Key=dest_key)
#     s3.delete_object(Bucket=BUCKET_NAME, Key=file_key)
#     logger.info(f"Moved {file_key} → {dest_key}")

# def lambda_handler(event, context):
#     """Lambda entry point for preliminary checks."""
#     s3 = boto3.client('s3')

#     try:
#         # Step 1: List all files in input folder
#         response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=INPUT_PREFIX)
#         files = [obj['Key'] for obj in response.get('Contents', [])]

#         if not files:
#             logger.warning("No files found in input folder.")
#             return {"status": "Fail", "reason": "No input files"}

#         passed_files = []

#         for file_key in files:
#             logger.info(f"Validating file: {file_key}")

#             # Step 2: Basic checks
#             if not file_key.endswith(".csv"):
#                 move_file(s3, file_key, VALIDATED_FAIL_PREFIX)
#                 logger.error("Invalid file format")
#                 continue

#             response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
#             file_content = response['Body'].read().decode('utf-8')
#             reader = csv.reader(io.StringIO(file_content))
#             rows = list(reader)

#             if not rows or len(rows) < 2:
#                 move_file(s3, file_key, VALIDATED_FAIL_PREFIX)
#                 logger.error("Empty file or no data rows")
#                 continue

#             headers = rows[0]
#             if not validate_headers(headers):
#                 move_file(s3, file_key, VALIDATED_FAIL_PREFIX)
#                 logger.error("Header mismatch")
#                 continue

#             # Step 3: Mandatory fields check
#             name_idx = headers.index("Name")
#             address_idx = headers.index("Address")
#             valid_rows = [
#                 r for r in rows[1:]
#                 if len(r) > max(name_idx, address_idx)
#                 and (r[name_idx].strip() or r[address_idx].strip())
#             ]

#             if not valid_rows:
#                 move_file(s3, file_key, VALIDATED_FAIL_PREFIX)
#                 logger.error("Missing mandatory fields")
#                 continue

#             # Step 4: Passed preliminary checks
#             move_file(s3, file_key, VALIDATED_PASS_PREFIX)
#             passed_files.append(file_key)
#             logger.info("Validation passed")

#         # Step 5: After all files processed
#         if passed_files:
#             return {"status": "Pass", "files": passed_files}
#         else:
#             return {"status": "Fail", "reason": "No files passed validation"}

#     except Exception as e:
#         logger.error(f"Error during validation: {str(e)}")
#         return {"status": "Fail", "reason": str(e)} """


# ''' 
# Commented - Version 1 of Lambda preliminary checks function: 
# import boto3
# import csv
# import io
# import logging

# # Initialize logger
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# # Expected raw schema (normalized)
# EXPECTED_HEADERS = [
#     "name", "address", "type", "bedroom limit",
#     "guest limit", "expiration date", "location", "x", "y"
# ]

# def normalize_header(header):
#     """Normalize header text for comparison."""
#     return header.strip().lower().replace("_", " ")

# def validate_headers(headers):
#     """Validate headers after normalization."""
#     normalized = [normalize_header(h) for h in headers]
#     return normalized == EXPECTED_HEADERS

# def lambda_handler(event, context):
#     """Lambda entry point for preliminary checks."""
#     try:
#         # Extract bucket and file name from event
#         bucket_name = event['Records'][0]['s3']['bucket']['name']
#         file_key = event['Records'][0]['s3']['object']['key']

#         s3 = boto3.client('s3')
#         response = s3.get_object(Bucket=bucket_name, Key=file_key)
#         file_content = response['Body'].read().decode('utf-8')

#         # Read CSV
#         reader = csv.reader(io.StringIO(file_content))
#         rows = list(reader)

#         if not rows or len(rows) < 2:
#             logger.error("File is empty or missing data rows.")
#             return {"status": "Fail", "reason": "Empty file or no data rows"}

#         headers = rows[0]
#         if not validate_headers(headers):
#             logger.error("Header mismatch after normalization.")
#             return {"status": "Fail", "reason": "Header mismatch"}

#         # Mandatory field presence check
#         name_idx = headers.index("Name")
#         address_idx = headers.index("Address")
#         valid_rows = [
#             r for r in rows[1:]
#             if len(r) > max(name_idx, address_idx)
#             and (r[name_idx].strip() or r[address_idx].strip())
#         ]

#         if not valid_rows:
#             logger.error("All records missing Name and Address.")
#             return {"status": "Fail", "reason": "Missing mandatory fields"}

#         logger.info(f"Validation passed for file: {file_key}")
#         return {"status": "Pass", "file": file_key}

#     except Exception as e:
#         logger.error(f"Error during validation: {str(e)}")
#         return {"status": "Fail", "reason": str(e)}

# '''