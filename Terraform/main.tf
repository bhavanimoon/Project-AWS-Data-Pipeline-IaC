terraform {
  backend "s3" {
    bucket         = "bmoon-terraform-state"
    key            = "iac/terraform.tfstate"
    region         = "ap-south-2"
    use_lockfile   = true
    # dynamodb_table = "terraform-locks"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# Provider Region:
provider "aws" {
  region = "ap-south-2" # Asia Pacific (Hyderabad)
}

# Create S3 bucket:
resource "aws_s3_bucket" "test_bucket" {
  bucket = "bmoon-terraform-test-bucket"

  tags = {
    Name        = "bmoon-terraform-test-bucket"
    Environment = "Dev"
  }
}

# S3 bucket ownership control for the main bmoon-terraform-test-bucket:
resource "aws_s3_bucket_ownership_controls" "test_bucket_ownership" {
  bucket = aws_s3_bucket.test_bucket.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Create S3 bucket - to log terraform states:
resource "aws_s3_bucket" "state_bucket" {
  bucket = "bmoon-terraform-state"

  tags = {
    Name        = "bmoon-terraform-state"
    Environment = "Dev"
  }
}

# S3 bucket ownership control for the Terraform State bucket - bmoon-terraform-state:
resource "aws_s3_bucket_ownership_controls" "state_bucket_ownership" {
  bucket = aws_s3_bucket.state_bucket.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Create DynamoDB table for Terraform Locks:
resource "aws_dynamodb_table" "terraform_locks" {
  name           = "terraform-locks"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Name        = "terraform-locks"
    Environment = "Dev"
  }
}

# Create lambda function for lambda_preliminary_checks.py:
resource "aws_lambda_function" "lambda_preprocessor" {
  function_name = "lambda-preprocessor-function"
  runtime       = "python3.11"
  handler       = "lambda_preliminary_checks.lambda_handler"
  role          = "arn:aws:iam::834889206747:role/lambda-exec-role"
  filename      = "lambda_preliminary_checks.zip"
  source_code_hash = filebase64sha256("lambda_preliminary_checks.zip")
  timeout          = 181   # timeout in 3 minutes 1 second
}

# Create glue job for glue_data_processing.py
resource "aws_glue_job" "glue_processor" {
  name     = "glue-data-processor-job"
  role_arn = "arn:aws:iam::834889206747:role/glue-exec-role"
  command {
    name            = "glueetl"
    script_location = "s3://bmoon-terraform-test-bucket/Scripts/glue_data_processing.py"
    python_version  = "3"
  }
  glue_version = "3.0"
  max_capacity = 2
  timeout      = 10   # timeout in minutes

  default_arguments = {
    "--JOB_NAME" = "glue-data-processor-job"
    # FIX: Combine all custom configs into a single space-separated string key
    # "--conf"     = "spark.task.maxFailures=1 spark.speculation=false spark.sql.legacy.timeParserPolicy=LEGACY"
  }
}

# Create Step Functions - State Machine:
resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "ETL_Pipeline_StateMachine"
  role_arn = "arn:aws:iam::834889206747:role/step-func-exec-role"

  definition = jsonencode({
    StartAt = "ValidateFile",
    States = {
      ValidateFile = {
        Type     = "Task",
        Resource = "arn:aws:states:::lambda:invoke",
        Parameters = {
          #FunctionName = aws_lambda_function.hello_world.arn
          FunctionName = aws_lambda_function.lambda_preprocessor.arn,
          Payload = {
            "bucket.$" = "$.bucket"
          }
        },
        Next = "CheckValidationResult"
      },
      CheckValidationResult = {
        Type = "Choice",
        Choices = [
          {
            Variable = "$.Payload.status",
            StringEquals = "Pass",
            Next = "RunGlueJob"
          }
        ],
        Default = "ValidationFailed"
      },
      RunGlueJob = {
        Type     = "Task",
        Resource = "arn:aws:states:::glue:startJobRun.sync",
        Parameters = {
          #JobName = aws_glue_job.simple_glue.name
          JobName = aws_glue_job.glue_processor.name,
          Arguments = {
            "--BUCKET_NAME.$" = "$.Payload.bucket",
            "--DATE_FOLDER.$" = "$.Payload.date_folder",
            "--VALIDATED_PREFIX.$" = "$.Payload.validated_prefix",
            "--FILES.$"            = "$.Payload.files"
          }
        },
        TimeoutSeconds = 720
        End = true
      },
      ValidationFailed = {
        Type = "Fail",
        Error = "ValidationFailed",
        Cause = "$.Payload.reason"
      }
    }
  })
}

# Create Event Bridge - Rule:
resource "aws_cloudwatch_event_rule" "etl_schedule" {
  name                = "ETL_Schedule"
  description         = "Trigger ETL pipeline every day at 2 AM"
  schedule_expression = "cron(30 14 29 6 ? 2026)"
}

# Link Event Bridge Rule to ETL Target:
resource "aws_cloudwatch_event_target" "etl_target" {
  rule      = aws_cloudwatch_event_rule.etl_schedule.name
  target_id = "StepFunctionTrigger"
  arn       = aws_sfn_state_machine.etl_pipeline.arn
  role_arn  = "arn:aws:iam::834889206747:role/eventbridge-invoke-stepfun-role"

  input = jsonencode({
    bucket = aws_s3_bucket.test_bucket.bucket
  })
}