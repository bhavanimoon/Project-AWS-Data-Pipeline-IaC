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

# S3 bucket ownership control:
resource "aws_s3_bucket_ownership_controls" "test_bucket_ownership" {
  bucket = aws_s3_bucket.test_bucket.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

#  acl is deprecated. So commenting out the below section added for acl bucket creation.
#resource "aws_s3_bucket_acl" "test_bucket_acl" {
#	bucket = aws_s3_bucket.test_bucket.id
#	acl = "private"
#}

# Create Lambda function:
# Sample lambda function created to test out terraform init, plan and apply
# Commented on 21-Apr-2026
# resource "aws_lambda_function" "hello_world" {
#   function_name = "hello-world-function"
#   runtime       = "python3.11"   # use supported runtime
#   handler       = "lambda_function.lambda_handler"
#   role          = "arn:aws:iam::834889206747:role/lambda-exec-role"
#   filename      = "lambda_function.zip"
# }

# Create Glue job:
# Sample glue job created to test out terraform init, plan and apply
# Commented on 21-Apr-2026
# resource "aws_glue_job" "simple_glue" {
#   name     = "my-glue-job"
#   role_arn = "arn:aws:iam::834889206747:role/glue-exec-role"
#   command {
#     name            = "glueetl"
#     script_location = "s3://bmoon-terraform-test-bucket/Scripts/glue_script.py"
# 	python_version = "3"
#   }
  
#   glue_version = "3.0"
#   max_capacity = 2
# }

# Create lambda function for lambda_preliminary_checks.py:
resource "aws_lambda_function" "lambda_preprocessor" {
  function_name = "lambda-preprocessor-function"
  runtime       = "python3.11"
  handler       = "lambda_preliminary_checks.lambda_handler"
  role          = "arn:aws:iam::834889206747:role/lambda-exec-role"
  filename      = "lambda_preliminary_checks.zip"
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
          FunctionName = aws_lambda_function.lambda_preprocessor.arn
        },
        Next = "RunGlueJob"
      },
      RunGlueJob = {
        Type     = "Task",
        Resource = "arn:aws:states:::glue:startJobRun.sync",
        Parameters = {
          #JobName = aws_glue_job.simple_glue.name
          JobName = aws_glue_job.glue_processor.name
        },
        End = true
      }
    }
  })
}

# Create Event Bridge - Rule:
resource "aws_cloudwatch_event_rule" "etl_schedule" {
  name                = "ETL_Schedule"
  description         = "Trigger ETL pipeline every day at 2 AM"
  schedule_expression = "cron(30 2 * * ? *)"
}

# Link Event Bridge Rule to ETL Target:
resource "aws_cloudwatch_event_target" "etl_target" {
  rule      = aws_cloudwatch_event_rule.etl_schedule.name
  target_id = "StepFunctionTrigger"
  arn       = aws_sfn_state_machine.etl_pipeline.arn
  role_arn  = "arn:aws:iam::834889206747:role/eventbridge-invoke-stepfun-role"
}