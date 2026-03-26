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
resource "aws_lambda_function" "hello_world" {
  function_name = "hello-world-function"
  runtime       = "python3.11"   # use supported runtime
  handler       = "lambda_function.lambda_handler"
  role          = "arn:aws:iam::834889206747:role/lambda-exec-role"
  filename      = "lambda_function.zip"
}

# Create Glue job:
resource "aws_glue_job" "simple_glue" {
  name     = "my-glue-job"
  role_arn = "arn:aws:iam::834889206747:role/glue-exec-role"
  command {
    name            = "glueetl"
    script_location = "s3://my-bucket/scripts/glue_script.py"
  }
}
