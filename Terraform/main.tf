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
  role          = aws_iam_role.lambda_exec.arn
  filename      = "lambda_function.zip"
}

# IAM Role for Lambda function:
resource "aws_iam_role" "lambda_exec" {
  name = "lambda-exec-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
        Effect = "Allow"
      }
    ]
  })
}

# IAM Role Policy attachment:
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}