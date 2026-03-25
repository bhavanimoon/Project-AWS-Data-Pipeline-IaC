provider "aws" {
  region = "ap-south-2" # Asia Pacific (Hyderabad)
}

resource "aws_s3_bucket" "test_bucket" {
  bucket = "bmoon-terraform-test-bucket"   # bucket name must be globally unique
#  acl    = "private" # acl is deprecated. So adding another resource section for acl.
}

resource "aws_s3_bucket_acl" "test_bucket_acl" {
	bucket = aws_s3_bucket.test_bucket.id
	acl = "private"
}