provider "aws" {
  region = "ap-south-2" # Asia Pacific (Hyderabad)
}

resource "aws_s3_bucket" "test_bucket" {
  bucket = "bmoon-terraform-test-bucket"

  tags = {
    Name        = "bmoon-terraform-test-bucket"
    Environment = "Dev"
  }
}

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


