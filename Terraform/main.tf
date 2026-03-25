provider "aws" {
  region = "ap-south-2" # Asia Pacific (Hyderabad)
}

resource "aws_s3_bucket" "test_bucket" {
  bucket = "bmoon-terraform-test-bucket"   # bucket name must be globally unique
  
  # Enforce bucket owner ownership (disables ACLs)
  object_ownership = "BucketOwnerEnforced"
  
  tags = {
	Name = "bmoon-terraform-test-bucket"
	Environment = "Dev"
	}
}

#  acl is deprecated. So commenting out the below section added for acl bucket creation.
#resource "aws_s3_bucket_acl" "test_bucket_acl" {
#	bucket = aws_s3_bucket.test_bucket.id
#	acl = "private"
#}