data "aws_caller_identity" "current" {}

locals {
  bucket_name = "techstore-lakehouse-${data.aws_caller_identity.current.account_id}"
  prefixes = {
    bronze  = "bronze/"
    silver  = "silver/"
    gold    = "gold/"
    scripts = "scripts/"
    athena  = "athena-results/"
  }
}

resource "aws_s3_bucket" "lakehouse" {
  bucket        = local.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

