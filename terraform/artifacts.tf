data "archive_file" "src" {
  type        = "zip"
  output_path = "${path.module}/build/src.zip"

  dynamic "source" {
    for_each = fileset("${path.module}/../src", "**/*.py")
    content {
      content  = file("${path.module}/../src/${source.value}")
      filename = "src/${source.value}"
    }
  }
}

resource "aws_s3_object" "src_zip" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "scripts/src.zip"
  source = data.archive_file.src.output_path
  etag   = data.archive_file.src.output_md5
}

resource "aws_s3_object" "bronze_to_silver_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "scripts/bronze_to_silver.py"
  source = "${path.module}/../deployment/glue/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../deployment/glue/bronze_to_silver.py")
}