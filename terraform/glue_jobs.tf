locals {
  catalog = "glue_catalog"

  iceberg_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.${local.catalog}=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.${local.catalog}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.${local.catalog}.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.${local.catalog}.warehouse=s3://${aws_s3_bucket.lakehouse.bucket}/warehouse/",
  ])
}

resource "aws_glue_job" "bronze_to_silver" {
  name              = "techstore-bronze-to-silver"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2

  timeout     = 20
  max_retries = 0

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"        = "iceberg"
    "--conf"                    = local.iceberg_conf
    "--catalog"                 = local.catalog
    "--enable-glue-datacatalog" = "true"
    "--bucket"                  = aws_s3_bucket.lakehouse.bucket
    "--bronze_db"               = aws_glue_catalog_database.bronze.name
    "--silver_db"               = aws_glue_catalog_database.silver.name
    "--extra-py-files"          = "s3://${aws_s3_bucket.lakehouse.bucket}/scripts/src.zip"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}