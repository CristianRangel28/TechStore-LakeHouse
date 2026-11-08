resource "aws_glue_crawler" "bronze" {

  name          = "techstore-bronze-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.bronze.name
  table_prefix  = "raw_"

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/ventas/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/clientes/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/productos/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/detalle_ventas/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/marcas/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/sucursales/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/metodos_pago/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/devoluciones/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/garantias/"
  }
}
