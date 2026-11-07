output "bucket_name" {
  description = "Nombre del bucket S3 de TechStore"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "s3_path" {
  description = "Ruta base del bucket S3 de TechStore"
  value       = "s3://${aws_s3_bucket.lakehouse.bucket}"
}

output "glue_role_arn" {
  description = "ARN del rol IAM utilizado por AWS Glue"
  value       = aws_iam_role.glue.arn
}