data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }

}

data "aws_iam_policy_document" "glue_s3_access" {


  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = ["${aws_s3_bucket.lakehouse.arn}/*"]
  }
}

resource "aws_iam_role" "glue" {
  name               = "techstore-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "lakehouse_access" {
  name   = "techstore-s3-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3_access.json
}

