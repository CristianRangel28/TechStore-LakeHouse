data "aws_iam_policy_document" "sfn_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "sfn_glue_access" {
  statement {
    effect = "Allow"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun"
    ]
    resources = [
      aws_glue_job.bronze_to_silver.arn,
      aws_glue_job.silver_to_gold.arn
    ]
  }
}

resource "aws_iam_role" "step_functions" {
  name               = "techstore-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_role.json
}

resource "aws_iam_role_policy" "step_functions_glue" {
  name   = "techstore-sfn-glue-access"
  role   = aws_iam_role.step_functions.id
  policy = data.aws_iam_policy_document.sfn_glue_access.json
}

resource "aws_sfn_state_machine" "lakehouse_pipeline" {
  name     = "techstore-lakehouse-pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = jsonencode({
    Comment = "Pipeline TechStore: Bronze a Silver y Silver a Gold"
    StartAt = "BronzeToSilver"
    States = {
      BronzeToSilver = {
        Type           = "Task"
        Resource       = "arn:aws:states:::glue:startJobRun.sync"
        Parameters     = { JobName = aws_glue_job.bronze_to_silver.name }
        TimeoutSeconds = 1500
        Catch          = [{ ErrorEquals = ["States.ALL"], Next = "Fallo" }]
        Next           = "SilverToGold"
      }
      SilverToGold = {
        Type           = "Task"
        Resource       = "arn:aws:states:::glue:startJobRun.sync"
        Parameters     = { JobName = aws_glue_job.silver_to_gold.name }
        TimeoutSeconds = 1500
        Catch          = [{ ErrorEquals = ["States.ALL"], Next = "Fallo" }]
        Next           = "Exito"
      }
      Exito = { Type = "Succeed" }
      Fallo = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "Un job de Glue fallo"
      }
    }
  })
}

