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

