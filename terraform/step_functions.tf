resource "aws_sfn_state_machine" "lakehouse_pipeline" {
  name     = "techstore-lakehouse-pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = jsonencode({
    Comment = "Pipeline TechStore: Bronze a Silver y Silver a Gold"
    StartAt = "PrepararBatchId"
    States = {
      PrepararBatchId = {
        Type = "Choice"
        Choices = [
          {
            And = [
              { Variable = "$.batch_id", IsPresent = true },
              { Variable = "$.batch_id", IsNull = false }
            ]
            Next = "UsarBatchIdProvisto"
          }
        ]
        Default = "GenerarBatchId"
      }
      UsarBatchIdProvisto = {
        Type = "Pass"
        Parameters = {
          "batch_id.$" = "$.batch_id"
        }
        ResultPath = "$._batch"
        Next       = "EvaluarSourceBatch"
      }
      GenerarBatchId = {
        Type = "Pass"
        Parameters = {
          "batch_id.$" = "States.Format('batch_{}', $$.Execution.Name)"
        }
        ResultPath = "$._batch"
        Next       = "EvaluarSourceBatch"
      }
      EvaluarSourceBatch = {
        Type = "Choice"
        Choices = [
          {
            And = [
              { Variable = "$.source_batch", IsPresent = true },
              { Variable = "$.source_batch", IsNull = false }
            ]
            Next = "BronzeToSilverConSourceBatch"
          }
        ]
        Default = "BronzeToSilver"
      }
      BronzeToSilverConSourceBatch = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_to_silver.name
          Arguments = {
            "--batch_id.$"     = "$._batch.batch_id"
            "--source_batch.$" = "$.source_batch"
          }
        }
        ResultPath     = "$.bronze_result"
        TimeoutSeconds = 1500
        Catch          = [{ ErrorEquals = ["States.ALL"], Next = "Fallo" }]
        Next           = "SilverToGold"
      }
      BronzeToSilver = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_to_silver.name
          Arguments = {
            "--batch_id.$" = "$._batch.batch_id"
          }
        }
        ResultPath     = "$.bronze_result"
        TimeoutSeconds = 1500
        Catch          = [{ ErrorEquals = ["States.ALL"], Next = "Fallo" }]
        Next           = "SilverToGold"
      }
      SilverToGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.silver_to_gold.name
          Arguments = {
            "--batch_id.$" = "$._batch.batch_id"
          }
        }
        ResultPath     = "$.silver_result"
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

