import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext


def initialize_job():
    """
    Inicializa el entorno de AWS Glue y Apache Spark.

    Recupera los argumentos proporcionados por Terraform,
    crea el contexto de Spark y configura el Glue Job.
    """

    required_arguments = [
        "JOB_NAME",
        "bucket",
        "bronze_db",
        "silver_db",
        "catalog"
    ]

    args = getResolvedOptions(
        sys.argv,
        required_arguments
    )

    # Inicializar Spark
    spark_context = SparkContext.getOrCreate()

    # Inicializar Glue
    glue_context = GlueContext(spark_context)

    # Obtener SparkSession
    spark = glue_context.spark_session

    # Inicializar el Job
    job = Job(glue_context)

    job.init(
        args["JOB_NAME"],
        args
    )

    # Configurar logger
    logger = glue_context.get_logger()

    logger.info("====================================")
    logger.info("TECHSTORE - DATA LAKEHOUSE")
    logger.info("Inicializando Bronze to Silver")
    logger.info(f"Bucket: {args['bucket']}")
    logger.info(f"Bronze DB: {args['bronze_db']}")
    logger.info(f"Silver DB: {args['silver_db']}")
    logger.info(f"Catalog: {args['catalog']}")
    logger.info("====================================")

    return {
        "args": args,
        "spark": spark,
        "glue_context": glue_context,
        "job": job,
        "logger": logger
    }