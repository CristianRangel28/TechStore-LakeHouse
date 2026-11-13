"""
Inicialización y configuración del entorno de ejecución de Apache Spark y AWS Glue.
"""

import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from src.common.logging_utils import GlueLoggerAdapter

logger = logging.getLogger(__name__)

DEFAULT_COMMON_ARGUMENTS = [
    "bucket",
    "catalog",
    "bronze_db",
    "silver_db",
    "gold_db",
    "batch_id",
    "tables"
]


def initialize_job(required_arguments=None, optional_arguments=None):
    """
    Inicializa el entorno de AWS Glue y Apache Spark de manera flexible.

    Permite configurar argumentos requeridos y opcionales para soportar diferentes
    etapas del LakeHouse (Bronze -> Silver, Silver -> Gold), conservando
    compatibilidad con llamadas sin parámetros explícitos.

    :param required_arguments: Lista de nombres de argumentos obligatorios (sin '--').
                               Si es None, se utiliza por defecto ['JOB_NAME'].
    :param optional_arguments: Lista de nombres de argumentos opcionales (sin '--').
                               Solo se resuelven si están presentes en sys.argv.
                               Si es None, inspecciona argumentos estándar del LakeHouse.
    :return: Diccionario con 'args', 'spark', 'glue_context', 'job' y 'logger'.
    """
    if required_arguments is None:
        required_arguments = ["JOB_NAME"]
        default_candidates = DEFAULT_COMMON_ARGUMENTS
    else:
        default_candidates = []

    if optional_arguments is None:
        optional_arguments = default_candidates
    else:
        combined = list(optional_arguments)
        for candidate in default_candidates:
            if candidate not in combined:
                combined.append(candidate)
        optional_arguments = combined

    # Filtrar únicamente los opcionales que efectivamente fueron provistos en sys.argv
    present_optional = [
        arg for arg in optional_arguments
        if f"--{arg}" in sys.argv and arg not in required_arguments
    ]

    arguments_to_resolve = list(required_arguments) + present_optional

    args = getResolvedOptions(
        sys.argv,
        arguments_to_resolve
    )

    # Glue registra JOB_RUN_ID por su cuenta; leerlo sin pasarlo al parser.
    try:
        job_run_id_index = sys.argv.index("--JOB_RUN_ID")
        job_run_id = sys.argv[job_run_id_index + 1]
    except (ValueError, IndexError):
        job_run_id = None
    args["JOB_RUN_ID"] = job_run_id

    # Asignar None por defecto a los opcionales no provistos para evitar KeyError
    for opt in optional_arguments:
        args.setdefault(opt, None)

    # Inicializar Spark Context
    spark_context = SparkContext.getOrCreate()

    # Inicializar Glue Context
    glue_context = GlueContext(spark_context)

    # Obtener SparkSession
    spark = glue_context.spark_session

    # Inicializar Job de Glue
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Obtener logger nativo de Glue
    glue_logger = GlueLoggerAdapter(glue_context.get_logger())

    glue_logger.info("====================================")
    glue_logger.info("TECHSTORE - DATA LAKEHOUSE")
    glue_logger.info(f"Job inicializado: {args.get('JOB_NAME')}")
    for key in sorted(args.keys()):
        if key != "JOB_NAME" and args[key] is not None:
            glue_logger.info(f"Argumento '{key}': {args[key]}")
    glue_logger.info("====================================")

    return {
        "args": args,
        "spark": spark,
        "glue_context": glue_context,
        "job": job,
        "logger": glue_logger
    }
