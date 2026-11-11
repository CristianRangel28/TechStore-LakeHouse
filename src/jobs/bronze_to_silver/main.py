"""
TECHSTORE DATA LAKEHOUSE

Job: Bronze to Silver
"""

from awsglue.utils import getResolvedOptions

import sys

from src.common.spark_session import initialize_job

from src.jobs.bronze_to_silver.extract import extract_all

from src.jobs.bronze_to_silver.transform import (
    transform_dataframe,
    transform_financial_columns
)

from src.jobs.bronze_to_silver.load_iceberg import load_iceberg


def main():

    # Inicializar Glue y Spark.
    context = initialize_job()

    args = context["args"]
    spark = context["spark"]
    job = context["job"]
    logger = context["logger"]

    bucket = args["bucket"]
    catalog = args["catalog"]
    bronze_db = args["bronze_db"]
    silver_db = args["silver_db"]

    batch_id = "lote_1"

    logger.info("Iniciando Bronze to Silver")

    # 1. EXTRACCIÓN
    selected_tables = None

    if "--tables" in sys.argv:
        extra_args = getResolvedOptions(
            sys.argv,
            ["tables"]
        )

        selected_tables = [
            table.strip()
            for table in extra_args["tables"].split(",")
        ]

    dataframes = extract_all(
        spark,
        catalog,
        bronze_db,
        selected_tables
    )

    # 2. TRANSFORMACIÓN Y CARGA
    for table_name, dataframe in dataframes.items():

        logger.info(f"Procesando tabla: {table_name}")

        transformed = transform_dataframe(
            dataframe,
            batch_id
        )

        transformed = transform_financial_columns(
            transformed
        )

        load_iceberg(
            transformed,
            spark,
            catalog,
            silver_db,
            table_name
        )

    logger.info("Bronze to Silver finalizado correctamente")

    job.commit()


if __name__ == "__main__":
    main()