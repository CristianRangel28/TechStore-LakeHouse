"""
TECHSTORE DATA LAKEHOUSE

Extracción de tablas Iceberg desde la capa Silver hacia Gold.
Utiliza el catálogo Apache Iceberg (ej. glue_catalog) configurado en AWS Glue.
"""

import logging
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
from src.common.s3_readers import read_iceberg_table

logger = logging.getLogger(__name__)

# Entidades requeridas de la capa Silver para construir el modelo dimensional Gold
SILVER_TABLES = [
    "customers",
    "brands",
    "products",
    "branches",
    "payment_methods",
    "sales",
    "sales_details",
    "warranties",
    "returns"
]


def extract_silver_table(
    spark: SparkSession,
    catalog: str,
    silver_db: str,
    table_name: str
) -> DataFrame:
    """
    Lee una tabla Iceberg individual de la capa Silver.

    :param spark: SparkSession activa con extensiones Iceberg configuradas.
    :param catalog: Nombre del catálogo Iceberg (ej. glue_catalog).
    :param silver_db: Nombre de la base de datos Silver (ej. techstore_silver).
    :param table_name: Nombre de la tabla en Silver (ej. customers, sales).
    :return: DataFrame de PySpark con los datos de la tabla.
    """
    if table_name not in SILVER_TABLES:
        logger.error("[EXTRACT-SILVER] Tabla Silver desconocida solicitada: %s", table_name)
        raise ValueError(
            f"Tabla Silver desconocida: '{table_name}'. "
            f"Tablas válidas: {SILVER_TABLES}"
        )

    logger.info(
        "[EXTRACT-SILVER] Extrayendo tabla Iceberg Silver: %s.%s.%s",
        catalog, silver_db, table_name
    )

    dataframe = read_iceberg_table(
        spark=spark,
        catalog=catalog,
        database=silver_db,
        table_name=table_name
    )

    return dataframe


def extract_all(
    spark: SparkSession,
    catalog: str,
    silver_db: str,
    selected_tables: Optional[List[str]] = None
) -> Dict[str, DataFrame]:
    """
    Extrae las tablas seleccionadas o la totalidad de tablas de la capa Silver.

    :param spark: SparkSession activa.
    :param catalog: Nombre del catálogo Iceberg.
    :param silver_db: Nombre de la base de datos Silver.
    :param selected_tables: Lista opcional con los nombres de tablas a extraer.
    :return: Diccionario con pares {nombre_tabla: DataFrame}.
    """
    tables_to_extract = selected_tables if selected_tables is not None else SILVER_TABLES
    dataframes: Dict[str, DataFrame] = {}

    logger.info(
        "[EXTRACT-SILVER] Iniciando extracción de tablas Silver: %s",
        tables_to_extract
    )

    for table_name in tables_to_extract:
        dataframes[table_name] = extract_silver_table(
            spark=spark,
            catalog=catalog,
            silver_db=silver_db,
            table_name=table_name
        )

    logger.info(
        "[EXTRACT-SILVER] Extracción Silver finalizada. %d tabla(s) cargada(s).",
        len(dataframes)
    )

    return dataframes
