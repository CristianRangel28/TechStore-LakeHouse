"""
Extracción de datos desde Bronze mediante AWS Glue Data Catalog.
"""

import logging
from src.common.s3_readers import read_catalog_table

logger = logging.getLogger(__name__)

SOURCES = {
    "customers": "raw_clientes",
    "brands": "raw_marcas",
    "products": "raw_productos",
    "branches": "raw_sucursales",
    "payment_methods": "raw_metodos_pago",
    "sales": "raw_ventas",
    "sales_details": "raw_detalle_ventas",
    "warranties": "raw_garantias",
    "returns": "raw_devoluciones"
}


def extract_table(spark, bronze_db, table_name):
    """
    Lee una tabla estándar de la capa Bronze en Glue Data Catalog.
    Utiliza el catálogo de sesión de Spark para no entrar en conflicto con el catálogo Iceberg.

    :param spark: SparkSession activa.
    :param bronze_db: Base de datos en Glue Catalog (ej. techstore_bronze).
    :param table_name: Nombre de la tabla física descubierta por el Crawler (ej. raw_clientes).
    :return: DataFrame de PySpark.
    """
    logger.info("[EXTRACT] Extrayendo fuente física: %s.%s", bronze_db, table_name)
    return read_catalog_table(spark, bronze_db, table_name)


def extract_all(spark, bronze_db, selected_tables=None, catalog=None):
    """
    Extrae las tablas seleccionadas o las nueve fuentes completas de la capa Bronze.

    :param spark: SparkSession activa.
    :param bronze_db: Base de datos Bronze.
    :param selected_tables: Lista opcional con los nombres lógicos a extraer.
    :param catalog: Parámetro obsoleto mantenido por retrocompatibilidad (no se utiliza).
    :return: Diccionario con pares {nombre_logico: DataFrame}.
    """
    if selected_tables is None:
        selected_tables = list(SOURCES.keys())

    dataframes = {}

    for name in selected_tables:
        if name not in SOURCES:
            logger.error("Fuente desconocida solicitada: %s", name)
            raise ValueError(f"Fuente desconocida: {name}. Fuentes válidas: {list(SOURCES.keys())}")

        raw_table_name = SOURCES[name]
        dataframes[name] = extract_table(
            spark,
            bronze_db,
            raw_table_name
        )

    logger.info("Extracción completada para %d tabla(s) seleccionada(s).", len(dataframes))
    return dataframes
