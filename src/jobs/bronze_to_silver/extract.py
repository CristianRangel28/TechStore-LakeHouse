"""
Extracción de datos desde Bronze mediante AWS Glue Data Catalog.
"""

import logging

# Configurar el logger (en AWS Glue suele heredarse del contexto de ejecución del trabajo)
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


def extract_table(spark, catalog, database, table_name):
    full_table_name = f"{catalog}.{database}.{table_name}"

    logger.info("[EXTRACT] Leyendo: %s", full_table_name)

    dataframe = spark.table(full_table_name)

    logger.info("[EXTRACT] Columnas de %s: %s", table_name, dataframe.columns)

    return dataframe


def extract_all(spark, catalog, bronze_db, selected_tables=None):
    if selected_tables is None:
        selected_tables = list(SOURCES.keys())

    dataframes = {}

    for name in selected_tables:
        if name not in SOURCES:
            logger.error("Fuente desconocida solicitada: %s", name)
            raise ValueError(f"Fuente desconocida: {name}")

        table_name = SOURCES[name]

        dataframes[name] = extract_table(
            spark,
            catalog,
            bronze_db,
            table_name
        )

    logger.info("Extracción completada para todas las tablas seleccionadas.")
    return dataframes