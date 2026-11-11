"""
Carga de DataFrames en tablas Iceberg de Silver.
"""

import logging
from pyspark.sql import functions as F

# Configurar el logger
logger = logging.getLogger(__name__)

PRIMARY_KEYS = {
    "customers": "customer_id",
    "brands": "brand_id",
    "products": "product_id",
    "branches": "branch_id",
    "payment_methods": "payment_method_id",
    "sales": "sale_id",
    "sales_details": "sale_detail_id",
    "warranties": "after_sales_id",
    "returns": "after_sales_id"
}


def load_iceberg(
    dataframe,
    spark,
    catalog,
    silver_db,
    table_name
):
    """
    Crea la tabla Iceberg si no existe y realiza MERGE.
    """
    full_table_name = f"{catalog}.{silver_db}.{table_name}"

    primary_key = PRIMARY_KEYS[table_name]

    dataframe.createOrReplaceTempView("source_data")

    # Crear tabla Iceberg si todavía no existe.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name}
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '2'
        )
        AS SELECT * FROM source_data WHERE 1 = 0
    """)

    # Insertar o actualizar registros.
    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING source_data AS source
        ON target.{primary_key} = source.{primary_key}

        WHEN MATCHED THEN UPDATE SET *

        WHEN NOT MATCHED THEN INSERT *
    """)

    logger.info("[LOAD] Tabla Iceberg actualizada: %s", full_table_name)