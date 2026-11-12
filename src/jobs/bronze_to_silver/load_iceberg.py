"""
Carga de DataFrames en tablas Apache Iceberg de la capa Silver.
"""

import logging
from src.common.s3_writers import upsert_iceberg_table

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

PARTITION_SCHEMAS = {
    "sales": ["months(sale_timestamp)"]
}


def load_iceberg(
    dataframe,
    spark,
    catalog,
    silver_db,
    table_name
):
    """
    Carga o actualiza registros válidos en una tabla Iceberg de la capa Silver mediante upsert_iceberg_table.

    - Catálogo: Utiliza el catálogo Iceberg configurado (ej. glue_catalog).
    - Formato: Iceberg v2 con 'write.distribution-mode' = 'hash'.
    - Exclusión: Preserva el `_ingestion_timestamp` y `_source_file` original en actualizaciones.
    - Integridad: Recibe datos previamente validados por la capa de transformación. upsert_iceberg_table
      actúa como salvaguarda estricta (falla si recibe claves primarias nulas).
    - Conflictos intra-lote: No utiliza deduplicación arbitraria por timestamps (`dedup_order_col=None`).
      Si existen duplicados contradictorios sin criterio confiable, el proceso se detiene con error.

    :param dataframe: DataFrame limpio, estandarizado y validado.
    :param spark: SparkSession activa.
    :param catalog: Nombre del catálogo Iceberg en Spark (ej. glue_catalog).
    :param silver_db: Nombre de la base de datos Silver (ej. techstore_silver).
    :param table_name: Nombre lógico de la tabla destino.
    """
    if table_name not in PRIMARY_KEYS:
        raise ValueError(
            f"Tabla no reconocida: '{table_name}'. "
            f"Claves primarias configuradas: {list(PRIMARY_KEYS.keys())}"
        )

    primary_key = PRIMARY_KEYS[table_name]

    # 1. Configurar particionamiento si corresponde
    partition_cols = PARTITION_SCHEMAS.get(table_name)

    logger.info(
        "[LOAD-SILVER] Iniciando carga Iceberg para '%s' en %s.%s (PK: %s)",
        table_name, catalog, silver_db, primary_key
    )

    # 2. Ejecutar UPSERT mediante el helper común (recibe el DataFrame validado directamente)
    upsert_iceberg_table(
        dataframe=dataframe,
        spark=spark,
        catalog=catalog,
        database=silver_db,
        table_name=table_name,
        primary_keys=primary_key,
        partition_cols=partition_cols,
        table_properties={
            "format-version": "2",
            "write.distribution-mode": "hash"
        },
        exclude_update_cols=["_ingestion_timestamp", "_source_file"],
        dedup_order_col=None
    )

    logger.info("[LOAD-SILVER] Carga Iceberg completada exitosamente para '%s'", table_name)
