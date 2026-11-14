"""
TECHSTORE DATA LAKEHOUSE

Carga de DataFrames en tablas Apache Iceberg de la capa Gold.
Reutiliza `upsert_iceberg_table` de `src.common.s3_writers` para garantizar idempotencia,
validaciones estrictas de integridad y operaciones MERGE INTO con comparación nulo-segura.
"""

import logging
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
from src.common.s3_writers import upsert_iceberg_table

logger = logging.getLogger(__name__)

# Configuración de claves primarias por entidad Gold
GOLD_PRIMARY_KEYS: Dict[str, List[str]] = {
    "dim_customer": ["customer_id"],
    "dim_product": ["product_id"],
    "dim_branch": ["branch_id"],
    "dim_payment_method": ["payment_method_id"],
    "dim_date": ["date_sk"],
    "fact_sales": ["sale_detail_id"],
    "fact_warranties": ["after_sales_id"],
    "fact_returns": ["after_sales_id"]
}

# Configuración de particionamiento Iceberg
GOLD_PARTITION_SCHEMAS: Dict[str, Optional[List[str]]] = {
    "dim_customer": None,
    "dim_product": None,
    "dim_branch": None,
    "dim_payment_method": None,
    "dim_date": None,
    "fact_sales": ["months(sale_timestamp)"],
    "fact_warranties": None,
    "fact_returns": None
}

# Columnas de auditoría que no deben sobreescribirse en caso de actualización (UPDATE).
# Nota: Las columnas inmutables de trazabilidad (_created_batch, _ingestion_timestamp,
# _ingestion_batch, _source_file) se excluyen centralmente en upsert_iceberg_table
# de src.common.s3_writers. No se duplican aquí para evitar divergencias.
# Gold no tiene _source_file, por lo que no es necesario agregarla a la lista.


def load_gold_table(
    dataframe: DataFrame,
    spark: SparkSession,
    catalog: str,
    gold_db: str,
    table_name: str
) -> None:
    """
    Carga o actualiza registros en una tabla Apache Iceberg de la capa Gold.

    - Catálogo: Iceberg Data Catalog (ej. glue_catalog).
    - Idempotencia: Si un registro tiene exactamente los mismos atributos de negocio,
      no se genera actualización.
    - Manejo de esquema: Crea la tabla Iceberg v2 si aún no existe en el catálogo.

    :param dataframe: DataFrame transformado y validado a persistir.
    :param spark: SparkSession activa.
    :param catalog: Nombre del catálogo Iceberg (ej. glue_catalog).
    :param gold_db: Nombre de la base de datos Gold (ej. techstore_gold).
    :param table_name: Nombre de la tabla destino en Gold.
    """
    if table_name not in GOLD_PRIMARY_KEYS:
        raise ValueError(
            f"Tabla Gold no reconocida: '{table_name}'. "
            f"Tablas configuradas: {list(GOLD_PRIMARY_KEYS.keys())}"
        )

    primary_keys = GOLD_PRIMARY_KEYS[table_name]
    partition_cols = GOLD_PARTITION_SCHEMAS.get(table_name)

    logger.info(
        "[LOAD-GOLD] Iniciando upsert Iceberg en `%s`.`%s`.`%s` | PKs: %s | Partición: %s",
        catalog, gold_db, table_name, primary_keys, partition_cols
    )

    upsert_iceberg_table(
        dataframe=dataframe,
        spark=spark,
        catalog=catalog,
        database=gold_db,
        table_name=table_name,
        primary_keys=primary_keys,
        partition_cols=partition_cols
    )

    logger.info(
        "[LOAD-GOLD] Carga completada exitosamente para `%s`.`%s`.`%s`",
        catalog, gold_db, table_name
    )
