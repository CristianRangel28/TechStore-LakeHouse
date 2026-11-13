"""
TECHSTORE DATA LAKEHOUSE

Job ETL: Silver to Gold
Orquesta la extracción desde Silver Iceberg, la construcción del modelo dimensional
y la carga idempotente en las tablas Iceberg de Gold.

Soporta ejecución completa (todas las dimensiones y hechos) y ejecución selectiva (--tables).
Cuando se selecciona un hecho individual, identifica y carga automáticamente sus dimensiones
dependientes en Gold respetando el orden canónico antes de construir y cargar el hecho.
"""

import datetime
import logging
import uuid
from typing import Dict, List, Set
from pyspark.storagelevel import StorageLevel
from src.common.spark_session import initialize_job
from src.jobs.silver_to_gold.extract import extract_all
from src.jobs.silver_to_gold.load_iceberg import GOLD_PRIMARY_KEYS, load_gold_table
from src.jobs.silver_to_gold.transform import (
    build_dim_branch,
    build_dim_customer,
    build_dim_date,
    build_dim_payment_method,
    build_dim_product,
    build_fact_returns,
    build_fact_sales,
    build_fact_warranties
)

logger = logging.getLogger(__name__)

# Dependencias de dimensiones requeridas para cada tabla de hechos
FACT_DIMENSION_DEPENDENCIES: Dict[str, List[str]] = {
    "fact_sales": ["dim_date", "dim_branch", "dim_payment_method", "dim_customer", "dim_product"],
    "fact_warranties": ["dim_date", "dim_customer", "dim_product"],
    "fact_returns": ["dim_date", "dim_customer", "dim_product"]
}

# Fuentes Silver requeridas directamente por cada entidad Gold
ENTITY_SILVER_SOURCES: Dict[str, List[str]] = {
    "dim_date": [],
    "dim_branch": ["branches"],
    "dim_payment_method": ["payment_methods"],
    "dim_customer": ["customers"],
    "dim_product": ["products", "brands"],
    "fact_sales": ["sales", "sales_details"],
    "fact_warranties": ["warranties"],
    "fact_returns": ["returns"]
}

# Secuencia canónica de dimensiones
DIMENSION_PROCESSING_ORDER: List[str] = [
    "dim_date",
    "dim_branch",
    "dim_payment_method",
    "dim_customer",
    "dim_product"
]

# Secuencia canónica de hechos
FACT_PROCESSING_ORDER: List[str] = [
    "fact_sales",
    "fact_warranties",
    "fact_returns"
]

# Secuencia canónica completa
GOLD_PROCESSING_ORDER: List[str] = DIMENSION_PROCESSING_ORDER + FACT_PROCESSING_ORDER


def resolve_execution_plan(requested_tables: List[str]):
    """
    Determina qué dimensiones y hechos deben cargarse en Gold, y qué fuentes Silver
    son estrictamente necesarias extraer.

    Regla: Si se solicita un hecho, sus dimensiones dependientes se incorporan
    automáticamente para ser cargadas en Gold antes que el hecho.

    :param requested_tables: Lista de tablas Gold solicitadas explícitamente.
    :return: Tupla (dimensions_to_load, facts_to_load, required_silver_tables).
    """
    dims_requested = [d for d in DIMENSION_PROCESSING_ORDER if d in requested_tables]
    facts_requested = [f for f in FACT_PROCESSING_ORDER if f in requested_tables]

    # Identificar dimensiones requeridas por los hechos seleccionados
    needed_by_facts: Set[str] = set()
    for fact_name in facts_requested:
        for dep_dim in FACT_DIMENSION_DEPENDENCIES.get(fact_name, []):
            needed_by_facts.add(dep_dim)

    # Dimensiones finales a cargar en Gold, respetando el orden canónico
    dimensions_to_load = [
        d for d in DIMENSION_PROCESSING_ORDER
        if d in dims_requested or d in needed_by_facts
    ]
    facts_to_load = facts_requested

    # Determinar fuentes Silver estrictamente necesarias
    entities_to_build = set(dimensions_to_load + facts_to_load)
    required_silver: Set[str] = set()
    for entity in entities_to_build:
        for src in ENTITY_SILVER_SOURCES.get(entity, []):
            required_silver.add(src)

    return dimensions_to_load, facts_to_load, sorted(list(required_silver))


def main() -> None:
    """
    Punto de entrada principal para el procesamiento Silver to Gold.
    """
    # ==========================================================================
    # [PASO 1/8] Inicialización del entorno de ejecución y resolución de argumentos
    # ==========================================================================
    context = initialize_job(
        required_arguments=["JOB_NAME", "catalog", "silver_db", "gold_db"],
        optional_arguments=["batch_id", "tables"]
    )

    args = context["args"]
    spark = context["spark"]
    job = context["job"]
    glue_logger = context["logger"]

    catalog = args["catalog"]
    silver_db = args["silver_db"]
    gold_db = args["gold_db"]

    batch_id = args.get("batch_id") or args.get("JOB_RUN_ID")
    if not batch_id:
        timestamp_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique_suffix = uuid.uuid4().hex[:8]
        batch_id = f"batch_{timestamp_str}_{unique_suffix}"
        glue_logger.info(f"[PASO 1/8] Identificador de lote no provisto. Generado ID único: {batch_id}")
    else:
        glue_logger.info(f"[PASO 1/8] Utilizando identificador de ejecución/lote: {batch_id}")

    glue_logger.info(f"[PASO 1/8] Inicialización completada. Catálogo: {catalog} | Silver DB: {silver_db} | Gold DB: {gold_db}")

    # ==========================================================================
    # [PASO 2/8] Identificación de tablas objetivo y resolución de dimensiones dependientes
    # ==========================================================================
    if args.get("tables") is not None:
        raw_tables = [
            t.strip()
            for t in args["tables"].split(",")
            if t.strip()
        ]

        if not raw_tables:
            raise ValueError(
                "El argumento 'tables' fue proporcionado, "
                "pero no contiene nombres de tablas válidos."
            )

        for t in raw_tables:
            if t not in GOLD_PRIMARY_KEYS:
                raise ValueError(
                    f"Tabla Gold solicitada no válida: '{t}'. "
                    f"Tablas válidas: {list(GOLD_PRIMARY_KEYS.keys())}"
                )

        requested_tables = raw_tables
        glue_logger.info(
            f"[PASO 2/8] Tablas Gold solicitadas explícitamente: {requested_tables}"
        )
    else:
        requested_tables = list(GOLD_PROCESSING_ORDER)
        glue_logger.info(
            f"[PASO 2/8] No se especificó argumento 'tables'. "
            f"Se procesará el modelo dimensional completo ({len(requested_tables)} tablas)."
        )

    dimensions_to_load, facts_to_load, required_silver_tables = resolve_execution_plan(requested_tables)

    glue_logger.info(f"[PASO 2/8] Dimensiones a cargar en Gold ({len(dimensions_to_load)}): {dimensions_to_load}")
    glue_logger.info(f"[PASO 2/8] Hechos a cargar en Gold ({len(facts_to_load)}): {facts_to_load}")

    # ==========================================================================
    # [PASO 3/8] Resolución y validación de fuentes Silver requeridas
    # ==========================================================================
    glue_logger.info(f"[PASO 3/8] Tablas Silver estrictamente requeridas ({len(required_silver_tables)}): {required_silver_tables}")

    # ==========================================================================
    # [PASO 4/8] Extracción de tablas desde la capa Silver (Iceberg)
    # ==========================================================================
    silver_dataframes = {}
    if required_silver_tables:
        glue_logger.info(f"[PASO 4/8] Extrayendo {len(required_silver_tables)} tabla(s) desde Silver Iceberg...")
        silver_dataframes = extract_all(
            spark=spark,
            catalog=catalog,
            silver_db=silver_db,
            selected_tables=required_silver_tables
        )
        glue_logger.info(f"[PASO 4/8] Extracción Silver completada exitosamente.")
    else:
        glue_logger.info(f"[PASO 4/8] No se requieren fuentes Silver externas para la ejecución solicitada.")

    # ==========================================================================
    # [PASO 5/8] Construcción y persistencia en memoria de dimensiones
    # ==========================================================================
    execution_metrics = {}
    dim_dfs = {}

    try:
        if dimensions_to_load:
            glue_logger.info(f"[PASO 5/8] Iniciando construcción en memoria de {len(dimensions_to_load)} dimensión(es)...")

            for dim_name in dimensions_to_load:
                glue_logger.info(f"[PASO 5/8] Construyendo dimensión: {dim_name}")

                if dim_name == "dim_date":
                    df = build_dim_date(spark=spark, batch_id=batch_id)
                elif dim_name == "dim_branch":
                    df = build_dim_branch(branches_df=silver_dataframes["branches"], batch_id=batch_id)
                elif dim_name == "dim_payment_method":
                    df = build_dim_payment_method(payment_methods_df=silver_dataframes["payment_methods"], batch_id=batch_id)
                elif dim_name == "dim_customer":
                    df = build_dim_customer(customers_df=silver_dataframes["customers"], batch_id=batch_id)
                elif dim_name == "dim_product":
                    df = build_dim_product(
                        products_df=silver_dataframes["products"],
                        brands_df=silver_dataframes["brands"],
                        batch_id=batch_id
                    )
                else:
                    raise ValueError(f"Dimensión no soportada para construcción: {dim_name}")

                df.persist(StorageLevel.MEMORY_AND_DISK)
                dim_dfs[dim_name] = df
                glue_logger.info(f"[PASO 5/8] Dimensión {dim_name} persistida en memoria.")

            glue_logger.info(f"[PASO 5/8] Construcción de dimensiones completada exitosamente.")

        # ==========================================================================
        # [PASO 6/8] Carga de dimensiones requeridas en la capa Gold (Iceberg)
        # ==========================================================================
        if dimensions_to_load:
            glue_logger.info(f"[PASO 6/8] Iniciando carga en Gold de {len(dimensions_to_load)} dimensión(es) en orden canónico...")

            for dim_name in dimensions_to_load:
                df = dim_dfs[dim_name]
                record_count = df.count()
                glue_logger.info(f"[PASO 6/8] Ejecutando upsert para {dim_name} ({record_count} registros)...")

                load_gold_table(
                    dataframe=df,
                    spark=spark,
                    catalog=catalog,
                    gold_db=gold_db,
                    table_name=dim_name
                )
                execution_metrics[dim_name] = {"registros_procesados": record_count}
                glue_logger.info(f"[PASO 6/8] Dimensión {dim_name} cargada exitosamente en Gold.")

            glue_logger.info(f"[PASO 6/8] Carga de dimensiones en Gold completada.")

        # ==========================================================================
        # [PASO 7/8] Construcción y carga de tablas de hechos en la capa Gold (Iceberg)
        # ==========================================================================
        if facts_to_load:
            glue_logger.info(f"[PASO 7/8] Iniciando procesamiento de {len(facts_to_load)} tabla(s) de hechos...")

            for fact_name in facts_to_load:
                glue_logger.info(f"[PASO 7/8] --------------------------------------------------")
                glue_logger.info(f"[PASO 7/8] Procesando hecho: {fact_name}")

                if fact_name == "fact_sales":
                    fact_df = build_fact_sales(
                        sales_df=silver_dataframes["sales"],
                        sales_details_df=silver_dataframes["sales_details"],
                        dim_customer_df=dim_dfs["dim_customer"],
                        dim_product_df=dim_dfs["dim_product"],
                        dim_branch_df=dim_dfs["dim_branch"],
                        dim_payment_method_df=dim_dfs["dim_payment_method"],
                        dim_date_df=dim_dfs["dim_date"],
                        batch_id=batch_id
                    )
                elif fact_name == "fact_warranties":
                    fact_df = build_fact_warranties(
                        warranties_df=silver_dataframes["warranties"],
                        dim_customer_df=dim_dfs["dim_customer"],
                        dim_product_df=dim_dfs["dim_product"],
                        dim_date_df=dim_dfs["dim_date"],
                        batch_id=batch_id
                    )
                elif fact_name == "fact_returns":
                    fact_df = build_fact_returns(
                        returns_df=silver_dataframes["returns"],
                        dim_customer_df=dim_dfs["dim_customer"],
                        dim_product_df=dim_dfs["dim_product"],
                        dim_date_df=dim_dfs["dim_date"],
                        batch_id=batch_id
                    )
                else:
                    raise ValueError(f"Tabla de hechos no soportada: {fact_name}")

                fact_df.persist(StorageLevel.MEMORY_AND_DISK)
                try:
                    fact_count = fact_df.count()
                    glue_logger.info(f"[PASO 7/8] Cargando {fact_name} en Gold Iceberg ({fact_count} registros)...")
                    load_gold_table(
                        dataframe=fact_df,
                        spark=spark,
                        catalog=catalog,
                        gold_db=gold_db,
                        table_name=fact_name
                    )
                    execution_metrics[fact_name] = {"registros_procesados": fact_count}
                    glue_logger.info(f"[PASO 7/8] {fact_name} cargada exitosamente en Gold.")
                finally:
                    fact_df.unpersist()

            glue_logger.info(f"[PASO 7/8] Procesamiento de tablas de hechos completado.")
        else:
            glue_logger.info(f"[PASO 7/8] No se seleccionaron tablas de hechos para procesamiento.")

    finally:
        # Liberación garantizada de memoria para dimensiones persistidas
        for dim_name, df in dim_dfs.items():
            try:
                df.unpersist()
            except Exception as e:
                logger.warning(f"Error al liberar memoria de {dim_name}: {e}")

    # ==========================================================================
    # [PASO 8/8] Resumen de ejecución, confirmación del Job y cierre
    # ==========================================================================
    glue_logger.info(f"==================================================")
    glue_logger.info(f"[PASO 8/8] RESUMEN DE EJECUCIÓN SILVER -> GOLD")
    glue_logger.info(f"[PASO 8/8] Identificador de Lote (_ingestion_batch): {batch_id}")
    for table_name, metrics in execution_metrics.items():
        glue_logger.info(f"[PASO 8/8]  - {table_name}: {metrics['registros_procesados']} registros procesados/cargados")
    glue_logger.info(f"==================================================")

    job.commit()
    glue_logger.info(f"[PASO 8/8] Job Silver to Gold finalizado exitosamente (job.commit ejecutado).")


if __name__ == "__main__":
    main()
