"""
TECHSTORE DATA LAKEHOUSE

Job ETL: Bronze to Silver
Orquesta la extracción desde Bronze, transformaciones de limpieza y carga en tablas Iceberg Silver.
"""

import datetime
import logging
import uuid
from pyspark.storagelevel import StorageLevel
from src.common.spark_session import initialize_job
from src.jobs.bronze_to_silver.extract import extract_all
from src.jobs.bronze_to_silver.load_iceberg import load_iceberg
from src.jobs.bronze_to_silver.transform import (
    PRIMARY_KEYS,
    get_rejected_records,
    get_valid_records,
    transform_entity
)

logger = logging.getLogger(__name__)


def main():
    """
    Punto de entrada principal para el procesamiento Bronze a Silver.
    """
    # 1. INICIALIZACIÓN FLEXIBLE
    context = initialize_job(
        required_arguments=["JOB_NAME", "bucket", "catalog", "bronze_db", "silver_db"],
        optional_arguments=["batch_id", "tables", "source_batch"]
    )

    args = context["args"]
    spark = context["spark"]
    job = context["job"]
    glue_logger = context["logger"]

    catalog = args["catalog"]
    bronze_db = args["bronze_db"]
    silver_db = args["silver_db"]

    # 2. RESOLUCIÓN DE TRAZABILIDAD (batch_id)
    # Prioridad: 1) batch_id explícito provisto como argumento (--batch_id)
    #            2) Generación automática de identificador único (para ejecuciones manuales sin argumento)
    batch_id = args.get("batch_id")
    if not batch_id:
        timestamp_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique_suffix = uuid.uuid4().hex[:8]
        batch_id = f"batch_{timestamp_str}_{unique_suffix}"
        glue_logger.info(f"Identificador de lote no provisto. Generado ID único: {batch_id}")
    else:
        glue_logger.info(f"Utilizando identificador de ejecución/lote: {batch_id}")

    glue_logger.info("Iniciando pipeline Bronze to Silver")

    # 3. EXTRACCIÓN
    selected_tables = None
    if args.get("tables"):
        selected_tables = [table.strip() for table in args["tables"].split(",") if table.strip()]
        glue_logger.info(f"Tablas seleccionadas para extracción: {selected_tables}")

    source_batch = args.get("source_batch")
    if source_batch:
        glue_logger.info(f"Extracción incremental activada: lote={source_batch}")

    dataframes = extract_all(
        spark=spark,
        bronze_db=bronze_db,
        selected_tables=selected_tables,
        source_batch=source_batch
    )

    # 4. TRANSFORMACIÓN Y CARGA OPTIMIZADA
    execution_metrics = {}

    for table_name, raw_dataframe in dataframes.items():
        glue_logger.info("==================================================")
        glue_logger.info(f"Procesando entidad: {table_name}")

        # 1. Transformación, detección de rechazos antes del casteo y tipado
        transformed_df = transform_entity(
            dataframe=raw_dataframe,
            entity_name=table_name,
            batch_id=batch_id
        )

        # 2. Persistir el DataFrame transformado en MEMORY_AND_DISK
        # Permite realizar el cálculo de métricas, la extracción de muestras diagnósticas
        # y la carga en Iceberg sobre el mismo conjunto cacheado en memoria/disco local.
        transformed_df.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            # 3. Contabilizar leídos, válidos y desglose de motivos en una única acción agregada
            # sobre la caché. No se ejecuta un count() previo en frío sobre S3.
            metric_counts = (
                transformed_df
                .groupBy("_rejection_reason")
                .count()
                .collect()
            )

            valid_count = 0
            rejected_count = 0
            individual_causes = {}
            combinations_breakdown = {}

            for row in metric_counts:
                reason_str = row["_rejection_reason"]
                cnt = row["count"]
                if reason_str is None:
                    valid_count += cnt
                else:
                    rejected_count += cnt
                    combinations_breakdown[reason_str] = cnt
                    # Desglosar motivos individuales sin duplicar el total de filas rechazadas
                    causes = [c.strip() for c in reason_str.split(";") if c.strip()]
                    for cause in causes:
                        individual_causes[cause] = individual_causes.get(cause, 0) + cnt

            raw_count = valid_count + rejected_count

            execution_metrics[table_name] = {
                "leidos": raw_count,
                "validos": valid_count,
                "rechazados": rejected_count,
                "causas_individuales": individual_causes,
                "combinaciones_rechazo": combinations_breakdown
            }

            glue_logger.info(f"[MÉTRICAS] {table_name} -> Leídos: {raw_count} | Válidos: {valid_count} | Rechazados: {rejected_count}")

            # 4. Si existen registros rechazados, registrar causas individuales y muestra diagnóstica mínima (PK y motivo)
            if rejected_count > 0:
                glue_logger.warning(f"[VALIDATION] Se detectaron {rejected_count} registro(s) rechazado(s) en '{table_name}'. Causas individuales: {individual_causes}")
                pk_col = PRIMARY_KEYS.get(table_name)
                sample_cols = (
                    [pk_col, "_rejection_reason"]
                    if pk_col and pk_col in transformed_df.columns
                    else ["_rejection_reason"]
                )
                sample_rejected = (
                    get_rejected_records(transformed_df)
                    .select(*sample_cols)
                    .limit(5)
                    .collect()
                )
                sample_dicts = [row.asDict() for row in sample_rejected]
                glue_logger.warning(f"[VALIDATION] Muestra diagnóstica de registros rechazados en '{table_name}' (PK y motivo): {sample_dicts}")

            # 5. Carga en Iceberg de los registros válidos (se excluyen rechazados y se remueve _rejection_reason)
            # Notar que 'validos' representa registros aptos para carga; upsert_iceberg_table aplicará
            # deduplicación determinística de duplicados exactos y MERGE INTO según cambios reales.
            valid_df = get_valid_records(transformed_df)

            load_iceberg(
                dataframe=valid_df,
                spark=spark,
                catalog=catalog,
                silver_db=silver_db,
                table_name=table_name
            )

        finally:
            # 6. Liberar memoria y disco garantizadamente para evitar fugas entre tablas
            transformed_df.unpersist()

    # 5. RESUMEN DE EJECUCIÓN
    glue_logger.info("==================================================")
    glue_logger.info("RESUMEN DE EJECUCIÓN BRONZE -> SILVER")
    glue_logger.info(f"Identificador de Lote: {batch_id}")
    for table_name, metrics in execution_metrics.items():
        if metrics['rechazados'] > 0:
            glue_logger.info(f" - {table_name}: {metrics['leidos']} leídos, {metrics['validos']} válidos, {metrics['rechazados']} rechazados (causas individuales: {metrics['causas_individuales']})")
            glue_logger.info(f"   Combinaciones de rechazo: {metrics['combinaciones_rechazo']}")
        else:
            glue_logger.info(f" - {table_name}: {metrics['leidos']} leídos, {metrics['validos']} válidos, 0 rechazados")
    glue_logger.info("==================================================")

    glue_logger.info("Bronze to Silver finalizado exitosamente.")
    job.commit()


if __name__ == "__main__":
    main()
