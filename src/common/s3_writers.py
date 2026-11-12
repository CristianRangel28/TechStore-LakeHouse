"""
Escritura y gestión de tablas Apache Iceberg en Amazon S3 mediante Apache Spark.

Optimizado para cargas masivas (9M+ registros) en AWS Glue 5.1 con Iceberg v2.
Implementa validaciones estrictas de integridad, resolución determinística de duplicados
y operaciones MERGE INTO idempotentes con comparación nulo-segura.
"""

import logging
import uuid
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

DEFAULT_ICEBERG_PROPERTIES = {
    "format-version": "2",
    "write.distribution-mode": "hash"
}


def create_iceberg_table_if_not_exists(
    spark,
    catalog,
    database,
    table_name,
    schema_dataframe,
    partition_cols=None,
    table_properties=None
):
    """
    Crea una tabla en formato Apache Iceberg si aún no existe en el catálogo.

    Deduce el esquema inicial a partir del DataFrame provisto (`schema_dataframe`),
    creando la estructura vacía con `WHERE 1 = 0`. No altera el esquema ni el
    particionamiento si la tabla ya existe previamente.

    :param spark: SparkSession activa con extensiones y catálogo Iceberg configurados.
    :param catalog: Nombre del catálogo Iceberg (ej. glue_catalog).
    :param database: Base de datos destino (ej. techstore_silver, techstore_gold).
    :param table_name: Nombre de la tabla destino.
    :param schema_dataframe: DataFrame de PySpark del cual se infiere la estructura DDL.
    :param partition_cols: Lista o tupla de expresiones de partición (ej. ['days(sale_timestamp)']).
    :param table_properties: Diccionario opcional de propiedades Iceberg (TBLPROPERTIES).
    :return: Nombre calificado y escapado de la tabla (`catalog.database.table_name`).
    """
    full_table_name = f"`{catalog}`.`{database}`.`{table_name}`"

    # Generar identificador de vista único para evitar colisiones concurrentes
    view_uid = uuid.uuid4().hex[:8]
    temp_view = f"_tmp_schema_{table_name.replace('-', '_')}_{view_uid}"

    schema_dataframe.createOrReplaceTempView(temp_view)

    # Configurar propiedades de la tabla
    properties = dict(DEFAULT_ICEBERG_PROPERTIES)
    if table_properties:
        properties.update(table_properties)

    props_clause = ", ".join([f"'{k}' = '{v}'" for k, v in properties.items()])

    # Configurar cláusula de particionamiento
    partition_clause = ""
    if partition_cols:
        if isinstance(partition_cols, str):
            partition_cols = [partition_cols]
        partition_clause = f"PARTITIONED BY ({', '.join(partition_cols)})"

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name}
        USING iceberg
        {partition_clause}
        TBLPROPERTIES (
            {props_clause}
        )
        AS SELECT * FROM {temp_view} WHERE 1 = 0
    """

    logger.info("[WRITE-ICEBERG] Verificando/Creando tabla Iceberg: %s", full_table_name)
    try:
        spark.sql(create_sql)
    finally:
        spark.catalog.dropTempView(temp_view)

    return full_table_name


def upsert_iceberg_table(
    dataframe,
    spark,
    catalog,
    database,
    table_name,
    primary_keys,
    partition_cols=None,
    table_properties=None,
    exclude_update_cols=None,
    dedup_order_col=None
):
    """
    Inserta o actualiza (upsert) registros en una tabla Iceberg mediante MERGE INTO.

    Validaciones y reglas de integridad:
    1. Detección de DataFrames vacíos: Retorna inmediatamente sin disparar DDL ni generar
       snapshots vacíos en Iceberg.
    2. Claves primarias:
       - Valida que existan y estén presentes tanto en el DataFrame origen como en la tabla destino.
       - Rechaza explícitamente cualquier registro que contenga valores NULL en algún componente
         de la clave primaria (detiene la ejecución con diagnóstico detallado; no los descarta en silencio).
    3. Desduplicación determinística:
       - Elimina registros completamente idénticos en todas las columnas mediante `dropDuplicates()`.
       - Si existen registros con la misma clave primaria pero datos contradictorios:
         * Si no se proporciona `dedup_order_col`, lanza ValueError con muestra de claves conflictivas.
         * Si se proporciona `dedup_order_col`, selecciona el registro más reciente usando
           `orderBy(desc_nulls_last())`. Si existen empates (mismo valor de ordenamiento con datos
           diferentes), lanza ValueError para no resolver de forma arbitraria.
    4. Validación rigurosa de esquema e integridad:
       - Valida que columnas marcadas como no anulables (nullable=False) en la tabla destino
         estén provistas por el DataFrame origen y no contengan valores nulos (lanza ValueError
         con conteo y muestra si se detectan nulos).
       - Exige compatibilidad estricta de tipos de datos en columnas coincidentes (lanza ValueError
         ante cualquier discrepancia para no depender de conversiones implícitas de Spark).
       - Manejo explícito de columnas divergentes: columnas adicionales en origen se omiten del MERGE
         (sin evolución automática de esquema) y columnas opcionales faltantes en origen reciben advertencia
         (se asignarán NULL o valor por defecto en INSERT).
    5. Idempotencia y optimización de actualización:
       - No ejecuta UPDATE cuando los atributos de negocio de origen y destino sean idénticos,
         utilizando comparaciones nulo-seguras (`NOT (target.col <=> source.col)`).
       - Excluye claves primarias y columnas técnicas/auditoría (`_ingestion_*`, `_source_*`).
       - Si no existen columnas de negocio modificables, omite la cláusula WHEN MATCHED para evitar
         actualizaciones incondicionales innecesarias.
       - Inserta registros no coincidentes (WHEN NOT MATCHED).
    6. Aislamiento y limpieza:
       - Vistas temporales con sufijos UUID aislados, eliminadas de forma garantizada en `finally`.

    :param dataframe: DataFrame con los datos a persistir.
    :param spark: SparkSession activa.
    :param catalog: Nombre del catálogo Iceberg (ej. glue_catalog).
    :param database: Base de datos destino.
    :param table_name: Nombre de la tabla destino.
    :param primary_keys: Clave o lista de claves primarias utilizadas en la condición ON del MERGE.
    :param partition_cols: Columnas de partición si la tabla debe ser creada.
    :param table_properties: Propiedades adicionales de la tabla si debe ser creada.
    :param exclude_update_cols: Lista de columnas a omitir en UPDATE (ej. ['_ingestion_timestamp']).
    :param dedup_order_col: Columna para desempate determinístico (DESC) ante duplicados de PK con datos distintos.
    """
    # 1. Normalización y validación de presencia de claves primarias en entrada
    if isinstance(primary_keys, str):
        primary_keys = [primary_keys]

    if not primary_keys:
        raise ValueError("upsert_iceberg_table requiere al menos una clave primaria.")

    missing_pks = [pk for pk in primary_keys if pk not in dataframe.columns]
    if missing_pks:
        raise ValueError(
            f"Las claves primarias {missing_pks} no existen en el DataFrame origen. "
            f"Columnas disponibles: {dataframe.columns}"
        )

    # 2. Manejo de DataFrames vacíos antes de crear tabla o snapshot
    if dataframe.rdd.isEmpty():
        logger.warning(
            "[WRITE-ICEBERG] El DataFrame para `%s`.`%s`.`%s` está vacío (0 filas). "
            "Omitiendo operaciones DDL y MERGE INTO para evitar snapshots innecesarios.",
            catalog, database, table_name
        )
        return

    # 3. Validación estricta de componentes de clave primaria no nulos
    pk_null_conditions = [f"`{pk}` IS NULL" for pk in primary_keys]
    null_pks_df = dataframe.filter(" OR ".join(pk_null_conditions))
    if not null_pks_df.rdd.isEmpty():
        null_count = null_pks_df.count()
        sample_nulls = null_pks_df.limit(5).collect()
        raise ValueError(
            f"Se rechazó la operación en `{catalog}`.`{database}`.`{table_name}`: "
            f"se encontraron {null_count} registro(s) con componentes de clave primaria nulos "
            f"para {primary_keys}. Muestra de registros inválidos: {sample_nulls}"
        )

    # 4. Desduplicación segura intra-lote
    # Paso A: Eliminar duplicados 100% idénticos en todas las columnas
    dedup_df = dataframe.dropDuplicates()

    # Paso B: Manejar o detectar duplicados con misma PK pero datos distintos
    if dedup_order_col:
        if dedup_order_col not in dedup_df.columns:
            raise ValueError(
                f"La columna de orden para desduplicación '{dedup_order_col}' "
                f"no existe en el DataFrame. Columnas disponibles: {dedup_df.columns}"
            )

        # Detectar empates con datos diferentes: mismo PK y mismo dedup_order_col
        # Como ya se aplicó dropDuplicates(), si existen filas con mismo (PK, dedup_order_col),
        # significa que tienen datos distintos en los demás atributos.
        ties_df = (
            dedup_df
            .groupBy(*[F.col(pk) for pk in primary_keys], F.col(dedup_order_col))
            .count()
            .filter(F.col("count") > 1)
        )
        if not ties_df.rdd.isEmpty():
            tie_samples = ties_df.limit(5).collect()
            raise ValueError(
                f"Empate no resoluble en deduplicación para `{catalog}`.`{database}`.`{table_name}`: "
                f"existen registros con la misma clave primaria {primary_keys} y el mismo valor "
                f"en '{dedup_order_col}', pero con datos diferentes en los demás atributos. "
                f"Spark no puede desempatar arbitrariamente. Muestra de empates: {tie_samples}"
            )

        logger.info(
            "[WRITE-ICEBERG] Aplicando resolución determinística de duplicados para `%s`.`%s`.`%s` "
            "usando columna de orden '%s' DESC (NULLs al final)",
            catalog, database, table_name, dedup_order_col
        )
        window_spec = (
            Window
            .partitionBy(*[F.col(pk) for pk in primary_keys])
            .orderBy(F.col(dedup_order_col).desc_nulls_last())
        )
        dedup_df = (
            dedup_df
            .withColumn("_ts_dedup_rank", F.row_number().over(window_spec))
            .filter(F.col("_ts_dedup_rank") == 1)
            .drop("_ts_dedup_rank")
        )
    else:
        # Detectar si existen filas conflictivas con la misma PK y valores distintos
        conflicts_df = (
            dedup_df
            .groupBy(*[F.col(pk) for pk in primary_keys])
            .count()
            .filter(F.col("count") > 1)
        )
        if not conflicts_df.rdd.isEmpty():
            conflict_samples = conflicts_df.limit(5).collect()
            raise ValueError(
                f"Conflicto de clave primaria en `{catalog}`.`{database}`.`{table_name}`: "
                f"existen registros con las mismas claves {primary_keys} pero valores diferentes en atributos "
                f"sin un criterio explícito de deduplicación. Especifique 'dedup_order_col'. "
                f"Muestra de claves conflictivas: {conflict_samples}"
            )

    # 5. Asegurar existencia de la tabla en Iceberg
    create_iceberg_table_if_not_exists(
        spark=spark,
        catalog=catalog,
        database=database,
        table_name=table_name,
        schema_dataframe=dedup_df,
        partition_cols=partition_cols,
        table_properties=table_properties
    )

    full_table_name = f"`{catalog}`.`{database}`.`{table_name}`"

    # 6. Validación rigurosa de compatibilidad de esquemas
    target_table = spark.table(full_table_name)
    target_schema = target_table.schema
    target_columns = target_table.columns
    source_schema = dedup_df.schema
    source_columns = dedup_df.columns

    # Validar que las PKs existan en destino
    target_missing_pks = [pk for pk in primary_keys if pk not in target_columns]
    if target_missing_pks:
        raise ValueError(
            f"Incompatibilidad crítica: las claves primarias {target_missing_pks} "
            f"no existen en la tabla destino {full_table_name}. Columnas destino: {target_columns}"
        )

    matching_columns = [col for col in source_columns if col in target_columns]
    if not matching_columns:
        raise ValueError(
            f"Incompatibilidad de esquemas: no existen columnas coincidentes entre "
            f"el DataFrame origen y la tabla destino {full_table_name}."
        )

    # Validar columnas obligatorias (nullable=False) requeridas por la tabla destino
    mandatory_target_cols = [
        field.name for field in target_schema.fields if not field.nullable
    ]
    missing_mandatory_cols = [
        col for col in mandatory_target_cols if col not in source_columns
    ]
    if missing_mandatory_cols:
        raise ValueError(
            f"Incompatibilidad de esquema en {full_table_name}: la tabla destino requiere "
            f"las columnas obligatorias (nullable=False) {missing_mandatory_cols}, "
            f"pero no están presentes en el DataFrame origen. Columnas disponibles: {source_columns}"
        )

    # Validar que los registros a persistir no contengan valores nulos en columnas obligatorias
    if mandatory_target_cols:
        mandatory_null_conditions = [f"`{col}` IS NULL" for col in mandatory_target_cols]
        null_mandatory_df = dedup_df.filter(" OR ".join(mandatory_null_conditions))
        if not null_mandatory_df.rdd.isEmpty():
            null_count = null_mandatory_df.count()
            null_sample = null_mandatory_df.limit(5).collect()
            cols_with_nulls = [
                col for col in mandatory_target_cols
                if not dedup_df.filter(F.col(col).isNull()).rdd.isEmpty()
            ]
            raise ValueError(
                f"Violación de integridad en {full_table_name}: se encontraron {null_count} "
                f"registro(s) con valores nulos en columnas obligatorias (nullable=False): {cols_with_nulls}. "
                f"Muestra de registros inválidos: {null_sample}"
            )

    # Validar compatibilidad estricta de tipos de datos en columnas coincidentes
    # Rechaza cualquier incompatibilidad antes de MERGE sin depender de conversiones implícitas
    type_divergences = []
    for col_name in matching_columns:
        target_type = target_schema[col_name].dataType
        source_type = source_schema[col_name].dataType
        if target_type != source_type:
            type_divergences.append(
                f"'{col_name}' (origen: {source_type.simpleString()} vs destino: {target_type.simpleString()})"
            )
    if type_divergences:
        raise ValueError(
            f"Incompatibilidad estricta de tipos de datos en {full_table_name}: "
            f"{'; '.join(type_divergences)}. "
            f"Corrija los tipos en el DataFrame origen antes de ejecutar MERGE INTO."
        )

    # Advertir sobre columnas adicionales en origen (no ignorar silenciosamente)
    extra_source_cols = [col for col in source_columns if col not in target_columns]
    if extra_source_cols:
        logger.warning(
            "[WRITE-ICEBERG] Esquema divergente en %s: columnas en origen pero no en destino "
            "(se omitirán en el MERGE, sin evolución automática de esquema): %s",
            full_table_name, extra_source_cols
        )

    missing_target_cols = [col for col in target_columns if col not in source_columns]
    if missing_target_cols:
        logger.warning(
            "[WRITE-ICEBERG] Esquema divergente en %s: columnas en destino pero no en origen "
            "(recibirán NULL o valor predeterminado en INSERT): %s",
            full_table_name, missing_target_cols
        )

    # 7. Construcción de sentencia MERGE INTO
    join_conditions = " AND ".join([f"target.`{pk}` = source.`{pk}`" for pk in primary_keys])

    # Construcción de cláusula UPDATE (excluyendo PKs y columnas inmutables/auditoría)
    exclude_set = set(primary_keys)
    if exclude_update_cols:
        exclude_set.update(exclude_update_cols)

    update_columns = [col for col in matching_columns if col not in exclude_set]

    matched_clause = ""
    if update_columns:
        set_statements = ", ".join([f"target.`{col}` = source.`{col}`" for col in update_columns])

        # Condición de idempotencia: solo actualizar cuando al menos un valor de negocio haya cambiado
        # Utiliza comparación nulo-segura <=> (NOT target.col <=> source.col)
        # Excluye columnas técnicas/auditoría (como _ingestion_timestamp, _source_file)
        business_update_cols = [
            col for col in update_columns
            if not col.startswith("_ingestion_") and not col.startswith("_source_") and not col.startswith("_ts_")
        ]
        if business_update_cols:
            diff_conditions = " OR ".join([
                f"NOT (target.`{col}` <=> source.`{col}`)" for col in business_update_cols
            ])
            matched_clause = f"WHEN MATCHED AND ({diff_conditions}) THEN UPDATE SET {set_statements}"
        else:
            logger.info(
                "[WRITE-ICEBERG] No se detectaron columnas de negocio modificables para %s "
                "(solo existen columnas técnicas o claves primarias). "
                "Omitiendo cláusula WHEN MATCHED para garantizar idempotencia y evitar updates incondicionales.",
                full_table_name
            )

    # Construcción de cláusula INSERT
    insert_cols_str = ", ".join([f"`{col}`" for col in matching_columns])
    insert_vals_str = ", ".join([f"source.`{col}`" for col in matching_columns])
    not_matched_clause = f"WHEN NOT MATCHED THEN INSERT ({insert_cols_str}) VALUES ({insert_vals_str})"

    # 8. Ejecución con aislamiento de vista temporal mediante try/finally
    view_uid = uuid.uuid4().hex[:8]
    temp_source_view = f"_src_{table_name.replace('-', '_')}_{view_uid}"

    dedup_df.createOrReplaceTempView(temp_source_view)

    merge_sql = f"""
        MERGE INTO {full_table_name} AS target
        USING {temp_source_view} AS source
        ON {join_conditions}
        {matched_clause}
        {not_matched_clause}
    """

    logger.info(
        "[WRITE-ICEBERG] Ejecutando MERGE INTO en: %s (PK: %s, Columnas coincidentes: %d)",
        full_table_name, primary_keys, len(matching_columns)
    )

    try:
        spark.sql(merge_sql)
        logger.info("[WRITE-ICEBERG] MERGE INTO completado exitosamente para: %s", full_table_name)
    finally:
        spark.catalog.dropTempView(temp_source_view)
