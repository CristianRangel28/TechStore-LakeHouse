"""
Transformaciones y estandarización de datos desde Bronze a Silver.
"""

import logging
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType
)

logger = logging.getLogger(__name__)

# Diccionario canónico de equivalencias español -> inglés snake_case
# Incluye mapeo de partición física Bronze 'lote' a 'source_batch'
COLUMN_RENAMING = {
    "id_cliente": "customer_id",
    "id_marca": "brand_id",
    "id_producto": "product_id",
    "id_sucursal": "branch_id",
    "id_metodo_pago": "payment_method_id",
    "id_venta": "sale_id",
    "id_detalle": "sale_detail_id",
    "id_postventa": "after_sales_id",
    "nombre": "name",
    "apellido": "last_name",
    "correo": "email",
    "telefono": "phone",
    "ciudad": "city",
    "departamento": "department",
    "fecha_registro": "registration_date",
    "fecha_venta": "sale_timestamp",
    "fecha_solicitud": "request_timestamp",
    "fecha_resolucion": "resolution_timestamp",
    "fecha_lanzamiento": "launch_date",
    "fecha_apertura": "opening_date",
    "fecha": "date",
    "subtotal": "subtotal_amount",
    "descuento": "discount_amount",
    "impuesto": "tax_amount",
    "total": "total_amount",
    "precio_actual": "current_price",
    "costo_adquisicion": "acquisition_cost",
    "precio_unitario": "unit_price",
    "cantidad": "quantity",
    "cantidad_comprada": "purchased_quantity",
    "cantidad_devuelta": "returned_quantity",
    "numero_serie": "serial_number",
    "pais_origen": "country_of_origin",
    "categoria": "category",
    "modelo": "model",
    "direccion": "address",
    "estado": "status",
    "descripcion": "description",
    "tipo": "type",
    "producto": "product",
    "detalle": "details",
    "motivo": "reason",
    "observacion": "observation",
    "reembolso": "refund",
    "porcentaje_aprobado": "approved_percentage",
    "valor": "value",
    "moneda": "currency",
    "lote": "source_batch"
}

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

FINANCIAL_COLUMNS = [
    "subtotal_amount",
    "discount_amount",
    "tax_amount",
    "total_amount",
    "current_price",
    "acquisition_cost",
    "unit_price",
    "refund_amount",
    "value"
]

DATE_COLUMNS = [
    "registration_date",
    "launch_date",
    "opening_date",
    "date"
]

TIMESTAMP_COLUMNS = [
    "sale_timestamp",
    "request_timestamp",
    "resolution_timestamp"
]

INTEGER_COLUMNS = [
    "customer_id",
    "brand_id",
    "product_id",
    "branch_id",
    "payment_method_id",
    "sale_id",
    "sale_detail_id",
    "after_sales_id",
    "quantity",
    "purchased_quantity",
    "returned_quantity",
    "approved_refund_percentage",
    "approved_percentage",
    "refund_percentage"
]


def flatten_post_sales(dataframe, entity_name):
    """
    Aplana de forma controlada las estructuras anidadas de garantías y devoluciones,
    preservando todos los campos respaldados por los generadores JSON reales.

    Para devoluciones (returns):
    - reembolso.valor -> refund_amount
    - reembolso.moneda -> refund_currency
    - reembolso.porcentaje_aprobado -> approved_refund_percentage

    Para garantías (warranties):
    - producto.* -> id_producto, cantidad_comprada, numero_serie
    - detalle.* -> motivo, descripcion, estado, observacion
    """
    if entity_name == "warranties":
        has_producto = "producto" in dataframe.columns and isinstance(dataframe.schema["producto"].dataType, StructType)
        has_detalle = "detalle" in dataframe.columns and isinstance(dataframe.schema["detalle"].dataType, StructType)

        if has_producto or has_detalle:
            cols = [F.col(c) for c in dataframe.columns if c not in ("producto", "detalle")]
            if has_producto:
                cols.extend([
                    F.col("producto.id_producto").alias("id_producto"),
                    F.col("producto.cantidad_comprada").alias("cantidad_comprada"),
                    F.col("producto.numero_serie").alias("numero_serie"),
                ])
            if has_detalle:
                cols.extend([
                    F.col("detalle.motivo.categoria").alias("motivo"),
                    F.col("detalle.motivo.descripcion").alias("descripcion"),
                    F.col("detalle.estado.nombre").alias("estado"),
                    F.col("detalle.estado.observacion").alias("observacion"),
                ])
            return dataframe.select(cols)

    elif entity_name == "returns":
        has_producto = "producto" in dataframe.columns and isinstance(dataframe.schema["producto"].dataType, StructType)
        has_detalle = "detalle" in dataframe.columns and isinstance(dataframe.schema["detalle"].dataType, StructType)
        has_reembolso = "reembolso" in dataframe.columns and isinstance(dataframe.schema["reembolso"].dataType, StructType)

        if has_producto or has_detalle or has_reembolso:
            cols = [F.col(c) for c in dataframe.columns if c not in ("producto", "detalle", "reembolso")]
            if has_producto:
                cols.extend([
                    F.col("producto.id_producto").alias("id_producto"),
                    F.col("producto.cantidad_comprada").alias("cantidad_comprada"),
                    F.col("producto.cantidad_devuelta").alias("cantidad_devuelta"),
                ])
            if has_detalle:
                cols.extend([
                    F.col("detalle.motivo.categoria").alias("motivo"),
                    F.col("detalle.motivo.descripcion").alias("descripcion"),
                    F.col("detalle.estado.nombre").alias("estado"),
                    F.col("detalle.estado.observacion").alias("observacion"),
                ])
            if has_reembolso:
                cols.extend([
                    F.col("reembolso.porcentaje_aprobado").alias("approved_refund_percentage"),
                    F.col("reembolso.valor").alias("refund_amount"),
                    F.col("reembolso.moneda").alias("refund_currency"),
                ])
            return dataframe.select(cols)

    return dataframe


def rename_columns(dataframe):
    """
    Renombra las columnas utilizando el diccionario canónico de equivalencias.
    """
    for old_name, new_name in COLUMN_RENAMING.items():
        if old_name in dataframe.columns:
            dataframe = dataframe.withColumnRenamed(old_name, new_name)
    return dataframe


def normalize_strings(dataframe):
    """
    Normaliza columnas de texto aplicando trim para eliminar espacios en blanco sobrantes.
    """
    for field in dataframe.schema.fields:
        if isinstance(field.dataType, StringType):
            dataframe = dataframe.withColumn(field.name, F.trim(F.col(field.name)))
    return dataframe


def cast_column_types(dataframe):
    """
    Convierte fechas, marcas temporales, importes financieros y números enteros
    a sus tipos de datos nativos correspondientes utilizando try_cast tolerante a errores.
    Garantiza compatibilidad tanto con spark.sql.ansi.enabled=true como false,
    evitando excepciones en tiempo de ejecución ante valores inválidos.
    """
    # 1. Columnas monetarias a Decimal(18, 2)
    for col_name in FINANCIAL_COLUMNS:
        if col_name in dataframe.columns:
            dataframe = dataframe.withColumn(
                col_name,
                F.expr(f"try_cast(`{col_name}` AS DECIMAL(18, 2))")
            )

    # 2. Fechas (YYYY-MM-DD)
    for col_name in DATE_COLUMNS:
        if col_name in dataframe.columns:
            dataframe = dataframe.withColumn(
                col_name,
                F.expr(f"try_cast(`{col_name}` AS DATE)")
            )

    # 3. Timestamps (YYYY-MM-DD HH:MM:SS)
    for col_name in TIMESTAMP_COLUMNS:
        if col_name in dataframe.columns:
            dataframe = dataframe.withColumn(
                col_name,
                F.expr(f"try_cast(`{col_name}` AS TIMESTAMP)")
            )

    # 4. Identificadores y cantidades a LongType (BIGINT)
    for col_name in INTEGER_COLUMNS:
        if col_name in dataframe.columns:
            dataframe = dataframe.withColumn(
                col_name,
                F.expr(f"try_cast(`{col_name}` AS BIGINT)")
            )

    return dataframe


def add_audit_columns(dataframe, batch_id):
    """
    Añade información de auditoría y trazabilidad a nivel de fila:
    - _ingestion_timestamp: marca temporal de primera inserción (inmutable).
    - _ts_updated: marca temporal de última modificación real.
    - _created_batch: identificador del lote de primera inserción (inmutable).
    - _updated_batch: identificador del último lote que modificó el registro.
    - _source_file: ruta del archivo origen.
    - _ingestion_batch: alias de compatibilidad hacia atrás.
    """
    source_file_expr = (
        F.when(
            F.input_file_name().isNotNull() & (F.trim(F.input_file_name()) != ""),
            F.input_file_name()
        ).otherwise(F.lit("unknown"))
    )
    now_ts = F.current_timestamp()

    return (
        dataframe
        .withColumn("_ingestion_timestamp", now_ts)
        .withColumn("_ts_updated", now_ts)
        .withColumn("_created_batch", F.lit(batch_id))
        .withColumn("_updated_batch", F.lit(batch_id))
        .withColumn("_ingestion_batch", F.lit(batch_id))
        .withColumn("_source_file", source_file_expr)
    )


def attach_rejection_reasons(dataframe, entity_name):
    """
    Detecta y acumula todos los motivos de rechazo para cada registro antes de que
    sus valores originales sean alterados o sobrescritos durante el casteo de tipos.

    Diferenciación de motivos:
    - PK estructuralmente ausente: lanza ValueError inmediato.
    - PK originalmente nula o vacía: 'NULL_OR_EMPTY_PK:<col>'
    - PK con valor no convertible a número: 'INVALID_TYPE_PK:<col>'
    - Columnas monetarias no convertibles: 'INVALID_TYPE_FINANCIAL:<col>'
    - Fechas no convertibles: 'INVALID_TYPE_DATE:<col>'
    - Marcas temporales no convertibles: 'INVALID_TYPE_TIMESTAMP:<col>'
    - Cantidades/enteros no convertibles: 'INVALID_TYPE_INTEGER:<col>'

    Valores legítimamente nulos (originalmente nulos o vacíos en campos no obligatorios)
    no generan motivos de rechazo.
    Múltiples motivos en una misma fila se acumulan concatenados por '; '.
    Si la fila es 100% válida, '_rejection_reason' se asigna como NULL.
    """
    reason_exprs = []

    # 1. Validación de Clave Primaria
    if entity_name in PRIMARY_KEYS:
        pk_col = PRIMARY_KEYS[entity_name]
        if pk_col not in dataframe.columns:
            raise ValueError(
                f"Falla estructural en '{entity_name}': la columna PK obligatoria '{pk_col}' "
                f"no existe en el DataFrame. Columnas disponibles: {dataframe.columns}"
            )

        pk_raw_str = F.trim(F.col(pk_col).cast(StringType()))
        is_pk_null_or_empty = F.col(pk_col).isNull() | (pk_raw_str == "")
        is_pk_not_empty = F.col(pk_col).isNotNull() & (pk_raw_str != "")
        is_pk_unconvertible = is_pk_not_empty & F.expr(f"try_cast(`{pk_col}` AS BIGINT)").isNull()

        reason_exprs.append(
            F.when(is_pk_null_or_empty, F.lit(f"NULL_OR_EMPTY_PK:{pk_col}"))
        )
        reason_exprs.append(
            F.when(is_pk_unconvertible, F.lit(f"INVALID_TYPE_PK:{pk_col}"))
        )

    # 2. Columnas monetarias / financieras (DecimalType(18, 2))
    for col_name in FINANCIAL_COLUMNS:
        if col_name in dataframe.columns:
            raw_str = F.trim(F.col(col_name).cast(StringType()))
            is_not_empty = F.col(col_name).isNotNull() & (raw_str != "")
            is_unconvertible = is_not_empty & F.expr(f"try_cast(`{col_name}` AS DECIMAL(18, 2))").isNull()
            reason_exprs.append(
                F.when(is_unconvertible, F.lit(f"INVALID_TYPE_FINANCIAL:{col_name}"))
            )

    # 3. Fechas (DateType - YYYY-MM-DD)
    for col_name in DATE_COLUMNS:
        if col_name in dataframe.columns:
            raw_str = F.trim(F.col(col_name).cast(StringType()))
            is_not_empty = F.col(col_name).isNotNull() & (raw_str != "")
            is_unconvertible = is_not_empty & F.expr(f"try_cast(`{col_name}` AS DATE)").isNull()
            reason_exprs.append(
                F.when(is_unconvertible, F.lit(f"INVALID_TYPE_DATE:{col_name}"))
            )

    # 4. Marcas temporales (TimestampType - YYYY-MM-DD HH:MM:SS)
    for col_name in TIMESTAMP_COLUMNS:
        if col_name in dataframe.columns:
            raw_str = F.trim(F.col(col_name).cast(StringType()))
            is_not_empty = F.col(col_name).isNotNull() & (raw_str != "")
            is_unconvertible = is_not_empty & F.expr(f"try_cast(`{col_name}` AS TIMESTAMP)").isNull()
            reason_exprs.append(
                F.when(is_unconvertible, F.lit(f"INVALID_TYPE_TIMESTAMP:{col_name}"))
            )

    # 5. Cantidades e identificadores secundarios (LongType)
    pk_col_name = PRIMARY_KEYS.get(entity_name)
    for col_name in INTEGER_COLUMNS:
        if col_name in dataframe.columns and col_name != pk_col_name:
            raw_str = F.trim(F.col(col_name).cast(StringType()))
            is_not_empty = F.col(col_name).isNotNull() & (raw_str != "")
            is_unconvertible = is_not_empty & F.expr(f"try_cast(`{col_name}` AS BIGINT)").isNull()
            reason_exprs.append(
                F.when(is_unconvertible, F.lit(f"INVALID_TYPE_INTEGER:{col_name}"))
            )

    if reason_exprs:
        reasons_concat = F.concat_ws("; ", *reason_exprs)
        return dataframe.withColumn(
            "_rejection_reason",
            F.when(F.length(reasons_concat) > 0, reasons_concat).otherwise(F.lit(None).cast(StringType()))
        )

    return dataframe.withColumn("_rejection_reason", F.lit(None).cast(StringType()))


def get_valid_records(dataframe):
    """
    Filtra y devuelve exclusivamente los registros válidos (sin motivos de rechazo),
    removiendo la columna técnica temporal _rejection_reason.
    """
    if "_rejection_reason" in dataframe.columns:
        return dataframe.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
    return dataframe


def get_rejected_records(dataframe):
    """
    Filtra y devuelve exclusivamente los registros que presentan motivos de rechazo.
    """
    if "_rejection_reason" in dataframe.columns:
        return dataframe.filter(F.col("_rejection_reason").isNotNull())
    return dataframe.filter(F.lit(False))


def validate_and_filter(dataframe, entity_name):
    """
    Función de compatibilidad: evalúa rechazos y devuelve el DataFrame filtrado
    con los registros válidos.
    """
    df_evaluated = attach_rejection_reasons(dataframe, entity_name)
    return get_valid_records(df_evaluated)


def transform_entity(dataframe, entity_name, batch_id):
    """
    Pipeline completo de transformación para una entidad:
    1. Aplanado de JSON anidados (garantías y devoluciones con nombres canónicos).
    2. Renombrado a inglés técnico snake_case y mapeo de 'lote' a 'source_batch'.
    3. Normalización de cadenas (trim).
    4. Detección y acumulación de motivos de rechazo ANTES de sobreescribir con el casteo.
    5. Conversión de tipos de datos (Decimal, Date, Timestamp, Long).
    6. Inyección de columnas de auditoría y trazabilidad (_ingestion_timestamp, _ingestion_batch, _source_file).

    :param dataframe: DataFrame crudo de Bronze.
    :param entity_name: Nombre de la entidad.
    :param batch_id: Identificador único de ejecución.
    :return: DataFrame completo con tipos corregidos, columnas de auditoría y columna _rejection_reason.
    """
    # 1. Aplanado de estructuras anidadas
    df = flatten_post_sales(dataframe, entity_name)

    # 2. Renombrado de columnas (incluye lote -> source_batch)
    df = rename_columns(df)

    # 3. Normalización de cadenas
    df = normalize_strings(df)

    # 4. Detección y acumulación de motivos de rechazo antes del tipado
    df = attach_rejection_reasons(df, entity_name)

    # 5. Tipado de datos
    df = cast_column_types(df)

    # 6. Trazabilidad (_created_batch, _updated_batch, _ingestion_timestamp, _ts_updated, _source_file)
    df = add_audit_columns(df, batch_id)

    return df


# Funciones de compatibilidad hacia atrás
def transform_dataframe(dataframe, batch_id, entity_name=None):
    """
    Función de compatibilidad que aplica transformaciones estándar.
    """
    if entity_name:
        transformed_df = transform_entity(dataframe, entity_name, batch_id)
        return get_valid_records(transformed_df)

    df = rename_columns(dataframe)
    df = normalize_strings(df)
    df = cast_column_types(df)
    df = add_audit_columns(df, batch_id)
    return df


def transform_financial_columns(dataframe):
    """
    Función de compatibilidad para conversión tolerante a errores de columnas monetarias.
    """
    for col_name in FINANCIAL_COLUMNS:
        if col_name in dataframe.columns:
            dataframe = dataframe.withColumn(
                col_name,
                F.expr(f"try_cast(`{col_name}` AS DECIMAL(18, 2))")
            )
    return dataframe
