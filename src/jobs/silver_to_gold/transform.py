"""
TECHSTORE DATA LAKEHOUSE

Transformaciones del modelo dimensional para la capa Gold.
Construye dimensiones (dim_customer, dim_product, dim_branch, dim_payment_method, dim_date)
y tablas de hechos (fact_sales, fact_warranties, fact_returns) utilizando claves sustitutas
determinísticas (SHA-256 para entidades de negocio y entero YYYYMMDD para fechas).

Garantía de Unicidad y Manejo de Duplicados:
Las tablas de la capa Silver garantizan previamente la unicidad de sus claves primarias
mediante las validaciones estrictas y la operación MERGE INTO ejecutada en el Job 1 (Bronze -> Silver).
Para no enmascarar conflictos de datos ni descartar registros arbitrariamente entre filas con la
misma clave pero atributos divergentes, este módulo NO utiliza dropDuplicates([columna_clave]).
Se aplica dropDuplicates() sobre la totalidad de atributos para eliminar exclusivamente duplicados
100% idénticos. Si existieran registros con la misma clave primaria pero datos contradictorios,
la validación estricta de upsert_iceberg_table() en src/common/s3_writers.py detectará el conflicto
e interrumpirá la ejecución de manera controlada.

Incluye tratamiento estricto de registros desconocidos (UNKNOWN/-1) para resolución de FKs
nulas o huérfanas sin generar claves artificiales para IDs inexistentes.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.common.surrogate_keys import add_surrogate_key

logger = logging.getLogger(__name__)

# Valor centinela para claves naturales de registros desconocidos
UNKNOWN_ID = -1
UNKNOWN_STRING = "DESCONOCIDO"


# ==============================================================================
# HELPERS DE RESOLUCIÓN DE CLAVES FORÁNEAS Y REGISTROS DESCONOCIDOS
# ==============================================================================

def get_dimension_unknown_sk(dim_df: DataFrame, id_col: str, sk_col: str) -> str:
    """
    Extrae el valor de la clave sustituta del registro desconocido (-1) de una dimensión.

    :param dim_df: DataFrame de la dimensión que contiene el registro desconocido.
    :param id_col: Nombre de la columna clave natural (ej. customer_id).
    :param sk_col: Nombre de la columna clave sustituta (ej. customer_sk).
    :return: Cadena con el valor de la SK desconocida.
    """
    row = dim_df.filter(F.col(id_col) == UNKNOWN_ID).select(sk_col).first()
    if row is None or row[sk_col] is None:
        raise ValueError(
            f"No se encontró el registro desconocido ({id_col} = {UNKNOWN_ID}) en la dimensión."
        )
    return row[sk_col]


def resolve_dimension_fk(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fk_col: str,
    sk_col: str,
    is_broadcast: bool = False
) -> DataFrame:
    """
    Resuelve una clave foránea en una tabla de hechos contra su dimensión.

    - Si la FK coincide con un registro existente en la dimensión, asigna su SK correspondiente.
    - Si la FK es NULL o no existe en la dimensión (huérfana), asigna la SK del registro
      desconocido de dicha dimensión.
    - Garantiza que NO se genera una clave sustituta a partir de un ID inexistente.
    - Elimina la columna FK original tras resolver la SK.

    :param fact_df: DataFrame de hechos origen.
    :param dim_df: DataFrame de la dimensión destino con el registro desconocido incluido.
    :param fk_col: Nombre de la clave foránea en la tabla de hechos (ej. customer_id).
    :param sk_col: Nombre de la clave sustituta a generar (ej. customer_sk).
    :param is_broadcast: Indica si se debe sugerir un broadcast join para dimensiones pequeñas.
    :return: DataFrame de hechos con la columna sk_col resuelta y fk_col eliminada.
    """
    unknown_sk_val = get_dimension_unknown_sk(dim_df, fk_col, sk_col)

    # Proyectar solo registros de negocio válidos de la dimensión (excluyendo el desconocido)
    dim_lookup = dim_df.filter(F.col(fk_col) != UNKNOWN_ID).select(
        F.col(fk_col),
        F.col(sk_col).alias(f"_resolved_{sk_col}")
    )

    if is_broadcast:
        dim_lookup = F.broadcast(dim_lookup)

    joined = fact_df.join(dim_lookup, on=fk_col, how="left")

    result = (
        joined
        .withColumn(
            sk_col,
            F.coalesce(F.col(f"_resolved_{sk_col}"), F.lit(unknown_sk_val))
        )
        .drop(f"_resolved_{sk_col}", fk_col)
    )

    return result


def resolve_date_fk(
    fact_df: DataFrame,
    timestamp_col: str,
    dim_date_df: DataFrame,
    target_sk_col: str = "date_sk"
) -> DataFrame:
    """
    Resuelve la clave foránea temporal contra dim_date.

    - Extrae el entero YYYYMMDD a partir de la marca temporal provista.
    - Valida existencia contra dim_date.
    - Si el timestamp es NULL o fuera de rango, asigna -1 (registro desconocido de fecha).

    :param fact_df: DataFrame de hechos.
    :param timestamp_col: Columna con marca temporal (ej. sale_timestamp, request_timestamp).
    :param dim_date_df: DataFrame de dim_date.
    :param target_sk_col: Nombre de la columna de salida (date_sk).
    :return: DataFrame de hechos con la clave date_sk resuelta como entero.
    """
    date_lookup = F.broadcast(
        dim_date_df.filter(F.col("date_sk") != UNKNOWN_ID).select(
            F.col("date_sk").alias("_valid_date_sk")
        )
    )

    with_raw_date = fact_df.withColumn(
        "_raw_date_int",
        F.date_format(F.col(timestamp_col), "yyyyMMdd").cast("int")
    )

    joined = with_raw_date.join(
        date_lookup,
        F.col("_raw_date_int") == F.col("_valid_date_sk"),
        how="left"
    )

    result = (
        joined
        .withColumn(
            target_sk_col,
            F.coalesce(F.col("_valid_date_sk"), F.lit(UNKNOWN_ID))
        )
        .drop("_valid_date_sk", "_raw_date_int")
    )

    return result


# ==============================================================================
# CONSTRUCCIÓN DE DIMENSIONES
# ==============================================================================

def build_dim_customer(
    customers_df: DataFrame,
    batch_id: Optional[str] = None
) -> DataFrame:
    """
    Construye la dimensión dim_customer.

    - Clave natural: customer_id (BIGINT).
    - Clave sustituta: customer_sk (SHA-256 de customer_id).
    - Incorpora fila desconocida (-1 / DESCONOCIDO).
    - Desduplicación: dropDuplicates() sobre todas las columnas para evitar selección arbitraria.
    - Columnas de auditoría: _ingestion_timestamp, _ingestion_batch.

    :param customers_df: DataFrame de la tabla Silver customers.
    :param batch_id: Identificador de lote/ejecución.
    :return: DataFrame de dim_customer.
    """
    logger.info("[TRANSFORM-GOLD] Construyendo dim_customer")
    spark = customers_df.sparkSession

    # 1. Registros válidos de Silver (desduplicación exacta de tuplas completas)
    valid_customers = (
        customers_df
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates()
        .select(
            F.col("customer_id").cast("bigint"),
            F.coalesce(F.col("name"), F.lit(UNKNOWN_STRING)).alias("name"),
            F.coalesce(F.col("last_name"), F.lit(UNKNOWN_STRING)).alias("last_name"),
            F.coalesce(F.col("email"), F.lit(UNKNOWN_STRING)).alias("email"),
            F.coalesce(F.col("phone"), F.lit(UNKNOWN_STRING)).alias("phone"),
            F.coalesce(F.col("city"), F.lit(UNKNOWN_STRING)).alias("city"),
            F.coalesce(F.col("department"), F.lit(UNKNOWN_STRING)).alias("department"),
            F.col("registration_date").cast("date"),
            F.coalesce(F.col("status"), F.lit(UNKNOWN_STRING)).alias("status")
        )
    )
    valid_customers = add_surrogate_key(valid_customers, ["customer_id"], "customer_sk", algorithm="sha256")

    # 2. Registro desconocido
    unknown_customer = (
        spark.range(1)
        .select(
            F.lit(UNKNOWN_ID).cast("bigint").alias("customer_id"),
            F.lit(UNKNOWN_STRING).alias("name"),
            F.lit(UNKNOWN_STRING).alias("last_name"),
            F.lit("desconocido@techstore.com").alias("email"),
            F.lit("0000000000").alias("phone"),
            F.lit(UNKNOWN_STRING).alias("city"),
            F.lit(UNKNOWN_STRING).alias("department"),
            F.lit(None).cast("date").alias("registration_date"),
            F.lit(UNKNOWN_STRING).alias("status")
        )
    )
    unknown_customer = add_surrogate_key(unknown_customer, ["customer_id"], "customer_sk", algorithm="sha256")

    # 3. Unificar y añadir auditoría
    dim_customer = (
        valid_customers
        .unionByName(unknown_customer)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_batch", F.lit(batch_id))
    )

    return dim_customer


def build_dim_product(
    products_df: DataFrame,
    brands_df: DataFrame,
    batch_id: Optional[str] = None
) -> DataFrame:
    """
    Construye la dimensión dim_product con marcas desnormalizadas.

    - Desnormaliza la información de marcas (brand_name, country_of_origin, brand_status)
      directamente dentro de dim_product.
    - Clave natural: product_id (BIGINT).
    - Clave sustituta: product_sk (SHA-256 de product_id).
    - Incorpora fila desconocida (-1 / DESCONOCIDO).
    - Desduplicación: dropDuplicates() sobre todas las columnas para evitar descartes arbitrarios.
    - Columnas de auditoría: _ingestion_timestamp, _ingestion_batch.

    :param products_df: DataFrame de la tabla Silver products.
    :param brands_df: DataFrame de la tabla Silver brands.
    :param batch_id: Identificador de lote/ejecución.
    :return: DataFrame de dim_product.
    """
    logger.info("[TRANSFORM-GOLD] Construyendo dim_product (marcas desnormalizadas)")
    spark = products_df.sparkSession

    # 1. Preparar marcas desduplicadas de forma exacta
    brands_clean = (
        brands_df
        .filter(F.col("brand_id").isNotNull())
        .dropDuplicates()
        .select(
            F.col("brand_id").cast("bigint"),
            F.col("name").alias("brand_name"),
            F.col("country_of_origin"),
            F.col("status").alias("brand_status")
        )
    )

    # 2. Desnormalizar productos con marcas
    products_clean = (
        products_df
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates()
    )

    joined_products = (
        products_clean
        .join(F.broadcast(brands_clean), on="brand_id", how="left")
        .select(
            F.col("product_id").cast("bigint"),
            F.coalesce(F.col("name"), F.lit(UNKNOWN_STRING)).alias("product_name"),
            F.coalesce(F.col("category"), F.lit(UNKNOWN_STRING)).alias("category"),
            F.coalesce(F.col("model"), F.lit(UNKNOWN_STRING)).alias("model"),
            F.col("current_price").cast("decimal(12,2)"),
            F.col("acquisition_cost").cast("decimal(12,2)"),
            F.col("launch_date").cast("date"),
            F.coalesce(F.col("status"), F.lit(UNKNOWN_STRING)).alias("product_status"),
            F.coalesce(F.col("brand_id"), F.lit(UNKNOWN_ID).cast("bigint")).alias("brand_id"),
            F.coalesce(F.col("brand_name"), F.lit(UNKNOWN_STRING)).alias("brand_name"),
            F.coalesce(F.col("country_of_origin"), F.lit(UNKNOWN_STRING)).alias("country_of_origin"),
            F.coalesce(F.col("brand_status"), F.lit(UNKNOWN_STRING)).alias("brand_status")
        )
    )
    valid_products = add_surrogate_key(joined_products, ["product_id"], "product_sk", algorithm="sha256")

    # 3. Registro desconocido
    unknown_product = (
        spark.range(1)
        .select(
            F.lit(UNKNOWN_ID).cast("bigint").alias("product_id"),
            F.lit(UNKNOWN_STRING).alias("product_name"),
            F.lit(UNKNOWN_STRING).alias("category"),
            F.lit(UNKNOWN_STRING).alias("model"),
            F.lit(0.00).cast("decimal(12,2)").alias("current_price"),
            F.lit(0.00).cast("decimal(12,2)").alias("acquisition_cost"),
            F.lit(None).cast("date").alias("launch_date"),
            F.lit(UNKNOWN_STRING).alias("product_status"),
            F.lit(UNKNOWN_ID).cast("bigint").alias("brand_id"),
            F.lit(UNKNOWN_STRING).alias("brand_name"),
            F.lit(UNKNOWN_STRING).alias("country_of_origin"),
            F.lit(UNKNOWN_STRING).alias("brand_status")
        )
    )
    unknown_product = add_surrogate_key(unknown_product, ["product_id"], "product_sk", algorithm="sha256")

    # 4. Unificar y auditoría
    dim_product = (
        valid_products
        .unionByName(unknown_product)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_batch", F.lit(batch_id))
    )

    return dim_product


def build_dim_branch(
    branches_df: DataFrame,
    batch_id: Optional[str] = None
) -> DataFrame:
    """
    Construye la dimensión dim_branch.

    - Clave natural: branch_id (BIGINT).
    - Clave sustituta: branch_sk (SHA-256 de branch_id).
    - Incorpora fila desconocida (-1 / DESCONOCIDO).
    - Desduplicación: dropDuplicates() sobre todas las columnas para evitar descartes arbitrarios.
    - Columnas de auditoría: _ingestion_timestamp, _ingestion_batch.

    :param branches_df: DataFrame de la tabla Silver branches.
    :param batch_id: Identificador de lote/ejecución.
    :return: DataFrame de dim_branch.
    """
    logger.info("[TRANSFORM-GOLD] Construyendo dim_branch")
    spark = branches_df.sparkSession

    valid_branches = (
        branches_df
        .filter(F.col("branch_id").isNotNull())
        .dropDuplicates()
        .select(
            F.col("branch_id").cast("bigint"),
            F.coalesce(F.col("name"), F.lit(UNKNOWN_STRING)).alias("branch_name"),
            F.coalesce(F.col("city"), F.lit(UNKNOWN_STRING)).alias("city"),
            F.coalesce(F.col("department"), F.lit(UNKNOWN_STRING)).alias("department"),
            F.coalesce(F.col("address"), F.lit(UNKNOWN_STRING)).alias("address"),
            F.col("opening_date").cast("date"),
            F.coalesce(F.col("status"), F.lit(UNKNOWN_STRING)).alias("status")
        )
    )
    valid_branches = add_surrogate_key(valid_branches, ["branch_id"], "branch_sk", algorithm="sha256")

    unknown_branch = (
        spark.range(1)
        .select(
            F.lit(UNKNOWN_ID).cast("bigint").alias("branch_id"),
            F.lit(UNKNOWN_STRING).alias("branch_name"),
            F.lit(UNKNOWN_STRING).alias("city"),
            F.lit(UNKNOWN_STRING).alias("department"),
            F.lit(UNKNOWN_STRING).alias("address"),
            F.lit(None).cast("date").alias("opening_date"),
            F.lit(UNKNOWN_STRING).alias("status")
        )
    )
    unknown_branch = add_surrogate_key(unknown_branch, ["branch_id"], "branch_sk", algorithm="sha256")

    dim_branch = (
        valid_branches
        .unionByName(unknown_branch)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_batch", F.lit(batch_id))
    )

    return dim_branch


def build_dim_payment_method(
    payment_methods_df: DataFrame,
    batch_id: Optional[str] = None
) -> DataFrame:
    """
    Construye la dimensión dim_payment_method.

    - Clave natural: payment_method_id (BIGINT).
    - Clave sustituta: payment_method_sk (SHA-256 de payment_method_id).
    - Incorpora fila desconocida (-1 / DESCONOCIDO).
    - Desduplicación: dropDuplicates() sobre todas las columnas para evitar descartes arbitrarios.
    - Columnas de auditoría: _ingestion_timestamp, _ingestion_batch.

    :param payment_methods_df: DataFrame de la tabla Silver payment_methods.
    :param batch_id: Identificador de lote/ejecución.
    :return: DataFrame de dim_payment_method.
    """
    logger.info("[TRANSFORM-GOLD] Construyendo dim_payment_method")
    spark = payment_methods_df.sparkSession

    valid_pm = (
        payment_methods_df
        .filter(F.col("payment_method_id").isNotNull())
        .dropDuplicates()
        .select(
            F.col("payment_method_id").cast("bigint"),
            F.coalesce(F.col("name"), F.lit(UNKNOWN_STRING)).alias("payment_method_name"),
            F.coalesce(F.col("description"), F.lit(UNKNOWN_STRING)).alias("description"),
            F.coalesce(F.col("status"), F.lit(UNKNOWN_STRING)).alias("status")
        )
    )
    valid_pm = add_surrogate_key(valid_pm, ["payment_method_id"], "payment_method_sk", algorithm="sha256")

    unknown_pm = (
        spark.range(1)
        .select(
            F.lit(UNKNOWN_ID).cast("bigint").alias("payment_method_id"),
            F.lit(UNKNOWN_STRING).alias("payment_method_name"),
            F.lit(UNKNOWN_STRING).alias("description"),
            F.lit(UNKNOWN_STRING).alias("status")
        )
    )
    unknown_pm = add_surrogate_key(unknown_pm, ["payment_method_id"], "payment_method_sk", algorithm="sha256")

    dim_pm = (
        valid_pm
        .unionByName(unknown_pm)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_batch", F.lit(batch_id))
    )

    return dim_pm


def build_dim_date(
    spark: SparkSession,
    start_date: str = "2015-01-01",
    end_date: str = "2030-12-31",
    batch_id: Optional[str] = None
) -> DataFrame:
    """
    Construye la dimensión temporal dim_date.

    - Utiliza date_sk como entero YYYYMMDD (ej. 20240302), conforme a la instrucción obligatoria.
    - No utiliza SHA-256 para fechas.
    - Incorpora fila desconocida (-1 / DESCONOCIDO).
    - Genera rango calendario completo (2015 a 2030) garantizando cobertura para todas las entidades.
    - Columnas de auditoría: _ingestion_timestamp, _ingestion_batch.

    :param spark: SparkSession activa.
    :param start_date: Fecha inicio en formato YYYY-MM-DD.
    :param end_date: Fecha fin en formato YYYY-MM-DD.
    :param batch_id: Identificador de lote/ejecución.
    :return: DataFrame de dim_date.
    """
    logger.info("[TRANSFORM-GOLD] Construyendo dim_date (%s a %s, date_sk YYYYMMDD)", start_date, end_date)

    calendar_df = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date
    """)

    calendar_dates = calendar_df.select(
        F.date_format(F.col("full_date"), "yyyyMMdd").cast("int").alias("date_sk"),
        F.date_format(F.col("full_date"), "yyyyMMdd").cast("int").alias("date_id"),
        F.col("full_date").cast("date"),
        F.year(F.col("full_date")).cast("int").alias("year"),
        F.month(F.col("full_date")).cast("int").alias("month"),
        F.dayofmonth(F.col("full_date")).cast("int").alias("day"),
        F.dayofweek(F.col("full_date")).cast("int").alias("day_of_week"),
        F.date_format(F.col("full_date"), "EEEE").alias("day_name"),
        F.date_format(F.col("full_date"), "MMMM").alias("month_name"),
        F.quarter(F.col("full_date")).cast("int").alias("quarter"),
        F.dayofweek(F.col("full_date")).isin(1, 7).alias("is_weekend")
    )

    unknown_date = (
        spark.range(1)
        .select(
            F.lit(UNKNOWN_ID).cast("int").alias("date_sk"),
            F.lit(UNKNOWN_ID).cast("int").alias("date_id"),
            F.lit(None).cast("date").alias("full_date"),
            F.lit(UNKNOWN_ID).cast("int").alias("year"),
            F.lit(UNKNOWN_ID).cast("int").alias("month"),
            F.lit(UNKNOWN_ID).cast("int").alias("day"),
            F.lit(UNKNOWN_ID).cast("int").alias("day_of_week"),
            F.lit(UNKNOWN_STRING).alias("day_name"),
            F.lit(UNKNOWN_STRING).alias("month_name"),
            F.lit(UNKNOWN_ID).cast("int").alias("quarter"),
            F.lit(False).alias("is_weekend")
        )
    )

    dim_date = (
        calendar_dates
        .unionByName(unknown_date)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_batch", F.lit(batch_id))
    )

    return dim_date


# ==============================================================================
# CONSTRUCCIÓN DE TABLAS DE HECHOS
# ==============================================================================

def build_fact_sales(
    sales_df: DataFrame,
    sales_details_df: DataFrame,
    dim_customer_df: DataFrame,
    dim_product_df: DataFrame,
    dim_branch_df: DataFrame,
    dim_payment_method_df: DataFrame,
    dim_date_df: DataFrame,
    batch_id: Optional[str] = None
) -> DataFrame:
    """
    Construye la tabla de hechos fact_sales.

    - Granularidad: 1 fila por línea de detalle de venta (sale_detail_id).
    - Clave natural / primaria: sale_detail_id.
    - Dimensión degenerada: sale_id (para trazabilidad con la cabecera).
    - Claves foráneas resueltas contra dimensiones con asignación estricta de registros desconocidos.
    - Medidas de detalle: quantity, unit_price, line_discount_amount, line_subtotal_amount.
    - NO incorpora montos de cabecera (subtotal_amount, discount_amount, tax_amount, total_amount de sales)
      para evitar duplicación analítica.
    - Desduplicación: dropDuplicates() sobre todas las columnas para evitar descartes arbitrarios.

    :param sales_df: DataFrame Silver de sales (cabeceras).
    :param sales_details_df: DataFrame Silver de sales_details (líneas de detalle).
    :param dim_customer_df: Dimensión clientes.
    :param dim_product_df: Dimensión productos.
    :param dim_branch_df: Dimensión sucursales.
    :param dim_payment_method_df: Dimensión métodos de pago.
    :param dim_date_df: Dimensión temporal.
    :param batch_id: Identificador de lote/ejecución.
    :return: DataFrame de fact_sales.
    """
    logger.info("[TRANSFORM-GOLD] Construyendo fact_sales (granularidad: 1 fila por sale_detail_id)")

    # 1. Cabeceras de ventas: solo atributos relacionales y degenerados (SIN montos de cabecera)
    sales_header = (
        sales_df
        .filter(F.col("sale_id").isNotNull())
        .dropDuplicates()
        .select(
            F.col("sale_id").cast("bigint"),
            F.col("customer_id").cast("bigint"),
            F.col("branch_id").cast("bigint"),
            F.col("payment_method_id").cast("bigint"),
            F.col("sale_timestamp").cast("timestamp"),
            F.coalesce(F.col("status"), F.lit(UNKNOWN_STRING)).alias("sale_status")
        )
    )

    # 2. Detalles de ventas: driver principal con medidas de línea
    details_base = (
        sales_details_df
        .filter(F.col("sale_detail_id").isNotNull())
        .dropDuplicates()
        .select(
            F.col("sale_detail_id").cast("bigint"),
            F.col("sale_id").cast("bigint"),
            F.col("product_id").cast("bigint"),
            F.col("quantity").cast("bigint"),
            F.col("unit_price").cast("decimal(12,2)"),
            F.col("discount_amount").cast("decimal(12,2)").alias("line_discount_amount"),
            F.col("subtotal_amount").cast("decimal(12,2)").alias("line_subtotal_amount")
        )
    )

    # 3. Join de líneas con cabecera (LEFT JOIN para no descartar detalles huérfanos)
    fact_base = details_base.join(sales_header, on="sale_id", how="left")

    # 4. Resolución de claves sustitutas con fallback a registros desconocidos
    fact_with_customer = resolve_dimension_fk(
        fact_df=fact_base,
        dim_df=dim_customer_df,
        fk_col="customer_id",
        sk_col="customer_sk",
        is_broadcast=False
    )

    fact_with_product = resolve_dimension_fk(
        fact_df=fact_with_customer,
        dim_df=dim_product_df,
        fk_col="product_id",
        sk_col="product_sk",
        is_broadcast=False
    )

    fact_with_branch = resolve_dimension_fk(
        fact_df=fact_with_product,
        dim_df=dim_branch_df,
        fk_col="branch_id",
        sk_col="branch_sk",
        is_broadcast=True
    )

    fact_with_pm = resolve_dimension_fk(
        fact_df=fact_with_branch,
        dim_df=dim_payment_method_df,
        fk_col="payment_method_id",
        sk_col="payment_method_sk",
        is_broadcast=True
    )

    fact_with_date = resolve_date_fk(
        fact_df=fact_with_pm,
        timestamp_col="sale_timestamp",
        dim_date_df=dim_date_df,
        target_sk_col="date_sk"
    )

    # 5. Proyección ordenada y auditoría
    fact_sales = (
        fact_with_date
        .select(
            F.col("sale_detail_id"),
            F.col("sale_id"),
            F.col("customer_sk"),
            F.col("product_sk"),
            F.col("branch_sk"),
            F.col("payment_method_sk"),
            F.col("date_sk"),
            F.col("sale_timestamp"),
            F.col("quantity"),
            F.col("unit_price"),
            F.col("line_discount_amount"),
            F.col("line_subtotal_amount"),
            F.col("sale_status"),
            F.current_timestamp().alias("_ingestion_timestamp"),
            F.lit(batch_id).alias("_ingestion_batch")
        )
    )

    return fact_sales


def build_fact_warranties(
    warranties_df: DataFrame,
    dim_customer_df: DataFrame,
    dim_product_df: DataFrame,
    dim_date_df: DataFrame,
    batch_id: Optional[str] = None
) -> DataFrame:
    """
    Construye la tabla de hechos fact_warranties.

    - Granularidad: 1 fila por solicitud de postventa de garantía (after_sales_id).
    - Clave natural / primaria: after_sales_id.
    - Dimensión degenerada: sale_id (trazabilidad con la venta).
    - Claves foráneas: customer_sk, product_sk, date_sk (a partir de request_timestamp).
    - Desduplicación: dropDuplicates() sobre todas las columnas para evitar descartes arbitrarios.

    :param warranties_df: DataFrame Silver de warranties.
    :param dim_customer_df: Dimensión clientes.
    :param dim_product_df: Dimensión productos.
    :param dim_date_df: Dimensión temporal.
    :param batch_id: Identificador de lote/ejecución.
    :return: DataFrame de fact_warranties.
    """
    logger.info("[TRANSFORM-GOLD] Construyendo fact_warranties")

    warranties_base = (
        warranties_df
        .filter(F.col("after_sales_id").isNotNull())
        .dropDuplicates()
        .select(
            F.col("after_sales_id").cast("bigint"),
            F.col("sale_id").cast("bigint"),
            F.col("customer_id").cast("bigint"),
            F.col("product_id").cast("bigint"),
            F.col("request_timestamp").cast("timestamp"),
            F.col("resolution_timestamp").cast("timestamp"),
            F.coalesce(F.col("type"), F.lit(UNKNOWN_STRING)).alias("warranty_type"),
            F.col("purchased_quantity").cast("bigint"),
            F.coalesce(F.col("serial_number"), F.lit(UNKNOWN_STRING)).alias("serial_number"),
            F.coalesce(F.col("reason"), F.lit(UNKNOWN_STRING)).alias("reason"),
            F.coalesce(F.col("description"), F.lit(UNKNOWN_STRING)).alias("description"),
            F.coalesce(F.col("status"), F.lit(UNKNOWN_STRING)).alias("status"),
            F.coalesce(F.col("observation"), F.lit(UNKNOWN_STRING)).alias("observation")
        )
    )

    fact_with_customer = resolve_dimension_fk(
        fact_df=warranties_base,
        dim_df=dim_customer_df,
        fk_col="customer_id",
        sk_col="customer_sk",
        is_broadcast=False
    )

    fact_with_product = resolve_dimension_fk(
        fact_df=fact_with_customer,
        dim_df=dim_product_df,
        fk_col="product_id",
        sk_col="product_sk",
        is_broadcast=False
    )

    fact_with_date = resolve_date_fk(
        fact_df=fact_with_product,
        timestamp_col="request_timestamp",
        dim_date_df=dim_date_df,
        target_sk_col="date_sk"
    )

    fact_warranties = (
        fact_with_date
        .select(
            F.col("after_sales_id"),
            F.col("sale_id"),
            F.col("customer_sk"),
            F.col("product_sk"),
            F.col("date_sk"),
            F.col("request_timestamp"),
            F.col("resolution_timestamp"),
            F.col("warranty_type"),
            F.col("purchased_quantity"),
            F.col("serial_number"),
            F.col("reason"),
            F.col("description"),
            F.col("status"),
            F.col("observation"),
            F.current_timestamp().alias("_ingestion_timestamp"),
            F.lit(batch_id).alias("_ingestion_batch")
        )
    )

    return fact_warranties


def build_fact_returns(
    returns_df: DataFrame,
    dim_customer_df: DataFrame,
    dim_product_df: DataFrame,
    dim_date_df: DataFrame,
    batch_id: Optional[str] = None
) -> DataFrame:
    """
    Construye la tabla de hechos fact_returns.

    - Granularidad: 1 fila por solicitud de devolución (after_sales_id).
    - Clave natural / primaria: after_sales_id.
    - Dimensión degenerada: sale_id (trazabilidad con la venta).
    - Medidas de devolución: purchased_quantity, returned_quantity, approved_refund_percentage, refund_amount.
    - Claves foráneas: customer_sk, product_sk, date_sk (a partir de request_timestamp).
    - Desduplicación: dropDuplicates() sobre todas las columnas para evitar descartes arbitrarios.

    :param returns_df: DataFrame Silver de returns.
    :param dim_customer_df: Dimensión clientes.
    :param dim_product_df: Dimensión productos.
    :param dim_date_df: Dimensión temporal.
    :param batch_id: Identificador de lote/ejecución.
    :return: DataFrame de fact_returns.
    """
    logger.info("[TRANSFORM-GOLD] Construyendo fact_returns")

    returns_base = (
        returns_df
        .filter(F.col("after_sales_id").isNotNull())
        .dropDuplicates()
        .select(
            F.col("after_sales_id").cast("bigint"),
            F.col("sale_id").cast("bigint"),
            F.col("customer_id").cast("bigint"),
            F.col("product_id").cast("bigint"),
            F.col("request_timestamp").cast("timestamp"),
            F.col("resolution_timestamp").cast("timestamp"),
            F.coalesce(F.col("type"), F.lit(UNKNOWN_STRING)).alias("return_type"),
            F.col("purchased_quantity").cast("bigint"),
            F.col("returned_quantity").cast("bigint"),
            F.coalesce(F.col("reason"), F.lit(UNKNOWN_STRING)).alias("reason"),
            F.coalesce(F.col("description"), F.lit(UNKNOWN_STRING)).alias("description"),
            F.coalesce(F.col("status"), F.lit(UNKNOWN_STRING)).alias("status"),
            F.coalesce(F.col("observation"), F.lit(UNKNOWN_STRING)).alias("observation"),
            F.col("approved_refund_percentage").cast("bigint"),
            F.col("refund_amount").cast("decimal(12,2)"),
            F.coalesce(F.col("refund_currency"), F.lit("COP")).alias("refund_currency")
        )
    )

    fact_with_customer = resolve_dimension_fk(
        fact_df=returns_base,
        dim_df=dim_customer_df,
        fk_col="customer_id",
        sk_col="customer_sk",
        is_broadcast=False
    )

    fact_with_product = resolve_dimension_fk(
        fact_df=fact_with_customer,
        dim_df=dim_product_df,
        fk_col="product_id",
        sk_col="product_sk",
        is_broadcast=False
    )

    fact_with_date = resolve_date_fk(
        fact_df=fact_with_product,
        timestamp_col="request_timestamp",
        dim_date_df=dim_date_df,
        target_sk_col="date_sk"
    )

    fact_returns = (
        fact_with_date
        .select(
            F.col("after_sales_id"),
            F.col("sale_id"),
            F.col("customer_sk"),
            F.col("product_sk"),
            F.col("date_sk"),
            F.col("request_timestamp"),
            F.col("resolution_timestamp"),
            F.col("return_type"),
            F.col("purchased_quantity"),
            F.col("returned_quantity"),
            F.col("reason"),
            F.col("description"),
            F.col("status"),
            F.col("observation"),
            F.col("approved_refund_percentage"),
            F.col("refund_amount"),
            F.col("refund_currency"),
            F.current_timestamp().alias("_ingestion_timestamp"),
            F.lit(batch_id).alias("_ingestion_batch")
        )
    )

    return fact_returns
