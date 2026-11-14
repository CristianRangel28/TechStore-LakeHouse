"""
Lectura estandarizada de datos desde Amazon S3 y AWS Glue Data Catalog.
"""

import logging

logger = logging.getLogger(__name__)


def read_catalog_table(spark, database, table_name, partition_filter=None):
    """
    Lee una tabla estándar registrada en el Glue Data Catalog (capa Bronze).

    Importante: Esta función accede a las tablas a través del catálogo de sesión
    por defecto de Spark (SerDe Hive/CSV/JSON creadas por el Glue Crawler).
    NO utiliza el prefijo del catálogo Iceberg para evitar errores de incompatibilidad
    de formato.

    Utiliza la referencia programática `spark.table(f"{database}.{table_name}")` sin
    backticks literales para asegurar compatibilidad entre versiones del metastore.

    :param spark: SparkSession activa.
    :param database: Nombre de la base de datos en Glue Data Catalog (ej. techstore_bronze).
    :param table_name: Nombre de la tabla física (ej. raw_clientes).
    :param partition_filter: Condición Column de PySpark opcional para aplicar partition pruning.
    :return: DataFrame de PySpark.
    """
    if not database or not str(database).strip():
        raise ValueError("El parámetro 'database' no puede ser nulo ni vacío.")
    if not table_name or not str(table_name).strip():
        raise ValueError("El parámetro 'table_name' no puede ser nulo ni vacío.")

    table_identifier = f"{database.strip()}.{table_name.strip()}"
    logger.info("[READ-CATALOG] Leyendo tabla del catálogo estándar: %s", table_identifier)

    try:
        dataframe = spark.table(table_identifier)
        if partition_filter is not None:
            logger.info("[READ-CATALOG] Aplicando filtro de partición sobre %s", table_identifier)
            dataframe = dataframe.filter(partition_filter)
    except Exception as exc:
        logger.error(
            "[READ-CATALOG] Error al intentar leer la tabla '%s' desde Glue Data Catalog: %s",
            table_identifier, exc
        )
        raise

    logger.info("[READ-CATALOG] Columnas cargadas de %s: %s", table_name, dataframe.columns)
    return dataframe


def read_iceberg_table(spark, catalog, database, table_name):
    """
    Lee una tabla Iceberg desde el catálogo configurado (capas Silver y Gold).

    Utiliza el catálogo Iceberg registrado en Spark (ej. glue_catalog) donde
    residen las tablas en formato Iceberg v2.

    :param spark: SparkSession activa.
    :param catalog: Nombre del catálogo SparkIceberg (ej. glue_catalog).
    :param database: Nombre de la base de datos (ej. techstore_silver, techstore_gold).
    :param table_name: Nombre de la tabla Iceberg (ej. customers, fact_sales).
    :return: DataFrame de PySpark.
    """
    if not catalog or not database or not table_name:
        raise ValueError("catalog, database y table_name son obligatorios para leer tablas Iceberg.")

    full_table_name = f"`{catalog}`.`{database}`.`{table_name}`"
    logger.info("[READ-ICEBERG] Leyendo tabla Iceberg: %s", full_table_name)
    dataframe = spark.table(full_table_name)
    logger.info("[READ-ICEBERG] Columnas cargadas de %s: %s", table_name, dataframe.columns)
    return dataframe


def read_s3_csv(
    spark,
    path,
    header=True,
    infer_schema=False,
    schema=None,
    delimiter=",",
    options=None
):
    """
    Lee archivos CSV directamente desde una ruta de Amazon S3.

    :param spark: SparkSession activa.
    :param path: Ruta en S3 (ej. s3://bucket/bronze/ventas/lote=1/).
    :param header: Booleano indicando si la primera línea contiene encabezados.
    :param infer_schema: Booleano para inferencia automática de tipos (desaconsejado para 9M+ registros).
    :param schema: StructType opcional para definición explícita de esquema.
    :param delimiter: Delimitador de campos (por defecto coma).
    :param options: Diccionario opcional con configuraciones adicionales de Spark CSV.
    :return: DataFrame de PySpark.
    """
    logger.info("[READ-S3-CSV] Leyendo archivos CSV desde: %s", path)
    reader = (
        spark.read
        .format("csv")
        .option("header", str(header).lower())
        .option("delimiter", delimiter)
    )

    if schema is not None:
        reader = reader.schema(schema)
    elif infer_schema:
        reader = reader.option("inferSchema", "true")

    if options:
        for key, value in options.items():
            reader = reader.option(key, value)

    return reader.load(path)


def read_s3_json(
    spark,
    path,
    multiline=False,
    schema=None,
    options=None
):
    """
    Lee archivos JSON directamente desde una ruta de Amazon S3.

    :param spark: SparkSession activa.
    :param path: Ruta en S3 (ej. s3://bucket/bronze/garantias/lote=1/).
    :param multiline: Booleano para admitir registros JSON multilínea.
    :param schema: StructType opcional para schema enforcement.
    :param options: Diccionario opcional con configuraciones adicionales de Spark JSON.
    :return: DataFrame de PySpark.
    """
    logger.info("[READ-S3-JSON] Leyendo archivos JSON desde: %s", path)
    reader = (
        spark.read
        .format("json")
        .option("multiline", str(multiline).lower())
    )

    if schema is not None:
        reader = reader.schema(schema)

    if options:
        for key, value in options.items():
            reader = reader.option(key, value)

    return reader.load(path)
