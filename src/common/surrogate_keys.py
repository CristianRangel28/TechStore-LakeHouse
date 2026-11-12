"""
Generación distribuida y determinística de claves subrogadas (Surrogate Keys).

Diseñado para soportar modelos dimensionales en Apache Spark sin degradación de rendimiento.
Utiliza serialización estructurada JSON para prevenir colisiones por delimitadores y nulos.
"""

import logging
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def generate_surrogate_key_expr(
    source_cols,
    algorithm="sha256"
):
    """
    Genera una expresión Column de PySpark para calcular una clave subrogada determinística.

    Serialización estructurada (Struct -> JSON):
    - Se construye una estructura `F.struct(*source_cols)` que preserva los tipos nativos
      de Spark (Integer, String, Timestamp, Decimal, etc.).
    - Se serializa a JSON canónico mediante `F.to_json(..., options={'ignoreNullFields': 'false'})`
      para que los atributos con valor `NULL` se serialicen explícitamente como `null`,
      evitando ambigüedades con cadenas vacías, tokens centinelas o caracteres especiales.
    - Elimina completamente las vulnerabilidades de colisión por delimitador presentes en
      concatenaciones planas (ej. ('A||B', 'C') vs ('A', 'B||C')).

    Determinismo:
    - Las claves son estrictamente determinísticas para una misma tupla de entrada bajo
      la misma configuración de Spark (respetando la zona horaria para marcas temporales).

    Algoritmos soportados:
    - 'sha256' (Predeterminado - Recomendado para unicidad dimensional absoluta):
        Genera un hash SHA-256 de 256 bits (cadena hexadecimal de 64 caracteres).
        Probabilidad de colisión matemáticamente nula (< 10^-70 para 9M registros).
    - 'xxhash64' (Opción secundaria para máxima velocidad en joins):
        Genera un entero de 64 bits (LongType). Alta velocidad y menor espacio de almacenamiento,
        con una probabilidad teórica de colisión de ~2.2 x 10^-6 para 9M registros.

    Restricciones de diseño:
    - No utiliza `row_number()` global (evita OutOfMemory al no forzar una sola partición).
    - No utiliza `monotonically_increasing_id()` (no determinístico entre ejecuciones).

    :param source_cols: Nombre de columna o lista de nombres de columnas origen.
    :param algorithm: 'sha256' (predeterminado) o 'xxhash64' (insensible a mayúsculas).
    :return: Expresión Column de PySpark.
    """
    if isinstance(source_cols, str):
        source_cols = [source_cols]

    if not source_cols:
        raise ValueError("source_cols debe contener al menos un nombre de columna.")

    algo = algorithm.lower().strip()
    if algo not in ("sha256", "xxhash64"):
        raise ValueError(f"Algoritmo '{algorithm}' no soportado. Debe ser 'sha256' o 'xxhash64'.")

    # Empaquetado estructurado conservando tipos nativos
    struct_col = F.struct(*[F.col(c) for c in source_cols])

    # Serialización a JSON canónico con conservación explícita de campos nulos
    json_col = F.to_json(struct_col, options={"ignoreNullFields": "false"})

    if algo == "sha256":
        return F.sha2(json_col, 256)
    else:
        return F.xxhash64(json_col)


def add_surrogate_key(
    dataframe,
    source_cols,
    target_col,
    algorithm="sha256"
):
    """
    Añade una columna de clave subrogada determinística a un DataFrame de PySpark.

    :param dataframe: DataFrame de PySpark origen.
    :param source_cols: Columna o lista de columnas que componen la clave natural de negocio.
    :param target_col: Nombre de la nueva columna destino.
    :param algorithm: Algoritmo de hash a utilizar ('sha256' o 'xxhash64').
    :return: Nuevo DataFrame con la columna añadida.
    """
    if isinstance(source_cols, str):
        source_cols = [source_cols]

    if not source_cols:
        raise ValueError("source_cols debe contener al menos un nombre de columna.")

    # Validar existencia de columnas en el DataFrame
    missing_cols = [col for col in source_cols if col not in dataframe.columns]
    if missing_cols:
        raise ValueError(
            f"Las columnas fuente {missing_cols} no existen en el DataFrame. "
            f"Columnas disponibles: {dataframe.columns}"
        )

    logger.info(
        "[SURROGATE-KEY] Generando clave '%s' desde %s mediante %s (JSON Struct)",
        target_col,
        source_cols,
        algorithm.lower()
    )

    key_expression = generate_surrogate_key_expr(
        source_cols=source_cols,
        algorithm=algorithm
    )

    return dataframe.withColumn(target_col, key_expression)
