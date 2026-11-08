"""Funciones compartidas para generar registros de postventa."""
from functools import lru_cache
import json
import random

from datetime import datetime, timedelta


from .config import (
    JSON_ENCODING,
    JSON_OUTPUT_DIR,
    MAX_DAYS_AFTER_SALE,
    MIN_DAYS_AFTER_SALE,
    POST_SALES_COUNTS,
    POST_SALES_SEED,
)

from scripts.generators.config import COUNTS


# ==========================================================
# ASIGNACIÓN DE VENTAS
# ==========================================================

@lru_cache(maxsize=1)
def build_post_sales_assignment():
    """
    Asigna ventas únicas a garantías y devoluciones.

    Una venta solamente puede tener un proceso de posventa:
    garantía o devolución, nunca ambos.

    La asignación se calcula una sola vez por ejecución
    y se reutiliza mediante caché.
    """

    total_sales = COUNTS["sales"]
    total_warranties = POST_SALES_COUNTS["warranties"]
    total_returns = POST_SALES_COUNTS["returns"]
    total_post_sales = total_warranties + total_returns

    if total_post_sales > total_sales:
        raise ValueError(
            "La cantidad de procesos de posventa supera "
            "la cantidad de ventas disponibles."
        )

    rng = random.Random(POST_SALES_SEED)

    selected_sales = rng.sample(
        range(1, total_sales + 1),
        total_post_sales
    )

    warranty_sales = tuple(
        selected_sales[:total_warranties]
    )

    return_sales = tuple(
        selected_sales[total_warranties:]
    )

    return warranty_sales, return_sales


# ==========================================================
# GENERACIÓN DE FECHAS
# ==========================================================

def create_request_date(sale_datetime, rng):
    """
    Genera una fecha de solicitud posterior a la venta.

    La solicitud ocurre entre 1 y 30 días después.
    """

    sale_date = datetime.strptime(
        sale_datetime,
        "%Y-%m-%d %H:%M:%S",
    )

    request_date = sale_date + timedelta(
        days=rng.randint(
            MIN_DAYS_AFTER_SALE,
            MAX_DAYS_AFTER_SALE,
        ),
        seconds=rng.randint(0, 86399),
    )

    return request_date.strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# ==========================================================
# ESCRITURA JSON LINES
# ==========================================================

def write_json_lines(records, filename):
    """
    Escribe registros JSON uno por línea.

    Permite procesar grandes volúmenes sin mantener
    todos los registros en memoria.
    """

    JSON_OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = JSON_OUTPUT_DIR / filename

    count = 0

    with output_path.open(
        "w",
        encoding=JSON_ENCODING,
    ) as file:

        for record in records:

            json_line = json.dumps(
                record,
                ensure_ascii=False,
            )

            file.write(json_line + "\n")

            count += 1

    return output_path, count
