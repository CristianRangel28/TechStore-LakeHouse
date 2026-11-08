"""Configuración central de los generadores de TechStore."""

from pathlib import Path
import argparse

# La carpeta de salida queda dentro del proyecto, independientemente
# del directorio desde el que se ejecute un script.
PROJECT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_DIR / "data"
LOTE_DEFAULT = 1


def parse_lote():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lote", type=int, default=LOTE_DEFAULT)
    lote = parser.parse_args().lote
    if lote != LOTE_DEFAULT:
        parser.error("la generación para lotes distintos de 1 aún no está soportada")
    return lote

SEED = 20261106

# ==========================================================
# CONFIGURACIÓN DEL VOLUMEN DE DATOS
# ==========================================================
# Volumen total objetivo: 7.920.155 registros CSV.
# Incluye dimensiones, ventas y detalles de ventas.
# Cada venta tendrá entre 1 y 5 productos.
# ==========================================================

COUNTS = {
    "customers": 500_000,
    "brands": 100,
    "products": 100_000,
    "branches": 50,
    "payment_methods": 5,
    "sales": 1_500_000,
    "sales_details": 5_820_000,
}

# Distribución exacta de productos por venta.
# La suma de estos valores debe ser igual a COUNTS["sales"].

SALES_DETAIL_DISTRIBUTION = {
    1: 75_000,
    2: 150_000,
    3: 225_000,
    4: 480_000,
    5: 570_000,
}


START_DATE = "2023-01-01"
END_DATE = "2026-10-31"

CSV_ENCODING = "utf-8"
CSV_DELIMITER = ","
BATCH_SIZE = 25_000

# -------------------------------------------------
# VALIDACIONES DE CONFIGURACIÓN
# -------------------------------------------------

assert sum(SALES_DETAIL_DISTRIBUTION.values()) == COUNTS["sales"], (
    "La distribución de productos no coincide con el número de ventas."
)

expected_details = sum(
    products_per_sale * number_of_sales
    for products_per_sale, number_of_sales
    in SALES_DETAIL_DISTRIBUTION.items()
)

assert expected_details == COUNTS["sales_details"], (
    "La distribución no coincide con el número de detalles esperado."
)
