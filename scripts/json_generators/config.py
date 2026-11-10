"""Configuración de los generadores JSON de TechStore."""

from scripts.generators.config import (
    COUNTS,
    DATA_DIR,
    LOTE_DEFAULT,
    SEED,
)

# ==========================================================
# DIRECTORIOS
# ==========================================================

# ==========================================================
# VOLUMEN DE DATOS
# ==========================================================

POST_SALES_COUNTS = {
    "warranties": 500_000,
    "returns": 494_495,
}

TOTAL_POST_SALES = sum(POST_SALES_COUNTS.values())


# ==========================================================
# CONFIGURACIÓN GENERAL
# ==========================================================

POST_SALES_SEED = SEED + 100_000

JSON_ENCODING = "utf-8"

MIN_DAYS_AFTER_SALE = 1
MAX_DAYS_AFTER_SALE = 30


# ==========================================================
# GARANTÍAS
# ==========================================================

WARRANTY_STATUSES = [
    "APROBADA",
    "RECHAZADA",
    "EN_REVISION",
]

WARRANTY_STATUS_WEIGHTS = [
    45,
    15,
    40,
]

WARRANTY_REASONS = [
    "FALLA_TECNICA",
    "DEFECTO_FABRICACION",
    "DANO_COMPONENTE",
    "SOBRECALENTAMIENTO",
    "FALLA_ELECTRICA",
]


# ==========================================================
# DEVOLUCIONES
# ==========================================================

RETURN_STATUSES = [
    "APROBADA",
    "RECHAZADA",
    "PENDIENTE",
    "COMPLETADA",
]

RETURN_STATUS_WEIGHTS = [
    30,
    15,
    20,
    35,
]

RETURN_REASONS = [
    "PRODUCTO_EQUIVOCADO",
    "PRODUCTO_DANADO",
    "INSATISFACCION",
    "PRODUCTO_INCOMPLETO",
    "ERROR_EN_PEDIDO",
]


# ==========================================================
# VALIDACIONES
# ==========================================================

assert TOTAL_POST_SALES <= COUNTS["sales"], (
    "La cantidad de procesos de postventa supera "
    "el número de ventas disponibles."
)

assert all(
    count >= 0
    for count in POST_SALES_COUNTS.values()
), "Las cantidades no pueden ser negativas."

assert len(WARRANTY_STATUSES) == len(WARRANTY_STATUS_WEIGHTS)

assert len(RETURN_STATUSES) == len(RETURN_STATUS_WEIGHTS)
