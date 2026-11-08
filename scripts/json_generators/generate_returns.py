"""Generador de devoluciones de TechStore."""

import random
from .config import POST_SALES_COUNTS
from decimal import Decimal, ROUND_HALF_UP

from scripts.generators.sales_data import build_sale_and_details

from .config import (
    POST_SALES_SEED,
    RETURN_REASONS,
    RETURN_STATUSES,
    RETURN_STATUS_WEIGHTS,
)

from .utils import (
    build_post_sales_assignment,
    create_request_date,
    write_json_lines,
)
from scripts.generators.config import LOTE_DEFAULT, parse_lote


# ==========================================================
# DESCRIPCIONES
# ==========================================================

RETURN_DESCRIPTIONS = {
    "PRODUCTO_EQUIVOCADO":
        "El cliente recibio un producto diferente al solicitado.",

    "PRODUCTO_DANADO":
        "El producto llego con danos visibles.",

    "INSATISFACCION":
        "El cliente no esta satisfecho con el producto.",

    "PRODUCTO_INCOMPLETO":
        "El pedido no incluye todos los elementos anunciados.",

    "ERROR_EN_PEDIDO":
        "Se identifico un error en la preparacion del pedido.",
}


RETURN_OBSERVATIONS = {
    "APROBADA":
        "La devolucion fue aprobada y esta pendiente de completar.",

    "RECHAZADA":
        "La solicitud de devolucion fue rechazada.",

    "PENDIENTE":
        "La solicitud esta pendiente de validacion.",

    "COMPLETADA":
        "La devolucion fue procesada correctamente.",
}


# ==========================================================
# CÁLCULO DE REEMBOLSOS
# ==========================================================

def calculate_refund(value, percentage):

    amount = (
        Decimal(value) * Decimal(percentage) / Decimal(100)
    ).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )

    return str(amount)


# ==========================================================
# GENERACIÓN DE REGISTROS
# ==========================================================

def return_records():

    _, return_ids = build_post_sales_assignment()

    for post_sale_id, sale_id in enumerate(
        return_ids,
        start=POST_SALES_COUNTS["warranties"] + 1,
    ):

        sale, details = build_sale_and_details(sale_id)

        rng = random.Random(
            POST_SALES_SEED + sale_id + 2_000_000_000
        )

        selected_detail = rng.choice(details)

        status = rng.choices(
            RETURN_STATUSES,
            weights=RETURN_STATUS_WEIGHTS,
            k=1,
        )[0]

        reason = rng.choice(RETURN_REASONS)

        request_date = create_request_date(
            sale["fecha_venta"],
            rng,
        )

        # Solo las devoluciones aprobadas o completadas
        # pueden tener un reembolso.
        if status in ("APROBADA", "COMPLETADA"):

            refund_percentage = rng.choice(
                [50, 75, 100]
            )

        else:

            refund_percentage = 0

        refund_value = calculate_refund(
            selected_detail["subtotal"],
            refund_percentage,
        )

        resolution_date = (
            request_date
            if status in ("RECHAZADA", "COMPLETADA")
            else None
        )

        yield {
            "id_postventa": post_sale_id,

            "id_venta": sale["id_venta"],

            "id_cliente": sale["id_cliente"],

            "fecha_solicitud": request_date,

            "tipo": "DEVOLUCION",

            "producto": {
                "id_producto": selected_detail["id_producto"],

                "cantidad_comprada": selected_detail["cantidad"],

                "cantidad_devuelta": (
                    selected_detail["cantidad"]
                    if refund_percentage > 0
                    else 0
                ),
            },

            "detalle": {
                "motivo": {
                    "categoria": reason,

                    "descripcion": RETURN_DESCRIPTIONS[reason],
                },

                "estado": {
                    "nombre": status,

                    "observacion": RETURN_OBSERVATIONS[status],
                },
            },

            "reembolso": {
                "porcentaje_aprobado": refund_percentage,

                "valor": refund_value,

                "moneda": "COP",
            },

            "fecha_resolucion": resolution_date,
        }


# ==========================================================
# EJECUCIÓN
# ==========================================================

def generate_returns(lote=LOTE_DEFAULT):

    return write_json_lines(
        return_records(),
        "devoluciones.json",
        lote,
    )


if __name__ == "__main__":

    path, count = generate_returns(parse_lote())

    print(f"Archivo generado: {path}")

    print(f"Registros: {count:,}")
