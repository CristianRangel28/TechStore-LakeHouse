"""Generador de garantías de TechStore."""

import random

from scripts.generators.sales_data import build_sale_and_details

from .config import (
    POST_SALES_SEED,
    WARRANTY_REASONS,
    WARRANTY_STATUSES,
    WARRANTY_STATUS_WEIGHTS,
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

WARRANTY_DESCRIPTIONS = {
    "FALLA_TECNICA":
        "El producto presenta una falla durante su funcionamiento.",

    "DEFECTO_FABRICACION":
        "Se identifico un posible defecto de fabricacion.",

    "DANO_COMPONENTE":
        "Uno de los componentes presenta danos.",

    "SOBRECALENTAMIENTO":
        "El producto presenta temperaturas anormales.",

    "FALLA_ELECTRICA":
        "El producto presenta una falla en el sistema electrico.",
}


WARRANTY_OBSERVATIONS = {
    "APROBADA":
        "La solicitud cumple las condiciones de garantia.",

    "RECHAZADA":
        "La solicitud no cumple las condiciones de garantia.",

    "EN_REVISION":
        "Pendiente de evaluacion tecnica.",
}


# ==========================================================
# GENERACIÓN DE REGISTROS
# ==========================================================

def warranty_records():

    warranty_ids, _ = build_post_sales_assignment()

    for post_sale_id, sale_id in enumerate(
        warranty_ids,
        start=1,
    ):

        # Reconstruir venta y detalles.
        sale, details = build_sale_and_details(sale_id)

        rng = random.Random(
            POST_SALES_SEED + sale_id + 1_000_000_000
        )

        # Seleccionar producto comprado.
        selected_detail = rng.choice(details)

        # Seleccionar estado y motivo.
        status = rng.choices(
            WARRANTY_STATUSES,
            weights=WARRANTY_STATUS_WEIGHTS,
            k=1,
        )[0]

        reason = rng.choice(WARRANTY_REASONS)

        request_date = create_request_date(
            sale["fecha_venta"],
            rng,
        )

        resolution_date = (
            request_date
            if status != "EN_REVISION"
            else None
        )

        yield {
            "id_postventa": post_sale_id,

            "id_venta": sale["id_venta"],

            "id_cliente": sale["id_cliente"],

            "fecha_solicitud": request_date,

            "tipo": "GARANTIA",

            "producto": {
                "id_producto": selected_detail["id_producto"],

                "cantidad_comprada": selected_detail["cantidad"],

                "numero_serie": (
                    f"TS-{selected_detail['id_producto']:06d}"
                    f"-{sale_id:08d}"
                ),
            },

            "detalle": {
                "motivo": {
                    "categoria": reason,

                    "descripcion": WARRANTY_DESCRIPTIONS[reason],
                },

                "estado": {
                    "nombre": status,

                    "observacion": WARRANTY_OBSERVATIONS[status],
                },
            },

            "fecha_resolucion": resolution_date,
        }


# ==========================================================
# EJECUCIÓN
# ==========================================================

def generate_warranties(lote=LOTE_DEFAULT):

    return write_json_lines(
        warranty_records(),
        "garantias.json",
        lote,
    )


if __name__ == "__main__":

    path, count = generate_warranties(parse_lote())

    print(f"Archivo generado: {path}")

    print(f"Registros: {count:,}")
