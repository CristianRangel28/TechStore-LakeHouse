"""Genera detalle_ventas.csv. Requiere productos y la misma config que ventas."""

from .config import COUNTS
from .sales_data import build_sale_and_details
from .utils import open_csv, print_completion

FIELDS = [
    "id_detalle", "id_venta", "id_producto", "cantidad",
    "precio_unitario", "descuento", "subtotal",
]


def generate():
    file_obj, writer, path = open_csv("detalle_ventas.csv", FIELDS)
    detail_id = 1

    try:
        for sale_id in range(1, COUNTS["sales"] + 1):
            _, detail_rows = build_sale_and_details(sale_id)

            for row in detail_rows:
                row["id_detalle"] = detail_id
                writer.writerow(row)
                detail_id += 1

            if sale_id % 100_000 == 0:
                print(
                    f"Detalles generados: {detail_id - 1:,}/"
                    f"{COUNTS['sales_details']:,}"
                )
    finally:
        file_obj.close()

    generated = detail_id - 1
    expected = COUNTS["sales_details"]
    if generated != expected:
        raise ValueError(
            f"Se generaron {generated:,} detalles; se esperaban {expected:,}. "
            "Revisa COUNTS y SALES_WITH_TWO_DETAILS en config.py."
        )

    print_completion("detalle_ventas.csv", generated, path)


if __name__ == "__main__":
    generate()
