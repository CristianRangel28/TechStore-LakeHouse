"""Genera ventas.csv. Requiere las dimensiones ya generadas."""

from .config import COUNTS
from .sales_data import build_sale_and_details
from .utils import open_csv, print_completion

FIELDS = [
    "id_venta", "id_cliente", "id_sucursal", "id_metodo_pago",
    "fecha_venta", "subtotal", "descuento", "impuesto", "total", "estado",
]


def generate():
    file_obj, writer, path = open_csv("ventas.csv", FIELDS)
    try:
        for sale_id in range(1, COUNTS["sales"] + 1):
            sale_row, _ = build_sale_and_details(sale_id)
            writer.writerow(sale_row)

            if sale_id % 100_000 == 0:
                print(f"Ventas generadas: {sale_id:,}/{COUNTS['sales']:,}")
    finally:
        file_obj.close()

    print_completion("ventas.csv", COUNTS["sales"], path)


if __name__ == "__main__":
    generate()
