"""Genera metodos_pago.csv."""

from .config import LOTE_DEFAULT, parse_lote
from .utils import open_csv, print_completion

PAYMENT_METHODS = [
    ("Efectivo", "Pago realizado en caja."),
    ("Tarjeta de credito", "Pago con tarjeta de credito."),
    ("Tarjeta debito", "Pago con tarjeta debito."),
    ("Transferencia bancaria", "Transferencia desde entidad financiera."),
    ("Billetera digital", "Pago mediante billetera digital."),
]
FIELDS = ["id_metodo_pago", "nombre", "descripcion", "estado"]


def generate(lote=LOTE_DEFAULT):
    file_obj, writer, path = open_csv("metodos_pago.csv", FIELDS, lote)
    try:
        for method_id, (name, description) in enumerate(PAYMENT_METHODS, start=1):
            writer.writerow({
                "id_metodo_pago": method_id,
                "nombre": name,
                "descripcion": description,
                "estado": "ACTIVO",
            })
    finally:
        file_obj.close()

    print_completion("metodos_pago.csv", len(PAYMENT_METHODS), path)


if __name__ == "__main__":
    generate(parse_lote())
