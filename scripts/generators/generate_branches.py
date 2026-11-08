"""Genera sucursales.csv."""

import random

from .config import COUNTS, END_DATE, LOTE_DEFAULT, SEED, parse_lote
from .utils import open_csv, print_completion, random_date, write_batches

LOCATIONS = [
    ("Bogota", "Cundinamarca"), ("Medellin", "Antioquia"),
    ("Cali", "Valle del Cauca"), ("Barranquilla", "Atlantico"),
    ("Bucaramanga", "Santander"), ("Cucuta", "Norte de Santander"),
    ("Cartagena", "Bolivar"), ("Pereira", "Risaralda"),
    ("Manizales", "Caldas"), ("Ibague", "Tolima"),
    ("Villavicencio", "Meta"), ("Santa Marta", "Magdalena"),
]
FIELDS = [
    "id_sucursal", "nombre", "ciudad", "departamento",
    "direccion", "fecha_apertura", "estado",
]


def generate(lote=LOTE_DEFAULT):
    rng = random.Random(SEED + 3)
    file_obj, writer, path = open_csv("sucursales.csv", FIELDS, lote)

    def rows():
        for branch_id in range(1, COUNTS["branches"] + 1):
            city, department = LOCATIONS[(branch_id - 1) % len(LOCATIONS)]
            yield {
                "id_sucursal": branch_id,
                "nombre": f"TechStore {city} {branch_id:02d}",
                "ciudad": city,
                "departamento": department,
                "direccion": (
                    f"Calle {rng.randint(1, 150)} # "
                    f"{rng.randint(1, 100)}-{rng.randint(1, 99)}"
                ),
                "fecha_apertura": random_date(
                    rng, "2015-01-01", END_DATE
                ).isoformat(),
                "estado": "ACTIVA" if rng.random() < 0.96 else "INACTIVA",
            }

    try:
        write_batches(writer, rows())
    finally:
        file_obj.close()

    print_completion("sucursales.csv", COUNTS["branches"], path)


if __name__ == "__main__":
    generate(parse_lote())
