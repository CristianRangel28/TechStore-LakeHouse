"""Genera marcas.csv."""

import random

from .config import COUNTS, END_DATE, LOTE_DEFAULT, SEED, parse_lote
from .utils import open_csv, print_completion, random_date, write_batches

KNOWN_BRANDS = [
    ("Samsung", "Corea del Sur"),
    ("Apple", "Estados Unidos"),
    ("Lenovo", "China"),
    ("HP", "Estados Unidos"),
    ("ASUS", "Taiwan"),
    ("Xiaomi", "China"),
    ("Acer", "Taiwan"),
    ("Dell", "Estados Unidos"),
    ("Motorola", "Estados Unidos"),
    ("LG", "Corea del Sur"),
]
COUNTRIES = ["China", "Estados Unidos", "Japon", "Corea del Sur", "Alemania"]

FIELDS = ["id_marca", "nombre", "pais_origen", "fecha_registro", "estado"]


def generate(lote=LOTE_DEFAULT):
    rng = random.Random(SEED + 1)
    file_obj, writer, path = open_csv("marcas.csv", FIELDS, lote)

    def rows():
        for brand_id in range(1, COUNTS["brands"] + 1):
            if brand_id <= len(KNOWN_BRANDS):
                name, country = KNOWN_BRANDS[brand_id - 1]
            else:
                name = f"MarcaTech{brand_id:03d}"
                country = rng.choice(COUNTRIES)

            yield {
                "id_marca": brand_id,
                "nombre": name,
                "pais_origen": country,
                "fecha_registro": random_date(
                    rng, "2010-01-01", END_DATE
                ).isoformat(),
                "estado": "ACTIVA" if rng.random() < 0.97 else "INACTIVA",
            }

    try:
        write_batches(writer, rows())
    finally:
        file_obj.close()

    print_completion("marcas.csv", COUNTS["brands"], path)


if __name__ == "__main__":
    generate(parse_lote())
