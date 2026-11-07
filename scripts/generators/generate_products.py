"""Genera productos.csv. Requiere que marcas.csv ya esté generado."""

import random

from .config import COUNTS, END_DATE, SEED
from .utils import open_csv, print_completion, random_date, write_batches

CATEGORIES = [
    "Computadores", "Celulares", "Perifericos", "Accesorios",
    "Componentes", "Almacenamiento", "Monitores", "Audio",
]
FIELDS = [
    "id_producto", "id_marca", "nombre", "categoria", "modelo",
    "precio_actual", "costo_adquisicion", "fecha_lanzamiento", "estado",
]


def generate():
    rng = random.Random(SEED + 2)
    file_obj, writer, path = open_csv("productos.csv", FIELDS)

    def rows():
        for product_id in range(1, COUNTS["products"] + 1):
            category = rng.choice(CATEGORIES)
            price = rng.randrange(50_000, 8_000_001, 1_000)
            cost = round(price * rng.uniform(0.55, 0.82), 2)

            yield {
                "id_producto": product_id,
                "id_marca": rng.randint(1, COUNTS["brands"]),
                "nombre": f"{category} modelo {product_id:06d}",
                "categoria": category,
                "modelo": f"TS-{rng.randrange(1000, 10000)}-{product_id:06d}",
                "precio_actual": f"{price:.2f}",
                "costo_adquisicion": f"{cost:.2f}",
                "fecha_lanzamiento": random_date(
                    rng, "2018-01-01", END_DATE
                ).isoformat(),
                "estado": "ACTIVO" if rng.random() < 0.92 else "INACTIVO",
            }

    try:
        write_batches(writer, rows())
    finally:
        file_obj.close()

    print_completion("productos.csv", COUNTS["products"], path)


if __name__ == "__main__":
    generate()
