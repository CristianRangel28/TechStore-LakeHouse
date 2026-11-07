"""Genera clientes.csv."""

import random

from .config import COUNTS, END_DATE, SEED, START_DATE
from .utils import open_csv, print_completion, random_date, write_batches

FIRST_NAMES = [
    "Sofia", "Valentina", "Isabella", "Camila", "Mariana", "Lucia",
    "Samuel", "Santiago", "Mateo", "Sebastian", "Daniel", "Nicolas",
    "David", "Alejandro", "Andres", "Gabriela", "Maria", "Juan",
]
LAST_NAMES = [
    "Rodriguez", "Gomez", "Martinez", "Lopez", "Garcia", "Hernandez",
    "Perez", "Sanchez", "Ramirez", "Torres", "Diaz", "Vargas",
    "Castro", "Rojas", "Moreno", "Jimenez", "Ruiz", "Mendoza",
]
LOCATIONS = [
    ("Bogota", "Cundinamarca"), ("Medellin", "Antioquia"),
    ("Cali", "Valle del Cauca"), ("Barranquilla", "Atlantico"),
    ("Bucaramanga", "Santander"), ("Cucuta", "Norte de Santander"),
    ("Cartagena", "Bolivar"), ("Pereira", "Risaralda"),
    ("Manizales", "Caldas"), ("Ibague", "Tolima"),
    ("Villavicencio", "Meta"), ("Santa Marta", "Magdalena"),
]

FIELDS = [
    "id_cliente", "nombre", "apellido", "correo", "telefono",
    "ciudad", "departamento", "fecha_registro", "estado",
]


def generate():
    rng = random.Random(SEED)
    file_obj, writer, path = open_csv("clientes.csv", FIELDS)

    def rows():
        for customer_id in range(1, COUNTS["customers"] + 1):
            first = rng.choice(FIRST_NAMES)
            last = rng.choice(LAST_NAMES)
            city, department = rng.choice(LOCATIONS)
            yield {
                "id_cliente": customer_id,
                "nombre": first,
                "apellido": last,
                "correo": f"cliente{customer_id}@techstore-demo.com",
                "telefono": f"3{rng.randrange(10**9):09d}",
                "ciudad": city,
                "departamento": department,
                "fecha_registro": random_date(rng, START_DATE, END_DATE).isoformat(),
                "estado": "ACTIVO" if rng.random() < 0.94 else "INACTIVO",
            }

    try:
        write_batches(writer, rows())
    finally:
        file_obj.close()

    print_completion("clientes.csv", COUNTS["customers"], path)


if __name__ == "__main__":
    generate()
