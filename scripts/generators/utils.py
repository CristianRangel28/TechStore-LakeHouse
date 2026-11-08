"""Funciones auxiliares compartidas por los generadores de TechStore."""

import csv
import random

from datetime import datetime, timedelta

from .config import (
    DATA_DIR,
    LOTE_DEFAULT,
    CSV_ENCODING,
    CSV_DELIMITER,
    START_DATE,
    END_DATE,
    BATCH_SIZE,
)


def open_csv(filename, fields, lote=LOTE_DEFAULT):
    """
    Crea un archivo CSV, escribe sus encabezados
    y devuelve el archivo, el writer y su ruta.
    """

    table_dir = DATA_DIR / filename.rsplit(".", 1)[0] / f"lote={lote}"
    table_dir.mkdir(parents=True, exist_ok=True)
    path = table_dir / filename

    file_obj = open(
        path,
        mode="w",
        newline="",
        encoding=CSV_ENCODING
    )

    writer = csv.DictWriter(
        file_obj,
        fieldnames=fields,
        delimiter=CSV_DELIMITER
    )

    writer.writeheader()

    return file_obj, writer, path


def random_date(rng=None, start_date=START_DATE, end_date=END_DATE):
    """Genera una fecha aleatoria entre dos fechas."""

    if rng is None:
        rng = random

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    days = (end - start).days

    generated_date = start + timedelta(
        days=rng.randint(0, days)
    )

    return generated_date


def write_batches(writer, rows, batch_size=BATCH_SIZE):
    """Escribe registros en lotes para controlar el uso de memoria."""

    batch = []

    for row in rows:
        batch.append(row)

        if len(batch) >= batch_size:
            writer.writerows(batch)
            batch.clear()

    if batch:
        writer.writerows(batch)


def print_completion(filename, count, path):
    """Muestra un resumen al finalizar la generación."""

    print("\n" + "=" * 55)
    print(f"Archivo generado: {filename}")
    print(f"Registros: {count:,}")
    print(f"Ubicación: {path}")
    print("=" * 55)
