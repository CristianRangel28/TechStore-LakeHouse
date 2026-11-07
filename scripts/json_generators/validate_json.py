"""Valida los archivos JSON de garantías y devoluciones."""
from datetime import datetime

import json
from decimal import Decimal, InvalidOperation
from pathlib import Path

from scripts.generators.config import COUNTS
from scripts.generators.sales_data import build_sale_and_details

from scripts.json_generators.config import (
    JSON_OUTPUT_DIR,
    POST_SALES_COUNTS,
    WARRANTY_STATUSES,
    RETURN_STATUSES,
)


EXPECTED_FIELDS = {
    "warranty": {
        "id_postventa",
        "id_venta",
        "id_cliente",
        "fecha_solicitud",
        "tipo",
        "producto",
        "detalle",
        "fecha_resolucion",
    },
    "return": {
        "id_postventa",
        "id_venta",
        "id_cliente",
        "fecha_solicitud",
        "tipo",
        "producto",
        "detalle",
        "fecha_resolucion",
        "reembolso",
    },
}


def iter_json_lines(path):
    """Lee un archivo NDJSON progresivamente."""

    with open(path, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                yield line_number, json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"JSON inválido en {path.name}, línea {line_number}: {error}"
                ) from error


def validate_structure(record, record_type):
    """Comprueba la estructura básica de un registro."""

    if not isinstance(record, dict):
        return False

    expected = EXPECTED_FIELDS[record_type]

    if not expected.issubset(record.keys()):
        return False

    # Validar producto
    if not isinstance(record.get("producto"), dict):
        return False

    # Validar detalle
    if not isinstance(record.get("detalle"), dict):
        return False

    detalle = record["detalle"]

    # Validar estado anidado
    if not isinstance(detalle.get("estado"), dict):
        return False

    # Validar motivo anidado
    if not isinstance(detalle.get("motivo"), dict):
        return False

    # Validar campos básicos del estado
    if not isinstance(detalle["estado"].get("nombre"), str):
        return False

    # Validar campos básicos del motivo
    if not isinstance(detalle["motivo"].get("categoria"), str):
        return False

    if not isinstance(detalle["motivo"].get("descripcion"), str):
        return False

    # Validar datos específicos de garantías
    if record_type == "warranty":

        if record.get("tipo") != "GARANTIA":
            return False

        if not isinstance(record["producto"].get("cantidad_comprada"), int):
            return False

        if not isinstance(record["producto"].get("numero_serie"), str):
            return False

    # Validar datos específicos de devoluciones
    elif record_type == "return":

        if record.get("tipo") != "DEVOLUCION":
            return False

        if not isinstance(record["producto"].get("cantidad_comprada"), int):
            return False

        if not isinstance(record["producto"].get("cantidad_devuelta"), int):
            return False

        if not isinstance(record.get("reembolso"), dict):
            return False

        refund = record["reembolso"]

        if not {"porcentaje_aprobado", "valor", "moneda"}.issubset(refund):
            return False

    return True


def validate_record_relationships(record):
    """Valida la relación entre la posventa y su venta original."""

    sale_id = record["id_venta"]

    if (
        not isinstance(sale_id, int)
        or not 1 <= sale_id <= COUNTS["sales"]
    ):
        return False, None, None

    sale, details = build_sale_and_details(sale_id)

    if record["id_cliente"] != sale["id_cliente"]:
        return False, None, None

    product_id = record["producto"].get("id_producto")

    matching_detail = next(
        (
            detail
            for detail in details
            if detail["id_producto"] == product_id
        ),
        None,
    )

    if matching_detail is None:
        return False, None, None

    return True, sale, matching_detail


def validate_dates(record, sale):
    """Comprueba que la solicitud sea posterior a la venta."""

    sale_date = datetime.strptime(
        sale["fecha_venta"],
        "%Y-%m-%d %H:%M:%S",
    )

    request_date = datetime.strptime(
        record["fecha_solicitud"],
        "%Y-%m-%d %H:%M:%S",
    )

    resolution_value = record.get("fecha_resolucion")

    if resolution_value is not None:
        resolution_date = datetime.strptime(
            resolution_value,
            "%Y-%m-%d %H:%M:%S",
        )

        if resolution_date < request_date:
            return False

    return request_date > sale_date


def validate_warranty(record):
    """Valida estados y fechas de resolución de garantías."""

    status = record["detalle"]["estado"].get("nombre")
    resolution_date = record.get("fecha_resolucion")

    if status not in WARRANTY_STATUSES:
        return False

    if status == "EN_REVISION":
        return resolution_date is None

    return resolution_date is not None


def validate_return(record, matching_detail):
    """Valida estados, cantidades, fechas y reembolsos."""

    status = record["detalle"]["estado"].get("nombre")
    quantity_returned = record["producto"].get("cantidad_devuelta")

    refund = record["reembolso"]

    refund_amount = refund.get("valor")
    refund_percentage = refund.get("porcentaje_aprobado")
    resolution_date = record.get("fecha_resolucion")

    if status not in RETURN_STATUSES:
        return False

    if not isinstance(quantity_returned, int):
        return False

    if quantity_returned < 0:
        return False

    if quantity_returned > matching_detail["cantidad"]:
        return False

    try:
        amount = Decimal(str(refund_amount))
        percentage = Decimal(str(refund_percentage))
        original_subtotal = Decimal(
            str(matching_detail["subtotal"])
        )
    except (InvalidOperation, TypeError, ValueError):
        return False

    if status == "PENDIENTE":
        return (
            resolution_date is None
            and quantity_returned == 0
            and amount == Decimal("0")
            and percentage == Decimal("0")
        )

    if status == "RECHAZADA":
        return (
            resolution_date is not None
            and quantity_returned == 0
            and amount == Decimal("0")
            and percentage == Decimal("0")
        )

    if status in ("APROBADA", "COMPLETADA"):

        if status == "APROBADA" and resolution_date is not None:
            return False

        if status == "COMPLETADA" and resolution_date is None:
            return False

        if quantity_returned <= 0:
            return False

        if percentage not in (
            Decimal("50"),
            Decimal("75"),
            Decimal("100"),
        ):
            return False

        expected_amount = (
            original_subtotal * percentage / Decimal("100")
        ).quantize(Decimal("0.01"))

        return amount == expected_amount

    return False


def validate_file(path, record_type, used_ids, used_sales):
    """Valida progresivamente un archivo NDJSON."""

    count = 0
    errors = []

    for line_number, record in iter_json_lines(path):
        count += 1

        try:
            if not validate_structure(record, record_type):
                errors.append(
                    f"Línea {line_number}: estructura incorrecta."
                )
                continue

            post_sale_id = record["id_postventa"]
            sale_id = record["id_venta"]

            if not isinstance(post_sale_id, int):
                errors.append(
                    f"Línea {line_number}: identificador inválido."
                )
                continue

            if post_sale_id in used_ids:
                errors.append(
                    f"Línea {line_number}: ID de posventa duplicado."
                )
                continue

            used_ids.add(post_sale_id)

            if sale_id in used_sales:
                errors.append(
                    f"Línea {line_number}: venta duplicada."
                )
                continue

            used_sales.add(sale_id)

            valid_relationship, sale, matching_detail = (
                validate_record_relationships(record)
            )

            if not valid_relationship:
                errors.append(
                    f"Línea {line_number}: relación con venta incorrecta."
                )
                continue

            if not validate_dates(record, sale):
                errors.append(
                    f"Línea {line_number}: fecha de solicitud inválida."
                )
                continue

            if record_type == "warranty":
                valid = validate_warranty(record)
            else:
                valid = validate_return(record, matching_detail)

            if not valid:
                errors.append(
                    f"Línea {line_number}: datos de posventa inválidos."
                )

        except (KeyError, TypeError, ValueError) as error:
            errors.append(
                f"Línea {line_number}: {error}"
            )

    return count, errors, used_sales


def main():
    print("=" * 65)
    print("VALIDACIÓN DE DATOS JSON - TECHSTORE")
    print("=" * 65)

    warranties_path = JSON_OUTPUT_DIR / "garantias.json"
    returns_path = JSON_OUTPUT_DIR / "devoluciones.json"

    if not warranties_path.exists() or not returns_path.exists():
        print("\n[ERROR] No se encontraron los archivos JSON.")
        return

    used_ids = set()

    print("\nValidando garantías...")

    warranty_count, warranty_errors, warranty_sales = validate_file(
        warranties_path,
        "warranty",
        used_ids,
        set(),
    )

    print(f"Registros procesados: {warranty_count:,}")

    print("\nValidando devoluciones...")

    return_count, return_errors, return_sales = validate_file(
        returns_path,
        "return",
        used_ids,
        set(),
    )

    print(f"Registros procesados: {return_count:,}")

    exclusivity_valid = warranty_sales.isdisjoint(return_sales)

    expected_warranties = POST_SALES_COUNTS["warranties"]
    expected_returns = POST_SALES_COUNTS["returns"]

    count_valid = (
        warranty_count == expected_warranties
        and return_count == expected_returns
    )

    identifiers_valid = not (warranty_errors or return_errors)

    checks = {
        "Cantidad de registros": count_valid,
        "Identificadores y relaciones": identifiers_valid,
        "Exclusividad de procesos": exclusivity_valid,
    }

    print("\n" + "=" * 65)
    print("RESULTADOS")
    print("=" * 65)

    for name, result in checks.items():
        print(f"[{'OK' if result else 'ERROR'}] {name}")

    all_valid = all(checks.values())

    if not all_valid:
        print("\nErrores encontrados:")

        for error in (warranty_errors + return_errors)[:20]:
            print(f"- {error}")

        print("\nSe muestran máximo 20 errores.")

    print("\n" + "=" * 65)
    print(
        "RESULTADO: "
        + (
            "TODAS LAS VALIDACIONES APROBADAS"
            if all_valid
            else "SE ENCONTRARON ERRORES"
        )
    )
    print("=" * 65)


if __name__ == "__main__":
    main()