"""Lógica compartida para generar ventas y detalles de ventas."""

import random

from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

from .config import (
    COUNTS,
    SEED,
    SALES_DETAIL_DISTRIBUTION,
    START_DATE,
    END_DATE,
)


def format_money(value):
    """Convierte centavos a un valor monetario con dos decimales."""

    return str(
        (Decimal(value) / Decimal(100)).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP
        )
    )


def build_sale_and_details(sale_id):
    """
    Genera una venta y sus detalles.

    La semilla depende del ID de venta para garantizar
    que ambos generadores produzcan exactamente los
    mismos datos cuando se ejecutan por separado.
    """

    rng = random.Random(SEED + sale_id)

    # -------------------------------------------------
    # 1. Determinar cantidad de detalles
    # -------------------------------------------------

    cumulative_sales = 0
    number_of_details = None

    for quantity, sales_count in SALES_DETAIL_DISTRIBUTION.items():
        cumulative_sales += sales_count

        if sale_id <= cumulative_sales:
            number_of_details = quantity
            break

    if number_of_details is None:
        raise ValueError(
            f"No se pudo determinar la cantidad de productos "
            f"para la venta {sale_id}."
        )

    # -------------------------------------------------
    # 2. Generar fecha de venta
    # -------------------------------------------------

    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    days = (end - start).days

    sale_datetime = start + timedelta(
        days=rng.randint(0, days),
        seconds=rng.randint(0, 86399)
    )

    # -------------------------------------------------
    # 3. Generar detalles
    # -------------------------------------------------

    detail_rows = []

    gross_subtotal = 0
    total_discount = 0

    product_ids = rng.sample(
        range(1, COUNTS["products"] + 1),
        number_of_details
    )

    for product_id in product_ids:

        quantity = rng.randint(1, 3)

        # Precio unitario en centavos:
        # Entre $50.000 y $8.000.000 COP.

        unit_price = rng.randrange(
            5_000_000,
            800_000_001,
            100_000
        )

        line_gross = unit_price * quantity

        discount_percentage = rng.choice(
            [0, 5, 10, 15]
        )

        line_discount = (
            line_gross * discount_percentage
        ) // 100

        line_subtotal = line_gross - line_discount

        detail_rows.append({
            "id_venta": sale_id,
            "id_producto": product_id,
            "cantidad": quantity,
            "precio_unitario": format_money(unit_price),
            "descuento": format_money(line_discount),
            "subtotal": format_money(line_subtotal),
        })

        gross_subtotal += line_gross
        total_discount += line_discount

    # -------------------------------------------------
    # 4. Calcular valores de la venta
    # -------------------------------------------------

    net_subtotal = gross_subtotal - total_discount

    tax = (
        net_subtotal * 19
    ) // 100

    total = net_subtotal + tax

    # -------------------------------------------------
    # 5. Construir registro de venta
    # -------------------------------------------------

    sale_row = {
        "id_venta": sale_id,
        "id_cliente": rng.randint(1, COUNTS["customers"]),
        "id_sucursal": rng.randint(1, COUNTS["branches"]),
        "id_metodo_pago": rng.randint(1, COUNTS["payment_methods"]),
        "fecha_venta": sale_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        "subtotal": format_money(gross_subtotal),
        "descuento": format_money(total_discount),
        "impuesto": format_money(tax),
        "total": format_money(total),
        "estado": rng.choice([
            "COMPLETADA",
            "COMPLETADA",
            "COMPLETADA",
            "CANCELADA",
            "PENDIENTE",
        ]),
    }

    return sale_row, detail_rows