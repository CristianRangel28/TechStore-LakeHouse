"""
Generador del Lote 2 para datos CSV de TechStore.
Genera exactamente:
- Clientes: 300 nuevos (500001-500300) + 150 actualizados (1-150) = 450
- Marcas: 5 nuevas (101-105) + 5 actualizadas (1-5) = 10
- Productos: 150 nuevos (100001-100150) + 100 actualizados (1-100) = 250
- Sucursales: 2 nuevas (51-52) + 5 actualizadas (1-5) = 7
- Métodos de pago: 0 nuevos + 2 actualizados (1-2) = 2
- Ventas: 800 nuevas (1500001-1500800) + 348 actualizadas (1-348) = 1148
- Detalle ventas: 2400 nuevos (5820001-5822400) + 200 actualizados (1-200) = 2600

Total CSV lote 2: 3657 nuevos + 810 actualizados = 4467 registros.
"""

import csv
import random
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from scripts.generators.config import DATA_DIR, CSV_ENCODING, CSV_DELIMITER
from scripts.generators.utils import open_csv, print_completion
from scripts.generators.generate_customers import FIRST_NAMES, LAST_NAMES, LOCATIONS, FIELDS as CUSTOMER_FIELDS
from scripts.generators.generate_brands import FIELDS as BRAND_FIELDS, COUNTRIES
from scripts.generators.generate_products import FIELDS as PRODUCT_FIELDS, CATEGORIES
from scripts.generators.generate_branches import FIELDS as BRANCH_FIELDS, LOCATIONS as BRANCH_LOCATIONS

LOTE = 2
SEED_BATCH_2 = 20261113


def format_money(value_cents):
    """Convierte centavos a formato monetario con dos decimales."""
    return str(
        (Decimal(value_cents) / Decimal(100)).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP
        )
    )


def read_lote_1_csv(filename, limit=None):
    """Lee registros del lote 1 de forma segura."""
    path = DATA_DIR / filename.rsplit(".", 1)[0] / "lote=1" / filename
    records = []
    with open(path, "r", encoding=CSV_ENCODING) as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        for i, row in enumerate(reader):
            if limit and i >= limit:
                break
            records.append(dict(row))
    return records


# ----------------------------------------------------------------------
# 1. CLIENTES: 300 nuevos (500001-500300) + 150 actualizados
# ----------------------------------------------------------------------
def generate_customers():
    rng = random.Random(SEED_BATCH_2)
    file_obj, writer, path = open_csv("clientes.csv", CUSTOMER_FIELDS, lote=LOTE)
    count = 0

    try:
        # 300 Nuevos
        for customer_id in range(500001, 500301):
            first = rng.choice(FIRST_NAMES)
            last = rng.choice(LAST_NAMES)
            city, department = rng.choice(LOCATIONS)
            reg_date = f"2026-11-{rng.randint(1, 10):02d}"
            writer.writerow({
                "id_cliente": customer_id,
                "nombre": first,
                "apellido": last,
                "correo": f"cliente{customer_id}@techstore-demo.com",
                "telefono": f"3{rng.randrange(10**9):09d}",
                "ciudad": city,
                "departamento": department,
                "fecha_registro": reg_date,
                "estado": "ACTIVO",
            })
            count += 1

        # 150 Actualizados (leer 1-150 de lote 1)
        lote1_customers = read_lote_1_csv("clientes.csv", limit=150)
        for row in lote1_customers:
            # Modificar atributos reales de negocio: teléfono y ubicación o estado
            new_city, new_dept = rng.choice(LOCATIONS)
            row["telefono"] = f"3{rng.randrange(10**9):09d}"
            row["ciudad"] = new_city
            row["departamento"] = new_dept
            row["estado"] = "ACTIVO" if rng.random() < 0.90 else "INACTIVO"
            writer.writerow(row)
            count += 1

    finally:
        file_obj.close()

    print_completion("clientes.csv", count, path)
    return count


# ----------------------------------------------------------------------
# 2. MARCAS: 5 nuevas (101-105) + 5 actualizadas (1-5)
# ----------------------------------------------------------------------
def generate_brands():
    rng = random.Random(SEED_BATCH_2 + 1)
    file_obj, writer, path = open_csv("marcas.csv", BRAND_FIELDS, lote=LOTE)
    count = 0

    NEW_BRANDS = [
        (101, "Razer", "Estados Unidos"),
        (102, "Corsair", "Estados Unidos"),
        (103, "Logitech", "Suiza"),
        (104, "Kingston", "Estados Unidos"),
        (105, "MSI", "Taiwan"),
    ]

    try:
        for brand_id, name, country in NEW_BRANDS:
            writer.writerow({
                "id_marca": brand_id,
                "nombre": name,
                "pais_origen": country,
                "fecha_registro": "2026-11-01",
                "estado": "ACTIVA",
            })
            count += 1

        lote1_brands = read_lote_1_csv("marcas.csv", limit=5)
        for row in lote1_brands:
            # Modificación real de negocio: cambio de estado o país
            row["estado"] = "INACTIVA" if row["estado"] == "ACTIVA" else "ACTIVA"
            writer.writerow(row)
            count += 1

    finally:
        file_obj.close()

    print_completion("marcas.csv", count, path)
    return count


# ----------------------------------------------------------------------
# 3. PRODUCTOS: 150 nuevos (100001-100150) + 100 actualizados (1-100)
# ----------------------------------------------------------------------
def generate_products():
    rng = random.Random(SEED_BATCH_2 + 2)
    file_obj, writer, path = open_csv("productos.csv", PRODUCT_FIELDS, lote=LOTE)
    count = 0

    try:
        for product_id in range(100001, 100151):
            category = rng.choice(CATEGORIES)
            price = rng.randrange(50_000, 8_000_001, 1_000)
            cost = round(price * rng.uniform(0.55, 0.82), 2)
            brand_id = rng.randint(1, 105)
            writer.writerow({
                "id_producto": product_id,
                "id_marca": brand_id,
                "nombre": f"{category} Pro modelo {product_id:06d}",
                "categoria": category,
                "modelo": f"TS-B2-{rng.randrange(1000, 10000)}-{product_id:06d}",
                "precio_actual": f"{price:.2f}",
                "costo_adquisicion": f"{cost:.2f}",
                "fecha_lanzamiento": f"2026-11-{rng.randint(1, 10):02d}",
                "estado": "ACTIVO",
            })
            count += 1

        lote1_products = read_lote_1_csv("productos.csv", limit=100)
        for row in lote1_products:
            # Modificación real de negocio: ajuste de precio_actual (+8%) y costo
            old_price = Decimal(row["precio_actual"])
            new_price = (old_price * Decimal("1.08")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            old_cost = Decimal(row["costo_adquisicion"])
            new_cost = (old_cost * Decimal("1.05")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            row["precio_actual"] = f"{new_price:.2f}"
            row["costo_adquisicion"] = f"{new_cost:.2f}"
            writer.writerow(row)
            count += 1

    finally:
        file_obj.close()

    print_completion("productos.csv", count, path)
    return count


# ----------------------------------------------------------------------
# 4. SUCURSALES: 2 nuevas (51-52) + 5 actualizadas (1-5)
# ----------------------------------------------------------------------
def generate_branches():
    rng = random.Random(SEED_BATCH_2 + 3)
    file_obj, writer, path = open_csv("sucursales.csv", BRANCH_FIELDS, lote=LOTE)
    count = 0

    NEW_BRANCHES = [
        (51, "TechStore Ibague 51", "Ibague", "Tolima", "Carrera 5 # 38-12", "2026-11-01"),
        (52, "TechStore Santa Marta 52", "Santa Marta", "Magdalena", "Calle 22 # 3-45", "2026-11-01"),
    ]

    try:
        for branch_id, name, city, dept, address, opening in NEW_BRANCHES:
            writer.writerow({
                "id_sucursal": branch_id,
                "nombre": name,
                "ciudad": city,
                "departamento": dept,
                "direccion": address,
                "fecha_apertura": opening,
                "estado": "ACTIVA",
            })
            count += 1

        lote1_branches = read_lote_1_csv("sucursales.csv", limit=5)
        for row in lote1_branches:
            # Modificación real de negocio: cambio de dirección
            row["direccion"] = f"Avenida Principal # {rng.randint(10, 99)}-{rng.randint(10, 99)}"
            writer.writerow(row)
            count += 1

    finally:
        file_obj.close()

    print_completion("sucursales.csv", count, path)
    return count


# ----------------------------------------------------------------------
# 5. MÉTODOS DE PAGO: 0 nuevos + 2 actualizados (1-2)
# ----------------------------------------------------------------------
def generate_payment_methods():
    fields = ["id_metodo_pago", "nombre", "descripcion", "estado"]
    file_obj, writer, path = open_csv("metodos_pago.csv", fields, lote=LOTE)
    count = 0

    try:
        lote1_pm = read_lote_1_csv("metodos_pago.csv", limit=2)
        for row in lote1_pm:
            # Modificación real de negocio: actualización de descripción
            row["descripcion"] = f"{row['descripcion']} Incluye comprobante electronico y validacion biometrica."
            writer.writerow(row)
            count += 1
    finally:
        file_obj.close()

    print_completion("metodos_pago.csv", count, path)
    return count


# ----------------------------------------------------------------------
# 6. VENTAS Y DETALLES:
#    Ventas: 800 nuevas (1500001-1500800) + 348 actualizadas (1-348)
#    Detalles: 2400 nuevos (5820001-5822400) + 200 actualizados (1-200)
# ----------------------------------------------------------------------
def generate_sales_and_details():
    rng = random.Random(SEED_BATCH_2 + 4)
    sales_fields = [
        "id_venta", "id_cliente", "id_sucursal", "id_metodo_pago",
        "fecha_venta", "subtotal", "descuento", "impuesto", "total", "estado",
    ]
    detail_fields = [
        "id_detalle", "id_venta", "id_producto", "cantidad",
        "precio_unitario", "descuento", "subtotal",
    ]

    sales_file, sales_writer, sales_path = open_csv("ventas.csv", sales_fields, lote=LOTE)
    details_file, details_writer, details_path = open_csv("detalle_ventas.csv", detail_fields, lote=LOTE)

    new_sales_count = 0
    new_details_count = 0
    updated_sales_count = 0
    updated_details_count = 0

    try:
        current_detail_id = 5820001

        # 800 Nuevas Ventas con exactamente 3 detalles cada una (800 * 3 = 2400 detalles)
        for sale_id in range(1500001, 1500801):
            customer_id = rng.randint(1, 500300)
            branch_id = rng.randint(1, 52)
            payment_method_id = rng.randint(1, 5)

            # Fechas en noviembre 2026
            sale_dt = datetime(2026, 11, rng.randint(1, 10), rng.randint(8, 20), rng.randint(0, 59), rng.randint(0, 59))
            sale_dt_str = sale_dt.strftime("%Y-%m-%d %H:%M:%S")

            gross_subtotal = 0
            total_discount = 0

            # 3 detalles por venta
            for _ in range(3):
                product_id = rng.randint(1, 100150)
                qty = rng.randint(1, 3)
                unit_price = rng.randrange(5_000_000, 800_000_001, 100_000)
                line_gross = unit_price * qty
                disc_pct = rng.choice([0, 5, 10, 15])
                line_discount = (line_gross * disc_pct) // 100
                line_subtotal = line_gross - line_discount

                details_writer.writerow({
                    "id_detalle": current_detail_id,
                    "id_venta": sale_id,
                    "id_producto": product_id,
                    "cantidad": qty,
                    "precio_unitario": format_money(unit_price),
                    "descuento": format_money(line_discount),
                    "subtotal": format_money(line_subtotal),
                })
                current_detail_id += 1
                new_details_count += 1

                gross_subtotal += line_gross
                total_discount += line_discount

            net_subtotal = gross_subtotal - total_discount
            tax = (net_subtotal * 19) // 100
            total = net_subtotal + tax

            sales_writer.writerow({
                "id_venta": sale_id,
                "id_cliente": customer_id,
                "id_sucursal": branch_id,
                "id_metodo_pago": payment_method_id,
                "fecha_venta": sale_dt_str,
                "subtotal": format_money(gross_subtotal),
                "descuento": format_money(total_discount),
                "impuesto": format_money(tax),
                "total": format_money(total),
                "estado": rng.choice(["COMPLETADA", "COMPLETADA", "COMPLETADA", "PENDIENTE"]),
            })
            new_sales_count += 1

        # 348 Ventas Actualizadas (leer 1-348 de lote 1)
        lote1_sales = read_lote_1_csv("ventas.csv", limit=348)
        for row in lote1_sales:
            # Modificación real de negocio: actualizar estado
            row["estado"] = "COMPLETADA" if row["estado"] in ("PENDIENTE", "CANCELADA") else "CANCELADA"
            sales_writer.writerow(row)
            updated_sales_count += 1

        # 200 Detalles Actualizados (leer 1-200 de lote 1)
        lote1_details = read_lote_1_csv("detalle_ventas.csv", limit=200)
        for row in lote1_details:
            # Modificación real de negocio: ajustar cantidad y recalcular subtotal
            old_qty = int(row["cantidad"])
            new_qty = old_qty + 1 if old_qty < 5 else 1
            unit_p = Decimal(row["precio_unitario"])
            disc = Decimal(row["descuento"])
            new_subtotal = (Decimal(new_qty) * unit_p) - disc
            row["cantidad"] = new_qty
            row["subtotal"] = f"{new_subtotal:.2f}"
            details_writer.writerow(row)
            updated_details_count += 1

    finally:
        sales_file.close()
        details_file.close()

    print_completion("ventas.csv", new_sales_count + updated_sales_count, sales_path)
    print_completion("detalle_ventas.csv", new_details_count + updated_details_count, details_path)
    return (new_sales_count, updated_sales_count, new_details_count, updated_details_count)


def generate_all():
    print("Iniciando generación de datos CSV Lote 2...")
    generate_customers()
    generate_brands()
    generate_products()
    generate_branches()
    generate_payment_methods()
    generate_sales_and_details()
    print("Generación CSV Lote 2 completada.")


if __name__ == "__main__":
    generate_all()
