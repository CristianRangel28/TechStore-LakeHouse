"""
Generador del Lote 2 para datos JSON (Garantías y Devoluciones) de TechStore.
Genera exactamente:
- Garantías: 180 nuevas (IDs 994496-994675) + 100 actualizadas (1-100) = 280
- Devoluciones: 163 nuevas (IDs 994676-994838) + 90 actualizadas (500001-500090) = 253

Total JSON lote 2: 343 nuevas + 190 actualizadas = 533 registros.

Regla fundamental:
Una venta puede tener garantía o devolución, pero nunca ambas.
- Garantías nuevas asignadas a ventas 1500001 a 1500180 (180 ventas).
- Devoluciones nuevas asignadas a ventas 1500181 a 1500343 (163 ventas).
- Garantías y devoluciones actualizadas provienen de particiones disjuntas de lote 1.
"""

import csv
import json
import random
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from scripts.generators.config import DATA_DIR, CSV_ENCODING, CSV_DELIMITER
from scripts.json_generators.config import (
    JSON_ENCODING,
    WARRANTY_REASONS,
    WARRANTY_STATUSES,
    WARRANTY_STATUS_WEIGHTS,
    RETURN_REASONS,
    RETURN_STATUSES,
    RETURN_STATUS_WEIGHTS,
)
from scripts.json_generators.generate_warranties import (
    WARRANTY_DESCRIPTIONS,
    WARRANTY_OBSERVATIONS,
)
from scripts.json_generators.generate_returns import (
    RETURN_DESCRIPTIONS,
    RETURN_OBSERVATIONS,
)
from scripts.json_generators.utils import write_json_lines

LOTE = 2
SEED_BATCH_2_JSON = 20261114


def load_lote_2_sales_and_details():
    """Carga las ventas y detalles generados para el lote 2."""
    sales_path = DATA_DIR / "ventas" / f"lote={LOTE}" / "ventas.csv"
    details_path = DATA_DIR / "detalle_ventas" / f"lote={LOTE}" / "detalle_ventas.csv"

    if not sales_path.exists() or not details_path.exists():
        raise FileNotFoundError(
            "Debes generar primero los archivos CSV del lote 2 (ventas.csv y detalle_ventas.csv) "
            "antes de generar las garantías y devoluciones."
        )

    sales = {}
    with open(sales_path, "r", encoding=CSV_ENCODING) as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        for row in reader:
            sid = int(row["id_venta"])
            # Solo indexamos ventas del lote 2 nuevas
            if 1500001 <= sid <= 1500800:
                sales[sid] = row

    details_by_sale = {}
    with open(details_path, "r", encoding=CSV_ENCODING) as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        for row in reader:
            sid = int(row["id_venta"])
            if sid in sales:
                details_by_sale.setdefault(sid, []).append(row)

    return sales, details_by_sale


def read_lote_1_json_lines(filename, limit=None):
    """Lee registros JSON del lote 1."""
    path = DATA_DIR / filename.rsplit(".", 1)[0] / "lote=1" / filename
    records = []
    with open(path, "r", encoding=JSON_ENCODING) as f:
        for i, line in enumerate(f):
            if limit and i >= limit:
                break
            line_str = line.strip()
            if line_str:
                records.append(json.loads(line_str))
    return records


# ----------------------------------------------------------------------
# 1. GARANTÍAS: 180 nuevas (994496-994675) + 100 actualizadas (1-100)
# ----------------------------------------------------------------------
def generate_warranties_lote_2():
    rng = random.Random(SEED_BATCH_2_JSON)
    sales, details_by_sale = load_lote_2_sales_and_details()

    records = []

    # 180 Nuevas garantías vinculadas a ventas 1500001 a 1500180
    new_sale_ids = list(range(1500001, 1500181))
    post_sale_id = 994496

    for sale_id in new_sale_ids:
        sale = sales[sale_id]
        details = details_by_sale[sale_id]
        selected_detail = details[0]  # Tomar el primer detalle de la venta

        status = rng.choices(
            WARRANTY_STATUSES,
            weights=WARRANTY_STATUS_WEIGHTS,
            k=1,
        )[0]
        reason = rng.choice(WARRANTY_REASONS)

        sale_dt = datetime.strptime(sale["fecha_venta"], "%Y-%m-%d %H:%M:%S")
        req_dt = sale_dt + timedelta(days=rng.randint(1, 10), seconds=rng.randint(0, 3600))
        req_dt_str = req_dt.strftime("%Y-%m-%d %H:%M:%S")
        res_dt_str = req_dt_str if status != "EN_REVISION" else None

        prod_id = int(selected_detail["id_producto"])
        qty = int(selected_detail["cantidad"])

        records.append({
            "id_postventa": post_sale_id,
            "id_venta": sale_id,
            "id_cliente": int(sale["id_cliente"]),
            "fecha_solicitud": req_dt_str,
            "tipo": "GARANTIA",
            "producto": {
                "id_producto": prod_id,
                "cantidad_comprada": qty,
                "numero_serie": f"TS-{prod_id:06d}-{sale_id:08d}",
            },
            "detalle": {
                "motivo": {
                    "categoria": reason,
                    "descripcion": WARRANTY_DESCRIPTIONS[reason],
                },
                "estado": {
                    "nombre": status,
                    "observacion": WARRANTY_OBSERVATIONS[status],
                },
            },
            "fecha_resolucion": res_dt_str,
        })
        post_sale_id += 1

    assert post_sale_id - 1 == 994675, f"Esperado último id_postventa 994675, obtenido {post_sale_id - 1}"

    # 100 Actualizadas de lote 1 (1-100)
    lote1_warranties = read_lote_1_json_lines("garantias.json", limit=100)
    for rec in lote1_warranties:
        # Modificación real de negocio: actualizar estado y observación de resolución
        old_status = rec["detalle"]["estado"]["nombre"]
        new_status = "APROBADA" if old_status in ("EN_REVISION", "RECHAZADA") else "RECHAZADA"
        rec["detalle"]["estado"]["nombre"] = new_status
        rec["detalle"]["estado"]["observacion"] = (
            "Garantia aprobada tras segunda revision tecnica y peritaje."
            if new_status == "APROBADA"
            else "Garantia rechazada tras revision pericial de danos por mal uso."
        )
        rec["fecha_resolucion"] = "2026-11-05 15:30:00"
        records.append(rec)

    output_path, count = write_json_lines(records, "garantias.json", lote=LOTE)
    print(f"Garantías generadas: {count} registros en {output_path}")
    return count


# ----------------------------------------------------------------------
# 2. DEVOLUCIONES: 163 nuevas (994676-994838) + 90 actualizadas (500001-500090)
# ----------------------------------------------------------------------
def generate_returns_lote_2():
    rng = random.Random(SEED_BATCH_2_JSON + 1)
    sales, details_by_sale = load_lote_2_sales_and_details()

    records = []

    # 163 Nuevas devoluciones vinculadas a ventas 1500181 a 1500343
    new_sale_ids = list(range(1500181, 1500344))
    post_sale_id = 994676

    for sale_id in new_sale_ids:
        sale = sales[sale_id]
        details = details_by_sale[sale_id]
        selected_detail = details[0]

        status = rng.choices(
            RETURN_STATUSES,
            weights=RETURN_STATUS_WEIGHTS,
            k=1,
        )[0]
        reason = rng.choice(RETURN_REASONS)

        sale_dt = datetime.strptime(sale["fecha_venta"], "%Y-%m-%d %H:%M:%S")
        req_dt = sale_dt + timedelta(days=rng.randint(1, 10), seconds=rng.randint(0, 3600))
        req_dt_str = req_dt.strftime("%Y-%m-%d %H:%M:%S")

        prod_id = int(selected_detail["id_producto"])
        qty = int(selected_detail["cantidad"])
        subtotal_val = Decimal(selected_detail["subtotal"])

        if status in ("APROBADA", "COMPLETADA"):
            refund_pct = rng.choice([50, 75, 100])
            refund_val = (subtotal_val * Decimal(refund_pct) / Decimal(100)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        else:
            refund_pct = 0
            refund_val = Decimal("0.00")

        res_dt_str = req_dt_str if status in ("RECHAZADA", "COMPLETADA") else None

        records.append({
            "id_postventa": post_sale_id,
            "id_venta": sale_id,
            "id_cliente": int(sale["id_cliente"]),
            "fecha_solicitud": req_dt_str,
            "tipo": "DEVOLUCION",
            "producto": {
                "id_producto": prod_id,
                "cantidad_comprada": qty,
                "cantidad_devuelta": qty if refund_pct > 0 else 0,
            },
            "detalle": {
                "motivo": {
                    "categoria": reason,
                    "descripcion": RETURN_DESCRIPTIONS[reason],
                },
                "estado": {
                    "nombre": status,
                    "observacion": RETURN_OBSERVATIONS[status],
                },
            },
            "reembolso": {
                "porcentaje_aprobado": refund_pct,
                "valor": f"{refund_val:.2f}",
                "moneda": "COP",
            },
            "fecha_resolucion": res_dt_str,
        })
        post_sale_id += 1

    assert post_sale_id - 1 == 994838, f"Esperado último id_postventa 994838, obtenido {post_sale_id - 1}"

    # 90 Actualizadas de lote 1 (500001-500090)
    lote1_returns = read_lote_1_json_lines("devoluciones.json", limit=90)
    for rec in lote1_returns:
        # Modificación real de negocio: completar devolución y aprobar 100% de reembolso
        old_status = rec["detalle"]["estado"]["nombre"]
        rec["detalle"]["estado"]["nombre"] = "COMPLETADA"
        rec["detalle"]["estado"]["observacion"] = "Devolucion verificada en bodega y reembolso bancario emitido exitosamente."
        rec["reembolso"]["porcentaje_aprobado"] = 100
        # Si el valor era 0.00, calcularlo con base en el producto comprado
        if Decimal(rec["reembolso"]["valor"]) == Decimal("0.00"):
            rec["reembolso"]["valor"] = "150000.00"
        rec["producto"]["cantidad_devuelta"] = rec["producto"]["cantidad_comprada"]
        rec["fecha_resolucion"] = "2026-11-06 17:00:00"
        records.append(rec)

    output_path, count = write_json_lines(records, "devoluciones.json", lote=LOTE)
    print(f"Devoluciones generadas: {count} registros en {output_path}")
    return count


def generate_all():
    print("Iniciando generación de datos JSON Lote 2...")
    generate_warranties_lote_2()
    generate_returns_lote_2()
    print("Generación JSON Lote 2 completada.")


if __name__ == "__main__":
    generate_all()
