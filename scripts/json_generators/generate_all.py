"""Ejecuta todos los generadores de datos JSON de TechStore."""

from scripts.json_generators.config import (
    POST_SALES_COUNTS,
    TOTAL_POST_SALES,
)
from scripts.json_generators.utils import build_post_sales_assignment

from scripts.json_generators.generate_warranties import generate_warranties
from scripts.json_generators.generate_returns import generate_returns


def main():
    print("=" * 65)
    print("GENERACIÓN DE DATOS JSON - TECHSTORE")
    print("=" * 65)

    # Obtener la asignación de ventas una sola vez.
    warranty_sales, return_sales = build_post_sales_assignment()

    # Comprobar cantidades antes de generar.
    assert len(warranty_sales) == POST_SALES_COUNTS["warranties"]
    assert len(return_sales) == POST_SALES_COUNTS["returns"]
    assert len(warranty_sales) + len(return_sales) == TOTAL_POST_SALES

    print("\nAsignación de ventas validada.")
    print(f"Ventas asignadas a garantías: {len(warranty_sales):,}")
    print(f"Ventas asignadas a devoluciones: {len(return_sales):,}")


    print("\nGenerando garantías...")
    generate_warranties()

    print("\nGenerando devoluciones...")
    generate_returns()

    print("\n" + "=" * 65)
    print("GENERACIÓN JSON COMPLETADA")
    print("=" * 65)


if __name__ == "__main__":
    main()