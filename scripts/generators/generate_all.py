"""Ejecuta todos los generadores en el orden correcto."""

from .config import (
    COUNTS,
    DATA_DIR,
    LOTE_DEFAULT,
    parse_lote,
    SALES_DETAIL_DISTRIBUTION,
)

from .generate_customers import generate as generate_customers
from .generate_brands import generate as generate_brands
from .generate_products import generate as generate_products
from .generate_branches import generate as generate_branches
from .generate_payment_methods import generate as generate_payment_methods
from .generate_sales import generate as generate_sales
from .generate_sales_details import generate as generate_sales_details


def validate_configuration():
    """Valida que la configuración de ventas sea consistente."""

    # Validar que la distribución sume el total de ventas.
    total_sales = sum(SALES_DETAIL_DISTRIBUTION.values())

    if total_sales != COUNTS["sales"]:
        raise ValueError(
            f"La distribución contiene {total_sales:,} ventas, "
            f"pero se configuraron {COUNTS['sales']:,}."
        )

    # Calcular el número esperado de detalles.
    expected_details = sum(
        products_per_sale * number_of_sales
        for products_per_sale, number_of_sales
        in SALES_DETAIL_DISTRIBUTION.items()
    )

    if expected_details != COUNTS["sales_details"]:
        raise ValueError(
            f"La distribución genera {expected_details:,} detalles, "
            f"pero se configuraron {COUNTS['sales_details']:,}."
        )

    print("Configuración validada correctamente.")


def main(lote=LOTE_DEFAULT):
    validate_configuration()

    print("=" * 60)
    print("GENERACIÓN COMPLETA DE DATOS CSV - TECHSTORE")
    print("=" * 60)

    generators = [
        ("Clientes", generate_customers),
        ("Marcas", generate_brands),
        ("Productos", generate_products),
        ("Sucursales", generate_branches),
        ("Métodos de pago", generate_payment_methods),
        ("Ventas", generate_sales),
        ("Detalles de ventas", generate_sales_details),
    ]

    for label, generator in generators:
        print(f"\n--- {label} ---")
        generator(lote)

    total = sum(COUNTS.values())
    print("\nGeneración terminada.")
    print(f"Total configurado: {total:,} registros.")
    print(f"Directorio: {DATA_DIR}")


if __name__ == "__main__":
    main(parse_lote())
