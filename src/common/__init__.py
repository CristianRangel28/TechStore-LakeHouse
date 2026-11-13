"""
Capa común y utilitarios compartidos para TechStore LakeHouse.

Utiliza imports diferidos para evitar cargar dependencias de AWS Glue
cuando solamente se necesitan utilidades independientes.
"""

from importlib import import_module


_MODULE_EXPORTS = {
    "initialize_job": "spark_session",
    "read_catalog_table": "s3_readers",
    "read_iceberg_table": "s3_readers",
    "read_s3_csv": "s3_readers",
    "read_s3_json": "s3_readers",
    "create_iceberg_table_if_not_exists": "s3_writers",
    "upsert_iceberg_table": "s3_writers",
    "add_surrogate_key": "surrogate_keys",
    "generate_surrogate_key_expr": "surrogate_keys",
}

__all__ = list(_MODULE_EXPORTS)


def __getattr__(name):
    """
    Importa el módulo correspondiente únicamente cuando se solicita
    uno de sus atributos públicos.
    """
    if name not in _MODULE_EXPORTS:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        )

    module_name = _MODULE_EXPORTS[name]
    module = import_module(f".{module_name}", package=__name__)
    attribute = getattr(module, name)

    globals()[name] = attribute

    return attribute


def __dir__():
    """Incluye los exports públicos en las herramientas de introspección."""
    return sorted(set(globals()) | set(__all__))
