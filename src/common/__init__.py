"""
Capa común y utilitarios compartidos para TechStore LakeHouse.
"""

from src.common.spark_session import initialize_job
from src.common.s3_readers import (
    read_catalog_table,
    read_iceberg_table,
    read_s3_csv,
    read_s3_json
)
from src.common.s3_writers import (
    create_iceberg_table_if_not_exists,
    upsert_iceberg_table
)
from src.common.surrogate_keys import (
    add_surrogate_key,
    generate_surrogate_key_expr
)

__all__ = [
    "initialize_job",
    "read_catalog_table",
    "read_iceberg_table",
    "read_s3_csv",
    "read_s3_json",
    "create_iceberg_table_if_not_exists",
    "upsert_iceberg_table",
    "add_surrogate_key",
    "generate_surrogate_key_expr"
]
