"""
Transformaciones iniciales de Bronze a Silver.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


COLUMN_RENAMING = {
    "id_cliente": "customer_id",
    "id_marca": "brand_id",
    "id_producto": "product_id",
    "id_sucursal": "branch_id",
    "id_metodo_pago": "payment_method_id",
    "id_venta": "sale_id",
    "id_detalle": "sale_detail_id",
    "id_postventa": "after_sales_id",
    "nombre": "name",
    "apellido": "last_name",
    "correo": "email",
    "telefono": "phone",
    "ciudad": "city",
    "departamento": "department",
    "fecha_registro": "registration_date",
    "fecha_venta": "sale_timestamp",
    "fecha_solicitud": "request_timestamp",
    "fecha_resolucion": "resolution_timestamp",
    "fecha_lanzamiento": "launch_date",
    "fecha_apertura": "opening_date",
    "fecha": "date",
    "subtotal": "subtotal_amount",
    "descuento": "discount_amount",
    "impuesto": "tax_amount",
    "total": "total_amount",
    "precio_actual": "current_price",
    "costo_adquisicion": "acquisition_cost",
    "precio_unitario": "unit_price",
    "cantidad": "quantity",
    "cantidad_comprada": "purchased_quantity",
    "cantidad_devuelta": "returned_quantity",
    "numero_serie": "serial_number",
    "pais_origen": "country_of_origin",
    "categoria": "category",
    "modelo": "model",
    "direccion": "address",
    "estado": "status",
    "descripcion": "description",
    "tipo": "type",
    "producto": "product",
    "detalle": "details",
    "motivo": "reason",
    "observacion": "observation",
    "reembolso": "refund",
    "porcentaje_aprobado": "approved_percentage",
    "valor": "value",
    "moneda": "currency"
}


def rename_columns(dataframe):
    """
    Renombra las columnas utilizando el diccionario de equivalencias.
    """

    for old_name, new_name in COLUMN_RENAMING.items():

        if old_name in dataframe.columns:

            dataframe = dataframe.withColumnRenamed(
                old_name,
                new_name
            )

    return dataframe


def add_audit_columns(dataframe, batch_id):
    """
    Añade información de auditoría.
    """

    return (
        dataframe
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_batch", F.lit(batch_id))
    )


def transform_dataframe(dataframe, batch_id):
    """
    Aplica transformaciones comunes.
    """

    dataframe = rename_columns(dataframe)

    dataframe = add_audit_columns(
        dataframe,
        batch_id
    )

    return dataframe


def transform_financial_columns(dataframe):
    """
    Convierte columnas monetarias a Decimal(18,2).
    """

    monetary_columns = [
        "subtotal_amount",
        "discount_amount",
        "tax_amount",
        "total_amount",
        "current_price",
        "acquisition_cost",
        "unit_price"
    ]

    for column in monetary_columns:

        if column in dataframe.columns:

            dataframe = dataframe.withColumn(
                column,
                F.col(column).cast(DecimalType(18, 2))
            )

    return dataframe