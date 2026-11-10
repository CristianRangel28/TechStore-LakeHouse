import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["catalog", "silver_db", "gold_db"])

spark = GlueContext(SparkContext()).spark_session

# Las tablas Iceberg de Silver se leen con el prefijo del catalogo
df = spark.table(f"{args['catalog']}.{args['silver_db']}.test_metodos_pago")

# Surrogate key: un numero propio de la dimension, distinto del id original
w = Window.orderBy("id_metodo_pago")
dim = df.select(
    row_number().over(w).alias("payment_key"),
    col("id_metodo_pago").alias("payment_method_id"),
    col("nombre").alias("payment_method_name"),
    col("estado").alias("status"),
)
print("Filas dimension:", dim.count())

(dim.writeTo(f"{args['catalog']}.{args['gold_db']}.test_dim_payment_method")
    .using("iceberg")
    .createOrReplace())

print("Tabla Iceberg escrita en Gold")