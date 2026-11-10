import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ["catalog", "bronze_db", "silver_db"])

spark = GlueContext(SparkContext()).spark_session

df = (
    spark.table(f"{args['bronze_db']}.raw_metodos_pago")
    .drop("lote")
    .filter(col("id_metodo_pago").isNotNull())
)
print("Filas leidas:", df.count())

(df.writeTo(f"{args['catalog']}.{args['silver_db']}.test_metodos_pago")
   .using("iceberg")
   .createOrReplace())

print("Tabla Iceberg escrita en Silver")