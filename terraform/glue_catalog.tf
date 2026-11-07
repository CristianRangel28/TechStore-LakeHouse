resource "aws_glue_catalog_database" "bronze" {
  name        = "techstore_bronze"
  description = "Catalogo de datos crudos provenientes de las fuentes originales."
}

resource "aws_glue_catalog_database" "silver" {
  name        = "techstore_silver"
  description = "Catalogo de datos limpios, validados y transformados."
}

resource "aws_glue_catalog_database" "gold" {
  name        = "techstore_gold"
  description = "Catalogo de datos preparados para analisis y consultas de negocio."
}