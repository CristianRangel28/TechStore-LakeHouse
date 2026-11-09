# Bitácora - Luis David Maldonado Suárez (Infraestructura / Terraform)

Proyecto: TechStore Lakehouse. 

Formato de cada entrada: Qué hice / Qué aprendí o qué falló / Evidencia.

---

## 6 de noviembre de 2026 - Base de Terraform

**Qué hice**
- Configuré el perfil `techstore` en AWS CLI con las llaves del usuario IAM compartido (las llaves quedan en `~/.aws/credentials`, fuera del repo).
- Verifiqué la conexión con `aws sts get-caller-identity --profile techstore`.
- Escribí el provider AWS (región us-east-1, perfil techstore) con `default_tags`: Project, Course, Owner, Environment, ManagedBy.
- Ejecuté `terraform init` (se generó `.terraform.lock.hcl`, que sí se sube a Git).
- Definí el recurso `aws_budgets_budget` (10 USD mensual) con 5 alertas: 2 USD, 5 USD, 80%, 100% real y 100% proyectado. Los correos de ambos integrantes están en `var.alert_email`.

**Qué aprendí**
- El provider es el traductor entre Terraform y AWS. Terraform no tiene permisos propios: usa los del usuario IAM.
- `default_tags` pone etiquetas a todos los recursos sin repetirlas.
- Budgets avisa pero no frena el gasto. `ABSOLUTE_VALUE` se lee en dólares y `PERCENTAGE` depende del límite.
- `terraform plan` solo muestra; `apply` es el que crea.

**Evidencia**
![Inicializar terraform](evidencias/terraform/terraform_init.png)
![Plan](evidencias/terraform/terraform_plan.png)
![Apply](evidencias/terraform/terraform_apply.png)
---

## 7 de noviembre de 2026 - Bucket S3, revisión de datos e IAM

**Qué hice**
- Decisión de diseño: un solo bucket S3 (aprobado por el profesor). Las capas son prefijos: `bronze/`, `silver/`, `gold/`, `scripts/`, `athena-results/`.
- El tfstate queda local. Solo yo corro `apply`, para no tener dos estados distintos.
- Organicé el código por archivos: `s3.tf`, `locals.tf`, `outputs.tf`, `iam.tf`, `glue_catalog.tf`.
- Creé el bucket `techstore-lakehouse-<id-cuenta>` con `aws_s3_bucket` y `aws_s3_bucket_public_access_block` (todo bloqueado). El nombre se arma con un `local` y `aws_caller_identity`.
- `force_destroy = true`: decisión consciente para que el `terraform destroy` del 13 de nov funcione aunque el bucket tenga datos.
- Verifiqué el bucket con `aws s3 ls --profile techstore`.
- Creé el rol IAM de Glue (`techstore-glue-role`): trust policy para `glue.amazonaws.com`, política administrada `AWSGlueServiceRole` y política propia con acceso al bucket (ListBucket sobre el bucket y Get/Put/DeleteObject sobre `/*`).
- Escribí las tres bases de datos del catálogo: `techstore_bronze`, `techstore_silver`, `techstore_gold` (apply pendiente, confirmar).

**Qué aprendí**
- Los nombres de bucket son únicos en todo AWS, por eso el id de cuenta en el nombre.
- En S3 las carpetas no existen: un prefijo aparece cuando alguien sube un objeto con esa ruta.
- El tfstate no es una copia de seguridad: es el mapa de lo que Terraform maneja. Lo que recrea todo son los archivos `.tf` en Git.
- Trust policy = quién puede asumir el rol. Política de permisos = qué puede hacer. Darle a Glue mi usuario IAM iría contra el mínimo privilegio.
- Glue es el servicio que corre Spark sin administrar un cluster; el DPU es su unidad de poder.
- El Data Catalog es uno solo por cuenta y región; las bases (bronze, silver, gold) son como carpetas dentro de él. Borrar una base no borra los archivos de S3.
- El Crawler descubre esquemas de Bronze. Silver y Gold los crean los jobs de Spark al escribir en Iceberg.
- Un lote es una entrega de datos en un momento distinto: lote 1 = carga inicial; lote 2 = cambios (4.000 ventas nuevas y 1.000 actualizadas), que dispara el MERGE.

**Qué quedó pendiente / riesgos**

- Mínimo privilegio del usuario IAM: ajustar al final, cuando se conozcan todos los permisos que usa Terraform.

**Evidencia**

![s3 rol](evidencias/terraform/s3.png)
![glue rol](evidencias/terraform/glue_rol.png)
![data catalog](evidencias/terraform/catalog.png)

## 8 de noviembre de 2026 - AWS Glue Crawler y organización de Bronze

**Qué hice**

- Definí la estructura de almacenamiento de los datos en Bronze, organizándolos por entidad y lote: `bronze/<entidad>/lote=<N>/`.
- Revisé las rutas S3 que utilizará el Glue Crawler para descubrir las entidades del proyecto, por ejemplo: `bronze/metodos_pago/`.
- Establecí que los objetivos del Crawler apuntan a la carpeta principal de cada entidad, no directamente a los archivos CSV o JSON.
- Definí que el Crawler se utilizará para descubrir los esquemas de Bronze y registrar las tablas en `techstore_bronze`.
- Aclaré el flujo de procesamiento: los Glue Jobs de Spark transformarán los datos y registrarán las tablas Iceberg de Silver y Gold en sus respectivas bases del Data Catalog.
- Definí el procedimiento de carga de datos: utilizar `aws s3 sync` para subir la carpeta local `data/` al prefijo `bronze/` del bucket.
- Revisé cómo manejar las entregas incrementales: el lote 2 se almacena en una ruta diferente al lote 1, evitando sobrescribir los archivos anteriores.
- Confirmé que mi compañero puede ejecutar `terraform init`, `fmt` y `validate` en su entorno sin crear recursos en AWS. El `apply` se mantiene centralizado para evitar conflictos con el estado de Terraform.

**Qué aprendí**

- En S3 las rutas son claves de objetos (*Object Keys*); las carpetas son prefijos virtuales.
- El Crawler puede examinar varios archivos y lotes dentro de la ruta configurada y descubrir sus esquemas.
- El Data Catalog almacena metadatos de las tablas, no los registros. Los datos permanecen en S3.
- Los Glue Jobs pueden registrar las tablas en el Data Catalog al escribir las tablas Iceberg; no necesitan un Crawler para Silver y Gold.
- `aws s3 sync` permite subir los datos por partes. Sin `--delete`, no elimina los objetos existentes que no estén en el origen.
- Los lotes se conservan en Bronze; las operaciones de inserción y actualización mediante MERGE se realizan posteriormente en Silver.

**Qué quedó pendiente / riesgos**

- Terminar la configuración del Glue Crawler en Terraform.
- Ejecutar `terraform plan` y revisar los recursos antes del `apply`.
- Subir los archivos generados a las rutas correspondientes de Bronze.
- Ejecutar el Crawler y verificar que las tablas aparezcan correctamente en `techstore_bronze`.
- Validar que las nueve entidades sean descubiertas correctamente, incluyendo los archivos JSON.

**Evidencia**

![data bronze](evidencias/terraform/data_bronze.png)
![glue rol](evidencias/terraform/crawler_apply.png)


## 9 de noviembre de 2026 - Athena


**Qué hice**

- Creé el workgroup techstore-workgroup (aws_athena_workgroup): resultados en athena-results/, cifrado SSE_S3, motor versión 3, métricas en CloudWatch y tope de 5 GB escaneados por consulta (variable).
- Probé SELECT COUNT(*) FROM raw_ventas: 1.500.000 filas, 137,31 MB escaneados.
- Revisé ProcessedBytes en CloudWatch (AWS/Athena).

**Qué aprendí**

- Athena cobra por datos escaneados (unos 5 USD por TB, mínimo 10 MB por consulta), no por tiempo. La consulta de 137 MB cuesta unos 0,0007 USD.
- Mis primeras consultas corrieron en el workgroup primary y guardaron resultados en un bucket de un taller anterior. Hay que elegir techstore-workgroup.
- tfvars tiene prioridad sobre el default de una variable.
- Con CSV, Athena escanea el archivo completo aunque pida una columna. Parquet/Iceberg debería reducir ese escaneo (por comprobar en Silver).

**Evidencia**


![athena apply](evidencias/terraform/athena.png)
![consulta athena](evidencias/terraform/consulta.png)
![athena results](evidencias/terraform/athena_results.png)