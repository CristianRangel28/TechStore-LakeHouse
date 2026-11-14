# TechStore LakeHouse

<p align="center">
  <strong>Arquitectura Data Lakehouse en AWS para la integración, procesamiento y análisis de datos de una cadena de tiendas de tecnología.</strong>
</p>

---

## 1. Descripción del proyecto

**TechStore LakeHouse** es un proyecto académico que implementa una arquitectura **Data Lakehouse sobre Amazon Web Services (AWS)**, orientada a centralizar, procesar, transformar y analizar grandes volúmenes de información provenientes de diferentes sistemas de una cadena ficticia de tiendas de tecnología.

La solución integra dos fuentes principales de información:

- **Sistema transaccional (OLTP):** datos estructurados almacenados en archivos CSV, relacionados con clientes, productos, marcas, sucursales, ventas, detalles de ventas y métodos de pago.
- **Sistema de posventa:** datos semiestructurados almacenados en archivos JSON, relacionados con garantías y devoluciones, incluyendo atributos anidados.

La infraestructura se define mediante **Infrastructure as Code (IaC)** utilizando Terraform. El procesamiento de datos se implementa con AWS Glue y Apache Spark (PySpark), mientras que Amazon S3 proporciona el almacenamiento de las diferentes capas del Lakehouse.

La arquitectura utiliza **Apache Iceberg** para administrar tablas analíticas, soportar operaciones de inserción y actualización, mantener metadatos transaccionales y aprovechar funcionalidades como Time Travel.

Finalmente, Amazon Athena permite realizar consultas SQL sobre los datos procesados y AWS Step Functions coordina la ejecución de los procesos ETL.

## 2. Objetivos

- Diseñar e implementar una arquitectura Data Lakehouse utilizando servicios de AWS.
- Integrar información estructurada y semiestructurada proveniente de diferentes fuentes.
- Procesar millones de registros mediante procesos ETL desarrollados con PySpark.
- Organizar los datos en capas de procesamiento Landing, Bronze, Silver y Gold.
- Implementar tablas analíticas utilizando Apache Iceberg.
- Desarrollar operaciones incrementales de inserción y actualización de registros.
- Construir un modelo dimensional orientado al análisis de información.
- Automatizar el aprovisionamiento de infraestructura mediante Terraform.
- Orquestar los procesos de transformación utilizando AWS Step Functions.
- Facilitar la exploración y consulta de información mediante Amazon Athena.

## 3. Arquitectura de la solución

La arquitectura está organizada bajo el enfoque **Medallion Architecture**, utilizando cuatro capas que permiten controlar el ciclo de vida de los datos desde su recepción hasta su disponibilidad para análisis.

### 3.1. Capas del Data Lakehouse

| Capa | Descripción |
|---|---|
| **Landing** | Recibe los archivos originales provenientes de las fuentes de datos CSV y JSON. |
| **Bronze** | Almacena los datos ingeridos, conservando su estructura de origen y agregando información de trazabilidad de la ingesta. |
| **Silver** | Contiene los datos limpios, estandarizados, validados y transformados mediante procesos ETL. |
| **Gold** | Contiene información preparada para el análisis, organizada mediante un modelo dimensional de dimensiones y hechos. |

### 3.2. Flujo general de procesamiento

```text
                  FUENTES DE DATOS
                         |
              +----------+----------+
              |                     |
         SISTEMA OLTP          SISTEMA POSVENTA
              |                     |
             CSV                   JSON
              |                     |
              +----------+----------+
                         |
                         v
                  AMAZON S3
                    LANDING
                         |
                         v
                  PROCESO DE INGESTA
                         |
                         v
                  AMAZON S3
                    BRONZE
                         |
                         v
                    AWS GLUE
                  BRONZE TO SILVER
                         |
                         v
                  AMAZON S3
                    SILVER
                         |
                         v
                    AWS GLUE
                  SILVER TO GOLD
                         |
                         v
                  AMAZON S3
                     GOLD
                         |
                         v
                  AMAZON ATHENA
                CONSULTAS ANALÍTICAS
```

Los procesos ETL son coordinados mediante **AWS Step Functions**, que controla la secuencia de ejecución de los trabajos de AWS Glue.

El **AWS Glue Data Catalog** permite registrar y consultar los metadatos de las tablas utilizadas durante el procesamiento.

Apache Iceberg proporciona la administración transaccional de las tablas analíticas almacenadas en Amazon S3.

### 3.3. Componentes principales

| Componente | Responsabilidad |
|---|---|
| Amazon S3 | Almacenamiento de las capas del Data Lakehouse. |
| AWS Glue Jobs | Ejecución de los procesos ETL mediante PySpark. |
| AWS Glue Crawlers | Descubrimiento y actualización de metadatos de las fuentes de datos. |
| AWS Glue Data Catalog | Catálogo centralizado de bases de datos y tablas. |
| AWS Step Functions | Orquestación y control de los procesos ETL. |
| Amazon Athena | Consultas analíticas mediante SQL. |
| Apache Iceberg | Administración transaccional y evolución de tablas analíticas. |
| Terraform | Definición y aprovisionamiento de infraestructura como código. |

## 4. Fuentes de datos

El proyecto integra dos sistemas de información.

### 4.1. Sistema transaccional (OLTP)

Los datos estructurados se reciben mediante archivos CSV.

| Archivo | Descripción |
|---|---|
| clientes.csv | Información de los clientes. |
| productos.csv | Catálogo de productos comercializados. |
| marcas.csv | Marcas de los productos. |
| sucursales.csv | Información de las sucursales. |
| metodos_pago.csv | Métodos de pago disponibles. |
| ventas.csv | Encabezados y datos generales de las ventas. |
| detalle_ventas.csv | Productos, cantidades y valores asociados a cada venta. |

### 4.2. Sistema de posventa

Los datos semiestructurados se reciben mediante archivos JSON.

| Archivo | Descripción |
|---|---|
| garantias.json | Solicitudes y estados de garantías. |
| devoluciones.json | Solicitudes, estados y valores de devoluciones. |

Los documentos JSON contienen estructuras anidadas para representar información como:

- Producto asociado al proceso de posventa.
- Motivo de la solicitud.
- Estado y observaciones.
- Información de reembolsos.
- Fechas de solicitud y resolución.

## 5. Volumen de datos y procesamiento incremental

El proyecto contempla un volumen inicial aproximado de **8,9 millones de registros**, distribuido entre las fuentes transaccionales y de posventa.

Adicionalmente, se implementó un segundo lote incremental de **5.000 registros**, compuesto por:

- 4.000 registros nuevos.
- 1.000 registros destinados a actualizar información existente.

### 5.1. Distribución del segundo lote

| Fuente | Registros nuevos | Registros actualizados | Total |
|---|---:|---:|---:|
| Clientes | 300 | 150 | 450 |
| Marcas | 5 | 5 | 10 |
| Productos | 150 | 100 | 250 |
| Sucursales | 2 | 5 | 7 |
| Métodos de pago | 0 | 2 | 2 |
| Ventas | 800 | 348 | 1.148 |
| Detalles de ventas | 2.400 | 200 | 2.600 |
| Garantías | 180 | 100 | 280 |
| Devoluciones | 163 | 90 | 253 |
| **Total** | **4.000** | **1.000** | **5.000** |

### 5.2. Estrategia incremental

El procesamiento incremental permite incorporar nuevos registros y actualizar entidades existentes sin tener que reconstruir completamente las tablas analíticas.

La solución utiliza operaciones **MERGE INTO** sobre tablas Apache Iceberg para implementar la lógica de UPSERT.

El comportamiento contempla:

- Inserción de registros que todavía no existen.
- Actualización de registros cuando se identifican cambios en sus atributos de negocio.
- Conservación de información histórica de creación e ingesta.
- Identificación de los lotes responsables de cada operación.
- Prevención de duplicados mediante validaciones de claves primarias.
- Ejecuciones idempotentes para evitar modificaciones innecesarias al reprocesar información.

La trazabilidad se complementa con columnas de auditoría, como:

- `_ingestion_timestamp`
- `_ts_updated`
- `_created_batch`
- `_updated_batch`
- `_ingestion_batch`
- `_source_file`

Estas columnas permiten identificar el origen, momento y lote asociado a cada registro.

## 6. Procesos ETL

Los procesos de transformación están desarrollados modularmente utilizando Python, PySpark y Apache Iceberg.

### 6.1. Bronze → Silver

El primer proceso ETL se encarga de convertir los datos originales en información estructurada, limpia y validada.

Sus principales responsabilidades son:

1. Leer los datos desde AWS Glue Data Catalog.
2. Identificar las tablas y fuentes que deben procesarse.
3. Aplicar filtros para procesamientos incrementales por lote.
4. Estandarizar nombres de columnas.
5. Transformar estructuras JSON anidadas.
6. Validar claves primarias y atributos obligatorios.
7. Validar tipos de datos y conversiones.
8. Registrar información de auditoría.
9. Identificar y contabilizar registros rechazados.
10. Cargar los registros válidos en tablas Apache Iceberg de la capa Silver.

El proceso está organizado en módulos:

| Archivo | Responsabilidad |
|---|---|
| `main.py` | Controla la ejecución del trabajo ETL. |
| `extract.py` | Extrae los datos desde las fuentes registradas en el catálogo. |
| `transform.py` | Aplica limpieza, normalización y validaciones. |
| `load_iceberg.py` | Gestiona la escritura de los datos en las tablas Silver. |

### 6.2. Silver → Gold

El segundo proceso ETL construye el modelo dimensional a partir de los datos depurados de Silver.

Sus responsabilidades son:

1. Leer las tablas Silver.
2. Integrar las entidades mediante operaciones de JOIN.
3. Construir las dimensiones analíticas.
4. Construir las tablas de hechos.
5. Gestionar claves sustitutas.
6. Resolver relaciones inexistentes mediante registros desconocidos.
7. Incorporar información de auditoría.
8. Cargar las tablas finales mediante operaciones de UPSERT sobre Apache Iceberg.

Este proceso permite disponer de información organizada para realizar consultas analíticas y apoyar la toma de decisiones.

## 7. Modelo dimensional de Gold

La capa Gold utiliza un modelo dimensional compuesto por **cinco dimensiones y tres tablas de hechos**.

### 7.1. Dimensiones

| Tabla | Descripción |
|---|---|
| `dim_customer` | Información analítica de los clientes. |
| `dim_product` | Información de los productos, incluyendo sus marcas. |
| `dim_branch` | Información de las sucursales. |
| `dim_payment_method` | Información de los métodos de pago. |
| `dim_date` | Dimensión temporal para el análisis de fechas. |

Las dimensiones utilizan claves sustitutas para facilitar las relaciones con las tablas de hechos.

### 7.2. Tablas de hechos

| Tabla | Descripción | Granularidad |
|---|---|---|
| `fact_sales` | Información de las ventas y sus detalles. | Una fila por detalle de venta. |
| `fact_warranties` | Información de las solicitudes de garantía. | Una fila por proceso de posventa de tipo garantía. |
| `fact_returns` | Información de las solicitudes de devolución. | Una fila por proceso de posventa de tipo devolución. |

La tabla `fact_sales` integra la información de ventas y detalles, permitiendo analizar cantidades, precios, descuentos y subtotales por producto vendido.

Las tablas `fact_warranties` y `fact_returns` se mantienen independientes para evitar duplicaciones y relaciones incorrectas entre procesos de posventa.

### 7.3. Características del modelo

- Claves sustitutas para las dimensiones.
- Integración de dimensiones y hechos mediante claves foráneas.
- Dimensión temporal con formato de clave `YYYYMMDD`.
- Registro desconocido para relaciones sin correspondencia.
- Conservación de la granularidad de cada tabla de hechos.
- Información de auditoría para trazabilidad.
- Tablas administradas mediante Apache Iceberg.

## 8. Apache Iceberg

Apache Iceberg es una pieza central de la arquitectura porque permite administrar las tablas analíticas de manera transaccional sobre Amazon S3.

Su implementación facilita:

- Operaciones de inserción y actualización.
- Ejecución de instrucciones `MERGE INTO`.
- Control de snapshots de las tablas.
- Consultas históricas mediante Time Travel.
- Evolución del esquema.
- Consistencia de las operaciones sobre los datos.
- Procesamiento incremental e idempotente.

La solución utiliza tablas Iceberg en versión 2.

### Estrategia de UPSERT

La lógica de carga se centraliza en un componente reutilizable:

`src/common/s3_writers.py`

La función `upsert_iceberg_table()` permite realizar operaciones de inserción y actualización sobre diferentes tablas, evitando duplicar la lógica de escritura en cada proceso ETL.

Antes de ejecutar las operaciones, se realizan validaciones sobre:

- Existencia de datos.
- Claves primarias nulas.
- Duplicados.
- Conflictos entre registros.
- Compatibilidad del esquema.
- Tipos de datos.
- Columnas que deben conservarse durante una actualización.

Esta estrategia permite reutilizar el mismo mecanismo de carga tanto en Silver como en Gold.

## 9. Orquestación con AWS Step Functions

AWS Step Functions coordina la ejecución de los procesos ETL.

El flujo de trabajo controla la secuencia entre Bronze, Silver y Gold, permitiendo gestionar los resultados de cada etapa.

Entre sus responsabilidades se encuentran:

- Verificar y preparar el identificador de ejecución.
- Propagar el identificador de lote entre los trabajos.
- Evaluar si se debe realizar un procesamiento incremental.
- Ejecutar el trabajo Bronze → Silver.
- Ejecutar el trabajo Silver → Gold.
- Esperar la finalización de los trabajos de AWS Glue.
- Gestionar errores mediante estados de captura.
- Determinar el resultado final de la ejecución.

La orquestación utiliza integraciones de AWS Glue con el patrón de ejecución síncrona `.sync`, de modo que cada etapa espera la finalización de la anterior antes de continuar.

## 10. Infraestructura como código con Terraform

Terraform permite definir la infraestructura necesaria para la solución mediante archivos de configuración declarativos.

La infraestructura se organiza dentro del directorio `terraform/`.

Entre los recursos y componentes definidos se encuentran:

- Amazon S3.
- AWS Glue Data Catalog.
- AWS Glue Crawlers.
- AWS Glue Jobs.
- Roles y políticas IAM.
- Amazon Athena.
- AWS Step Functions.
- Artefactos de código utilizados por los trabajos ETL.

El uso de Infrastructure as Code permite mantener una infraestructura reproducible, organizada y versionada junto con el código de la aplicación.

### 10.1. Herramientas necesarias

Para trabajar con la infraestructura se requiere:

- Una cuenta de AWS.
- AWS CLI.
- Terraform.
- Credenciales AWS configuradas.
- Un perfil con los permisos necesarios.
- Git.

### 10.2. Validación de Terraform

Desde la raíz del repositorio:

```bash
cd terraform
```

Inicializar Terraform:

```bash
terraform init
```

Formatear los archivos:

```bash
terraform fmt -recursive
```

Validar la configuración:

```bash
terraform validate
```

Revisar los cambios propuestos:

```bash
terraform plan
```

La aplicación de cambios debe realizarse únicamente por el responsable autorizado de la infraestructura, después de revisar el plan y confirmar que las operaciones propuestas sean correctas.

**Importante:** no se deben compartir credenciales, archivos de estado ni variables locales con información sensible.

## 11. Consultas analíticas con Amazon Athena

Amazon Athena permite consultar las tablas registradas en AWS Glue Data Catalog mediante instrucciones SQL.

Las consultas pueden utilizarse para analizar:

- Comportamiento de los clientes.
- Productos y marcas más vendidos.
- Desempeño de las sucursales.
- Métodos de pago utilizados.
- Ingresos y descuentos.
- Evolución de las ventas.
- Solicitudes de garantías.
- Devoluciones y reembolsos.

La capa Gold proporciona una estructura dimensional que facilita la construcción de consultas analíticas sin tener que reconstruir las relaciones de las fuentes originales.

## 12. Estructura del repositorio

La organización del repositorio separa la infraestructura, los procesos ETL, los generadores de datos y los componentes de ejecución.

```text
TechStore-LakeHouse/
│
├── deployment/
│   └── glue/
│
├── src/
│   ├── common/
│   │   ├── s3_readers.py
│   │   ├── s3_writers.py
│   │   └── spark_session.py
│   │
│   └── jobs/
│       ├── bronze_to_silver/
│       │   ├── extract.py
│       │   ├── transform.py
│       │   ├── load_iceberg.py
│       │   └── main.py
│       │
│       └── silver_to_gold/
│           ├── transform.py
│           ├── load_iceberg.py
│           └── main.py
│
├── scripts/
│   ├── generators/
│   │   ├── config.py
│   │   ├── utils.py
│   │   └── generate_batch_2.py
│   │
│   ├── json_generators/
│   │   ├── config.py
│   │   ├── utils.py
│   │   └── generate_batch_2.py
│   │
│   └── datasets/
│
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── s3.tf
│   ├── iam.tf
│   ├── glue_catalog.tf
│   ├── glue_crawler.tf
│   ├── glue_jobs.tf
│   ├── artifacts.tf
│   ├── athena.tf
│   └── step_functions.tf
│
├── docs/
│
├── .gitignore
└── README.md
```

*Nota: el árbol representa la organización lógica de los componentes principales del proyecto. Los archivos y directorios específicos pueden variar según la versión de la rama.*

## 13. Tecnologías utilizadas

| Tecnología | Aplicación en el proyecto |
|---|---|
| Python | Desarrollo de generadores y componentes de procesamiento. |
| Apache Spark | Procesamiento distribuido de datos. |
| PySpark | Desarrollo de los procesos ETL. |
| Amazon S3 | Almacenamiento de datos. |
| AWS Glue | Ejecución de procesos ETL. |
| AWS Glue Data Catalog | Administración de metadatos. |
| AWS Glue Crawlers | Descubrimiento de esquemas. |
| AWS Step Functions | Orquestación del pipeline. |
| Amazon Athena | Consultas SQL analíticas. |
| Apache Iceberg | Administración transaccional de tablas. |
| Terraform | Aprovisionamiento de infraestructura. |
| Git | Control de versiones. |
| GitHub | Colaboración y gestión del repositorio. |

## 14. Validaciones y resultados

La solución fue desplegada y probada en el entorno de AWS.

Durante la implementación se trabajó en la validación de los siguientes componentes:

- Aprovisionamiento de infraestructura mediante Terraform.
- Almacenamiento de información en Amazon S3.
- Registro y consulta de metadatos mediante AWS Glue Data Catalog.
- Procesamiento de datos estructurados y semiestructurados.
- Ejecución de procesos ETL con PySpark.
- Transformación de datos entre las capas Bronze, Silver y Gold.
- Administración de tablas mediante Apache Iceberg.
- Operaciones incrementales de inserción y actualización.
- Orquestación de trabajos mediante AWS Step Functions.
- Consultas analíticas mediante Amazon Athena.

El resultado es una arquitectura Data Lakehouse funcional que integra diferentes fuentes de información, procesa grandes volúmenes de datos y proporciona una capa analítica preparada para consultas.

## 15. Seguridad y buenas prácticas

El proyecto considera las siguientes prácticas:

- No publicar credenciales de AWS.
- No versionar archivos `terraform.tfstate`.
- No versionar archivos `terraform.tfvars` con información sensible.
- Excluir entornos virtuales y cachés de Python.
- Evitar subir datasets generados de gran tamaño al repositorio.
- Aplicar permisos IAM siguiendo el principio de mínimo privilegio.
- Validar los cambios de Terraform antes de aplicarlos.
- Mantener los procesos ETL organizados y reutilizables.
- Incorporar información de auditoría para garantizar la trazabilidad.
- Utilizar control de versiones para mantener el historial de desarrollo.

## 16. Control de versiones y colaboración

El desarrollo se gestiona mediante Git y GitHub, utilizando ramas para separar funcionalidades, correcciones y documentación.

La integración de cambios se realiza mediante Pull Requests.

### Convención de ramas

```text
main
develop
feature/nombre-funcionalidad
fix/nombre-correccion
docs/nombre-documentacion
```

### Convención de commits

```text
feat: nueva funcionalidad
fix: corrección de errores
docs: cambios en documentación
refactor: reorganización del código
chore: tareas de mantenimiento
test: incorporación o modificación de pruebas
```

## 17. Estado del proyecto

**Estado: Implementado, desplegado y validado en AWS.**

TechStore LakeHouse cuenta con una arquitectura Data Lakehouse funcional que integra almacenamiento, procesamiento, transformación, administración transaccional y consultas analíticas.

La solución implementa:

- Arquitectura de cuatro capas: Landing, Bronze, Silver y Gold.
- Integración de fuentes CSV y JSON.
- Infraestructura como código mediante Terraform.
- Procesamiento ETL mediante AWS Glue y PySpark.
- Catálogo centralizado mediante AWS Glue Data Catalog.
- Administración de tablas con Apache Iceberg.
- Modelo dimensional con cinco dimensiones y tres tablas de hechos.
- Procesamiento incremental mediante operaciones UPSERT.
- Orquestación de procesos con AWS Step Functions.
- Consultas analíticas mediante Amazon Athena.

El proyecto demuestra la integración de diferentes tecnologías de ingeniería de datos para construir una plataforma escalable y organizada, capaz de transformar datos operacionales en información útil para el análisis y la toma de decisiones.

## 18. Equipo de desarrollo

Proyecto académico desarrollado por estudiantes de **Ingeniería de Sistemas de la Universidad de Pamplona**.

**Nombre del proyecto:** TechStore LakeHouse

**Repositorio:** TechStore-LakeHouse

---

<p align="center">
  <strong>TechStore LakeHouse</strong><br>
  Transformando datos en información para la toma de decisiones.
</p>