# TechStore LakeHouse

**Plataforma de Data Lakehouse para el procesamiento, transformación y análisis de datos de una cadena de tiendas de tecnología.**

## Descripción del proyecto

TechStore LakeHouse es un proyecto académico que implementa una arquitectura de **Data Lakehouse en Amazon Web Services (AWS)**, orientada a centralizar, procesar, transformar y analizar grandes volúmenes de información provenientes de diferentes sistemas de una cadena de tiendas de tecnología.

La solución integra datos transaccionales provenientes de una base de datos OLTP, almacenados en archivos CSV, y datos semiestructurados provenientes de sistemas de posventa, almacenados en formato JSON.

La infraestructura se aprovisiona mediante **Infrastructure as Code (IaC)** utilizando Terraform, mientras que los procesos de transformación y procesamiento de datos se desarrollan con AWS Glue y PySpark.

El almacenamiento utiliza Amazon S3 y Apache Iceberg, permitiendo gestionar tablas analíticas, realizar operaciones de actualización y aprovechar funcionalidades como Time Travel.

## Objetivos

- Implementar una arquitectura Data Lakehouse utilizando servicios de AWS.
- Integrar información proveniente de diferentes fuentes y formatos.
- Procesar grandes volúmenes de datos mediante procesos ETL.
- Organizar los datos en diferentes capas de procesamiento.
- Utilizar Apache Iceberg para la administración de tablas analíticas.
- Facilitar consultas analíticas mediante Amazon Athena.
- Automatizar el aprovisionamiento de infraestructura utilizando Terraform.
- Garantizar procesos reproducibles y escalables de procesamiento de datos.

## Arquitectura

La arquitectura está organizada en tres capas principales:

| Capa | Descripción |
|---|---|
| **Bronze** | Almacena los datos originales provenientes de las fuentes CSV y JSON, conservando su estructura inicial. |
| **Silver** | Contiene los datos limpios, validados, estandarizados y transformados mediante procesos ETL. |
| **Gold** | Contiene los datos estructurados para análisis, organizados mediante un modelo dimensional. |

### Flujo de procesamiento

```text
          FUENTES DE DATOS
                 |
        +--------+--------+
        |                 |
      CSV                 JSON
        |                 |
        +--------+--------+
                 |
                 v
          AMAZON S3
             BRONZE
                 |
                 v
          AWS GLUE JOB 1
        Bronze → Silver
                 |
                 v
          AMAZON S3
             SILVER
                 |
                 v
          AWS GLUE JOB 2
        Silver → Gold
                 |
                 v
          AMAZON S3
              GOLD
                 |
                 v
          AMAZON ATHENA
        Consultas analíticas
```

Los procesos ETL utilizan PySpark y Apache Iceberg para transformar y administrar los datos.

AWS Glue Data Catalog permite registrar y consultar los metadatos de las tablas del Lakehouse.

La ejecución de los procesos se integra mediante AWS Step Functions para coordinar la secuencia de procesamiento y gestionar los resultados de cada etapa.

## Fuentes de datos

El proyecto contempla dos fuentes principales:

### 1. Sistema transaccional (OLTP)

Información estructurada almacenada en archivos CSV:

- Clientes.
- Productos.
- Marcas.
- Sucursales.
- Ventas.
- Detalles de ventas.
- Métodos de pago.

### 2. Sistema de posventa

Información semiestructurada almacenada en archivos JSON:

- Garantías.
- Devoluciones.

Estos registros pueden contener atributos anidados relacionados con motivos, estados y demás información del proceso de posventa.

## Tecnologías utilizadas

| Tecnología | Propósito |
|---|---|
| **Amazon S3** | Almacenamiento de las diferentes capas del Lakehouse. |
| **AWS Glue** | Procesamiento ETL y administración del catálogo de datos. |
| **AWS Glue Data Catalog** | Registro y administración de metadatos. |
| **AWS Step Functions** | Orquestación de los procesos ETL. |
| **Amazon Athena** | Consultas analíticas utilizando SQL. |
| **Apache Spark / PySpark** | Procesamiento distribuido y transformación de datos. |
| **Apache Iceberg** | Administración de tablas analíticas y control de versiones de datos. |
| **Terraform** | Aprovisionamiento y administración de infraestructura como código. |
| **Python** | Desarrollo de generadores de datos y componentes de procesamiento. |
| **Git / GitHub** | Control de versiones y colaboración. |

## Estructura del repositorio

La organización del proyecto busca separar la infraestructura, el código de procesamiento, los componentes de despliegue y los generadores de datos.

```text
TechStore-LakeHouse/
│
├── deployment/
│   └── glue/
│       ├── bronze_to_silver.py
│       └── silver_to_gold.py
│
├── src/
│   ├── common/
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
│
├── scripts/
│   ├── generators/
│   ├── json_generators/
│   └── datasets/
│
├── terraform/
│   ├── athena.tf
│   ├── glue_catalog.tf
│   ├── glue_crawler.tf
│   ├── iam.tf
│   ├── main.tf
│   ├── outputs.tf
│   ├── s3.tf
│   └── variables.tf
│
└── README.md
```

*Nota: la estructura representa la organización lógica del proyecto; algunos componentes pueden encontrarse todavía en desarrollo.*

## Infraestructura como código

Terraform se utiliza para definir y aprovisionar los recursos necesarios en AWS.

Entre los componentes administrados se encuentran:

- Buckets de Amazon S3.
- Bases de datos del Glue Data Catalog.
- Crawlers de AWS Glue.
- Roles y políticas IAM.
- Trabajos ETL de AWS Glue.
- Recursos de Amazon Athena.
- Componentes de orquestación del pipeline.

La infraestructura puede inicializarse y validarse desde el directorio `terraform/`.

### Requisitos previos

Antes de ejecutar Terraform se necesita:

- Cuenta de AWS con permisos suficientes.
- AWS CLI instalado y configurado.
- Terraform instalado.
- Credenciales AWS configuradas localmente.
- Perfil de AWS con permisos sobre los recursos del proyecto.

### Inicialización

Desde la raíz del repositorio:

```bash
cd terraform
```

Inicializar Terraform:

```bash
terraform init
```

Validar la configuración:

```bash
terraform fmt -recursive
terraform validate
```

Revisar los cambios propuestos:

```bash
terraform plan
```

Aplicar los cambios:

```bash
terraform apply
```

**Importante:** antes de ejecutar `terraform apply`, se debe revisar cuidadosamente el plan para identificar los recursos que se crearán, modificarán o eliminarán.

No se deben compartir archivos de estado, credenciales ni variables locales con información sensible.

## Procesamiento de datos

Los procesos ETL se organizan modularmente para facilitar su mantenimiento y reutilización.

### Bronze → Silver

El primer proceso ETL es responsable de:

1. Extraer los datos originales desde AWS Glue Data Catalog.
2. Aplicar transformaciones y estandarización.
3. Normalizar nombres de columnas.
4. Realizar validaciones y conversiones de tipos de datos.
5. Incorporar información de auditoría.
6. Cargar los resultados en tablas Apache Iceberg de la capa Silver.

### Silver → Gold

El segundo proceso ETL tiene como objetivo:

1. Extraer información de las tablas Silver.
2. Integrar las entidades necesarias para el análisis.
3. Construir un modelo dimensional.
4. Generar las tablas de dimensiones y hechos.
5. Cargar los resultados en la capa Gold mediante Apache Iceberg.

## Consultas analíticas

Amazon Athena permite consultar las tablas registradas en Glue Data Catalog utilizando SQL.

Esto facilita el análisis de información relacionada con:

- Clientes.
- Productos y marcas.
- Ventas.
- Sucursales.
- Métodos de pago.
- Garantías y devoluciones.

Además, Apache Iceberg proporciona funcionalidades para administrar versiones de los datos y consultar estados anteriores de las tablas mediante Time Travel.

## Control de versiones y colaboración

El proyecto utiliza Git y GitHub para gestionar el desarrollo colaborativo.

Se trabaja mediante ramas independientes para implementar funcionalidades y posteriormente integrarlas a `develop` mediante Pull Requests.

Convención sugerida para las ramas:

```text
feature/nombre-funcionalidad
fix/nombre-correccion
docs/nombre-documentacion
```

Convención de commits:

```text
feat: nueva funcionalidad
fix: corrección de errores
docs: cambios en documentación
refactor: reorganización del código
chore: tareas de mantenimiento
```

## Seguridad y buenas prácticas

- No publicar credenciales de AWS.
- No versionar archivos `terraform.tfstate`.
- No versionar archivos `terraform.tfvars`.
- Excluir archivos temporales y cachés de Python.
- No subir datasets generados de gran tamaño al repositorio.
- Revisar los planes de Terraform antes de aplicarlos.
- Mantener los procesos ETL organizados y reutilizables.
- Utilizar políticas IAM con los permisos estrictamente necesarios.

## Estado del proyecto

El proyecto se encuentra en desarrollo.

Se han trabajado los siguientes componentes:

- Definición de la arquitectura Data Lakehouse.
- Aprovisionamiento de infraestructura mediante Terraform.
- Organización modular del código Python.
- Desarrollo del proceso Bronze → Silver.
- Integración de Apache Iceberg con AWS Glue.
- Preparación de los procesos de transformación hacia Gold.
- Configuración de componentes para la orquestación del pipeline.

Las pruebas de integración y validación de los procesos completos continúan según el avance del proyecto.

## Equipo de desarrollo

Proyecto académico desarrollado por estudiantes de Ingeniería de Sistemas de la Universidad de Pamplona.

**Repositorio:** TechStore-LakeHouse

---

*TechStore LakeHouse — Transformando datos en información para la toma de decisiones.*