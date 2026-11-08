# Manual de generación de datos - TechStore

**Proyecto:** TechStore Data Lakehouse  
**Módulo:** Generación de datos sintéticos  
**Versión:** 1.1  
**Fecha:** 7 de noviembre de 2026

---

## 1. Introducción

El proyecto TechStore cuenta con un conjunto de scripts desarrollados en Python para generar datos sintéticos que representan las operaciones de una cadena ficticia de tiendas de tecnología.

Estos datos permiten simular un entorno empresarial con información de clientes, productos, marcas, sucursales, métodos de pago y transacciones comerciales.

El objetivo es generar un volumen considerable de información que posteriormente será utilizado para alimentar la arquitectura Data Lakehouse implementada en AWS.

Los archivos generados se utilizarán en las diferentes etapas del procesamiento de datos:

- **Bronze:** almacenamiento inicial de los datos originales provenientes de las fuentes.
- **Silver:** limpieza, validación y transformación de los datos.
- **Gold:** preparación de información estructurada para análisis y consultas.

En esta arquitectura, los archivos generados se cargarán directamente en la capa Bronze de Amazon S3, sin utilizar una capa Landing.

## 2. Tecnologías utilizadas

Para la generación de datos se utilizan:

- Python 3.
- Módulos estándar de Python: `csv`, `random`, `datetime`, `decimal` y `pathlib`.
- Archivos CSV como formato de almacenamiento.
- Generación determinista mediante semillas aleatorias.

No es necesario instalar librerías externas para ejecutar los generadores actuales.

## 3. Estructura de los scripts

Todos los generadores se encuentran dentro de:

```text
scripts/generators/
```

A continuación, se describe la responsabilidad de cada archivo.

### 3.1. config.py

Contiene la configuración central del sistema de generación.

Define:

- Cantidad de registros que tendrá cada archivo.
- Semilla utilizada para generar datos reproducibles.
- Distribución de productos por venta.
- Fechas iniciales y finales de generación.
- Codificación de los archivos CSV.
- Separador de columnas.
- Tamaño de los lotes de escritura.

También contiene validaciones para comprobar que la cantidad de ventas y detalles configurados sea consistente.

Este archivo debe modificarse cuando se necesite cambiar el volumen de datos.

### 3.2. utils.py

Contiene funciones auxiliares compartidas por los diferentes generadores.

Sus responsabilidades incluyen:

- Crear el directorio de salida.
- Abrir y preparar archivos CSV.
- Configurar los encabezados de las columnas.
- Generar fechas aleatorias.
- Escribir registros por lotes.
- Mostrar información sobre los archivos generados.

### 3.3. sales_data.py

Contiene la lógica compartida para generar ventas y sus respectivos detalles.

Se encarga de:

- Determinar cuántos productos tendrá cada venta.
- Generar fechas de transacción.
- Seleccionar productos aleatorios sin repetirlos dentro de una misma venta.
- Generar cantidades y precios.
- Calcular descuentos.
- Calcular impuestos.
- Calcular el valor total de cada venta.

También garantiza que los generadores de ventas y detalles produzcan información consistente cuando se ejecutan por separado.

### 3.4. Generadores individuales

| Archivo | Responsabilidad | Archivo generado |
|---|---|---|
| generate_customers.py | Genera información de clientes. | clientes.csv |
| generate_brands.py | Genera las marcas de los productos. | marcas.csv |
| generate_products.py | Genera el catálogo de productos y sus marcas. | productos.csv |
| generate_branches.py | Genera las sucursales de la empresa. | sucursales.csv |
| generate_payment_methods.py | Genera los métodos de pago disponibles. | metodos_pago.csv |
| generate_sales.py | Genera las transacciones comerciales. | ventas.csv |
| generate_sales_details.py | Genera los productos asociados a cada venta. | detalle_ventas.csv |

### 3.5. generate_all.py

Es el archivo principal de ejecución.

Se encarga de:

1. Validar la configuración.
2. Crear el directorio de salida.
3. Ejecutar todos los generadores en el orden establecido.
4. Mostrar el progreso de generación.
5. Informar la cantidad total de registros configurados.

Este archivo permite generar todos los archivos CSV mediante un único comando.

---

## 4. Volumen de datos configurado

La configuración actual está diseñada para generar un total de **7.920.155 registros** distribuidos de la siguiente manera:

| Archivo | Cantidad de registros |
|---|---:|
| clientes.csv | 500.000 |
| marcas.csv | 100 |
| productos.csv | 100.000 |
| sucursales.csv | 50 |
| metodos_pago.csv | 5 |
| ventas.csv | 1.500.000 |
| detalle_ventas.csv | 5.820.000 |
| **TOTAL** | **7.920.155** |

Este volumen permite simular una operación comercial con una cantidad considerable de clientes, productos y transacciones.

### 4.1. Distribución de productos por venta

Las ventas no tienen una cantidad fija de productos.

Se utiliza la siguiente distribución:

| Productos por venta | Cantidad de ventas |
|---|---:|
| 1 producto | 75.000 |
| 2 productos | 150.000 |
| 3 productos | 225.000 |
| 4 productos | 480.000 |
| 5 productos | 570.000 |
| **TOTAL** | **1.500.000** |

De esta manera, cada venta tendrá entre uno y cinco productos asociados.

El número total de detalles se calcula multiplicando la cantidad de productos por venta por el número de ventas de cada grupo.

---

## 5. Configuración de los generadores

Las cantidades se administran desde el archivo:

```text
scripts/generators/config.py
```

La sección principal es:

```python
COUNTS = {
    "customers": 500_000,
    "brands": 100,
    "products": 100_000,
    "branches": 50,
    "payment_methods": 5,
    "sales": 1_500_000,
    "sales_details": 5_820_000,
}
```

La distribución de productos por venta se configura mediante:

```python
SALES_DETAIL_DISTRIBUTION = {
    1: 75_000,
    2: 150_000,
    3: 225_000,
    4: 480_000,
    5: 570_000,
}
```

**Importante:** si se modifican las cantidades de ventas, también se debe actualizar la distribución y el total esperado de detalles.

El sistema cuenta con validaciones para detectar inconsistencias antes de comenzar la generación.

---

## 6. Requisitos previos

Antes de ejecutar los scripts, se debe comprobar lo siguiente:

1. Tener Python 3 instalado.
2. Abrir una terminal en la raíz del proyecto.
3. Verificar que exista la carpeta `scripts/generators/`.
4. Comprobar que estén presentes los archivos de configuración y generadores.
5. Tener suficiente espacio disponible en disco para almacenar los archivos resultantes.

No es necesario instalar dependencias adicionales para los generadores CSV actuales.

Se recomienda comprobar la instalación de Python mediante:

```powershell
python --version
```

---

## 7. Ejecución de los generadores

### 7.1. Ubicarse en la raíz del proyecto

Abrir PowerShell o una terminal y ubicarse en la carpeta principal:

```powershell
cd "C:\Users\Usuario\Documents\GitHub\TechStore-lakehouse"
```

Es importante ejecutar los comandos desde la raíz del proyecto para que las importaciones de Python funcionen correctamente.

### 7.2. Ejecutar todos los generadores

Para generar todos los archivos CSV, ejecutar:

```powershell
python -m scripts.generators.generate_all --lote 1
```

Este comando ejecuta automáticamente los generadores en el siguiente orden:

1. Clientes.
2. Marcas.
3. Productos.
4. Sucursales.
5. Métodos de pago.
6. Ventas.
7. Detalles de ventas.

Al finalizar, la terminal mostrará un resumen de los archivos generados y sus respectivas cantidades.

### 7.3. Ejecutar un generador individual

También es posible ejecutar cada generador de manera independiente.

Por ejemplo, para generar únicamente los clientes:

```powershell
python -m scripts.generators.generate_customers --lote 1
```

Para generar las marcas:

```powershell
python -m scripts.generators.generate_brands --lote 1
```

Para generar los productos:

```powershell
python -m scripts.generators.generate_products --lote 1
```

Para generar las sucursales:

```powershell
python -m scripts.generators.generate_branches --lote 1
```

Para generar los métodos de pago:

```powershell
python -m scripts.generators.generate_payment_methods --lote 1
```

Para generar las ventas:

```powershell
python -m scripts.generators.generate_sales --lote 1
```

Para generar los detalles de ventas:

```powershell
python -m scripts.generators.generate_sales_details --lote 1
```

**Consideración:** los generadores individuales no ejecutan automáticamente los demás. Por eso, antes de generar ventas y detalles, se recomienda haber generado las dimensiones correspondientes. Los generadores de ventas utilizan los rangos de identificadores definidos en la configuración.

---

## 8. Ubicación de los archivos generados

Todos los archivos CSV se almacenan automáticamente en:

```text
data/<tabla>/lote=1/<tabla>.csv
```

Después de una ejecución completa, la estructura será:

```text
data/
├── clientes/lote=1/clientes.csv
├── marcas/lote=1/marcas.csv
├── productos/lote=1/productos.csv
├── sucursales/lote=1/sucursales.csv
├── metodos_pago/lote=1/metodos_pago.csv
├── ventas/lote=1/ventas.csv
└── detalle_ventas/lote=1/detalle_ventas.csv
```

Los archivos utilizan:

- Formato CSV.
- Codificación UTF-8.
- Separador de columnas mediante coma.
- Encabezados con nombres de columnas.
- Fechas y valores monetarios en formatos consistentes.

---

## 9. Validaciones y consideraciones importantes

El sistema incorpora mecanismos para reducir inconsistencias durante la generación.

Entre ellos:

- Validación de la cantidad total de ventas.
- Validación de la cantidad esperada de detalles.
- Identificadores generados dentro de los rangos configurados.
- Productos sin repetición dentro de una misma venta.
- Cantidades positivas por detalle.
- Cálculo de subtotales, descuentos e impuestos.
- Reproducción consistente de las ventas y sus detalles mediante una semilla determinista.

### 9.1. Sobrescritura de archivos

Los generadores utilizan el modo de escritura que reemplaza el contenido anterior del archivo.

Por lo tanto, al ejecutar nuevamente un generador, el CSV correspondiente será sobrescrito.

Se recomienda conservar una copia de los archivos si se necesita mantener una generación anterior.

### 9.2. Tiempo de ejecución y almacenamiento

La generación de millones de registros puede tardar varios minutos o más, dependiendo de las características del equipo.

También se requiere suficiente espacio en disco, especialmente para:

- ventas.csv
- detalle_ventas.csv

Se recomienda no cerrar la terminal mientras se ejecuta el proceso.

---

## 10. Verificación posterior a la generación

Una vez finalizada la ejecución, se debe comprobar que los siete archivos CSV existan dentro de `data/<tabla>/lote=1/`.

También se debe verificar que la cantidad de registros coincida con la configuración.

El total esperado es:

```text
7.920.155 registros
```

Este total corresponde a la suma de los registros de los siete archivos CSV, incluyendo las tablas de dimensiones.

La verificación de los archivos es importante antes de continuar con su carga a Amazon S3.

---

## 11. Integración con AWS Data Lakehouse

Los datos generados constituyen las fuentes iniciales del proyecto TechStore.

Una vez validados, se utilizarán para alimentar la arquitectura de almacenamiento y procesamiento en AWS.

**El flujo general será:**

```text
Generadores Python
        |
        v
Archivos CSV
        |
        v
Amazon S3 - Bronze
        |
        v
AWS Glue
        |
        v
Amazon S3 - Silver
        |
        v
Amazon S3 - Gold
        |
        v
Amazon Athena
```

### Descripción del flujo

- **Generadores Python:** producen los datos sintéticos que representan las operaciones de TechStore.
- **Archivos CSV:** contienen los datos originales generados localmente.
- **Amazon S3 - Bronze:** recibe y conserva los datos originales, sin pasar por una capa Landing.
- **AWS Glue:** permite descubrir, catalogar, limpiar y transformar los datos mediante sus componentes de procesamiento.
- **Amazon S3 - Silver:** almacena los datos limpios, validados y estructurados.
- **Amazon S3 - Gold:** contiene los datos preparados para análisis y consultas.
- **Amazon Athena:** permite realizar consultas SQL sobre los datos almacenados en S3 y trabajar con tablas Apache Iceberg.

---

**Fin del manual.**

Proyecto académico: TechStore Data Lakehouse.
