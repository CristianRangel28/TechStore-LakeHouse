# MANUAL DE GENERACIÓN DE DATOS JSON - TECHSTORE

**Proyecto:** TechStore Data Lakehouse  
**Módulo:** Generación de datos de posventa  
**Versión:** 1.0  
**Fecha:** 7 de noviembre de 2026  
**Tecnología:** Python 3, JSON Lines (NDJSON)

---

# 1. Introducción

El módulo `json_generators` es responsable de generar los datos correspondientes a los procesos de posventa de TechStore.

Estos procesos representan las solicitudes que realizan los clientes después de adquirir un producto, específicamente:

- Garantías.
- Devoluciones.

Los datos generados simulan operaciones de posventa relacionadas con las ventas existentes en el sistema transaccional de TechStore.

El objetivo es construir un conjunto de datos suficientemente amplio y consistente para utilizarlo posteriormente en el Data Lakehouse, donde será procesado mediante Apache Spark y AWS Glue.

Los archivos generados se almacenan en formato JSON Lines (NDJSON), permitiendo procesar grandes cantidades de registros de manera progresiva.

# 2. Objetivos del módulo

El módulo tiene los siguientes objetivos:

1. Generar 500.000 registros de garantías.
2. Generar 494.495 registros de devoluciones.
3. Relacionar cada proceso de posventa con una venta existente.
4. Garantizar que una venta tenga como máximo un proceso de posventa.
5. Mantener la integridad entre ventas, clientes y productos.
6. Generar datos reproducibles mediante semillas aleatorias.
7. Validar la estructura y consistencia de los registros.
8. Producir archivos compatibles con los procesos posteriores de Apache Spark.

# 3. Estructura del módulo

La organización de archivos es la siguiente:

```text
TechStore-LakeHouse/
│
├── scripts/
│   │
│   ├── generators/
│   │   ├── config.py
│   │   ├── sales_data.py
│   │   └── ...
│   │
│   ├── json_generators/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── utils.py
│   │   ├── generate_warranties.py
│   │   ├── generate_returns.py
│   │   ├── generate_all.py
│   │   ├── validate_json.py
│   │   └── README.md
│   │
│   └── datasets/
│       ├── csv/
│       └── json/
│           ├── garantias.json
│           └── devoluciones.json
│
└── docs/
    └── MANUAL_GENERACION_DATOS_JSON.md
```

Los archivos de generación se encuentran separados de los archivos de datos.

Esta separación permite mantener organizado el código fuente y evitar que los archivos generados interfieran con el control de versiones del proyecto.

# 4. Configuración de los datos

## 4.1. Archivo config.py

**Ubicación:** `scripts/json_generators/config.py`

Este archivo centraliza la configuración del módulo de generación JSON.

Sus responsabilidades principales son:

- Definir la ubicación de los archivos generados.
- Establecer la cantidad de garantías y devoluciones.
- Definir las semillas aleatorias.
- Configurar los estados y motivos disponibles.
- Establecer las reglas temporales de los procesos de posventa.

### Cantidades de registros

La configuración definitiva es:

```python
POST_SALES_COUNTS = {
    "warranties": 500_000,
    "returns": 494_495,
}
```

Por lo tanto:

| Tipo de proceso | Registros |
|---|---:|
| Garantías | 500.000 |
| Devoluciones | 494.495 |
| Total | 994.495 |

### Semilla aleatoria

Se utiliza una semilla independiente para los procesos de posventa, derivada de la semilla general del proyecto.

Esto permite generar datos reproducibles, de manera que una misma configuración produzca los mismos resultados.

### Estados de las garantías

Las garantías pueden presentar los siguientes estados:

- `APROBADA`: solicitud aceptada.
- `RECHAZADA`: solicitud rechazada.
- `EN_REVISION`: solicitud pendiente de evaluación.

### Estados de las devoluciones

Las devoluciones utilizan cuatro estados:

- `APROBADA`: solicitud aceptada.
- `RECHAZADA`: solicitud rechazada.
- `PENDIENTE`: solicitud todavía sin resolución.
- `COMPLETADA`: proceso de devolución finalizado.

### Motivos de las garantías

Entre los motivos contemplados se encuentran:

- `FALLA_TECNICA`
- `DEFECTO_FABRICACION`
- `DANO_COMPONENTE`
- `SOBRECALENTAMIENTO`
- `FALLA_ELECTRICA`

### Motivos de las devoluciones

Se contemplan los siguientes motivos:

- `PRODUCTO_EQUIVOCADO`
- `PRODUCTO_DANADO`
- `INSATISFACCION`
- `PRODUCTO_INCOMPLETO`
- `ERROR_EN_PEDIDO`

### Reglas temporales

Las solicitudes se generan entre 1 y 30 días después de la fecha de venta.

Esto permite representar procesos de posventa que ocurren después de la adquisición de los productos.

# 5. Utilidades compartidas

## 5.1. Archivo utils.py

**Ubicación:** `scripts/json_generators/utils.py`

Este archivo contiene las funciones auxiliares utilizadas por los generadores.

Su responsabilidad principal es gestionar la asignación de ventas a los procesos de posventa y proporcionar herramientas para escribir los archivos JSON.

### Función build_post_sales_assignment()

Esta función determina cuáles ventas tendrán una garantía y cuáles tendrán una devolución.

Su funcionamiento consiste en:

1. Consultar la cantidad de ventas disponibles.
2. Calcular el total de procesos de posventa.
3. Seleccionar aleatoriamente las ventas necesarias.
4. Separar las ventas seleccionadas en dos grupos.
5. Devolver las ventas asignadas a garantías y devoluciones.

La selección utiliza `random.sample()`, garantizando que una venta no sea seleccionada dos veces.

Además, utiliza `@lru_cache(maxsize=1)` para almacenar el resultado de la asignación durante la ejecución.

Esto evita repetir el proceso de selección cuando diferentes generadores necesitan consultar la misma distribución.

### Regla de exclusividad

Una venta puede tener:

- Una garantía.
- Una devolución.
- Ningún proceso de posventa.

Una venta nunca puede tener simultáneamente una garantía y una devolución.

Esta regla permite mantener la consistencia del conjunto de datos.

### Función create_request_date()

Esta función permite generar la fecha de solicitud de posventa a partir de la fecha de venta original.

Se establece un intervalo aleatorio de entre 1 y 30 días posteriores a la compra.

### Función write_json_lines()

Esta función se encarga de escribir los registros generados en archivos JSON Lines.

Cada registro se almacena como un objeto JSON independiente en una línea.

Esto facilita la lectura progresiva de los datos y evita tener que cargar un documento JSON completo en memoria.

# 6. Generador de garantías

## 6.1. Archivo generate_warranties.py

**Ubicación:** `scripts/json_generators/generate_warranties.py`

Este archivo genera los registros correspondientes a las solicitudes de garantía.

Su responsabilidad es construir registros relacionados con ventas y productos existentes.

### Funcionamiento

El proceso consiste en:

1. Obtener las ventas asignadas a garantías.
2. Recuperar la información original de cada venta.
3. Seleccionar uno de los productos adquiridos.
4. Generar un identificador de posventa.
5. Crear una fecha de solicitud.
6. Asignar un motivo de garantía.
7. Determinar el estado de la solicitud.
8. Generar una observación descriptiva.
9. Establecer la fecha de resolución según el estado.
10. Escribir el registro en el archivo correspondiente.

### Estructura de un registro

Ejemplo ilustrativo:

```json
{
    "id_postventa": 1,
    "id_venta": 1081751,
    "id_cliente": 405870,
    "fecha_solicitud": "2026-06-16 22:44:18",
    "tipo": "GARANTIA",
    "producto": {
        "id_producto": 99489,
        "cantidad_comprada": 1,
        "numero_serie": "TS-099489-01081751"
    },
    "detalle": {
        "motivo": {
            "categoria": "DEFECTO_FABRICACION",
            "descripcion": "Se identifico un posible defecto de fabricacion."
        },
        "estado": {
            "nombre": "RECHAZADA",
            "observacion": "La solicitud no cumple las condiciones de garantia."
        }
    },
    "fecha_resolucion": "2026-06-16 22:44:18"
}
```

### Descripción de los campos

| Campo | Descripción |
|---|---|
| `id_postventa` | Identificador único del proceso de posventa. |
| `id_venta` | Venta relacionada con la garantía. |
| `id_cliente` | Cliente que realizó la compra. |
| `fecha_solicitud` | Fecha en la que se solicita la garantía. |
| `tipo` | Identifica el proceso como GARANTIA. |
| `producto` | Información del producto involucrado. |
| `producto.id_producto` | Identificador del producto adquirido. |
| `producto.cantidad_comprada` | Cantidad adquirida en la venta. |
| `producto.numero_serie` | Número de serie simulado del producto. |
| `detalle.motivo` | Información sobre el motivo de la solicitud. |
| `detalle.estado` | Estado y observación de la solicitud. |
| `fecha_resolucion` | Fecha de resolución del proceso, cuando corresponde. |

### Reglas de negocio

- Cada garantía debe estar relacionada con una venta existente.
- El cliente debe coincidir con el cliente de la venta.
- El producto debe pertenecer a los detalles de esa venta.
- La solicitud debe realizarse después de la compra.
- Una garantía `EN_REVISION` no tiene fecha de resolución.
- Las garantías `APROBADA` y `RECHAZADA` tienen fecha de resolución.

### Archivo generado

```text
scripts/datasets/json/garantias.json
```

Cantidad esperada: **500.000 registros**.

# 7. Generador de devoluciones

## 7.1. Archivo generate_returns.py

**Ubicación:** `scripts/json_generators/generate_returns.py`

Este archivo genera los registros correspondientes a las solicitudes de devolución de productos.

Cada devolución está relacionada con una venta existente y con uno de los productos incluidos en ella.

### Funcionamiento

El proceso consiste en:

1. Obtener las ventas asignadas a devoluciones.
2. Recuperar la información de cada venta.
3. Seleccionar un producto adquirido.
4. Generar el identificador de posventa.
5. Crear la fecha de solicitud.
6. Asignar un motivo de devolución.
7. Determinar el estado del proceso.
8. Calcular la cantidad devuelta.
9. Calcular el porcentaje y valor del reembolso.
10. Establecer la fecha de resolución.
11. Escribir el registro en el archivo JSON correspondiente.

### Estructura de un registro

Ejemplo ilustrativo:

```json
{
    "id_postventa": 101,
    "id_venta": 1105718,
    "id_cliente": 163216,
    "fecha_solicitud": "2025-09-08 15:11:37",
    "tipo": "DEVOLUCION",
    "producto": {
        "id_producto": 635,
        "cantidad_comprada": 2,
        "cantidad_devuelta": 0
    },
    "detalle": {
        "motivo": {
            "categoria": "INSATISFACCION",
            "descripcion": "El cliente no esta satisfecho con el producto."
        },
        "estado": {
            "nombre": "RECHAZADA",
            "observacion": "La solicitud de devolucion fue rechazada."
        }
    },
    "reembolso": {
        "porcentaje_aprobado": 0,
        "valor": "0.00",
        "moneda": "COP"
    },
    "fecha_resolucion": "2025-09-08 15:11:37"
}
```

### Descripción de los campos

| Campo | Descripción |
|---|---|
| `id_postventa` | Identificador único del proceso de posventa. |
| `id_venta` | Venta relacionada con la devolución. |
| `id_cliente` | Cliente que realizó la compra. |
| `fecha_solicitud` | Fecha en la que se solicita la devolución. |
| `tipo` | Identifica el proceso como DEVOLUCION. |
| `producto` | Información del producto involucrado. |
| `producto.id_producto` | Identificador del producto adquirido. |
| `producto.cantidad_comprada` | Cantidad original adquirida. |
| `producto.cantidad_devuelta` | Cantidad de unidades devueltas. |
| `detalle.motivo` | Motivo de la solicitud. |
| `detalle.estado` | Estado y observación del proceso. |
| `reembolso` | Información económica de la devolución. |
| `reembolso.porcentaje_aprobado` | Porcentaje aplicado al reembolso. |
| `reembolso.valor` | Valor monetario del reembolso. |
| `reembolso.moneda` | Moneda utilizada, COP. |
| `fecha_resolucion` | Fecha de resolución cuando corresponde. |

### Reglas de negocio

Las devoluciones respetan las siguientes condiciones:

**PENDIENTE**
- La solicitud todavía no ha sido resuelta.
- No existe fecha de resolución.
- La cantidad devuelta es cero.
- El reembolso es cero.

**RECHAZADA**
- La solicitud fue rechazada.
- Existe fecha de resolución.
- La cantidad devuelta es cero.
- El reembolso es cero.

**APROBADA**
- La solicitud fue aceptada.
- Todavía no tiene fecha de resolución final.
- Existe una cantidad devuelta mayor que cero.
- Se calcula un reembolso.

**COMPLETADA**
- El proceso de devolución ha finalizado.
- Existe fecha de resolución.
- Existe una cantidad devuelta mayor que cero.
- Se registra el reembolso correspondiente.

### Cálculo del reembolso

Los porcentajes disponibles son:

- 50 %.
- 75 %.
- 100 %.

El valor se calcula utilizando el subtotal del producto seleccionado:

```text
Valor del reembolso =
Subtotal del producto × Porcentaje aprobado / 100
```

Los cálculos monetarios utilizan `Decimal` para evitar imprecisiones propias de los números de punto flotante.

### Archivo generado

```text
scripts/datasets/json/devoluciones.json
```

Cantidad esperada: **494.495 registros**.

# 8. Ejecución general de los generadores

## 8.1. Archivo generate_all.py

**Ubicación:** `scripts/json_generators/generate_all.py`

Este archivo funciona como punto de entrada para ejecutar todos los generadores JSON.

Su responsabilidad es coordinar el proceso de generación.

### Funcionamiento

1. Obtiene la asignación de ventas.
2. Comprueba las cantidades asignadas.
3. Ejecuta el generador de garantías.
4. Ejecuta el generador de devoluciones.
5. Finaliza el proceso.

La asignación se reutiliza mediante la caché implementada en `utils.py`.

### Comando de ejecución

Desde la raíz del proyecto:

```powershell
python -m scripts.json_generators.generate_all
```

### Resultado esperado

```text
=================================================================
GENERACIÓN DE DATOS JSON - TECHSTORE
=================================================================

Asignación de ventas validada.
Ventas asignadas a garantías: 500,000
Ventas asignadas a devoluciones: 494,495

Generando garantías...

Generando devoluciones...

=================================================================
GENERACIÓN JSON COMPLETADA
=================================================================
```

La ejecución genera los dos archivos JSON en la carpeta de datos correspondiente.

# 9. Validación de los datos JSON

## 9.1. Archivo validate_json.py

**Ubicación:** `scripts/json_generators/validate_json.py`

Este archivo permite comprobar la calidad y consistencia de los datos generados.

Su objetivo es detectar errores estructurales y problemas en las relaciones entre los registros de posventa y las ventas originales.

### Lectura progresiva

El validador utiliza una lectura progresiva de los archivos JSON Lines.

En lugar de cargar todos los registros en listas, procesa los objetos individualmente.

Esto permite trabajar con archivos de gran tamaño sin necesitar cargar aproximadamente un millón de objetos JSON simultáneamente en memoria.

### Validaciones implementadas

**1. Estructura JSON**

Comprueba que los registros tengan los campos obligatorios y que los objetos anidados tengan la estructura esperada.

**2. Cantidad de registros**

Comprueba que existan exactamente:

- 500.000 garantías.
- 494.495 devoluciones.

**3. Identificadores y relaciones**

Comprueba que los identificadores sean únicos y que las relaciones entre ventas, clientes y productos sean correctas.

**4. Integridad de las ventas**

Reconstruye los datos de las ventas originales utilizando la función `build_sale_and_details()` del módulo de generadores CSV.

De esta manera puede comprobar las relaciones sin depender de que los archivos CSV estén físicamente presentes.

**5. Exclusividad de procesos**

Comprueba que ninguna venta tenga simultáneamente una garantía y una devolución.

**6. Estados y fechas**

Comprueba que los estados sean válidos y que las fechas respeten las reglas establecidas.

**7. Cantidades y reembolsos**

Comprueba que las cantidades devueltas no superen las cantidades adquiridas y que los reembolsos coincidan con los porcentajes establecidos.

### Comando de ejecución

```powershell
python -m scripts.json_generators.validate_json
```

### Resultado de la validación definitiva

La ejecución realizada el 7 de noviembre de 2026 produjo el siguiente resultado:

```text
=================================================================
VALIDACIÓN DE DATOS JSON - TECHSTORE
=================================================================

Validando garantías...
Registros procesados: 500,000

Validando devoluciones...
Registros procesados: 494,495

=================================================================
RESULTADOS
=================================================================
[OK] Cantidad de registros
[OK] Identificadores y relaciones
[OK] Exclusividad de procesos

=================================================================
RESULTADO: TODAS LAS VALIDACIONES APROBADAS
=================================================================
```

Las validaciones finalizaron correctamente para los 994.495 registros.

# 10. Formato de almacenamiento: JSON Lines (NDJSON)

Los archivos generados utilizan el formato JSON Lines.

A diferencia de un documento JSON convencional, donde todos los registros pueden estar dentro de un único arreglo, JSON Lines almacena cada objeto JSON en una línea independiente.

Ejemplo:

```json
{"id_postventa":1,"id_venta":1081751,"tipo":"GARANTIA"}
{"id_postventa":2,"id_venta":1081752,"tipo":"GARANTIA"}
{"id_postventa":3,"id_venta":1081753,"tipo":"GARANTIA"}
```

### Ventajas para el proyecto

- Permite leer los registros progresivamente.
- Facilita el procesamiento distribuido.
- Es compatible con Apache Spark.
- Permite procesar grandes volúmenes de datos.
- No requiere cargar un arreglo JSON completo en memoria.

Este formato resulta apropiado para los procesos de ingestión y transformación del Data Lakehouse.

# 11. Volumen y almacenamiento generado

La generación definitiva produjo los siguientes resultados:

| Archivo | Registros | Tamaño aproximado |
|---|---:|---:|
| garantias.json | 500.000 | 242,85 MB |
| devoluciones.json | 494.495 | 275,14 MB |
| **Total** | **994.495** | **517,99 MB** |

Los tamaños corresponden a los archivos generados localmente.

# 12. Relación con el Data Lakehouse

Los archivos JSON constituyen una de las fuentes de datos del proyecto TechStore.

Su flujo de procesamiento será:

```text
Generadores Python
        |
        v
Archivos JSON (NDJSON)
        |
        v
Amazon S3 - Bronze
        |
        v
AWS Glue + Apache Spark
        |
        v
Amazon S3 - Silver (Apache Iceberg)
        |
        v
Amazon S3 - Gold (Apache Iceberg)
        |
        v
Amazon Athena
```

### Bronze

Se almacenan los archivos originales de garantías y devoluciones, conservando su estructura JSON.

### Silver

Se procesan los datos mediante Apache Spark para:

- Aplanar estructuras anidadas.
- Estandarizar nombres de columnas.
- Convertir tipos de datos.
- Validar relaciones.
- Limpiar registros.
- Construir tablas analíticas estructuradas.

Las garantías y devoluciones podrán convertirse en tablas independientes dentro de la base de datos Silver.

### Gold

Se podrán construir indicadores y agregaciones relacionados con los procesos de posventa, por ejemplo:

- Cantidad de garantías por estado.
- Cantidad de devoluciones por motivo.
- Porcentaje de solicitudes aprobadas y rechazadas.
- Valor total de reembolsos.
- Devoluciones por producto.
- Comportamiento de posventa por período.

Estas tablas se almacenarán utilizando Apache Iceberg.

# 13. Consideraciones importantes

1. Los datos generados son sintéticos y tienen fines académicos.
2. Los identificadores de ventas y clientes se relacionan con el generador transaccional del proyecto.
3. Si se modifican las semillas, configuraciones o reglas del generador original de ventas, se debe revisar la consistencia de los datos de posventa.
4. Los archivos JSON se generan localmente y posteriormente se utilizarán como fuentes para Amazon S3.
5. Los archivos de datos generados no deben subirse al repositorio Git.
6. Los procesos de generación y validación son independientes de la infraestructura AWS.
7. Las modificaciones analíticas de los datos se realizarán posteriormente mediante Apache Spark y Apache Iceberg.

# 14. Resumen de comandos

| Actividad | Comando |
|---|---|
| Generar garantías y devoluciones | `python -m scripts.json_generators.generate_all` |
| Generar únicamente garantías | `python -m scripts.json_generators.generate_warranties` |
| Generar únicamente devoluciones | `python -m scripts.json_generators.generate_returns` |
| Validar los archivos JSON | `python -m scripts.json_generators.validate_json` |

**Nota:** La generación individual utiliza la misma asignación determinista de ventas. Para producir el conjunto completo de forma coordinada, se recomienda utilizar `generate_all.py`.

---

**Fin del documento.**

**Proyecto TechStore Data Lakehouse**  
Generación de datos sintéticos para procesos de posventa.