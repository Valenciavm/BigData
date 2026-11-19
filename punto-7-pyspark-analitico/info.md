# Energía Colombia — Big Data Analytics Pipeline

## 1. Descripción del proyecto

Este proyecto desarrolla una arquitectura de procesamiento y análisis de datos para el sector energético e hidrocarburos de Colombia, utilizando servicios de Amazon Web Services (AWS).

El proyecto integra aproximadamente 13,9 millones de registros provenientes de diferentes fuentes públicas y bases de datos relacionadas con la distribución de combustibles, infraestructura energética, ventas de Gas Natural Comprimido Vehicular (GNCV), biocombustibles y generación eléctrica.

El objetivo es construir un Data Lake que permita integrar, transformar y analizar estas fuentes heterogéneas, generando resultados analíticos que respondan cinco preguntas de negocio.

La solución utiliza AWS S3 como almacenamiento central, AWS Glue y PySpark para el procesamiento distribuido, AWS Glue Data Catalog para la gestión de metadatos, Amazon Athena para consultas SQL y Amazon QuickSight para la visualización de resultados.

### Objetivos analíticos

El proyecto se estructura alrededor de cinco preguntas:

1. **Concentración del mercado mayorista:** identificar la distribución de los despachos entre proveedores, calcular participaciones de mercado y analizar indicadores de concentración.
2. **Impacto de los subsidios en zonas de frontera:** estudiar los volúmenes despachados en municipios fronterizos y no fronterizos, identificando comportamientos atípicos.
3. **Infraestructura y ventas de GNCV:** analizar la relación entre la presencia de agentes e infraestructura GNCV y las ventas municipales.
4. **Biocombustibles y trazabilidad:** comparar los despachos de productores de B100 y etanol con los despachos mayoristas correspondientes.
5. **Tendencias energéticas:** estudiar la evolución conjunta de la generación eléctrica, los despachos de combustibles y las ventas de GNCV.

---

## 2. Arquitectura general

La arquitectura sigue un modelo de tres capas:

- **Raw:** almacenamiento de los datos originales.
- **Trusted:** datos limpios, estandarizados y estructurados.
- **Analytics:** resultados agregados y transformados para responder las preguntas de negocio.

### Flujo de procesamiento

```text
                    DATA SOURCES
                          |
          +---------------+---------------+
          |               |               |
       Datos.gov.co     Kaggle          MariaDB
          |               |               |
          +---------------+---------------+
                          |
                          v
                 AMAZON S3 - RAW
                          |
                          v
                  AWS GLUE / PYSPARK
                 Data cleaning and ETL
                          |
                          v
                AMAZON S3 - TRUSTED
                          |
                   Glue Data Catalog
                          |
                          v
                 AWS GLUE / PYSPARK
                   Analytics Jobs
                          |
                          v
               AMAZON S3 - ANALYTICS
                          |
                   Glue Data Catalog
                          |
                          v
                    ATHENA
                SQL validation/querying
                          |
                          v
                   QUICKSIGHT
               Datasets and Dashboards
```

### Servicios utilizados

| Servicio | Responsabilidad |
|---|---|
| Amazon S3 | Almacenamiento de las capas Raw, Trusted y Analytics. |
| AWS Glue | Procesamiento ETL distribuido mediante PySpark. |
| Glue Data Catalog | Catálogo centralizado de tablas y metadatos. |
| Amazon Athena | Consultas SQL y validación de resultados. |
| Amazon QuickSight | Construcción de dashboards e indicadores. |
| Amazon RDS / MariaDB | Fuente de datos relacional para los conjuntos correspondientes. |
| Amazon EC2 | Infraestructura utilizada en actividades de acceso y procesamiento de fuentes. |

### Almacenamiento central

Bucket principal:

```text
s3://big-data-lake-energiacol/
```

Estructura general:

```text
big-data-lake-energiacol/
│
├── raw/
│
├── trusted/
│
├── analytics/
│
├── scripts/
│   └── punto-6-analisis/
│       └── jobs/
│
└── temp/
```

---

## 3. Fuentes de datos

El proyecto integra siete conjuntos de datos principales.

| Dataset | Fuente | Registros aproximados |
|---|---|---:|
| Distribuidores Mayoristas - Despacho | datos.gov.co | 7.190.000 |
| Estaciones de Servicio Automotriz y Fluvial | datos.gov.co | 6.500.000 |
| IEA Monthly Electricity Statistics | Kaggle | 180.000 |
| Ventas de GNCV | datos.gov.co | 135.000 |
| Beneficio Zona de Frontera | datos.gov.co | 90.400 |
| Productores B100 y Etanol | datos.gov.co / MariaDB | 47.000 |
| Agentes GNCV | datos.gov.co / MariaDB | 1.761 |

**Volumen aproximado:** 14,14 millones de registros, sujeto a las variaciones de las extracciones y los procesos de limpieza.

---

## 4. Capa Trusted

La capa Trusted contiene los datos transformados y estandarizados para facilitar su integración y análisis.

### Estándares de almacenamiento

- Formato: Apache Parquet.
- Compresión: Snappy.
- Convención de nombres: lowercase_snake_case.
- Fechas: DATE.
- Variables numéricas: DOUBLE o BIGINT según corresponda.
- Base de datos del catálogo: `energia_col_trusted`.

### Tablas y particiones

| Dataset | Tabla Glue | Ruta S3 | Partición |
|---|---|---|---|
| Distribuidores Mayoristas | `distribuidores_mayoristas` | `trusted/distribuidores_mayoristas/` | `anio_despacho` |
| Estaciones de Servicio | `estaciones_servicio` | `trusted/estaciones_servicio/` | `anio_despacho` |
| Beneficios de Frontera | `beneficios_frontera` | `trusted/beneficios_frontera/` | `anio_beneficio` |
| Ventas GNCV | `ventas_gncv` | `trusted/ventas_gncv/` | `anio_venta` |
| IEA Electricity | `iea_electricity` | `trusted/iea_electricity/` | `year` |
| Productores B100 y Etanol | `productores_b100e` | `trusted/productores_b100e/` | `anio_despacho` |
| Agentes GNCV | `agentes_gncv` | `trusted/agentes_gncv/` | Ninguna |

### Transformaciones relevantes

Los procesos de Trusted contemplan:

- Estandarización de nombres de columnas.
- Normalización de formatos de fecha.
- Conversión de tipos de datos.
- Limpieza de valores nulos y cadenas vacías.
- Normalización de categorías y etiquetas.
- Preparación de códigos municipales DANE para integraciones.
- Conversión de valores numéricos almacenados como texto.

#### Particularidades numéricas

Se identificaron dos formatos diferentes de representación numérica:

**Ventas GNCV:**

```text
1,919.61 → 1919.61
```

**Despachos de productores:**

```text
11.128,98 → 11128.98
```

Estas diferencias deben conservarse durante las transformaciones para evitar errores de escala en los indicadores analíticos.

---

## 5. Capa Analytics

La capa Analytics contiene cinco trabajos independientes desarrollados con AWS Glue y PySpark.

Cada trabajo responde una pregunta de negocio y genera resultados agregados en formato Parquet.

### Configuración general

| Parámetro | Configuración |
|---|---|
| Servicio | AWS Glue |
| Motor | PySpark |
| Versión | Glue 5.0 |
| Tipo de worker | G.1X |
| Workers iniciales | 5 |
| Formato de salida | Parquet |
| Compresión | Snappy |
| Bucket de salida | `big-data-lake-energiacol` |
| Directorio de scripts | `scripts/punto-6-analisis/jobs/` |
| Directorio temporal | `temp/` |
| Directorio Analytics | `analytics/` |

La configuración de recursos constituye una configuración inicial y puede ajustarse según las métricas de ejecución y el volumen procesado.

### 5.1. Q1 — Concentración del mercado mayorista

**Script:** `q1_concentracion_mercado.py`

**Fuentes:**
- `distribuidores_mayoristas`
- `estaciones_servicio`

**Objetivo:**

Analizar la distribución de los despachos mayoristas, calcular participaciones relativas y evaluar la concentración del mercado por producto y periodo.

**Procesamiento:**

1. Normalización de variables temporales y categóricas.
2. Agregación de volúmenes despachados por proveedor, producto y periodo.
3. Cálculo de participaciones de mercado.
4. Cálculo del índice Herfindahl-Hirschman (HHI).
5. Identificación de los principales proveedores.
6. Segmentación por tipo de comprador.
7. Análisis geográfico de proveedores y red de estaciones.

**Resultados generados:**

```text
analytics/q1_concentracion/
│
├── market_shares/
├── hhi/
├── top_concentration/
├── annual_market_shares/
├── annual_hhi/
├── buyer_segmentation/
├── station_network/
├── wholesale_geography/
└── concentration_summary/
```

**Indicadores principales:**

- Participación de mercado.
- Índice HHI.
- Clasificación orientativa de concentración.
- Participación acumulada de los cinco y diez principales grupos.
- Distribución de despachos por producto.
- Segmentación de compradores.

**Limitación metodológica:**

La estructura Trusted disponible no contiene un identificador empresarial único como NIT para todos los proveedores. Por esta razón, `tipo_proveedor` se utiliza como agrupación provisional para calcular participaciones y HHI.

Por tanto, el indicador obtenido mide concentración entre categorías de proveedores y no concentración empresarial efectiva.

Asimismo, los registros de estaciones no necesariamente representan establecimientos físicos únicos si no se dispone de un identificador individual.

### 5.2. Q2 — Subsidios y zonas de frontera

**Script:** `q2_subsidios_frontera.py`

**Fuentes:**
- `estaciones_servicio`
- `beneficios_frontera`

**Objetivo:**

Examinar los despachos de combustible en municipios de frontera y compararlos con los municipios no fronterizos, identificando diferencias y observaciones atípicas.

**Procesamiento:**

1. Normalización de indicadores de zona de frontera.
2. Agregación de despachos por municipio, producto y periodo.
3. Comparación entre municipios fronterizos y no fronterizos.
4. Cálculo de promedios y diferencias.
5. Identificación de anomalías mediante puntuaciones Z.
6. Agregación de beneficios por municipio, periodo y tipo.
7. Integración de beneficios y despachos municipales.

**Resultados generados:**

```text
analytics/q2_frontera/
│
├── municipal_dispatches/
├── border_vs_non_border/
├── border_anomalies/
├── benefits_monthly/
├── benefits_by_municipality/
└── benefits_and_dispatches/
```

**Indicadores principales:**

- Volumen despachado en zonas fronterizas.
- Comparación con zonas no fronterizas.
- Promedio de despachos por municipio.
- Municipios con volúmenes atípicos.
- Evolución temporal de beneficios.
- Relación entre beneficios y despachos.

**Limitación metodológica:**

Las diferencias de volumen no permiten atribuir directamente un efecto causal a los subsidios.

Los volúmenes deben interpretarse considerando diferencias de población, demanda, número de estaciones e infraestructura municipal.

Las anomalías estadísticas tampoco constituyen evidencia de fraude, contrabando o desvío de combustible.

### 5.3. Q3 — Infraestructura y ventas de GNCV

**Script:** `q3_infraestructura_gncv.py`

**Fuentes:**
- `agentes_gncv`
- `ventas_gncv`

**Objetivo:**

Analizar la distribución territorial de los agentes GNCV y su relación con los indicadores municipales de ventas y suministro.

**Procesamiento:**

1. Agregación de agentes por código DANE y municipio.
2. Clasificación del estado de los agentes.
3. Agregación de ventas mensuales por municipio y combustible.
4. Integración de infraestructura y ventas.
5. Cálculo de indicadores por agente.
6. Agregación anual.
7. Estimación de correlaciones entre infraestructura y actividad comercial.

**Resultados generados:**

```text
analytics/q3_gncv/
│
├── municipal_infrastructure/
├── infrastructure_by_type/
├── municipal_sales_monthly/
├── municipal_indicators/
├── municipal_annual/
├── correlations/
├── infrastructure_coverage/
└── agent_status/
```

**Indicadores principales:**

- Número de agentes por municipio.
- Distribución de agentes por tipo y estado.
- Volumen suministrado.
- Número de ventas.
- Vehículos atendidos.
- Ventas y suministro por agente.
- Correlaciones entre infraestructura y actividad comercial.

**Limitación metodológica:**

El registro de agentes puede representar una fotografía del estado actual y no necesariamente la infraestructura histórica de cada municipio.

Por ello, su comparación con ventas históricas no demuestra que la infraestructura observada estuviera presente durante todo el periodo analizado.

También debe distinguirse entre agentes registrados y estaciones activas reportadas en los datos de ventas.

Las correlaciones identifican asociaciones, no relaciones causales.

### 5.4. Q4 — Biocombustibles y despachos

**Script:** `q4_biocombustibles.py`

**Fuentes:**
- `productores_b100e`
- `distribuidores_mayoristas`

**Objetivo:**

Comparar los despachos de productores de biodiésel B100 y etanol con los despachos mayoristas, estudiando su distribución temporal y geográfica.

**Procesamiento:**

1. Clasificación de productos B100, biodiésel y etanol.
2. Normalización de fechas, departamentos y volúmenes.
3. Agregación mensual de despachos de productores.
4. Agregación mensual de despachos mayoristas.
5. Comparación por producto y periodo.
6. Comparación por departamento de destino.
7. Identificación del origen geográfico de productores.
8. Cálculo de diferencias y relaciones entre etapas de distribución.

**Resultados generados:**

```text
analytics/q4_biocombustibles/
│
├── national_monthly_comparison/
├── department_comparison/
├── producer_monthly/
├── wholesale_monthly/
├── producer_origin/
├── geographic_contribution/
├── product_summary/
└── product_classification_audit/
```

**Indicadores principales:**

- Despachos mensuales de productores.
- Despachos mayoristas por producto.
- Comparaciones entre etapas de distribución.
- Distribución departamental.
- Origen geográfico de los productores.
- Clasificación de productos.

**Limitación metodológica:**

Los despachos de productores y los despachos mayoristas representan etapas diferentes de la cadena de suministro.

Sus diferencias no deben interpretarse automáticamente como pérdidas, desvíos o incumplimientos.

Además, sin identificadores de lotes o transacciones que permitan seguir físicamente un mismo producto entre etapas, el análisis constituye una comparación agregada y no una trazabilidad física completa.

La evaluación de cumplimiento de obligaciones de mezcla requiere tasas regulatorias y denominadores compatibles para cada combustible.

B100 y etanol deben analizarse como corrientes de mezcla independientes.

### 5.5. Q5 — Tendencias energéticas

**Script:** `q5_tendencias_energeticas.py`

**Fuentes:**
- `iea_electricity`
- `distribuidores_mayoristas`
- `ventas_gncv`

**Objetivo:**

Estudiar la evolución conjunta de la generación eléctrica, los despachos de combustibles y las ventas de GNCV en Colombia.

**Procesamiento:**

1. Filtrado de registros correspondientes a Colombia.
2. Normalización de variables temporales.
3. Agregación mensual de generación eléctrica.
4. Agregación mensual de despachos mayoristas.
5. Agregación mensual de ventas GNCV.
6. Integración de indicadores por año y mes.
7. Cálculo de variaciones temporales.
8. Análisis de estacionalidad.
9. Identificación de periodos de máximos y mínimos.
10. Estimación de correlaciones.

**Resultados generados:**

```text
analytics/q5_tendencias_energeticas/
│
├── electricity_monthly/
├── dispatches_monthly/
├── dispatches_by_product/
├── gncv_sales_monthly/
├── macro_energy_monthly/
├── macro_by_electricity_indicator/
├── seasonality_fuels/
├── seasonality_electricity/
├── correlations/
└── product_comparison/
```

**Indicadores principales:**

- Generación eléctrica mensual.
- Despachos mayoristas mensuales.
- Ventas y suministro de GNCV.
- Variaciones interanuales.
- Estacionalidad de combustibles.
- Estacionalidad de generación eléctrica.
- Correlaciones entre indicadores energéticos.

**Limitación metodológica:**

La generación eléctrica y los despachos de combustibles no necesariamente representan magnitudes directamente comparables.

Se deben revisar las categorías de productos de la IEA antes de agregar indicadores de generación, evitando sumar categorías superpuestas.

Las correlaciones entre niveles temporales pueden ser espurias cuando existen tendencias comunes. Por ello, las variaciones y tasas de crecimiento constituyen una alternativa importante para evaluar relaciones temporales.

---

## 6. Integración y reutilización de código

Los cinco trabajos analíticos fueron desarrollados inicialmente como scripts independientes.

Esta decisión permite ejecutar y depurar cada pregunta de negocio sin establecer dependencias entre los trabajos.

Sin embargo, se identificaron funciones comunes que pueden centralizarse para reducir duplicación y facilitar el mantenimiento.

### Módulo compartido `common`

Se está desarrollando una librería compartida con utilidades para:

| Función | Responsabilidad |
|---|---|
| `get_logger()` | Configuración estandarizada de logging. |
| `validate_columns()` | Validación de columnas obligatorias. |
| `load_table()` | Lectura de tablas del Glue Data Catalog. |
| `normalize_text()` | Normalización de variables categóricas. |
| `parse_numeric_column()` | Conversión de valores numéricos con diferentes separadores. |
| `add_date_dimensions()` | Extracción de dimensiones temporales. |
| `write_parquet()` | Escritura estandarizada en Parquet/Snappy. |
| `log_dataframe_info()` | Diagnóstico de esquemas y DataFrames. |
| `log_null_counts()` | Inspección de valores nulos. |

Estructura prevista:

```text
common/
└── __init__.py

jobs/
├── validation.py
├── q1_concentracion_mercado.py
├── q2_subsidios_frontera.py
├── q3_infraestructura_gncv.py
├── q4_biocombustibles.py
└── q5_tendencias_energeticas.py
```

La librería se distribuirá a los trabajos de Glue mediante un archivo ZIP almacenado en S3.

**Estado:** el módulo base ha sido elaborado, pero la integración y refactorización de Q1–Q5 todavía están pendientes. Los cinco scripts actuales no dependen de esta librería.

---

## 7. Validación y consultas

Los resultados de los trabajos se almacenan en S3 y se registran en Glue Data Catalog para su consulta mediante Amazon Athena.

La validación se realiza mediante consultas SQL sobre las tablas analíticas.

Ejemplo de inspección de tablas:

```sql
SHOW TABLES IN energia_col_analytics;
```

Ejemplo de consulta de participaciones:

```sql
SELECT *
FROM energia_col_analytics.market_shares
LIMIT 10;
```

Ejemplo de inspección de concentración:

```sql
SELECT
    anio,
    mes,
    producto,
    hhi,
    nivel_concentracion,
    numero_categorias_proveedor
FROM energia_col_analytics.hhi
ORDER BY anio DESC, mes DESC
LIMIT 20;
```

Los nombres concretos de las tablas deben corresponder a los nombres registrados en el catálogo.

La validación debe contemplar:

- Existencia de las tablas.
- Esquemas y tipos de datos.
- Valores nulos.
- Registros duplicados.
- Consistencia de agregaciones.
- Coherencia de unidades.
- Integridad de las particiones.
- Correspondencia entre las salidas de Glue y las consultas de Athena.

---

## 8. Visualización con Amazon QuickSight

La siguiente etapa consiste en conectar los resultados analíticos con Amazon QuickSight.

La conexión se realizará mediante Athena, utilizando las tablas registradas en Glue Data Catalog.

Se propone una estructura de seis páginas de análisis:

### Dashboard ejecutivo

Presentará una visión general del sector energético a partir de indicadores como:

- Volumen de despachos mayoristas.
- Índice de concentración del mercado.
- Ventas y suministro de GNCV.
- Número de agentes GNCV.
- Evolución mensual de despachos.
- Distribución geográfica.
- Tendencias energéticas.

### Dashboard Q1 — Concentración del mercado

- Participaciones por proveedor o categoría.
- Evolución del HHI.
- Ranking de principales proveedores.
- Concentración por producto.
- Segmentación de compradores.

### Dashboard Q2 — Zonas de frontera

- Comparación entre municipios fronterizos y no fronterizos.
- Volúmenes por municipio.
- Identificación de observaciones atípicas.
- Evolución de beneficios.
- Distribución geográfica.

### Dashboard Q3 — Infraestructura GNCV

- Agentes por municipio.
- Distribución por tipo y estado.
- Ventas y suministro.
- Indicadores por agente.
- Correlación entre infraestructura y actividad comercial.

### Dashboard Q4 — Biocombustibles

- Despachos de B100 y etanol.
- Comparación entre productores y mayoristas.
- Evolución mensual.
- Distribución por departamento.
- Origen geográfico de productores.

### Dashboard Q5 — Tendencias energéticas

- Generación eléctrica.
- Despachos mayoristas.
- Ventas GNCV.
- Estacionalidad.
- Comparaciones temporales.
- Correlaciones entre indicadores.

Se recomienda utilizar SPICE para mejorar el rendimiento de las visualizaciones interactivas, actualizando los datasets cuando cambien los resultados de Analytics.

---

## 9. Estado actual del proyecto

| Componente | Estado |
|---|---|
| Integración de fuentes | Implementada |
| Almacenamiento Raw en S3 | Implementado |
| Procesamiento Trusted | Implementado |
| Tablas Trusted en Glue Catalog | Registradas |
| Consultas Trusted en Athena | Validadas |
| Desarrollo de Q1–Q5 | Implementado |
| Ejecución inicial de Q1–Q5 | Ejecutada |
| Corrección de incidencias de datos | En proceso |
| Resultados Analytics en S3 | Generados, sujetos a correcciones |
| Tablas Analytics en Glue Catalog | Registradas |
| Consultas Analytics en Athena | Ejecutadas |
| Módulo compartido `common` | Elaborado inicialmente |
| Integración de `common` en Q1–Q5 | Pendiente |
| Refactorización y pruebas de regresión | Pendientes |
| Dashboards en QuickSight | Pendientes |

La ejecución inicial de los cinco trabajos permitió identificar algunas incidencias relacionadas con los datos de origen y sus formatos. La corrección de estas incidencias se está realizando de manera paralela, sin necesidad de detener el desarrollo de la capa de visualización.

---

## 10. Consideraciones metodológicas generales

Los resultados deben interpretarse teniendo en cuenta las características y limitaciones de las fuentes.

1. **Asociación no implica causalidad.** Las correlaciones entre variables energéticas no demuestran que una variable cause cambios en otra.
2. **Capacidad no equivale a producción.** Los registros de infraestructura o capacidad instalada no representan necesariamente producción efectiva ni disponibilidad real.
3. **Despachos no equivalen a consumo.** Los volúmenes despachados representan movimientos dentro de la cadena de distribución y no necesariamente consumo final.
4. **Diferencias entre etapas no equivalen a pérdidas.** Los despachos de productores y mayoristas deben interpretarse como etapas distintas.
5. **Las anomalías no prueban irregularidades.** Los métodos estadísticos identifican observaciones atípicas, no conductas ilegales.
6. **La granularidad debe ser compatible.** Las integraciones temporales y geográficas deben realizarse con niveles de agregación coherentes.
7. **Los códigos municipales deben preservarse.** Los códigos DANE requieren especial atención para evitar pérdida de ceros iniciales o incompatibilidades de formato.
8. **Los indicadores deben conservar sus unidades.** No deben combinarse volúmenes, cantidades de ventas, capacidad y generación eléctrica sin una transformación metodológicamente justificada.

---

## 11. Resultado esperado

Al finalizar la implementación, el proyecto contará con una arquitectura de Data Lake capaz de integrar fuentes heterogéneas del sector energético colombiano, procesarlas de manera distribuida y producir indicadores analíticos para apoyar la toma de decisiones.

La solución permitirá:

- Centralizar información energética de distintas fuentes.
- Estandarizar y transformar grandes volúmenes de datos.
- Ejecutar análisis independientes y reproducibles.
- Consultar resultados mediante SQL.
- Explorar indicadores mediante dashboards interactivos.
- Facilitar futuras ampliaciones del Data Lake y de los análisis.

**Stack tecnológico:** Amazon S3 · AWS Glue · PySpark · Glue Data Catalog · Amazon Athena · Amazon QuickSight · MariaDB.

**Proyecto:** Energía Colombia — Big Data Analytics.
