**SI3007 Sistemas Distribuidos - ST0263 Tópicos Especiales en Telemática, 2026-2**

# **Proyecto 2 de BIG DATA**

## I. Integrantes del proyecto

* Martin Valencia, email-eafit
* Samuel Rivero, email-eafit
* Sergio Deluquez, sdeluque1@eafit.edu.co

**Repositorio GitHub del proyecto 2:** `[https://github.com/](https://github.com/)...`

Cada grupo de trabajo cargará este documento y lo compartirá con el profesor:

* OneDrive de Office365 con la cuenta de EAFIT, se compartirá a: `aeospinas@eafit.edu.co`

Deberán enviar un correo inicial por Eafit Interactiva, especificando los integrantes y que ya tienen compartido el documento con el profesor, la URL del documento y en cuál nube.

---

## II. Requerimientos

1. **Crear un caso de estudio** donde procese datos almacenados en fuentes de datos Bases de Datos, Archivos, y URL. Deberá conseguir un conjunto de datos, típicamente archivos en datasets públicos, los cuales pueda almacenar una parte en bases de datos como fuente. Además, en este caso de estudio, deberá plantear diferentes preguntas / hipótesis de negocio que contestará con estos datos (ej: si son datos climáticos, qué ciudad fue la más caliente en abril de 2025 en Colombia, u ordene los departamentos de Colombia por temperatura promedio durante un periodo de tiempo). Esto se irá refinando conforme va desarrollando el proyecto:
* Pregunta 1: ...
* Pregunta 2: ...
* Pregunta N: ...
Plantee al menos 5 preguntas de negocio sobre esos datos.




2. **Fuentes de datos:** Deberá simular datos en RDS MariaDB, archivos en una instancia de EC2 y URLs en Internet.


3. **Ingesta de datos** hacia el Datalake (AWS S3) en las zonas correctas. Deberá ser un proceso automático.


4. **Realizar procesamiento** orientado a la Preparación de datos (desde zona raw a trusted, o en medallion: bronze a silver), herramientas a utilizar: AWS Glue y Apache Spark.


5. **Realizar catalogación** de tablas SQL con herramientas como AWS Glue y EMR/Hive.


6. **Realizar consultas y procesamiento analítico descriptivo** con SQL utilizando AWS Athena, EMR/Hive y SparkSQL.


7. **Realizar consultas y procesamiento analítico descriptivo** con Spark en Python (`pyspark`).


8. **Realizar alguna aplicación de visualización de datos** (con herramientas como matplotlib, dash, powerBI, grafana, etc.) y alguna aplicación expuesta mediante API Gateway con Streamlit de Python.



En este documento, deberá adjuntar todas las evidencias de cada uno de los puntos, a través de descripciones, pantallazos y extractos de código importante.

También, deberá crear un repositorio en GitHub, creando un directorio explícito por cada uno de estos 8 puntos, adjuntando evidencias, código fuente y lo más importante: todas las instrucciones y archivos para que el profesor u otra persona pueda reproducir todo el proyecto.

---

## III. Desarrollo

* **Punto 1:** Formulación del caso de estudio del sector energético colombiano y 5 preguntas de negocio descriptivas y analíticas. Ver documentación en [`punto-1-caso-estudio/caso-estudio.md`](./punto-1-caso-estudio/caso-estudio.md).

* **Punto 2:** Simulación de fuentes de datos heterogéneas (RDS MariaDB, EC2 Linux y Socrata Open Data URLs). Ver mapeo técnico y justificaciones en [`punto-2-fuentes-datos/info.md`](./punto-2-fuentes-datos/info.md).

* **Punto 3:** Ingesta automática hacia el Data Lake S3 en capa Raw (Bronze). Scripts modulares de ingesta para RDS, EC2 y Socrata API. Ver documentación en [`punto-3-ingesta-s3/info.md`](./punto-3-ingesta-s3/info.md).

* **Punto 4:** Procesamiento y limpieza distribuida de datos (Raw a Trusted / Bronze a Silver) con AWS Glue y PySpark. Formato Apache Parquet con compresión Snappy y particionamiento físico optimizado. Ver documentación técnica, guía de ejecución local con Hadoop winutils y evidencias en [`punto-4-procesamiento/info.md`](./punto-4-procesamiento/info.md).

* **Punto 5:** Catalogación de tablas en AWS Glue Data Catalog y Metastore EMR/Hive mediante Crawler automatizado y scripts DDL. Ver documentación completa, capturas de verificación y scripts en [`punto-5-catalogacion/info.md`](./punto-5-catalogacion/info.md), [`01_create_tables.sql`](./punto-5-catalogacion/01_create_tables.sql) y [`crawler.py`](./punto-5-catalogacion/crawler.py).

* **Punto 6:** Consultas analíticas descriptivas resolviendo las 5 preguntas de negocio con AWS Athena, EMR Hive y SparkSQL. Ver documentación técnica detallada en [`punto-6-consultas-sql/info.md`](./punto-6-consultas-sql/info.md), scripts SQL en [`01_consultas_negocio_athena_hive.sql`](./punto-6-consultas-sql/01_consultas_negocio_athena_hive.sql) y script distribuido en [`spark_sql_analytics.py`](./punto-6-consultas-sql/spark_sql_analytics.py).

* **Punto 7:** Consultas y procesamiento analítico descriptivo con Apache Spark en Python (`pyspark`). Cinco pipelines modulares de alta concurrencia desarrollados en AWS Glue 5.0 (G.1X) para responder las 5 preguntas de negocio (Q1 a Q5), generando resultados analíticos en formato Parquet comprimidos con Snappy en `s3://big-data-lake-energiacol/analytics/`. Ver documentación técnica completa, scripts y evidencias en [`punto-7-pyspark-analitico/info.md`](./punto-7-pyspark-analitico/info.md) y [`jobs/`](./punto-7-pyspark-analitico/jobs/).

* **Punto 8:** Aplicación web interactiva de visualización de datos analíticos construida con Python, Streamlit y Plotly. Conexión directa y serverless a los datasets Parquet en Amazon S3 mediante `s3fs`, navegación multidimensional por análisis (Q1–Q5), métricas de calidad de datos, gráficos dinámicos interactivos y módulo de exportación a CSV. Ver documentación detallada, código y evidencias en [`punto-8-visualizacion/info.md`](./punto-8-visualizacion/info.md) y [`dashboard.py`](./punto-8-visualizacion/dashboard.py).
