# Data Architecture Specification: Raw → Trusted → Analytics
**Standardization Spec for Punto 4 (Procesamiento), Punto 5 (Catalogación), and Punto 6 (Consultas SQL)**

---

## 1. Global Standards & Conventions

| Property | Standard | Justification |
|---|---|---|
| **File Format** | **Apache Parquet (`.parquet`)** | Columnar storage, high compression ratio, optimized for Athena & Spark. |
| **Compression** | **Snappy** | Optimal balance between CPU compression speed and query scan time. |
| **Column & Partition Names** | `lowercase_snake_case` | Eliminates case-sensitivity and quoting issues across Spark, Hive, Glue, and Athena. |
| **Date Format** | `DATE` (`YYYY-MM-DD`) | Standard ISO-8601 SQL date. |
| **Numeric Formats** | `DOUBLE` / `BIGINT` | All volumes, coordinates, and monetary amounts cast from raw strings. |
| **Database Name** | `energia_col_trusted` | AWS Glue Data Catalog and Athena database name. |
| **Bucket URI** | `s3://big-data-lake-energiacol/` | Unified central S3 Big Data Lake. |

---

## 2. Storage Mapping (Raw → Trusted)

| Dataset | Raw Source Path | Target Trusted S3 Path | Target SQL Table | Partition Columns (Hive-style) |
|---|---|---|---|---|
| **Distribuidores Mayoristas** | `raw/api/distribuidores_mayorista/*.json` | `trusted/distribuidores_mayoristas/` | `distribuidores_mayoristas` | `anio_despacho`, `mes_despacho` |
| **Estaciones de Servicio** | `raw/api/estaciones_servicio/*.json` | `trusted/estaciones_servicio/` | `estaciones_servicio` | `anio_despacho`, `mes_despacho` |
| **Beneficios Frontera** | `raw/ec2/beneficios-frontera/*.csv` | `trusted/beneficios_frontera/` | `beneficios_frontera` | `anio_beneficio`, `mes_beneficio` |
| **Ventas GNCV** | `raw/ec2/ventas-gncv/*.csv` | `trusted/ventas_gncv/` | `ventas_gncv` | `anio_venta`, `mes_venta` |
| **IEA Electricity** | `raw/ec2/iea-monthly-electricity-statistics/file_zip.csv` | `trusted/iea_electricity/` | `iea_electricity` | `country`, `year` |
| **Productores B100E** | `raw/mariadb/productores_b100E/table.csv` | `trusted/productores_b100e/` | `productores_b100e` | `anio_despacho`, `mes_despacho` |
| **Agentes GNCV** | `raw/mariadb/agentes_gnvc/table.csv` | `trusted/agentes_gncv/` | `agentes_gncv` | `departamento` |

---

## 3. Detailed Schema & Transformation Specifications

### 3.1. `distribuidores_mayoristas`
* **Raw volume:** ~7.19M records (JSON chunks)
* **Transformations in Punto 4:**
  * Convert all keys to lowercase.
  * Cast `fecha_despacho` to `DATE`.
  * Cast `anio_despacho`, `mes_despacho`, `dia_despacho` to `INT`.
  * Cast `volumen_despachado`, `latitud`, `longitud` to `DOUBLE`.
  * Cast `codigo_municipio_dane`, `codigo_municipio_dane_destino` to `BIGINT`.

| Standard Column Name | Spark / Glue Type | Athena / SQL Type | Description / Notes |
|---|---|---|---|
| `fecha_despacho` | `DateType` | `DATE` | Dispatch date |
| `dia_despacho` | `IntegerType` | `INT` | Dispatch day |
| `tipo_proveedor` | `StringType` | `STRING` | e.g. `DISTRIBUIDOR MAYORISTA` |
| `codigo_municipio_dane` | `LongType` | `BIGINT` | Origin DANE municipal code |
| `departamento_proveedor` | `StringType` | `STRING` | Origin department |
| `municipio_proveedor` | `StringType` | `STRING` | Origin municipality |
| `tipo_comprador` | `StringType` | `STRING` | e.g. `DISTRIBUIDOR MINORISTA` |
| `subtipo_comprador` | `StringType` | `STRING` | e.g. `ESTACION DE SERVICIO AUTOMOTRIZ` |
| `codigo_municipio_dane_destino` | `LongType` | `BIGINT` | Destination DANE code |
| `departamento` | `StringType` | `STRING` | Destination department |
| `municipio` | `StringType` | `STRING` | Destination municipality |
| `producto` | `StringType` | `STRING` | e.g. `BIODIESEL CON MEZCLA`, `GASOLINA CORRIENTE` |
| `volumen_despachado` | `DoubleType` | `DOUBLE` | Fuel volume dispatched (gallons) |
| `latitud` | `DoubleType` | `DOUBLE` | Geographic latitude |
| `longitud` | `DoubleType` | `DOUBLE` | Geographic longitude |
| **`anio_despacho`** *(Partition)* | `IntegerType` | `INT` | **Partition key 1** (e.g. 2022, 2023, 2024) |
| **`mes_despacho`** *(Partition)* | `IntegerType` | `INT` | **Partition key 2** (1 to 12) |

---

### 3.2. `estaciones_servicio`
* **Raw volume:** ~6.50M records (JSON chunks)
* **Transformations in Punto 4:**
  * Convert keys to lowercase.
  * Cast `fecha_despacho` to `DATE`.
  * Cast `volumen_despachado`, `latitud`, `longitud` to `DOUBLE`.
  * Standardize `zona_frontera` to `STRING` (`SI` / `NO`).

| Standard Column Name | Spark / Glue Type | Athena / SQL Type | Description / Notes |
|---|---|---|---|
| `fecha_despacho` | `DateType` | `DATE` | Dispatch date |
| `dia_despacho` | `IntegerType` | `INT` | Dispatch day |
| `tipo_proveedor` | `StringType` | `STRING` | Provider type |
| `codigo_municipio_dane` | `LongType` | `BIGINT` | Origin DANE code |
| `departamento_proveedor` | `StringType` | `STRING` | Origin department |
| `municipio_proveedor` | `StringType` | `STRING` | Origin municipality |
| `tipo_comprador` | `StringType` | `STRING` | Buyer type |
| `subtipo_comprador` | `StringType` | `STRING` | Retail buyer subtype |
| `zona_frontera` | `StringType` | `STRING` | Border zone indicator (`SI`/`NO`) |
| `codigo_municipio_dane_destino` | `LongType` | `BIGINT` | Destination DANE code |
| `departamento` | `StringType` | `STRING` | Destination department |
| `municipio` | `StringType` | `STRING` | Destination municipality |
| `producto` | `StringType` | `STRING` | Fuel product |
| `volumen_despachado` | `DoubleType` | `DOUBLE` | Fuel volume dispatched (gallons) |
| `latitud` | `DoubleType` | `DOUBLE` | Latitude |
| `longitud` | `DoubleType` | `DOUBLE` | Longitude |
| **`anio_despacho`** *(Partition)* | `IntegerType` | `INT` | **Partition key 1** |
| **`mes_despacho`** *(Partition)* | `IntegerType` | `INT` | **Partition key 2** |

---

### 3.3. `beneficios_frontera`
* **Raw volume:** ~90.4K records (CSV)
* **Transformations in Punto 4:**
  * Convert headers from uppercase to lowercase.
  * Cast `FECHA_BENEFICIO` to `DATE`.
  * Cast `BENEFICIO`, `LATITUD`, `LONGITUD` to `DOUBLE`.

| Standard Column Name | Spark / Glue Type | Athena / SQL Type | Description / Notes |
|---|---|---|---|
| `fecha_beneficio` | `DateType` | `DATE` | Benefit assignment date |
| `dia_beneficio` | `IntegerType` | `INT` | Day |
| `tipo_agente` | `StringType` | `STRING` | Agent type |
| `tipo_beneficio` | `StringType` | `STRING` | Benefit type (e.g. `CUPO UPME`) |
| `codigo_municipio_dane_destino` | `LongType` | `BIGINT` | Destination DANE code |
| `departamento` | `StringType` | `STRING` | Border department |
| `municipio` | `StringType` | `STRING` | Border municipality |
| `beneficio` | `DoubleType` | `DOUBLE` | Benefit volume / quota allocated |
| `latitud` | `DoubleType` | `DOUBLE` | Latitude |
| `longitud` | `DoubleType` | `DOUBLE` | Longitude |
| **`anio_beneficio`** *(Partition)* | `IntegerType` | `INT` | **Partition key 1** |
| **`mes_beneficio`** *(Partition)* | `IntegerType` | `INT` | **Partition key 2** |

---

### 3.4. `ventas_gncv`
* **Raw volume:** ~135K records (CSV)
* **Transformations in Punto 4:**
  * Convert headers to lowercase.
  * Clean `CANTIDAD_VOLUMEN_SUMINISTRADO` (remove comma thousand separator `'1,919.61'` → `1919.61`) and cast to `DOUBLE`.
  * Cast `EDS_ACTIVAS`, `NUMERO_DE_VENTAS`, `VEHICULOS_ATENDIDOS` to `INT`.
  * Cast `FECHA_VENTA` to `DATE`.

| Standard Column Name | Spark / Glue Type | Athena / SQL Type | Description / Notes |
|---|---|---|---|
| `fecha_venta` | `DateType` | `DATE` | Date of sale |
| `dia_venta` | `IntegerType` | `INT` | Day |
| `codigo_municipio_dane` | `LongType` | `BIGINT` | DANE municipal code |
| `departamento` | `StringType` | `STRING` | Department |
| `municipio` | `StringType` | `STRING` | Municipality |
| `latitud` | `DoubleType` | `DOUBLE` | Latitude |
| `longitud` | `DoubleType` | `DOUBLE` | Longitude |
| `tipo_agente` | `StringType` | `STRING` | Agent type |
| `tipo_de_combustible` | `StringType` | `STRING` | e.g. `GNV` |
| `eds_activas` | `IntegerType` | `INT` | Active service stations count |
| `numero_de_ventas` | `IntegerType` | `INT` | Sales count |
| `vehiculos_atendidos` | `IntegerType` | `INT` | Vehicles served count |
| `cantidad_volumen_suministrado`| `DoubleType` | `DOUBLE` | Gas volume supplied ($m^3$) |
| **`anio_venta`** *(Partition)* | `IntegerType` | `INT` | **Partition key 1** |
| **`mes_venta`** *(Partition)* | `IntegerType` | `INT` | **Partition key 2** |

---

### 3.5. `productores_b100e`
* **Raw volume:** ~47K records (CSV from MariaDB export)
* **Transformations in Punto 4:**
  * Convert headers to lowercase.
  * Clean `VOLUMEN_DESPACHADO` (Colombian number format: replace dot thousand separator and comma decimal `'11.128,98'` → `11128.98`) and cast to `DOUBLE`.
  * Cast `FECHA_DESPACHO` to `DATE`.

| Standard Column Name | Spark / Glue Type | Athena / SQL Type | Description / Notes |
|---|---|---|---|
| `fecha_despacho` | `DateType` | `DATE` | Dispatch date |
| `dia_despacho` | `IntegerType` | `INT` | Day |
| `tipo_proveedor` | `StringType` | `STRING` | e.g. `PRODUCTOR` |
| `subtipo_proveedor` | `StringType` | `STRING` | e.g. `ALCOHOL CARBURANTE` |
| `codigo_municipio_dane_proveedor`| `LongType` | `BIGINT` | Origin DANE code |
| `departamento_proveedor` | `StringType` | `STRING` | Origin department |
| `municipio_proveedor` | `StringType` | `STRING` | Origin municipality |
| `tipo_comprador` | `StringType` | `STRING` | e.g. `DISTRIBUIDOR MAYORISTA` |
| `codigo_municipio_dane_destino` | `LongType` | `BIGINT` | Destination DANE code |
| `departamento` | `StringType` | `STRING` | Destination department |
| `municipio` | `StringType` | `STRING` | Destination municipality |
| `producto` | `StringType` | `STRING` | Biofuel product |
| `volumen_despachado` | `DoubleType` | `DOUBLE` | Volume dispatched |
| `latitud` | `DoubleType` | `DOUBLE` | Latitude |
| `longitud` | `DoubleType` | `DOUBLE` | Longitude |
| **`anio_despacho`** *(Partition)* | `IntegerType` | `INT` | **Partition key 1** |
| **`mes_despacho`** *(Partition)* | `IntegerType` | `INT` | **Partition key 2** |

---

### 3.6. `agentes_gncv`
* **Raw volume:** ~1,761 records (CSV from MariaDB export)
* **Transformations in Punto 4:**
  * Convert headers to lowercase.
  * Trim strings, normalize text.
  * Partition by `departamento`.

| Standard Column Name | Spark / Glue Type | Athena / SQL Type | Description / Notes |
|---|---|---|---|
| `tipo_agente` | `StringType` | `STRING` | Agent type (Taller, Conversión, etc.) |
| `nombre_comercial` | `StringType` | `STRING` | Commercial business name |
| `razon_social` | `StringType` | `STRING` | Legal entity name |
| `estado_agente` | `StringType` | `STRING` | Status (e.g. `ACTIVO`) |
| `municipio` | `StringType` | `STRING` | Municipality |
| `codigo_municipio_dane` | `LongType` | `BIGINT` | DANE municipal code |
| `latitud` | `DoubleType` | `DOUBLE` | Latitude |
| `longitud` | `DoubleType` | `DOUBLE` | Longitude |
| **`departamento`** *(Partition)*| `StringType` | `STRING` | **Partition key** (e.g. `ANTIOQUIA`, `CUNDINAMARCA`) |

---

### 3.7. `iea_electricity`
* **Raw volume:** ~180K records (CSV from Kaggle `file_zip.csv`)
* **Transformations in Punto 4:**
  * Convert headers to lowercase snake_case.
  * Cast `year`, `month`, `display_order` to `INT`.
  * Cast `value`, `year_to_date`, `previous_year_to_date`, `share` to `DOUBLE`.
  * Partition by `country`, `year`.

| Standard Column Name | Spark / Glue Type | Athena / SQL Type | Description / Notes |
|---|---|---|---|
| `code_time` | `StringType` | `STRING` | e.g. `JAN2010` |
| `time` | `StringType` | `STRING` | e.g. `January 2010` |
| `month` | `IntegerType` | `INT` | Month number (1–12) |
| `month_name` | `StringType` | `STRING` | Month name |
| `product` | `StringType` | `STRING` | Energy product (Hydro, Solar, Wind, etc.) |
| `value` | `DoubleType` | `DOUBLE` | Generation value in GWh |
| `display_order` | `IntegerType` | `INT` | Display order |
| `year_to_date` | `DoubleType` | `DOUBLE` | Cumulative year-to-date generation |
| `previous_year_to_date` | `DoubleType` | `DOUBLE` | Prior year cumulative generation |
| `share` | `DoubleType` | `DOUBLE` | Share of energy mix |
| **`country`** *(Partition)* | `StringType` | `STRING` | **Partition key 1** (e.g. `Australia`, `Colombia`) |
| **`year`** *(Partition)* | `IntegerType` | `INT` | **Partition key 2** (2010–2022) |

---

## 4. Alignment with Business Questions (Punto 1 & Punto 6)

| # | Question Focus | Primary Tables Involved | Join Keys | Partition Filter Optimization |
|---|---|---|---|---|
| **Q1** | Concentración Mercado Mayorista | `distribuidores_mayoristas`, `estaciones_servicio` | `departamento`, `producto` | `anio_despacho >= 2022` |
| **Q2** | Impacto Subsidios Frontera | `beneficios_frontera`, `estaciones_servicio` | `codigo_municipio_dane_destino`, `departamento` | `anio_beneficio`, `anio_despacho` |
| **Q3** | Correlación Infraestructura GNCV | `agentes_gncv`, `ventas_gncv` | `codigo_municipio_dane`, `municipio`, `departamento` | `departamento`, `anio_venta` |
| **Q4** | Trazabilidad Biocombustibles | `productores_b100e`, `distribuidores_mayoristas` | `producto`, `departamento` | `anio_despacho`, `mes_despacho` |
| **Q5** | Macro-tendencias Energéticas | `iea_electricity`, `ventas_gncv`, `distribuidores_mayoristas` | `year` / `anio_despacho` / `mes_despacho` | `country`, `year` |
