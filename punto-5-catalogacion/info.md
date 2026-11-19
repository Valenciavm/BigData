# Milestone 5: Data Cataloging (AWS Glue & Hive Metastore)

This module implements the **Data Cataloging layer** of the Big Data Lakehouse.
It bridges the gap between physical Parquet storage on Amazon S3 (`s3://big-data-lake-energiacol/trusted/`) and analytical SQL query engines (**AWS Athena**, **EMR/Hive**, and **SparkSQL**) by defining schemas, data types, and partition metadata in the **AWS Glue Data Catalog**.

---

## 1. Catalog Architecture

```
┌────────────────────────────────────────────────────────┐
│              S3: TRUSTED ZONE (Parquet)                │
│  s3://big-data-lake-energiacol/trusted/                │
│    ├── distribuidores_mayoristas/ (anio_despacho/mes)  │
│    ├── estaciones_servicio/       (anio_despacho/mes)  │
│    ├── beneficios_frontera/       (anio_beneficio/mes) │
│    ├── ventas_gncv/               (anio_venta/mes)     │
│    ├── productores_b100e/         (anio_despacho/mes)  │
│    ├── agentes_gncv/              (departamento)       │
│    └── iea_electricity/          (country/year)       │
└───────────────────────────┬────────────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
    ┌───────────────────┐       ┌───────────────────┐
    │  Method A: Glue   │       │  Method B: DDL    │
    │  Crawler (Python) │       │  Athena / Hive    │
    │  `crawler.py`     │       │  `01_create...sql`│
    └─────────┬─────────┘       └─────────┬─────────┘
              │                           │
              └─────────────┬─────────────┘
                            ▼
┌────────────────────────────────────────────────────────┐
│                 AWS GLUE DATA CATALOG                  │
│  Database: `energia_col_trusted`                       │
│  - 7 Strongly-typed Parquet External Tables            │
│  - Automated Partition Pruning Metadata                │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼ (Milestone 6: SQL Analytics)
┌────────────────────────────────────────────────────────┐
│               QUERY ENGINES (Punto 6)                  │
│       AWS Athena  │  Apache Hive (EMR)  │  SparkSQL   │
└────────────────────────────────────────────────────────┘
```

---

## 2. Cataloged Tables & Schema Summary

Database: **`energia_col_trusted`**

| Table Name | Storage Location (`s3://.../trusted/`) | Format | Compression | Partition Keys |
|---|---|---|---|---|
| **`distribuidores_mayoristas`** | `trusted/distribuidores_mayoristas/` | Parquet | SNAPPY | `anio_despacho` (INT), `mes_despacho` (INT) |
| **`estaciones_servicio`** | `trusted/estaciones_servicio/` | Parquet | SNAPPY | `anio_despacho` (INT), `mes_despacho` (INT) |
| **`beneficios_frontera`** | `trusted/beneficios_frontera/` | Parquet | SNAPPY | `anio_beneficio` (INT), `mes_beneficio` (INT) |
| **`ventas_gncv`** | `trusted/ventas_gncv/` | Parquet | SNAPPY | `anio_venta` (INT), `mes_venta` (INT) |
| **`productores_b100e`** | `trusted/productores_b100e/` | Parquet | SNAPPY | `anio_despacho` (INT), `mes_despacho` (INT) |
| **`agentes_gncv`** | `trusted/agentes_gncv/` | Parquet | SNAPPY | `departamento` (STRING) |
| **`iea_electricity`** | `trusted/iea_electricity/` | Parquet | SNAPPY | `country` (STRING), `year` (INT) |

---

## 3. Cataloging Methods Provided

### Method A: Automated AWS Glue Crawler (`crawler.py`)
Provisions the database and configures an AWS Glue Crawler targeting the entire `trusted/` S3 prefix.

**Prerequisites:**
* IAM Role with `AWSGlueServiceRole` and read access to `s3://big-data-lake-energiacol/trusted/`.
* Set `GLUE_ROLE_ARN` in your `.env`:
  ```env
  GLUE_ROLE_ARN=arn:aws:iam::<account_id>:role/<role-name>
  ```

**Execution:**
```powershell
# Create or update the Database and Crawler
python punto-5-catalogacion/crawler.py

# Or create AND trigger the crawler in one step
python punto-5-catalogacion/crawler.py --run
```

---

### Method B: Deterministic SQL DDL Scripts (`01_create_tables.sql`)
Can be executed directly inside the **AWS Athena Query Editor** or in an **EMR Hive CLI**:

1. Open **AWS Console → Athena → Query Editor**.
2. Select your query result S3 location (if prompted).
3. Paste and run the queries from [`01_create_tables.sql`](file:///c:\Users\valen\OneDrive\Escritorio\BigData\BigData\punto-5-catalogacion\01_create_tables.sql).
4. Run `MSCK REPAIR TABLE <table_name>;` to register all S3 partitions into the catalog.

---

## 4. Verification & Validation

Once cataloged, run these SQL queries in Athena to confirm table registration:

```sql
-- 1. Verify tables in catalog
SHOW TABLES IN energia_col_trusted;

-- 2. Verify partition discovery
SHOW PARTITIONS energia_col_trusted.distribuidores_mayoristas;

-- 3. Verify row sample
SELECT * FROM energia_col_trusted.ventas_gncv LIMIT 5;
```
