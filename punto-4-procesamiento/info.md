# Milestone 4: Data Processing (Raw → Trusted)

This module implements the data processing and transformation layer within the Lakehouse/Medallion architecture on AWS S3. 
It extracts raw, immutable data from the **Raw (Bronze)** zone and outputs clean, standardized, strongly-typed, and query-optimized datasets into the **Trusted (Silver)** zone using **AWS Glue** and **Apache Spark (PySpark)**.

---

## 1. Architecture & Data Flow

```
┌──────────────────────────────────────────────┐
│           S3: RAW ZONE (Bronze)              │
│  - Raw CSV files (MariaDB RDS, EC2)          │
│  - Paginated JSON chunks (Socrata Open Data) │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│       AWS Glue Job / Apache Spark ETL        │
│  1. Batch ingestion (CSV / JSON)             │
│  2. Common hygiene (trim, deduplication)     │
│  3. Locale numeric parsing (LatAm vs US)     │
│  4. Strict schema & data type casting        │
│  5. Analytical partitioning strategy         │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│         S3: TRUSTED ZONE (Silver)            │
│  - Optimized Columnar Parquet format         │
│  - Snappy compression                        │
│  - Hive-style partitioned folder layout      │
└──────────────────────────────────────────────┘
```

---

## 2. Technical Decisions & Justifications

### Why AWS Glue with PySpark?
- **Serverless Compute**: Eliminates the operational overhead of managing dedicated cluster infrastructure. Glue dynamically scales Data Processing Units (DPUs) and terminates compute upon job completion.
- **Distributed Big Data Engine**: PySpark seamlessly handles both small dimension tables (~1,700 rows) and massive transactional fact tables with over 7 million records (`distribuidores_mayorista`).
- **Data Catalog Ecosystem**: Direct integration with AWS Glue Data Catalog, paving the way for downstream query engines (Milestone 5 & 6: Hive Metastore & AWS Athena).

### Why Parquet with Snappy Compression?
- **Columnar Storage**: Future analytical SQL queries only read the specific columns requested in the `SELECT` / `WHERE` clauses, saving up to 80% on scanned bytes and I/O latency.
- **Snappy Compression**: High-speed compression/decompression balanced for analytical workloads.
- **Embedded Schema**: Enforces strict native datatypes (`IntegerType`, `DoubleType`, `DateType`) avoiding inference errors during SQL scans.

### Partitioning Strategy (Hive-Style)
To prevent costly Full Table Scans in Athena/EMR:
- **Transactional Fuel & Subsidy Datasets** (`productores_b100e`, `ventas_gncv`, `beneficios_frontera`, `distribuidores_mayoristas`, `estaciones_servicio`): Partitioned by **Year and Month** (`anio_despacho/mes_despacho`, `anio_venta/mes_venta`, `anio_beneficio/mes_beneficio`).
- **International Energy Metrics** (`iea_electricity`): Partitioned by **Country and Year** (`country/year`).
- **Agent Registry** (`agentes_gncv`): Partitioned by **State/Region** (`departamento`), since this is a static reference dimension queried by geography.

---

## 3. Dataset Mapping & S3 Paths

| Dataset | Raw S3 Source | Input Format | Trusted S3 Destination | Partition Columns |
|---|---|---|---|---|
| **Agentes GNCV** | `s3://.../raw/mariadb/agentes_gnvc/` | CSV | `s3://.../trusted/agentes_gncv/` | `departamento` |
| **Productores B100E** | `s3://.../raw/mariadb/productores_b100E/` | CSV | `s3://.../trusted/productores_b100e/` | `anio_despacho`, `mes_despacho` |
| **Beneficios Frontera** | `s3://.../raw/ec2/beneficios-frontera/` | CSV | `s3://.../trusted/beneficios_frontera/` | `anio_beneficio`, `mes_beneficio` |
| **Ventas GNCV** | `s3://.../raw/ec2/ventas-gncv/` | CSV | `s3://.../trusted/ventas_gncv/` | `anio_venta`, `mes_venta` |
| **IEA Electricity** | `s3://.../raw/ec2/iea-monthly-electricity-statistics/` | CSV | `s3://.../trusted/iea_electricity/` | `country`, `year` |
| **Distribuidores Mayoristas** | `s3://.../raw/api/distribuidores_mayorista/` | JSON | `s3://.../trusted/distribuidores_mayoristas/` | `anio_despacho`, `mes_despacho` |
| **Estaciones de Servicio** | `s3://.../raw/api/estaciones_servicio/` | JSON | `s3://.../trusted/estaciones_servicio/` | `anio_despacho`, `mes_despacho` |

---

## 4. Deployment & Execution Instructions

### Option A: AWS Glue Job Execution (Production)

1. **Upload the Glue Job script to S3**:
   ```bash
   aws s3 cp glue_jobs/job_raw_to_trusted.py s3://big-data-lake-energiacol/scripts/job_raw_to_trusted.py
   ```

2. **Configure the Job in AWS Glue**:
   - **Job Name**: `job_raw_to_trusted`
   - **IAM Role**: Assigned role with `AWSGlueServiceRole` and read/write access to `big-data-lake-energiacol`.
   - **Type**: Spark (Glue 4.0, Python 3).
   - **Script Path**: `s3://big-data-lake-energiacol/scripts/job_raw_to_trusted.py`.

3. **Run the Job for a specific dataset**:
   ```bash
   aws glue start-job-run \
       --job-name job_raw_to_trusted \
       --arguments '{"--dataset": "ventas_gncv"}'
   ```
   *(Repeat for each target dataset as needed).*

---

### Option B: Local PySpark Execution (Development / Testing)

A local validation runner is provided to test the transformation pipeline on sample files without spinning up cloud resources:

```bash
# Process a single dataset
python run_local_spark.py --dataset agentes_gncv

# Process all available local sample datasets
python run_local_spark.py --all
```
Generated partitioned Parquet files will be stored locally under `punto-4-procesamiento/output_trusted/` (which is excluded from Git via `.gitignore`).

#### Note for Windows Users:
Apache Spark relies on POSIX file permissions. When writing Parquet files to a local Windows filesystem, Spark requires `winutils.exe` and `hadoop.dll`. The runner script automatically resolves `HADOOP_HOME` if these binaries are placed in `punto-4-procesamiento/.hadoop/bin/`.