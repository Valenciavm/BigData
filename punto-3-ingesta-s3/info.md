# Punto 3 — Ingesta hacia S3 (Raw Layer)

## Bucket: `big-data-lake-energiacol`

| Script | Bucket |
|---|---|
| `ingest_rds.py` | `big-data-lake-energiacol` |
| `ingest_api.py` | `big-data-lake-energiacol` |
| `ingest_ec2.py` | `big-data-lake-energiacol` |

---

## S3 Raw Layer — Folder Structure

```
s3://big-data-lake-energiacol/
└── raw/
    ├── ec2/
    │   ├── beneficios-frontera/                 <- CSV  (~90K rows)
    │   ├── ventas-gncv/                         <- CSV  (~135K rows)
    │   └── iea-monthly-electricity-statistics/  <- file_zip.csv (~180K rows)
    │
    ├── mariadb/
    │   ├── productores_b100E/        <- table.csv  (~47K rows)
    │   └── agentes_gnvc/             <- table.csv  (~1,761 rows)
    │
    └── api/
        ├── distribuidores_mayorista/ <- data_0.json ... data_N.json  (~7.18M rows)
        └── estaciones_servicio/      <- data_0.json ... data_N.json  (~6.48M rows)
```

---

## Datasets & Sources

| # | Dataset | Source | Rows | AWS Component | Script |
|---|---|---|---|---|---|
| 1 | Beneficio Zona de Frontera | [datos.gov.co (shbt-hqy9)](https://www.datos.gov.co/Minas-y-Energ-a/Beneficio-de-estaciones-de-servicio-en-zona-de-fro/shbt-hqy9/data_preview) | ~90K | EC2 (CSV local) | `ingest_ec2.py` |
| 2 | Ventas de GNCV | [datos.gov.co (v8jr-kywh)](https://www.datos.gov.co/Minas-y-Energ-a/Consulta-Ventas-de-Gas-Natural-Comprimido-Vehicula/v8jr-kywh/about_data) | ~135K | EC2 (CSV local) | `ingest_ec2.py` |
| 3 | IEA Monthly Electricity Stats | [Kaggle (ccanb23)](https://www.kaggle.com/datasets/ccanb23/iea-monthly-electricity-statistics) | ~180K | EC2 (ZIP — descargado de Kaggle, copiado manualmente al servidor) | `ingest_ec2.py` |
| 4 | Productores B100 y Etanol | [datos.gov.co (macx-96u2)](https://www.datos.gov.co/Minas-y-Energ-a/Productores-Productores-de-B100-y-Etanol-Alcohol-C/macx-96u2/about_data) | ~47K | RDS MariaDB -> `productores_b100E` | `ingest_rds.py` |
| 5 | Agentes GNCV | [datos.gov.co (ba2i-v4xx)](https://www.datos.gov.co/Minas-y-Energ-a/Consulta-Agentes-de-Gas-Natural-Comprimido-Vehicul/ba2i-v4xx/data_preview) | ~1,761 | RDS MariaDB -> `agentes_gnvc` | `ingest_rds.py` |
| 6 | Distribuidores Mayoristas | [datos.gov.co (339g-zjac)](https://www.datos.gov.co/Minas-y-Energ-a/Distribuidores-Mayoristas-Despacho-de-combustibles/339g-zjac/about_data) | ~7.18M | Socrata API (paginated JSON) | `ingest_api.py` |
| 7 | Estaciones de Servicio | [datos.gov.co (fbht-2fzd)](https://www.datos.gov.co/Minas-y-Energ-a/Estaciones-de-Servicio-Automotriz-y-Fluvial-Despac/fbht-2fzd/about_data) | ~6.48M | Socrata API (paginated JSON) | `ingest_api.py` |

---

## Ingestion Scripts

### `ingest_ec2.py` — EC2 -> S3
Uses AWS SSM (`send_command`) to remotely run `aws s3 cp` on the EC2 instance.
No SSH required — the EC2 instance must have the SSM Agent running and an IAM role with `ssm:SendCommand` + `s3:PutObject` permissions.

Files copied:
- `/home/ec2-user/data/beneficio-frontera/*.csv` -> `raw/ec2/beneficios-frontera/`
- `/home/ec2-user/data/ventas-gncv/*.csv` -> `raw/ec2/ventas-gncv/`
- `/home/ec2-user/data/iea-monthly-electricity-statistics/file_zip.csv` -> `raw/ec2/iea-monthly-electricity-statistics/`

### `ingest_rds.py` — RDS MariaDB -> S3
Connects to MariaDB via SQLAlchemy, reads each table with pandas, serializes to CSV in memory, and uploads to S3 via `boto3.put_object`.

### `ingest_api.py` — datos.gov.co API -> S3
Paginates through the Socrata API in chunks of 50,000 records. Each chunk is serialized to JSON and uploaded as `data_0.json`, `data_1.json`, etc.
Requires `GOV_TOKEN` in `.env`.

### `sanity_check.py` — Pre-flight verification
Verifies environment variables, AWS IAM credentials via STS, S3 bucket reachability, RDS MariaDB connectivity & tables, EC2 instance state & SSM agent ping status, and open data Socrata API connectivity with token.

### `ingest_pipeline.py` — Orchestrator
Interactive menu to run pre-flight check (0), or pipelines 1, 2, 3 individually or all concurrently using `asyncio`.

```
Selection options: 0 | 1 | 2 | 3 | all | 1 3 | 1,2
```

---

## Environment Variables (`.env`)

| Variable | Used by | Description |
|---|---|---|
| `DB_HOST` | `ingest_rds.py`, `sanity_check.py` | RDS endpoint hostname |
| `DB_USER` | `ingest_rds.py`, `sanity_check.py` | RDS master username |
| `DB_PASSWORD` | `ingest_rds.py`, `sanity_check.py` | RDS master password |
| `DB_NAME` | `ingest_rds.py`, `sanity_check.py` | Target database name (`energia_col`) |
| `EC2_INSTANCE_ID` | `ingest_ec2.py`, `sanity_check.py` | EC2 instance ID |
| `GOV_TOKEN` | `ingest_api.py`, `sanity_check.py` | datos.gov.co App Token |
| `AWS_ACCESS_KEY_ID` | All AWS scripts (`boto3`) | Project-scoped AWS Access Key |
| `AWS_SECRET_ACCESS_KEY` | All AWS scripts (`boto3`) | Project-scoped AWS Secret Key |
| `AWS_SESSION_TOKEN` | All AWS scripts (`boto3`) | (Optional) AWS Academy / temporary session token |
| `AWS_DEFAULT_REGION` | All AWS scripts (`boto3`) | `us-east-2` |
