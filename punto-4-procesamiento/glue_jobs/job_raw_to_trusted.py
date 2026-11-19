import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, DateType

# ─────────────────────────────────────────────────────────────────────────────
# 1. Dataset Configurations (Standardized per specs.md)
# ─────────────────────────────────────────────────────────────────────────────
BUCKET = "big-data-lake-energiacol"
RAW = f"s3://{BUCKET}/raw"
TRUSTED = f"s3://{BUCKET}/trusted"

DATASETS = {
    # Sources from MariaDB RDS
    "agentes_gncv": {
        "format": "csv",
        "raw_path": f"{RAW}/mariadb/agentes_gnvc/",
        "trusted_path": f"{TRUSTED}/agentes_gncv/",
        "partition_by": ["departamento"]
    },
    "productores_b100e": {
        "format": "csv",
        "raw_path": f"{RAW}/mariadb/productores_b100E/",
        "trusted_path": f"{TRUSTED}/productores_b100e/",
        "partition_by": ["anio_despacho", "mes_despacho"]
    },
    # Sources from EC2 Instance
    "beneficios_frontera": {
        "format": "csv",
        "raw_path": f"{RAW}/ec2/beneficios-frontera/",
        "trusted_path": f"{TRUSTED}/beneficios_frontera/",
        "partition_by": ["anio_beneficio", "mes_beneficio"]
    },
    "ventas_gncv": {
        "format": "csv",
        "raw_path": f"{RAW}/ec2/ventas-gncv/",
        "trusted_path": f"{TRUSTED}/ventas_gncv/",
        "partition_by": ["anio_venta", "mes_venta"]
    },
    "iea_electricity": {
        "format": "csv",
        "raw_path": f"{RAW}/ec2/iea-monthly-electricity-statistics/",
        "trusted_path": f"{TRUSTED}/iea_electricity/",
        "partition_by": ["country", "year"]
    },
    # Large-scale Sources from Socrata API (JSON chunks)
    "distribuidores_mayoristas": {
        "format": "json",
        "raw_path": f"{RAW}/api/distribuidores_mayorista/",
        "trusted_path": f"{TRUSTED}/distribuidores_mayoristas/",
        "partition_by": ["anio_despacho", "mes_despacho"]
    },
    "estaciones_servicio": {
        "format": "json",
        "raw_path": f"{RAW}/api/estaciones_servicio/",
        "trusted_path": f"{TRUSTED}/estaciones_servicio/",
        "partition_by": ["anio_despacho", "mes_despacho"]
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. Helper Functions (Numeric Parsers)
# ─────────────────────────────────────────────────────────────────────────────
def parse_colombian_number(col_name):
    """Converts Colombian number format '11.128,98' to standard decimal '11128.98'."""
    return F.regexp_replace(
        F.regexp_replace(F.col(col_name), r"\.", ""),
        ",", "."
    ).cast(DoubleType())

def parse_us_number(col_name):
    """Converts US number format '1,919.61' to standard decimal '1919.61'."""
    return F.regexp_replace(F.col(col_name), ",", "").cast(DoubleType())

# ─────────────────────────────────────────────────────────────────────────────
# 3. Common Data Hygiene
# ─────────────────────────────────────────────────────────────────────────────
def clean_common(df):
    """Normalizes column names to lowercase, trims whitespace, drops exact duplicates and empty rows."""
    # 1. Normalize all column headers to lowercase snake_case
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower())

    # 2. Trim string whitespace
    for col_name in df.columns:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # 3. Deduplicate and drop fully-empty rows
    df = df.dropDuplicates()
    df = df.dropna(how="all")
    return df

# ─────────────────────────────────────────────────────────────────────────────
# 4. Dataset-Specific Transformations (Strict schema according to specs.md)
# ─────────────────────────────────────────────────────────────────────────────
def transform_agentes_gncv(df):
    return df.withColumn("latitud", F.col("latitud").cast(DoubleType())) \
             .withColumn("longitud", F.col("longitud").cast(DoubleType())) \
             .withColumn("codigo_municipio_dane", F.col("codigo_municipio_dane").cast(LongType()))

def transform_productores_b100e(df):
    return df.withColumn("fecha_despacho", F.to_date(F.col("fecha_despacho"), "yyyy-MM-dd")) \
             .withColumn("anio_despacho", F.col("anio_despacho").cast(IntegerType())) \
             .withColumn("mes_despacho", F.col("mes_despacho").cast(IntegerType())) \
             .withColumn("dia_despacho", F.col("dia_despacho").cast(IntegerType())) \
             .withColumn("volumen_despachado", parse_colombian_number("volumen_despachado")) \
             .withColumn("latitud", F.col("latitud").cast(DoubleType())) \
             .withColumn("longitud", F.col("longitud").cast(DoubleType())) \
             .withColumn("codigo_municipio_dane_proveedor", F.col("codigo_municipio_dane_proveedor").cast(LongType())) \
             .withColumn("codigo_municipio_dane_destino", F.col("codigo_municipio_dane_destino").cast(LongType()))

def transform_beneficios_frontera(df):
    return df.withColumn("fecha_beneficio", F.to_date(F.col("fecha_beneficio"), "yyyy-MM-dd")) \
             .withColumn("anio_beneficio", F.col("anio_beneficio").cast(IntegerType())) \
             .withColumn("mes_beneficio", F.col("mes_beneficio").cast(IntegerType())) \
             .withColumn("dia_beneficio", F.col("dia_beneficio").cast(IntegerType())) \
             .withColumn("beneficio", parse_colombian_number("beneficio")) \
             .withColumn("latitud", F.col("latitud").cast(DoubleType())) \
             .withColumn("longitud", F.col("longitud").cast(DoubleType())) \
             .withColumn("codigo_municipio_dane_destino", F.col("codigo_municipio_dane_destino").cast(LongType()))

def transform_ventas_gncv(df):
    return df.withColumn("fecha_venta", F.to_date(F.col("fecha_venta"), "yyyy-MM-dd")) \
             .withColumn("anio_venta", F.col("anio_venta").cast(IntegerType())) \
             .withColumn("mes_venta", F.col("mes_venta").cast(IntegerType())) \
             .withColumn("dia_venta", F.col("dia_venta").cast(IntegerType())) \
             .withColumn("eds_activas", F.col("eds_activas").cast(IntegerType())) \
             .withColumn("numero_de_ventas", F.col("numero_de_ventas").cast(IntegerType())) \
             .withColumn("vehiculos_atendidos", F.col("vehiculos_atendidos").cast(IntegerType())) \
             .withColumn("cantidad_volumen_suministrado", parse_us_number("cantidad_volumen_suministrado")) \
             .withColumn("latitud", F.col("latitud").cast(DoubleType())) \
             .withColumn("longitud", F.col("longitud").cast(DoubleType())) \
             .withColumn("codigo_municipio_dane", F.col("codigo_municipio_dane").cast(LongType()))

def transform_iea_electricity(df):
    # Rename display order and cumulative columns if present
    if "display_order" in df.columns:
        df = df.withColumn("display_order", F.col("display_order").cast(IntegerType()))
    if "yeartodate" in df.columns:
        df = df.withColumnRenamed("yeartodate", "year_to_date")
    if "previousyeartodate" in df.columns:
        df = df.withColumnRenamed("previousyeartodate", "previous_year_to_date")

    return df.withColumn("year", F.col("year").cast(IntegerType())) \
             .withColumn("month", F.col("month").cast(IntegerType())) \
             .withColumn("value", F.col("value").cast(DoubleType())) \
             .withColumn("share", F.col("share").cast(DoubleType())) \
             .withColumn("year_to_date", F.col("year_to_date").cast(DoubleType())) \
             .withColumn("previous_year_to_date", F.col("previous_year_to_date").cast(DoubleType()))

def transform_distribuidores_mayoristas(df):
    return df.withColumn("fecha_despacho", F.to_date(F.col("fecha_despacho"), "yyyy-MM-dd")) \
             .withColumn("anio_despacho", F.col("anio_despacho").cast(IntegerType())) \
             .withColumn("mes_despacho", F.col("mes_despacho").cast(IntegerType())) \
             .withColumn("dia_despacho", F.col("dia_despacho").cast(IntegerType())) \
             .withColumn("volumen_despachado", F.col("volumen_despachado").cast(DoubleType())) \
             .withColumn("latitud", F.col("latitud").cast(DoubleType())) \
             .withColumn("longitud", F.col("longitud").cast(DoubleType())) \
             .withColumn("codigo_municipio_dane", F.col("codigo_municipio_dane").cast(LongType())) \
             .withColumn("codigo_municipio_dane_destino", F.col("codigo_municipio_dane_destino").cast(LongType()))

def transform_estaciones_servicio(df):
    return df.withColumn("fecha_despacho", F.to_date(F.col("fecha_despacho"), "yyyy-MM-dd")) \
             .withColumn("anio_despacho", F.col("anio_despacho").cast(IntegerType())) \
             .withColumn("mes_despacho", F.col("mes_despacho").cast(IntegerType())) \
             .withColumn("dia_despacho", F.col("dia_despacho").cast(IntegerType())) \
             .withColumn("volumen_despachado", F.col("volumen_despachado").cast(DoubleType())) \
             .withColumn("latitud", F.col("latitud").cast(DoubleType())) \
             .withColumn("longitud", F.col("longitud").cast(DoubleType())) \
             .withColumn("codigo_municipio_dane", F.col("codigo_municipio_dane").cast(LongType())) \
             .withColumn("codigo_municipio_dane_destino", F.col("codigo_municipio_dane_destino").cast(LongType()))

TRANSFORMATIONS = {
    "agentes_gncv": transform_agentes_gncv,
    "productores_b100e": transform_productores_b100e,
    "beneficios_frontera": transform_beneficios_frontera,
    "ventas_gncv": transform_ventas_gncv,
    "iea_electricity": transform_iea_electricity,
    "distribuidores_mayoristas": transform_distribuidores_mayoristas,
    "estaciones_servicio": transform_estaciones_servicio
}

# ─────────────────────────────────────────────────────────────────────────────
# 5. Job Entrypoint
# ─────────────────────────────────────────────────────────────────────────────
def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "dataset"])
    dataset_name = args["dataset"]

    if dataset_name not in DATASETS:
        raise ValueError(f"Unknown dataset '{dataset_name}'. Available options: {list(DATASETS.keys())}")

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    config = DATASETS[dataset_name]
    print(f"--> Starting Raw -> Trusted processing for: {dataset_name}")
    print(f"    Source path: {config['raw_path']} (format: {config['format']})")

    # Read based on format
    if config["format"] == "json":
        df = spark.read.json(config["raw_path"])
    else:
        df = spark.read.option("header", "true") \
                       .option("inferSchema", "false") \
                       .csv(config["raw_path"])

    # Base hygiene and business transformations
    df_clean = clean_common(df)
    df_final = TRANSFORMATIONS[dataset_name](df_clean)

    # Write as partitioned Parquet with Snappy compression
    print(f"    Writing Parquet partitioned by {config['partition_by']}...")
    print(f"    Target path: {config['trusted_path']}")
    
    df_final.write \
        .mode("overwrite") \
        .partitionBy(*config["partition_by"]) \
        .parquet(config["trusted_path"])

    print(f"--> Processing completed successfully for: {dataset_name}")
    job.commit()

if __name__ == "__main__":
    main()
