import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType

# ─────────────────────────────────────────────────────────────────────────────
# 1. Dataset Configurations (S3 Paths, Formats, and Partitioning)
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
        "partition_by": ["DEPARTAMENTO"]
    },
    "productores_b100": {
        "format": "csv",
        "raw_path": f"{RAW}/mariadb/productores_b100E/",
        "trusted_path": f"{TRUSTED}/productores_b100/",
        "partition_by": ["ANIO_DESPACHO", "MES_DESPACHO"]
    },
    # Sources from EC2 Instance
    "beneficio_frontera": {
        "format": "csv",
        "raw_path": f"{RAW}/ec2/beneficios-frontera/",
        "trusted_path": f"{TRUSTED}/beneficio_frontera/",
        "partition_by": ["ANIO_BENEFICIO", "MES_BENEFICIO"]
    },
    "ventas_gncv": {
        "format": "csv",
        "raw_path": f"{RAW}/ec2/ventas-gncv/",
        "trusted_path": f"{TRUSTED}/ventas_gncv/",
        "partition_by": ["ANIO_VENTA", "MES_VENTA"]
    },
    "iea_electricity": {
        "format": "csv",
        "raw_path": f"{RAW}/ec2/iea-electricity/",
        "trusted_path": f"{TRUSTED}/iea_electricity/",
        "partition_by": ["COUNTRY", "YEAR"]
    },
    # Large-scale Sources from Socrata API (JSON chunks)
    "distribuidores_mayorista": {
        "format": "json",
        "raw_path": f"{RAW}/api/distribuidores_mayorista/",
        "trusted_path": f"{TRUSTED}/distribuidores_mayorista/",
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
    """Trims leading/trailing whitespace, drops exact duplicates and empty rows."""
    for col_name in df.columns:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
    df = df.dropDuplicates()
    df = df.dropna(how="all")
    return df

# ─────────────────────────────────────────────────────────────────────────────
# 4. Dataset-Specific Transformations
# ─────────────────────────────────────────────────────────────────────────────
def transform_agentes_gncv(df):
    return df.withColumn("LATITUD", F.col("LATITUD").cast(DoubleType())) \
             .withColumn("LONGITUD", F.col("LONGITUD").cast(DoubleType())) \
             .withColumn("CODIGO_MUNICIPIO_DANE", F.col("CODIGO_MUNICIPIO_DANE").cast(IntegerType()))

def transform_productores_b100(df):
    return df.withColumn("FECHA_DESPACHO", F.to_date(F.col("FECHA_DESPACHO"), "yyyy-MM-dd")) \
             .withColumn("ANIO_DESPACHO", F.col("ANIO_DESPACHO").cast(IntegerType())) \
             .withColumn("MES_DESPACHO", F.col("MES_DESPACHO").cast(IntegerType())) \
             .withColumn("DIA_DESPACHO", F.col("DIA_DESPACHO").cast(IntegerType())) \
             .withColumn("VOLUMEN_DESPACHADO", parse_colombian_number("VOLUMEN_DESPACHADO")) \
             .withColumn("LATITUD", F.col("LATITUD").cast(DoubleType())) \
             .withColumn("LONGITUD", F.col("LONGITUD").cast(DoubleType())) \
             .withColumn("CODIGO_MUNICIPIO_DANE_PROVEEDOR", F.col("CODIGO_MUNICIPIO_DANE_PROVEEDOR").cast(IntegerType())) \
             .withColumn("CODIGO_MUNICIPIO_DANE_DESTINO", F.col("CODIGO_MUNICIPIO_DANE_DESTINO").cast(IntegerType()))

def transform_beneficio_frontera(df):
    return df.withColumn("FECHA_BENEFICIO", F.to_date(F.col("FECHA_BENEFICIO"), "yyyy-MM-dd")) \
             .withColumn("ANIO_BENEFICIO", F.col("ANIO_BENEFICIO").cast(IntegerType())) \
             .withColumn("MES_BENEFICIO", F.col("MES_BENEFICIO").cast(IntegerType())) \
             .withColumn("DIA_BENEFICIO", F.col("DIA_BENEFICIO").cast(IntegerType())) \
             .withColumn("BENEFICIO", parse_colombian_number("BENEFICIO")) \
             .withColumn("LATITUD", F.col("LATITUD").cast(DoubleType())) \
             .withColumn("LONGITUD", F.col("LONGITUD").cast(DoubleType())) \
             .withColumn("CODIGO_MUNICIPIO_DANE_DESTINO", F.col("CODIGO_MUNICIPIO_DANE_DESTINO").cast(IntegerType()))

def transform_ventas_gncv(df):
    return df.withColumn("FECHA_VENTA", F.to_date(F.col("FECHA_VENTA"), "yyyy-MM-dd")) \
             .withColumn("ANIO_VENTA", F.col("ANIO_VENTA").cast(IntegerType())) \
             .withColumn("MES_VENTA", F.col("MES_VENTA").cast(IntegerType())) \
             .withColumn("DIA_VENTA", F.col("DIA_VENTA").cast(IntegerType())) \
             .withColumn("EDS_ACTIVAS", F.col("EDS_ACTIVAS").cast(IntegerType())) \
             .withColumn("NUMERO_DE_VENTAS", F.col("NUMERO_DE_VENTAS").cast(IntegerType())) \
             .withColumn("VEHICULOS_ATENDIDOS", F.col("VEHICULOS_ATENDIDOS").cast(IntegerType())) \
             .withColumn("CANTIDAD_VOLUMEN_SUMINISTRADO", parse_us_number("CANTIDAD_VOLUMEN_SUMINISTRADO")) \
             .withColumn("LATITUD", F.col("LATITUD").cast(DoubleType())) \
             .withColumn("LONGITUD", F.col("LONGITUD").cast(DoubleType())) \
             .withColumn("CODIGO_MUNICIPIO_DANE", F.col("CODIGO_MUNICIPIO_DANE").cast(IntegerType()))

def transform_iea_electricity(df):
    return df.withColumn("YEAR", F.col("YEAR").cast(IntegerType())) \
             .withColumn("MONTH", F.col("MONTH").cast(IntegerType())) \
             .withColumn("VALUE", F.col("VALUE").cast(DoubleType())) \
             .withColumn("share", F.col("share").cast(DoubleType())) \
             .withColumn("yearToDate", F.col("yearToDate").cast(DoubleType())) \
             .withColumn("previousYearToDate", F.col("previousYearToDate").cast(DoubleType()))

def transform_distribuidores_mayorista(df):
    return df.withColumn("fecha_despacho", F.to_date(F.col("fecha_despacho"), "yyyy-MM-dd")) \
             .withColumn("anio_despacho", F.col("anio_despacho").cast(IntegerType())) \
             .withColumn("mes_despacho", F.col("mes_despacho").cast(IntegerType())) \
             .withColumn("dia_despacho", F.col("dia_despacho").cast(IntegerType())) \
             .withColumn("volumen_despachado", F.col("volumen_despachado").cast(DoubleType())) \
             .withColumn("latitud", F.col("latitud").cast(DoubleType())) \
             .withColumn("longitud", F.col("longitud").cast(DoubleType())) \
             .withColumn("codigo_municipio_dane", F.col("codigo_municipio_dane").cast(IntegerType())) \
             .withColumn("codigo_municipio_dane_destino", F.col("codigo_municipio_dane_destino").cast(IntegerType()))

def transform_estaciones_servicio(df):
    return df.withColumn("fecha_despacho", F.to_date(F.col("fecha_despacho"), "yyyy-MM-dd")) \
             .withColumn("anio_despacho", F.col("anio_despacho").cast(IntegerType())) \
             .withColumn("mes_despacho", F.col("mes_despacho").cast(IntegerType())) \
             .withColumn("dia_despacho", F.col("dia_despacho").cast(IntegerType())) \
             .withColumn("volumen_despachado", F.col("volumen_despachado").cast(DoubleType())) \
             .withColumn("latitud", F.col("latitud").cast(DoubleType())) \
             .withColumn("longitud", F.col("longitud").cast(DoubleType())) \
             .withColumn("codigo_municipio_dane", F.col("codigo_municipio_dane").cast(IntegerType())) \
             .withColumn("codigo_municipio_dane_destino", F.col("codigo_municipio_dane_destino").cast(IntegerType()))

TRANSFORMATIONS = {
    "agentes_gncv": transform_agentes_gncv,
    "productores_b100": transform_productores_b100,
    "beneficio_frontera": transform_beneficio_frontera,
    "ventas_gncv": transform_ventas_gncv,
    "iea_electricity": transform_iea_electricity,
    "distribuidores_mayorista": transform_distribuidores_mayorista,
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

    # Write as partitioned Parquet
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
