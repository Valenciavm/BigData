"""
Local PySpark test runner for Raw -> Trusted processing.
Validates transformations using local sample data without requiring
an active AWS Glue cluster.

Usage:
    python run_local_spark.py --dataset agentes_gncv
    python run_local_spark.py --dataset ventas_gncv
    python run_local_spark.py --all
"""

import os
import sys
import argparse

# ─────────────────────────────────────────────────────────────────────────────
# 0. Windows Hadoop Setup (winutils for local I/O)
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HADOOP_HOME = os.path.join(BASE_DIR, ".hadoop")

if os.name == "nt":
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["hadoop.home.dir"] = HADOOP_HOME
    bin_dir = os.path.join(HADOOP_HOME, "bin")
    if bin_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType

PROJECT_DIR = os.path.dirname(BASE_DIR)
LOCAL_DATA_DIR = os.path.join(PROJECT_DIR, "punto-2-fuentes-datos", "datos")
LOCAL_OUTPUT_DIR = os.path.join(BASE_DIR, "output_trusted")

DATASETS_LOCAL = {
    "agentes_gncv": {
        "file": "Consulta_Agentes_de_Gas_Natural_Comprimido_Vehicular_(AUTOMATIZADO)_20261114.csv",
        "format": "csv",
        "partition_by": ["DEPARTAMENTO"]
    },
    "productores_b100": {
        "file": "Productores_Productores_de_B100_y_Etanol_-_Alcohol_Carburante_(AUTOMATIZADO)_20261114.csv",
        "format": "csv",
        "partition_by": ["ANIO_DESPACHO", "MES_DESPACHO"]
    },
    "beneficio_frontera": {
        "file": "Beneficio_de_estaciones_de_servicio_en_zona_de_frontera__Asignación_de_combustibles_líquidos_en_zona_de_frontera_(AUTOMATIZADO)_20261114.csv",
        "format": "csv",
        "partition_by": ["ANIO_BENEFICIO", "MES_BENEFICIO"]
    },
    "ventas_gncv": {
        "file": "Consulta_Ventas_de_Gas_Natural_Comprimido_Vehicular_(AUTOMATIZADO)_20261114.csv",
        "format": "csv",
        "partition_by": ["ANIO_VENTA", "MES_VENTA"]
    }
}

def parse_colombian_number(col_name):
    return F.regexp_replace(
        F.regexp_replace(F.col(col_name), r"\.", ""),
        ",", "."
    ).cast(DoubleType())

def parse_us_number(col_name):
    return F.regexp_replace(F.col(col_name), ",", "").cast(DoubleType())

def clean_common(df):
    for col_name in df.columns:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
    df = df.dropDuplicates()
    df = df.dropna(how="all")
    return df

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

TRANSFORMATIONS = {
    "agentes_gncv": transform_agentes_gncv,
    "productores_b100": transform_productores_b100,
    "beneficio_frontera": transform_beneficio_frontera,
    "ventas_gncv": transform_ventas_gncv
}

def process_dataset(spark, name):
    info = DATASETS_LOCAL[name]
    input_file = os.path.join(LOCAL_DATA_DIR, info["file"])
    output_path = os.path.join(LOCAL_OUTPUT_DIR, name)

    print(f"\n[LOCAL SPARK] Processing dataset: {name}")
    print(f"  Reading: {input_file}")

    df = spark.read.option("header", "true").option("inferSchema", "false").csv(input_file)
    df_clean = clean_common(df)
    df_transformed = TRANSFORMATIONS[name](df_clean)

    print(f"  Resulting schema:")
    df_transformed.printSchema()

    print(f"  Writing Parquet to: {output_path}")
    df_transformed.write.mode("overwrite").partitionBy(*info["partition_by"]).parquet(output_path)
    print(f"  [OK] Successfully completed: {name}")

def main():
    parser = argparse.ArgumentParser(description="Local PySpark processing runner for dataset validation")
    parser.add_argument("--dataset", choices=list(DATASETS_LOCAL.keys()), help="Specific dataset to process")
    parser.add_argument("--all", action="store_true", help="Process all available local datasets")
    args = parser.parse_args()

    if not args.dataset and not args.all:
        parser.print_help()
        sys.exit(1)

    spark = SparkSession.builder \
        .appName("LocalSparkDataProcessing") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    targets = list(DATASETS_LOCAL.keys()) if args.all else [args.dataset]
    for target in targets:
        process_dataset(spark, target)

    spark.stop()

if __name__ == "__main__":
    main()
