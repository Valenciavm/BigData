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
from pyspark.sql.types import DoubleType, IntegerType, LongType, DateType

PROJECT_DIR = os.path.dirname(BASE_DIR)
LOCAL_DATA_DIR = os.path.join(PROJECT_DIR, "punto-2-fuentes-datos", "datos")
LOCAL_OUTPUT_DIR = os.path.join(BASE_DIR, "output_trusted")

DATASETS_LOCAL = {
    "agentes_gncv": {
        "file": "Consulta_Agentes_de_Gas_Natural_Comprimido_Vehicular_(AUTOMATIZADO)_20261114.csv",
        "format": "csv",
        "partition_by": ["departamento"]
    },
    "productores_b100e": {
        "file": "Productores_Productores_de_B100_y_Etanol_-_Alcohol_Carburante_(AUTOMATIZADO)_20261114.csv",
        "format": "csv",
        "partition_by": ["anio_despacho", "mes_despacho"]
    },
    "beneficios_frontera": {
        "file": "Beneficio_de_estaciones_de_servicio_en_zona_de_frontera__Asignación_de_combustibles_líquidos_en_zona_de_frontera_(AUTOMATIZADO)_20261114.csv",
        "format": "csv",
        "partition_by": ["anio_beneficio", "mes_beneficio"]
    },
    "ventas_gncv": {
        "file": "Consulta_Ventas_de_Gas_Natural_Comprimido_Vehicular_(AUTOMATIZADO)_20261114.csv",
        "format": "csv",
        "partition_by": ["anio_venta", "mes_venta"]
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
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower())
    for col_name in df.columns:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
    df = df.dropDuplicates()
    df = df.dropna(how="all")
    return df

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

TRANSFORMATIONS = {
    "agentes_gncv": transform_agentes_gncv,
    "productores_b100e": transform_productores_b100e,
    "beneficios_frontera": transform_beneficios_frontera,
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
