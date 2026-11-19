
import sys
import re
import logging

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType


# ============================================================
# 0. CONFIGURATION
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

DATABASE = "energia_col_trusted"

STATIONS_TABLE = "estaciones_servicio"
BENEFITS_TABLE = "beneficios_frontera"

OUTPUT_BASE = (
    "s3://big-data-lake-energiacol/analytics/q2_frontera/"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q2_Submission_Analysis")


# ============================================================
# 1. GLUE / SPARK INITIALIZATION
# ============================================================

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================

def load_table(table_name):
    logger.info(f"Loading Glue table: {DATABASE}.{table_name}")

    return (
        spark.table(f"{DATABASE}.{table_name}")
    )


def validate_columns(df, required_columns, table_name):
    missing = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"{table_name} is missing required columns: {missing}"
        )


def normalize_text(column):
    return F.upper(
        F.trim(
            F.col(column).cast("string")
        )
    )


def normalize_municipality_code(column):
    """
    Normalize DANE municipality codes.

    The Trusted specification currently defines these fields
    as BIGINT, so leading zeroes may already have been removed.
    """

    return F.col(column).cast("string")


def normalize_border_flag(column):
    """
    Convert different representations of border-zone status
    into a consistent SI / NO classification.
    """

    value = normalize_text(column)

    return (
        F.when(
            value.isin("SI", "SÍ", "TRUE", "1", "YES", "Y"),
            F.lit("SI")
        )
        .when(
            value.isin("NO", "FALSE", "0", "N"),
            F.lit("NO")
        )
        .otherwise(F.lit("NO_DEFINIDO"))
    )


def write_parquet(df, output_path):
    logger.info(f"Writing output to {output_path}")

    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(output_path)
    )


def add_geography_key(df, code_column):
    """
    Prefer DANE code as the municipality identifier.
    Fall back to normalized department + municipality.
    """

    return (
        df
        .withColumn(
            "codigo_municipio_dane",
            normalize_municipality_code(code_column)
        )
        .withColumn(
            "departamento",
            F.upper(F.trim(F.col("departamento").cast("string")))
        )
        .withColumn(
            "municipio",
            F.upper(F.trim(F.col("municipio").cast("string")))
        )
        .withColumn(
            "municipality_key",
            F.when(
                F.col("codigo_municipio_dane").isNotNull(),
                F.concat(
                    F.lit("DANE_"),
                    F.col("codigo_municipio_dane")
                )
            ).otherwise(
                F.concat_ws(
                    "_",
                    F.col("departamento"),
                    F.col("municipio")
                )
            )
        )
    )


# ============================================================
# 3. LOAD TRUSTED DATASETS
# ============================================================

stations_raw = load_table(STATIONS_TABLE)
benefits_raw = load_table(BENEFITS_TABLE)


# ============================================================
# 4. VALIDATE INPUT SCHEMAS
# ============================================================

validate_columns(
    stations_raw,
    [
        "anio_despacho",
        "mes_despacho",
        "codigo_municipio_dane_destino",
        "departamento",
        "municipio",
        "producto",
        "volumen_despachado",
        "zona_frontera"
    ],
    STATIONS_TABLE
)

validate_columns(
    benefits_raw,
    [
        "anio_beneficio",
        "mes_beneficio",
        "codigo_municipio_dane_destino",
        "departamento",
        "municipio",
        "tipo_beneficio",
        "beneficio"
    ],
    BENEFITS_TABLE
)


# ============================================================
# 5. NORMALIZE STATION DISPATCH DATA
# ============================================================

stations = (
    stations_raw
    .withColumn(
        "anio",
        F.col("anio_despacho").cast("int")
    )
    .withColumn(
        "mes",
        F.col("mes_despacho").cast("int")
    )
    .withColumn(
        "producto",
        normalize_text("producto")
    )
    .withColumn(
        "volumen_despachado",
        F.col("volumen_despachado").cast(DoubleType())
    )
    .withColumn(
        "zona_frontera_normalizada",
        normalize_border_flag("zona_frontera")
    )
)

stations = add_geography_key(
    stations,
    "codigo_municipio_dane_destino"
)

stations = stations.filter(
    F.col("anio").isNotNull()
    & F.col("mes").between(1, 12)
    & F.col("volumen_despachado").isNotNull()
    & (F.col("volumen_despachado") >= 0)
)


# ============================================================
# 6. MONTHLY DISPATCHES BY MUNICIPALITY
# ============================================================

municipal_dispatches = (
    stations
    .groupBy(
        "anio",
        "mes",
        "municipality_key",
        "codigo_municipio_dane",
        "departamento",
        "municipio",
        "zona_frontera_normalizada",
        "producto"
    )
    .agg(
        F.sum("volumen_despachado").alias(
            "volumen_total_despachado"
        ),
        F.count("*").alias("numero_registros_despacho")
    )
    .withColumn(
        "es_frontera",
        F.when(
            F.col("zona_frontera_normalizada") == "SI",
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)


# ============================================================
# 7. BORDER VS NON-BORDER COMPARISON
# ============================================================

border_comparison = (
    municipal_dispatches
    .filter(
        F.col("zona_frontera_normalizada").isin("SI", "NO")
    )
    .groupBy(
        "anio",
        "mes",
        "producto",
        "zona_frontera_normalizada"
    )
    .agg(
        F.sum("volumen_total_despachado").alias(
            "volumen_total"
        ),
        F.countDistinct("municipality_key").alias(
            "municipios"
        ),
        F.sum("numero_registros_despacho").alias(
            "registros_despacho"
        )
    )
)

border_pivot = (
    border_comparison
    .groupBy("anio", "mes", "producto")
    .pivot("zona_frontera_normalizada", ["SI", "NO"])
    .agg(
        F.first("volumen_total").alias("volumen_total"),
        F.first("municipios").alias("municipios")
    )
)

# Pivot-generated column names:
# SI_volumen_total, NO_volumen_total,
# SI_municipios, NO_municipios.

border_comparison_final = (
    border_pivot
    .withColumnRenamed("SI_volumen_total", "volumen_frontera")
    .withColumnRenamed("NO_volumen_total", "volumen_no_frontera")
    .withColumnRenamed("SI_municipios", "municipios_frontera")
    .withColumnRenamed("NO_municipios", "municipios_no_frontera")
    .withColumn(
        "participacion_frontera",
        F.when(
            (
                F.col("volumen_frontera")
                + F.col("volumen_no_frontera")
            ) > 0,
            F.col("volumen_frontera") / (
                F.col("volumen_frontera")
                + F.col("volumen_no_frontera")
            )
        )
    )
    .withColumn(
        "diferencia_volumen",
        F.col("volumen_frontera")
        - F.col("volumen_no_frontera")
    )
    .withColumn(
        "volumen_promedio_por_municipio_frontera",
        F.when(
            F.col("municipios_frontera") > 0,
            F.col("volumen_frontera")
            / F.col("municipios_frontera")
        )
    )
    .withColumn(
        "volumen_promedio_por_municipio_no_frontera",
        F.when(
            F.col("municipios_no_frontera") > 0,
            F.col("volumen_no_frontera")
            / F.col("municipios_no_frontera")
        )
    )
)


# ============================================================
# 8. ANOMALY DETECTION IN BORDER MUNICIPALITIES
# ============================================================

border_municipal_dispatches = (
    municipal_dispatches
    .filter(
        F.col("zona_frontera_normalizada") == "SI"
    )
)

comparison_window = (
    Window
    .partitionBy("anio", "mes", "producto")
)

border_anomalies = (
    border_municipal_dispatches
    .withColumn(
        "promedio_frontera",
        F.avg("volumen_total_despachado").over(
            comparison_window
        )
    )
    .withColumn(
        "desviacion_estandar_frontera",
        F.stddev_pop("volumen_total_despachado").over(
            comparison_window
        )
    )
    .withColumn(
        "z_score",
        F.when(
            F.col("desviacion_estandar_frontera") > 0,
            (
                F.col("volumen_total_despachado")
                - F.col("promedio_frontera")
            ) / F.col("desviacion_estandar_frontera")
        )
    )
    .withColumn(
        "desviacion_porcentual",
        F.when(
            F.col("promedio_frontera") > 0,
            (
                F.col("volumen_total_despachado")
                - F.col("promedio_frontera")
            ) / F.col("promedio_frontera")
        )
    )
    .withColumn(
        "clasificacion_anomalia",
        F.when(
            F.col("z_score") >= 2,
            "VOLUMEN_INUSUALMENTE_ALTO"
        )
        .when(
            F.col("z_score") <= -2,
            "VOLUMEN_INUSUALMENTE_BAJO"
        )
        .otherwise("DENTRO_DEL_RANGO")
    )
    .withColumn(
        "municipios_comparados",
        F.count("*").over(comparison_window)
    )
)


# ============================================================
# 9. NORMALIZE BORDER BENEFITS DATA
# ============================================================

benefits = (
    benefits_raw
    .withColumn(
        "anio",
        F.col("anio_beneficio").cast("int")
    )
    .withColumn(
        "mes",
        F.col("mes_beneficio").cast("int")
    )
    .withColumn(
        "tipo_beneficio",
        normalize_text("tipo_beneficio")
    )
    .withColumn(
        "beneficio",
        F.col("beneficio").cast(DoubleType())
    )
)

benefits = add_geography_key(
    benefits,
    "codigo_municipio_dane_destino"
)

benefits = benefits.filter(
    F.col("anio").isNotNull()
    & F.col("mes").between(1, 12)
)

# Keep records with null benefit values for auditing,
# but exclude them from the sum.
benefits_monthly = (
    benefits
    .groupBy(
        "anio",
        "mes",
        "municipality_key",
        "codigo_municipio_dane",
        "departamento",
        "municipio",
        "tipo_beneficio"
    )
    .agg(
        F.sum("beneficio").alias("beneficio_total"),
        F.count("*").alias("numero_registros_beneficio"),
        F.count("beneficio").alias("registros_con_valor")
    )
)


# ============================================================
# 10. JOIN BENEFITS WITH MUNICIPAL DISPATCHES
# ============================================================

dispatches_for_join = (
    municipal_dispatches
    .groupBy(
        "anio",
        "mes",
        "municipality_key",
        "codigo_municipio_dane",
        "departamento",
        "municipio",
        "zona_frontera_normalizada"
    )
    .agg(
        F.sum("volumen_total_despachado").alias(
            "volumen_total_despachado"
        ),
        F.countDistinct("producto").alias(
            "productos_despachados"
        )
    )
)

benefits_with_dispatches = (
    benefits_monthly.alias("b")
    .join(
        dispatches_for_join.alias("d"),
        on=[
            F.col("b.anio") == F.col("d.anio"),
            F.col("b.mes") == F.col("d.mes"),
            F.col("b.municipality_key")
            == F.col("d.municipality_key")
        ],
        how="left"
    )
    .select(
        F.col("b.anio").alias("anio"),
        F.col("b.mes").alias("mes"),
        F.col("b.municipality_key").alias("municipality_key"),
        F.col("b.codigo_municipio_dane").alias(
            "codigo_municipio_dane"
        ),
        F.coalesce(
            F.col("d.departamento"),
            F.col("b.departamento")
        ).alias("departamento"),
        F.coalesce(
            F.col("d.municipio"),
            F.col("b.municipio")
        ).alias("municipio"),
        F.col("b.tipo_beneficio").alias("tipo_beneficio"),
        F.col("b.beneficio_total").alias("beneficio_total"),
        F.col("b.numero_registros_beneficio").alias(
            "numero_registros_beneficio"
        ),
        F.col("b.registros_con_valor").alias(
            "registros_con_valor"
        ),
        F.col("d.zona_frontera_normalizada").alias(
            "zona_frontera"
        ),
        F.col("d.volumen_total_despachado").alias(
            "volumen_total_despachado"
        ),
        F.col("d.productos_despachados").alias(
            "productos_despachados"
        )
    )
)


# ============================================================
# 11. BENEFIT DISTRIBUTION BY MUNICIPALITY
# ============================================================

benefits_by_municipality = (
    benefits_monthly
    .groupBy(
        "anio",
        "mes",
        "municipality_key",
        "codigo_municipio_dane",
        "departamento",
        "municipio"
    )
    .agg(
        F.sum("beneficio_total").alias(
            "beneficio_total_municipio"
        ),
        F.sum("numero_registros_beneficio").alias(
            "numero_registros_beneficio"
        ),
        F.countDistinct("tipo_beneficio").alias(
            "tipos_beneficio"
        )
    )
)


# ============================================================
# 12. WRITE ANALYTICAL OUTPUTS
# ============================================================

write_parquet(
    municipal_dispatches,
    OUTPUT_BASE + "municipal_dispatches/"
)

write_parquet(
    border_comparison_final,
    OUTPUT_BASE + "border_vs_non_border/"
)

write_parquet(
    border_anomalies,
    OUTPUT_BASE + "border_anomalies/"
)

write_parquet(
    benefits_monthly,
    OUTPUT_BASE + "benefits_monthly/"
)

write_parquet(
    benefits_by_municipality,
    OUTPUT_BASE + "benefits_by_municipality/"
)

write_parquet(
    benefits_with_dispatches,
    OUTPUT_BASE + "benefits_and_dispatches/"
)


# ============================================================
# 13. DATA QUALITY SUMMARY
# ============================================================

logger.info("========== Q2 DATA QUALITY SUMMARY ==========")

logger.info(
    f"Station records loaded: {stations.count()}"
)

logger.info(
    f"Municipal dispatch groups: {municipal_dispatches.count()}"
)

logger.info(
    f"Border anomaly groups: {border_anomalies.count()}"
)

logger.info(
    f"Benefits records loaded: {benefits.count()}"
)

logger.info(
    f"Benefits monthly groups: {benefits_monthly.count()}"
)

logger.info("Q2 ANALYSIS COMPLETED SUCCESSFULLY")

job.commit()
