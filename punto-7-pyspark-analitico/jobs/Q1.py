
import sys
import logging

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window


# ============================================================
# 0. CONFIGURATION
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

DATABASE = "energia_col_trusted"

WHOLESALE_TABLE = "distribuidores_mayoristas"
STATIONS_TABLE = "estaciones_servicio"

OUTPUT_BASE = (
    "s3://big-data-lake-energiacol/analytics/q1_concentracion/"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q1_Market_Concentration")


# ============================================================
# 1. GLUE / SPARK INITIALIZATION
# ============================================================

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)


# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================

def load_table(table_name):
    logger.info(f"Loading {DATABASE}.{table_name}")
    return spark.table(f"{DATABASE}.{table_name}")


def validate_columns(df, required_columns, table_name):
    missing = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"{table_name} is missing columns: {missing}"
        )


def normalize_text(column):
    return F.upper(
        F.trim(
            F.col(column).cast("string")
        )
    )


def write_parquet(df, path):
    logger.info(f"Writing: {path}")

    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(path)
    )


# ============================================================
# 3. LOAD TRUSTED DATASETS
# ============================================================

wholesale_raw = load_table(WHOLESALE_TABLE)
stations_raw = load_table(STATIONS_TABLE)


# ============================================================
# 4. VALIDATE SCHEMAS
# ============================================================

validate_columns(
    wholesale_raw,
    [
        "anio_despacho",
        "mes_despacho",
        "tipo_proveedor",
        "departamento_proveedor",
        "municipio_proveedor",
        "tipo_comprador",
        "subtipo_comprador",
        "producto",
        "volumen_despachado"
    ],
    WHOLESALE_TABLE
)

validate_columns(
    stations_raw,
    [
        "anio_despacho",
        "mes_despacho",
        "departamento",
        "municipio",
        "producto",
        "zona_frontera"
    ],
    STATIONS_TABLE
)


# ============================================================
# 5. NORMALIZE WHOLESALE DATA
# ============================================================

wholesale = (
    wholesale_raw
    .withColumn(
        "anio",
        F.col("anio_despacho").cast("int")
    )
    .withColumn(
        "mes",
        F.col("mes_despacho").cast("int")
    )
    .withColumn(
        "tipo_proveedor",
        normalize_text("tipo_proveedor")
    )
    .withColumn(
        "departamento_proveedor",
        normalize_text("departamento_proveedor")
    )
    .withColumn(
        "municipio_proveedor",
        normalize_text("municipio_proveedor")
    )
    .withColumn(
        "tipo_comprador",
        normalize_text("tipo_comprador")
    )
    .withColumn(
        "subtipo_comprador",
        normalize_text("subtipo_comprador")
    )
    .withColumn(
        "producto",
        normalize_text("producto")
    )
    .withColumn(
        "volumen_despachado",
        F.col("volumen_despachado").cast(DoubleType())
    )
)

wholesale = wholesale.filter(
    F.col("anio").isNotNull()
    & F.col("mes").between(1, 12)
    & F.col("volumen_despachado").isNotNull()
    & (F.col("volumen_despachado") >= 0)
    & F.col("producto").isNotNull()
    & (F.col("producto") != "")
    & F.col("tipo_proveedor").isNotNull()
    & (F.col("tipo_proveedor") != "")
)


# ============================================================
# 6. DEFINE THE ANALYSIS UNIT
# ============================================================

# IMPORTANT:
# The current Trusted schema does not include a unique
# company identifier such as NIT.
#
# Therefore, tipo_proveedor is used as a provisional
# grouping dimension. This is NOT firm-level market share.

wholesale = wholesale.withColumn(
    "actor_mercado",
    F.col("tipo_proveedor")
)

wholesale = wholesale.withColumn(
    "nivel_identificacion",
    F.lit("CATEGORIA_TIPO_PROVEEDOR")
)


# ============================================================
# 7. MARKET SHARE BY YEAR / MONTH / PRODUCT
# ============================================================

market_volume = (
    wholesale
    .groupBy(
        "anio",
        "mes",
        "producto"
    )
    .agg(
        F.sum("volumen_despachado").alias(
            "volumen_total_mercado"
        )
    )
)

actor_volume = (
    wholesale
    .groupBy(
        "anio",
        "mes",
        "producto",
        "actor_mercado",
        "nivel_identificacion"
    )
    .agg(
        F.sum("volumen_despachado").alias(
            "volumen_actor"
        ),
        F.count("*").alias(
            "registros_despacho"
        ),
        F.countDistinct("departamento_proveedor").alias(
            "departamentos_origen"
        )
    )
)

market_shares = (
    actor_volume
    .join(
        market_volume,
        on=["anio", "mes", "producto"],
        how="inner"
    )
    .withColumn(
        "participacion_mercado",
        F.when(
            F.col("volumen_total_mercado") > 0,
            F.col("volumen_actor")
            / F.col("volumen_total_mercado")
        )
    )
    .withColumn(
        "participacion_porcentual",
        F.col("participacion_mercado") * 100
    )
)


# ============================================================
# 8. MARKET RANKING
# ============================================================

ranking_window = (
    Window
    .partitionBy("anio", "mes", "producto")
    .orderBy(
        F.desc("volumen_actor"),
        F.asc("actor_mercado")
    )
)

market_shares = (
    market_shares
    .withColumn(
        "ranking_mercado",
        F.row_number().over(ranking_window)
    )
)


# ============================================================
# 9. HHI CALCULATION
# ============================================================

hhi = (
    market_shares
    .groupBy(
        "anio",
        "mes",
        "producto"
    )
    .agg(
        F.sum(
            F.pow(
                F.col("participacion_porcentual"),
                2
            )
        ).alias("hhi"),
        F.countDistinct("actor_mercado").alias(
            "numero_categorias_proveedor"
        ),
        F.max("volumen_total_mercado").alias(
            "volumen_total_mercado"
        )
    )
    .withColumn(
        "nivel_concentracion",
        F.when(
            F.col("hhi") < 1500,
            "BAJA"
        )
        .when(
            F.col("hhi") < 2500,
            "MODERADA"
        )
        .otherwise("ALTA")
    )
    .withColumn(
        "unidad_analisis",
        F.lit("CATEGORIA_TIPO_PROVEEDOR")
    )
)


# ============================================================
# 10. TOP 5 AND TOP 10 MARKET PARTICIPATION
# ============================================================

top_concentration = (
    market_shares
    .groupBy(
        "anio",
        "mes",
        "producto"
    )
    .agg(
        F.sum(
            F.when(
                F.col("ranking_mercado") <= 5,
                F.col("participacion_mercado")
            ).otherwise(0)
        ).alias("participacion_top_5"),
        F.sum(
            F.when(
                F.col("ranking_mercado") <= 10,
                F.col("participacion_mercado")
            ).otherwise(0)
        ).alias("participacion_top_10")
    )
    .withColumn(
        "participacion_top_5_pct",
        F.col("participacion_top_5") * 100
    )
    .withColumn(
        "participacion_top_10_pct",
        F.col("participacion_top_10") * 100
    )
)


# ============================================================
# 11. ANNUAL MARKET SHARES
# ============================================================

annual_actor_volume = (
    wholesale
    .groupBy(
        "anio",
        "producto",
        "actor_mercado",
        "nivel_identificacion"
    )
    .agg(
        F.sum("volumen_despachado").alias(
            "volumen_actor"
        )
    )
)

annual_market_volume = (
    wholesale
    .groupBy(
        "anio",
        "producto"
    )
    .agg(
        F.sum("volumen_despachado").alias(
            "volumen_total_mercado"
        )
    )
)

annual_market_shares = (
    annual_actor_volume
    .join(
        annual_market_volume,
        on=["anio", "producto"],
        how="inner"
    )
    .withColumn(
        "participacion_mercado",
        F.when(
            F.col("volumen_total_mercado") > 0,
            F.col("volumen_actor")
            / F.col("volumen_total_mercado")
        )
    )
    .withColumn(
        "participacion_porcentual",
        F.col("participacion_mercado") * 100
    )
)

annual_hhi = (
    annual_market_shares
    .groupBy("anio", "producto")
    .agg(
        F.sum(
            F.pow(
                F.col("participacion_porcentual"),
                2
            )
        ).alias("hhi"),
        F.countDistinct("actor_mercado").alias(
            "numero_categorias_proveedor"
        ),
        F.max("volumen_total_mercado").alias(
            "volumen_total_mercado"
        )
    )
    .withColumn(
        "nivel_concentracion",
        F.when(F.col("hhi") < 1500, "BAJA")
        .when(F.col("hhi") < 2500, "MODERADA")
        .otherwise("ALTA")
    )
)


# ============================================================
# 12. MARKET SEGMENTATION BY BUYER TYPE
# ============================================================

buyer_market = (
    wholesale
    .groupBy(
        "anio",
        "mes",
        "producto",
        "tipo_comprador",
        "subtipo_comprador"
    )
    .agg(
        F.sum("volumen_despachado").alias(
            "volumen_despachado"
        ),
        F.count("*").alias(
            "registros_despacho"
        )
    )
)

buyer_market_window = (
    Window
    .partitionBy(
        "anio",
        "mes",
        "producto"
    )
)

buyer_market = (
    buyer_market
    .withColumn(
        "volumen_total",
        F.sum("volumen_despachado").over(
            buyer_market_window
        )
    )
    .withColumn(
        "participacion_comprador",
        F.when(
            F.col("volumen_total") > 0,
            F.col("volumen_despachado")
            / F.col("volumen_total")
        )
    )
)


# ============================================================
# 13. STATION NETWORK ANALYSIS
# ============================================================

validate_columns(
    stations_raw,
    [
        "anio_despacho",
        "mes_despacho",
        "departamento",
        "municipio",
        "producto",
        "zona_frontera"
    ],
    STATIONS_TABLE
)

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
        "departamento",
        normalize_text("departamento")
    )
    .withColumn(
        "municipio",
        normalize_text("municipio")
    )
    .withColumn(
        "producto",
        normalize_text("producto")
    )
    .withColumn(
        "zona_frontera",
        normalize_text("zona_frontera")
    )
)

stations = stations.filter(
    F.col("anio").isNotNull()
    & F.col("mes").between(1, 12)
)

station_network = (
    stations
    .groupBy(
        "anio",
        "mes",
        "departamento",
        "municipio",
        "zona_frontera"
    )
    .agg(
        F.count("*").alias(
            "registros_estacion"
        ),
        F.countDistinct("producto").alias(
            "productos_registrados"
        )
    )
)


# ============================================================
# 14. WHOLESALE GEOGRAPHIC DISTRIBUTION
# ============================================================

wholesale_geography = (
    wholesale
    .groupBy(
        "anio",
        "mes",
        "producto",
        "departamento_proveedor",
        "municipio_proveedor"
    )
    .agg(
        F.sum("volumen_despachado").alias(
            "volumen_origen"
        ),
        F.count("*").alias(
            "registros_despacho"
        )
    )
)


# ============================================================
# 15. COMBINE CONCENTRATION INDICATORS
# ============================================================

concentration_summary = (
    hhi.alias("h")
    .join(
        top_concentration.alias("t"),
        on=["anio", "mes", "producto"],
        how="left"
    )
    .select(
        "anio",
        "mes",
        "producto",
        "hhi",
        "nivel_concentracion",
        "numero_categorias_proveedor",
        "volumen_total_mercado",
        "participacion_top_5",
        "participacion_top_10",
        "participacion_top_5_pct",
        "participacion_top_10_pct",
        "unidad_analisis"
    )
)


# ============================================================
# 16. WRITE ANALYTICAL OUTPUTS
# ============================================================

write_parquet(
    market_shares,
    OUTPUT_BASE + "market_shares/"
)

write_parquet(
    hhi,
    OUTPUT_BASE + "hhi/"
)

write_parquet(
    top_concentration,
    OUTPUT_BASE + "top_concentration/"
)

write_parquet(
    annual_market_shares,
    OUTPUT_BASE + "annual_market_shares/"
)

write_parquet(
    annual_hhi,
    OUTPUT_BASE + "annual_hhi/"
)

write_parquet(
    buyer_market,
    OUTPUT_BASE + "buyer_segmentation/"
)

write_parquet(
    station_network,
    OUTPUT_BASE + "station_network/"
)

write_parquet(
    wholesale_geography,
    OUTPUT_BASE + "wholesale_geography/"
)

write_parquet(
    concentration_summary,
    OUTPUT_BASE + "concentration_summary/"
)


# ============================================================
# 17. DATA QUALITY SUMMARY
# ============================================================

logger.info("========== Q1 DATA QUALITY SUMMARY ==========")

logger.info(
    f"Wholesale records: {wholesale.count()}"
)

logger.info(
    f"Market share groups: {market_shares.count()}"
)

logger.info(
    f"HHI groups: {hhi.count()}"
)

logger.info(
    f"Annual HHI groups: {annual_hhi.count()}"
)

logger.info(
    f"Station network groups: {station_network.count()}"
)

logger.info(
    f"Wholesale geographic groups: "
    f"{wholesale_geography.count()}"
)

logger.info(
    "IMPORTANT: HHI is calculated using tipo_proveedor "
    "as a provisional market actor, not individual companies."
)

logger.info("Q1 ANALYSIS COMPLETED SUCCESSFULLY")

job.commit()
