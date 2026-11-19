
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

AGENTS_TABLE = "agentes_gncv"
SALES_TABLE = "ventas_gncv"

OUTPUT_BASE = (
    "s3://big-data-lake-energiacol/analytics/q3_gncv/"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q3_GNCV_Analysis")


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


def normalize_dane_code(column):
    """
    Trusted specification defines DANE codes as BIGINT.
    Convert to string for joining and grouping.
    """

    return F.col(column).cast("string")


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

agents_raw = load_table(AGENTS_TABLE)
sales_raw = load_table(SALES_TABLE)


# ============================================================
# 4. VALIDATE SCHEMAS
# ============================================================

validate_columns(
    agents_raw,
    [
        "tipo_agente",
        "nombre_comercial",
        "razon_social",
        "estado_agente",
        "departamento",
        "municipio",
        "codigo_municipio_dane",
        "latitud",
        "longitud"
    ],
    AGENTS_TABLE
)

validate_columns(
    sales_raw,
    [
        "anio_venta",
        "mes_venta",
        "codigo_municipio_dane",
        "departamento",
        "municipio",
        "tipo_agente",
        "tipo_de_combustible",
        "eds_activas",
        "numero_de_ventas",
        "vehiculos_atendidos",
        "cantidad_volumen_suministrado"
    ],
    SALES_TABLE
)


# ============================================================
# 5. NORMALIZE AGENT REGISTRY
# ============================================================

agents = (
    agents_raw
    .withColumn(
        "codigo_municipio_dane",
        normalize_dane_code("codigo_municipio_dane")
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
        "tipo_agente",
        normalize_text("tipo_agente")
    )
    .withColumn(
        "estado_agente",
        normalize_text("estado_agente")
    )
    .withColumn(
        "nombre_comercial",
        F.trim(F.col("nombre_comercial").cast("string"))
    )
    .withColumn(
        "razon_social",
        F.trim(F.col("razon_social").cast("string"))
    )
    .withColumn(
        "latitud",
        F.col("latitud").cast(DoubleType())
    )
    .withColumn(
        "longitud",
        F.col("longitud").cast(DoubleType())
    )
)

# Prefer commercial name, then legal name.
# This is a fallback identifier, not a guaranteed unique ID.
agents = agents.withColumn(
    "identificador_agente",
    F.coalesce(
        F.col("razon_social"),
        F.col("nombre_comercial")
    )
)

# Active status is intentionally not inferred from unknown values.
# All registry records are counted in total infrastructure.
agents = agents.withColumn(
    "estado_normalizado",
    F.when(
        F.col("estado_agente").isin(
            "ACTIVO",
            "VIGENTE",
            "OPERANDO",
            "EN OPERACION",
            "EN OPERACIÓN",
            "HABILITADO"
        ),
        "ACTIVO"
    )
    .when(
        F.col("estado_agente").isin(
            "INACTIVO",
            "CANCELADO",
            "SUSPENDIDO",
            "CERRADO"
        ),
        "NO_ACTIVO"
    )
    .otherwise("NO_CLASIFICADO")
)


# ============================================================
# 6. MUNICIPAL INFRASTRUCTURE INDICATORS
# ============================================================

agents_with_code = agents.filter(
    F.col("codigo_municipio_dane").isNotNull()
    & F.col("codigo_municipio_dane") != ""
)

municipal_infrastructure = (
    agents_with_code
    .groupBy(
        "codigo_municipio_dane",
        "departamento",
        "municipio"
    )
    .agg(
        F.count("*").alias("agentes_registrados"),
        F.countDistinct("identificador_agente").alias(
            "agentes_identificados"
        ),
        F.sum(
            F.when(
                F.col("estado_normalizado") == "ACTIVO",
                1
            ).otherwise(0)
        ).alias("agentes_activos"),
        F.sum(
            F.when(
                F.col("estado_normalizado") == "NO_ACTIVO",
                1
            ).otherwise(0)
        ).alias("agentes_no_activos"),
        F.sum(
            F.when(
                F.col("estado_normalizado") == "NO_CLASIFICADO",
                1
            ).otherwise(0)
        ).alias("agentes_estado_no_clasificado"),
        F.countDistinct("tipo_agente").alias(
            "tipos_agente"
        ),
        F.avg("latitud").alias("latitud_promedio"),
        F.avg("longitud").alias("longitud_promedio")
    )
)


# ============================================================
# 7. INFRASTRUCTURE BY AGENT TYPE
# ============================================================

infrastructure_by_type = (
    agents_with_code
    .groupBy(
        "codigo_municipio_dane",
        "departamento",
        "municipio",
        "tipo_agente"
    )
    .agg(
        F.count("*").alias("agentes_registrados"),
        F.sum(
            F.when(
                F.col("estado_normalizado") == "ACTIVO",
                1
            ).otherwise(0)
        ).alias("agentes_activos")
    )
)


# ============================================================
# 8. NORMALIZE MONTHLY SALES
# ============================================================

sales = (
    sales_raw
    .withColumn(
        "anio",
        F.col("anio_venta").cast("int")
    )
    .withColumn(
        "mes",
        F.col("mes_venta").cast("int")
    )
    .withColumn(
        "codigo_municipio_dane",
        normalize_dane_code("codigo_municipio_dane")
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
        "tipo_agente",
        normalize_text("tipo_agente")
    )
    .withColumn(
        "tipo_de_combustible",
        normalize_text("tipo_de_combustible")
    )
    .withColumn(
        "cantidad_volumen_suministrado",
        F.col("cantidad_volumen_suministrado").cast(
            DoubleType()
        )
    )
    .withColumn(
        "numero_de_ventas",
        F.col("numero_de_ventas").cast(DoubleType())
    )
    .withColumn(
        "vehiculos_atendidos",
        F.col("vehiculos_atendidos").cast(DoubleType())
    )
    .withColumn(
        "eds_activas",
        F.col("eds_activas").cast(DoubleType())
    )
)

sales = sales.filter(
    F.col("anio").isNotNull()
    & F.col("mes").between(1, 12)
    & F.col("codigo_municipio_dane").isNotNull()
    & (F.col("codigo_municipio_dane") != "")
)


# ============================================================
# 9. MONTHLY SALES BY MUNICIPALITY
# ============================================================

municipal_sales_monthly = (
    sales
    .groupBy(
        "anio",
        "mes",
        "codigo_municipio_dane",
        "departamento",
        "municipio",
        "tipo_de_combustible"
    )
    .agg(
        F.sum("cantidad_volumen_suministrado").alias(
            "volumen_suministrado"
        ),
        F.sum("numero_de_ventas").alias(
            "numero_ventas"
        ),
        F.sum("vehiculos_atendidos").alias(
            "vehiculos_atendidos"
        ),
        F.max("eds_activas").alias(
            "eds_activas_reportadas"
        ),
        F.count("*").alias(
            "registros_ventas"
        )
    )
)


# ============================================================
# 10. MUNICIPAL SALES + INFRASTRUCTURE
# ============================================================

municipal_indicators = (
    municipal_sales_monthly.alias("s")
    .join(
        municipal_infrastructure.alias("i"),
        on="codigo_municipio_dane",
        how="left"
    )
    .select(
        F.col("s.anio").alias("anio"),
        F.col("s.mes").alias("mes"),
        F.col("s.codigo_municipio_dane").alias(
            "codigo_municipio_dane"
        ),
        F.coalesce(
            F.col("s.departamento"),
            F.col("i.departamento")
        ).alias("departamento"),
        F.coalesce(
            F.col("s.municipio"),
            F.col("i.municipio")
        ).alias("municipio"),
        F.col("s.tipo_de_combustible").alias(
            "tipo_de_combustible"
        ),
        F.col("s.volumen_suministrado").alias(
            "volumen_suministrado"
        ),
        F.col("s.numero_ventas").alias("numero_ventas"),
        F.col("s.vehiculos_atendidos").alias(
            "vehiculos_atendidos"
        ),
        F.col("s.eds_activas_reportadas").alias(
            "eds_activas_reportadas"
        ),
        F.coalesce(
            F.col("i.agentes_registrados"),
            F.lit(0)
        ).alias("agentes_registrados"),
        F.coalesce(
            F.col("i.agentes_identificados"),
            F.lit(0)
        ).alias("agentes_identificados"),
        F.coalesce(
            F.col("i.agentes_activos"),
            F.lit(0)
        ).alias("agentes_activos"),
        F.coalesce(
            F.col("i.agentes_no_activos"),
            F.lit(0)
        ).alias("agentes_no_activos")
    )
    .withColumn(
        "volumen_por_agente_registrado",
        F.when(
            F.col("agentes_registrados") > 0,
            F.col("volumen_suministrado")
            / F.col("agentes_registrados")
        )
    )
    .withColumn(
        "ventas_por_agente_registrado",
        F.when(
            F.col("agentes_registrados") > 0,
            F.col("numero_ventas")
            / F.col("agentes_registrados")
        )
    )
    .withColumn(
        "volumen_por_agente_activo",
        F.when(
            F.col("agentes_activos") > 0,
            F.col("volumen_suministrado")
            / F.col("agentes_activos")
        )
    )
)


# ============================================================
# 11. ANNUAL MUNICIPAL INDICATORS
# ============================================================

municipal_annual = (
    municipal_indicators
    .groupBy(
        "anio",
        "codigo_municipio_dane",
        "departamento",
        "municipio",
        "tipo_de_combustible"
    )
    .agg(
        F.sum("volumen_suministrado").alias(
            "volumen_anual"
        ),
        F.sum("numero_ventas").alias(
            "ventas_anuales"
        ),
        F.sum("vehiculos_atendidos").alias(
            "vehiculos_anuales_atendidos"
        ),
        F.max("agentes_registrados").alias(
            "agentes_registrados"
        ),
        F.max("agentes_activos").alias(
            "agentes_activos"
        )
    )
    .withColumn(
        "volumen_anual_por_agente",
        F.when(
            F.col("agentes_registrados") > 0,
            F.col("volumen_anual")
            / F.col("agentes_registrados")
        )
    )
)


# ============================================================
# 12. INFRASTRUCTURE / SALES CORRELATIONS
# ============================================================

correlation_input = (
    municipal_annual
    .filter(
        F.col("volumen_anual").isNotNull()
        & F.col("agentes_registrados").isNotNull()
    )
)

correlations = (
    correlation_input
    .groupBy("anio", "tipo_de_combustible")
    .agg(
        F.count("*").alias("municipios_observados"),
        F.corr(
            "agentes_registrados",
            "volumen_anual"
        ).alias(
            "correlacion_agentes_volumen"
        ),
        F.corr(
            "agentes_activos",
            "volumen_anual"
        ).alias(
            "correlacion_agentes_activos_volumen"
        ),
        F.corr(
            "agentes_registrados",
            "ventas_anuales"
        ).alias(
            "correlacion_agentes_ventas"
        ),
        F.corr(
            "agentes_registrados",
            "vehiculos_anuales_atendidos"
        ).alias(
            "correlacion_agentes_vehiculos"
        )
    )
)


# ============================================================
# 13. MUNICIPAL INFRASTRUCTURE COVERAGE
# ============================================================

sales_municipalities = (
    municipal_sales_monthly
    .select(
        "codigo_municipio_dane",
        "departamento",
        "municipio"
    )
    .distinct()
)

infrastructure_coverage = (
    sales_municipalities.alias("s")
    .join(
        municipal_infrastructure.alias("i"),
        on="codigo_municipio_dane",
        how="left"
    )
    .select(
        F.col("s.codigo_municipio_dane").alias(
            "codigo_municipio_dane"
        ),
        F.coalesce(
            F.col("s.departamento"),
            F.col("i.departamento")
        ).alias("departamento"),
        F.coalesce(
            F.col("s.municipio"),
            F.col("i.municipio")
        ).alias("municipio"),
        F.coalesce(
            F.col("i.agentes_registrados"),
            F.lit(0)
        ).alias("agentes_registrados"),
        F.coalesce(
            F.col("i.agentes_activos"),
            F.lit(0)
        ).alias("agentes_activos")
    )
    .withColumn(
        "tiene_infraestructura_registrada",
        F.when(
            F.col("agentes_registrados") > 0,
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)


# ============================================================
# 14. AGENT STATUS SUMMARY
# ============================================================

agent_status = (
    agents
    .groupBy(
        "departamento",
        "municipio",
        "tipo_agente",
        "estado_normalizado"
    )
    .agg(
        F.count("*").alias("numero_agentes")
    )
)


# ============================================================
# 15. WRITE ANALYTICAL OUTPUTS
# ============================================================

write_parquet(
    municipal_infrastructure,
    OUTPUT_BASE + "municipal_infrastructure/"
)

write_parquet(
    infrastructure_by_type,
    OUTPUT_BASE + "infrastructure_by_type/"
)

write_parquet(
    municipal_sales_monthly,
    OUTPUT_BASE + "municipal_sales_monthly/"
)

write_parquet(
    municipal_indicators,
    OUTPUT_BASE + "municipal_indicators/"
)

write_parquet(
    municipal_annual,
    OUTPUT_BASE + "municipal_annual/"
)

write_parquet(
    correlations,
    OUTPUT_BASE + "correlations/"
)

write_parquet(
    infrastructure_coverage,
    OUTPUT_BASE + "infrastructure_coverage/"
)

write_parquet(
    agent_status,
    OUTPUT_BASE + "agent_status/"
)


# ============================================================
# 16. DATA QUALITY SUMMARY
# ============================================================

logger.info("========== Q3 DATA QUALITY SUMMARY ==========")

logger.info(
    f"Agents loaded: {agents.count()}"
)

logger.info(
    f"Agents with DANE code: {agents_with_code.count()}"
)

logger.info(
    f"Municipal infrastructure groups: "
    f"{municipal_infrastructure.count()}"
)

logger.info(
    f"Sales records loaded: {sales.count()}"
)

logger.info(
    f"Monthly municipal sales groups: "
    f"{municipal_sales_monthly.count()}"
)

logger.info(
    f"Municipal indicators: {municipal_indicators.count()}"
)

logger.info(
    f"Correlation groups: {correlations.count()}"
)

logger.info("Q3 ANALYSIS COMPLETED SUCCESSFULLY")

job.commit()
