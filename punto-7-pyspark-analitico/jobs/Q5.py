
import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# Q5 - MACRO ENERGY TRENDS
# IEA Electricity vs Wholesale Fuel Dispatches vs GNCV
#
# Trusted specification:
#   iea_electricity       -> partition: year
#   distribuidores_mayoristas -> partition: anio_despacho
#   ventas_gncv           -> partition: anio_venta
#
# Glue database: energia_col_trusted
# ============================================================


# ============================================================
# 1. GLUE INITIALIZATION
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

DATABASE = "energia_col_trusted"

OUTPUT_BASE = (
    "s3://big-data-lake-energiacol/"
    "analytics/q5_tendencias_energeticas"
)


# ============================================================
# 2. LOAD TRUSTED TABLES
# ============================================================

def load_table(table_name):
    return (
        glue_context
        .create_dynamic_frame.from_catalog(
            database=DATABASE,
            table_name=table_name
        )
        .toDF()
    )


electricity = load_table("iea_electricity")

dispatches = load_table("distribuidores_mayoristas")

gncv = load_table("ventas_gncv")


# ============================================================
# 3. SCHEMA VALIDATION
# ============================================================

def require_columns(df, columns, dataset):
    missing = [
        column
        for column in columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"{dataset}: missing required columns {missing}. "
            f"Available columns: {df.columns}"
        )


require_columns(
    electricity,
    ["country", "year", "month", "product", "value"],
    "iea_electricity"
)

require_columns(
    dispatches,
    [
        "producto",
        "volumen_despachado",
        "anio_despacho",
        "mes_despacho"
    ],
    "distribuidores_mayoristas"
)

require_columns(
    gncv,
    [
        "cantidad_volumen_suministrado",
        "anio_venta",
        "mes_venta"
    ],
    "ventas_gncv"
)


# ============================================================
# 4. IEA ELECTRICITY DATA
# ============================================================

# The IEA dataset uses:
#   year  -> annual partition
#   month -> monthly observation
#   value -> generation in GWh
#   product -> energy source / product

electricity = (
    electricity
    .withColumn(
        "country_normalized",
        F.upper(F.trim(F.col("country").cast("string")))
    )
    .filter(
        F.col("country_normalized").isin(
            "COLOMBIA",
            "COL"
        )
    )
    .withColumn(
        "anio",
        F.col("year").cast("int")
    )
    .withColumn(
        "mes",
        F.col("month").cast("int")
    )
    .withColumn(
        "indicador_electricidad",
        F.trim(F.col("product").cast("string"))
    )
    .withColumn(
        "generacion_gwh",
        F.col("value").cast("double")
    )
    .filter(
        F.col("anio").isNotNull() &
        F.col("mes").between(1, 12) &
        F.col("indicador_electricidad").isNotNull() &
        F.col("generacion_gwh").isNotNull()
    )
)


# Aggregate by month and energy product.
# Do not sum all products into one total because the IEA
# product dimension may contain different generation categories.

electricity_monthly = (
    electricity
    .groupBy(
        "anio",
        "mes",
        "indicador_electricidad"
    )
    .agg(
        F.sum("generacion_gwh").alias(
            "generacion_electrica_gwh"
        ),
        F.count("*").alias(
            "registros_iea"
        )
    )
)


# ============================================================
# 5. WHOLESALE FUEL DISPATCHES
# ============================================================

dispatches = (
    dispatches
    .withColumn(
        "anio",
        F.col("anio_despacho").cast("int")
    )
    .withColumn(
        "mes",
        F.col("mes_despacho").cast("int")
    )
    .withColumn(
        "producto_normalizado",
        F.upper(
            F.trim(
                F.col("producto").cast("string")
            )
        )
    )
    .withColumn(
        "volumen_despachado",
        F.col("volumen_despachado").cast("double")
    )
    .filter(
        F.col("anio").isNotNull() &
        F.col("mes").between(1, 12) &
        F.col("volumen_despachado").isNotNull() &
        (F.col("volumen_despachado") >= 0)
    )
)


# Exclude explicitly identified biofuel products.
# Keep product-level output so the product classification
# can be inspected before interpreting the total as fossil fuel.

dispatches = dispatches.filter(
    ~F.col("producto_normalizado").rlike(
        r"B100|BIODIESEL|ETANOL|ALCOHOL CARBURANTE"
    )
)


# Monthly total of non-biofuel wholesale dispatches.

dispatches_monthly = (
    dispatches
    .groupBy("anio", "mes")
    .agg(
        F.sum("volumen_despachado").alias(
            "volumen_despachos_no_biocombustibles"
        ),
        F.count("*").alias(
            "registros_despachos"
        )
    )
)


# Product-level monthly dispatches.

dispatches_by_product = (
    dispatches
    .groupBy(
        "anio",
        "mes",
        "producto_normalizado"
    )
    .agg(
        F.sum("volumen_despachado").alias(
            "volumen_despachado"
        )
    )
)


# ============================================================
# 6. GNCV SALES
# ============================================================

gncv = (
    gncv
    .withColumn(
        "anio",
        F.col("anio_venta").cast("int")
    )
    .withColumn(
        "mes",
        F.col("mes_venta").cast("int")
    )
    .withColumn(
        "volumen_gncv",
        F.col("cantidad_volumen_suministrado").cast("double")
    )
    .filter(
        F.col("anio").isNotNull() &
        F.col("mes").between(1, 12) &
        F.col("volumen_gncv").isNotNull() &
        (F.col("volumen_gncv") >= 0)
    )
)


gncv_aggregations = [
    F.sum("volumen_gncv").alias(
        "volumen_ventas_gncv_m3"
    ),
    F.count("*").alias(
        "registros_gncv"
    )
]

if "numero_de_ventas" in gncv.columns:
    gncv_aggregations.append(
        F.sum(
            F.col("numero_de_ventas").cast("double")
        ).alias("numero_ventas_gncv")
    )

if "vehiculos_atendidos" in gncv.columns:
    gncv_aggregations.append(
        F.sum(
            F.col("vehiculos_atendidos").cast("double")
        ).alias("vehiculos_atendidos")
    )


gncv_monthly = (
    gncv
    .groupBy("anio", "mes")
    .agg(*gncv_aggregations)
)


# ============================================================
# 7. BUILD THE MONTHLY MACRO DATASET
# ============================================================

# Full outer join preserves months available in either dataset.
# Missing observations remain NULL, not zero.

macro_monthly = (
    dispatches_monthly
    .join(
        gncv_monthly,
        ["anio", "mes"],
        "full"
    )
)


# ============================================================
# 8. COMBINE MACRO DATA WITH IEA INDICATORS
# ============================================================

macro_by_indicator = (
    electricity_monthly
    .join(
        macro_monthly,
        ["anio", "mes"],
        "inner"
    )
)


# ============================================================
# 9. MONTHLY GROWTH RATES
# ============================================================

electricity_window = (
    Window
    .partitionBy("indicador_electricidad")
    .orderBy("anio", "mes")
)

electricity_monthly = (
    electricity_monthly
    .withColumn(
        "generacion_mes_anterior",
        F.lag("generacion_electrica_gwh").over(
            electricity_window
        )
    )
    .withColumn(
        "crecimiento_electricidad_pct",
        F.when(
            F.col("generacion_mes_anterior") > 0,
            (
                (
                    F.col("generacion_electrica_gwh") -
                    F.col("generacion_mes_anterior")
                ) /
                F.col("generacion_mes_anterior")
            ) * 100
        )
    )
)


macro_window = (
    Window
    .orderBy("anio", "mes")
)

macro_monthly = (
    macro_monthly
    .withColumn(
        "despachos_mes_anterior",
        F.lag(
            "volumen_despachos_no_biocombustibles"
        ).over(macro_window)
    )
    .withColumn(
        "gncv_mes_anterior",
        F.lag("volumen_ventas_gncv_m3").over(
            macro_window
        )
    )
    .withColumn(
        "crecimiento_despachos_pct",
        F.when(
            F.col("despachos_mes_anterior") > 0,
            (
                (
                    F.col("volumen_despachos_no_biocombustibles") -
                    F.col("despachos_mes_anterior")
                ) /
                F.col("despachos_mes_anterior")
            ) * 100
        )
    )
    .withColumn(
        "crecimiento_gncv_pct",
        F.when(
            F.col("gncv_mes_anterior") > 0,
            (
                (
                    F.col("volumen_ventas_gncv_m3") -
                    F.col("gncv_mes_anterior")
                ) /
                F.col("gncv_mes_anterior")
            ) * 100
        )
    )
)


# ============================================================
# 10. SEASONALITY ANALYSIS
# ============================================================

seasonality_fuels = (
    macro_monthly
    .groupBy("mes")
    .agg(
        F.avg(
            "volumen_despachos_no_biocombustibles"
        ).alias(
            "promedio_despachos_no_biocombustibles"
        ),
        F.avg(
            "volumen_ventas_gncv_m3"
        ).alias(
            "promedio_ventas_gncv_m3"
        )
    )
)


seasonality_electricity = (
    electricity_monthly
    .groupBy(
        "mes",
        "indicador_electricidad"
    )
    .agg(
        F.avg(
            "generacion_electrica_gwh"
        ).alias(
            "promedio_generacion_gwh"
        )
    )
)


# ============================================================
# 11. CORRELATION ANALYSIS
# ============================================================

# Calculate correlations separately for each IEA product.
# Only months with valid observations in all variables
# are considered.

correlation_data = (
    macro_by_indicator
    .dropna(
        subset=[
            "generacion_electrica_gwh",
            "volumen_despachos_no_biocombustibles",
            "volumen_ventas_gncv_m3"
        ]
    )
)

correlations = (
    correlation_data
    .groupBy("indicador_electricidad")
    .agg(
        F.count("*").alias(
            "observaciones"
        ),
        F.corr(
            "generacion_electrica_gwh",
            "volumen_despachos_no_biocombustibles"
        ).alias(
            "corr_electricidad_despachos"
        ),
        F.corr(
            "generacion_electrica_gwh",
            "volumen_ventas_gncv_m3"
        ).alias(
            "corr_electricidad_gncv"
        ),
        F.corr(
            "volumen_despachos_no_biocombustibles",
            "volumen_ventas_gncv_m3"
        ).alias(
            "corr_despachos_gncv"
        )
    )
)


# ============================================================
# 12. IDENTIFY MONTHLY PEAKS
# ============================================================

# Historical 90th percentile threshold.
# This identifies unusually high months relative to the
# entire observed period, not necessarily within each year.

macro_monthly = (
    macro_monthly
    .withColumn(
        "percentil_despachos",
        F.percent_rank().over(
            Window.orderBy(
                "volumen_despachos_no_biocombustibles"
            )
        )
    )
    .withColumn(
        "percentil_gncv",
        F.percent_rank().over(
            Window.orderBy(
                "volumen_ventas_gncv_m3"
            )
        )
    )
    .withColumn(
        "pico_despachos",
        F.when(
            F.col("percentil_despachos") >= 0.90,
            True
        ).otherwise(False)
    )
    .withColumn(
        "pico_gncv",
        F.when(
            F.col("percentil_gncv") >= 0.90,
            True
        ).otherwise(False)
    )
)


# ============================================================
# 13. BUILD MONTHLY PRODUCT COMPARISON
# ============================================================

product_comparison = (
    dispatches_by_product
    .join(
        gncv_monthly.select(
            "anio",
            "mes",
            "volumen_ventas_gncv_m3"
        ),
        ["anio", "mes"],
        "left"
    )
)


# ============================================================
# 14. SAVE OUTPUTS TO S3
# ============================================================

def save_parquet(df, path):
    (
        df
        .write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(path)
    )


save_parquet(
    electricity_monthly,
    f"{OUTPUT_BASE}/electricity_monthly/"
)

save_parquet(
    dispatches_monthly,
    f"{OUTPUT_BASE}/dispatches_monthly/"
)

save_parquet(
    dispatches_by_product,
    f"{OUTPUT_BASE}/dispatches_by_product/"
)

save_parquet(
    gncv_monthly,
    f"{OUTPUT_BASE}/gncv_sales_monthly/"
)

save_parquet(
    macro_monthly,
    f"{OUTPUT_BASE}/macro_energy_monthly/"
)

save_parquet(
    macro_by_indicator,
    f"{OUTPUT_BASE}/macro_by_electricity_indicator/"
)

save_parquet(
    seasonality_fuels,
    f"{OUTPUT_BASE}/seasonality_fuels/"
)

save_parquet(
    seasonality_electricity,
    f"{OUTPUT_BASE}/seasonality_electricity/"
)

save_parquet(
    correlations,
    f"{OUTPUT_BASE}/correlations/"
)

save_parquet(
    product_comparison,
    f"{OUTPUT_BASE}/product_comparison/"
)


# ============================================================
# 15. JOB COMPLETION
# ============================================================

print("============================================")
print("Q5 MACRO ENERGY ANALYSIS COMPLETED")
print("============================================")
print(f"Output location: {OUTPUT_BASE}")

job.commit()
