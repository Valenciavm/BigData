
import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# Q4 - BIOFUELS TRACEABILITY
# B100 and Ethanol
#
# Trusted database: energia_col_trusted
#
# Input tables:
#   productores_b100e
#   distribuidores_mayoristas
#
# Partitioning:
#   productores_b100e      -> anio_despacho
#   distribuidores_mayoristas -> anio_despacho
#
# Output:
#   s3://big-data-lake-energiacol/analytics/q4_biocombustibles/
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
    "analytics/q4_biocombustibles"
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


producers = load_table("productores_b100e")

dispatches = load_table("distribuidores_mayoristas")


# ============================================================
# 3. VALIDATE SCHEMAS
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
    producers,
    [
        "anio_despacho",
        "mes_despacho",
        "producto",
        "volumen_despachado",
        "departamento",
        "codigo_municipio_dane_proveedor",
        "departamento_proveedor"
    ],
    "productores_b100e"
)

require_columns(
    dispatches,
    [
        "anio_despacho",
        "mes_despacho",
        "producto",
        "volumen_despachado",
        "departamento"
    ],
    "distribuidores_mayoristas"
)


# ============================================================
# 4. STANDARDIZE PRODUCT CLASSIFICATION
# ============================================================

def classify_biofuel(column_name):

    product = F.upper(
        F.trim(
            F.regexp_replace(
                F.col(column_name).cast("string"),
                r"\s+",
                " "
            )
        )
    )

    return (
        F.when(
            product.rlike(
                r"B100|BIODIESEL"
            ),
            F.lit("B100")
        )
        .when(
            product.rlike(
                r"ETANOL|ALCOHOL CARBURANTE"
            ),
            F.lit("ETANOL")
        )
        .otherwise(
            F.lit("OTRO_NO_CLASIFICADO")
        )
    )


producers = (
    producers
    .withColumn(
        "producto_original",
        F.col("producto").cast("string")
    )
    .withColumn(
        "biocombustible",
        classify_biofuel("producto")
    )
)

dispatches = (
    dispatches
    .withColumn(
        "producto_original",
        F.col("producto").cast("string")
    )
    .withColumn(
        "biocombustible",
        classify_biofuel("producto")
    )
)


# ============================================================
# 5. STANDARDIZE YEAR, MONTH AND VOLUME
# ============================================================

def normalize_common_columns(df):

    return (
        df
        .withColumn(
            "anio",
            F.col("anio_despacho").cast("int")
        )
        .withColumn(
            "mes",
            F.col("mes_despacho").cast("int")
        )
        .withColumn(
            "volumen",
            F.col("volumen_despachado").cast("double")
        )
        .withColumn(
            "departamento",
            F.upper(
                F.trim(
                    F.col("departamento").cast("string")
                )
            )
        )
        .filter(
            F.col("anio").isNotNull() &
            F.col("mes").between(1, 12) &
            F.col("volumen").isNotNull() &
            (F.col("volumen") >= 0)
        )
        .withColumn(
            "departamento",
            F.coalesce(
                F.col("departamento"),
                F.lit("NO_ESPECIFICADO")
            )
        )
    )


producers = normalize_common_columns(producers)

dispatches = normalize_common_columns(dispatches)


# ============================================================
# 6. PRODUCT CLASSIFICATION AUDIT
# ============================================================

# This output allows us to inspect which product labels
# were classified as B100, ethanol or other.

producer_product_audit = (
    producers
    .groupBy(
        "producto_original",
        "biocombustible"
    )
    .agg(
        F.count("*").alias("registros"),
        F.sum("volumen").alias("volumen_total")
    )
    .withColumn(
        "fuente",
        F.lit("PRODUCTORES_B100E")
    )
)

dispatch_product_audit = (
    dispatches
    .groupBy(
        "producto_original",
        "biocombustible"
    )
    .agg(
        F.count("*").alias("registros"),
        F.sum("volumen").alias("volumen_total")
    )
    .withColumn(
        "fuente",
        F.lit("DISTRIBUIDORES_MAYORISTAS")
    )
)

product_classification_audit = (
    producer_product_audit
    .unionByName(dispatch_product_audit)
)


# ============================================================
# 7. FILTER B100 AND ETHANOL
# ============================================================

producers_bio = producers.filter(
    F.col("biocombustible").isin("B100", "ETANOL")
)

dispatches_bio = dispatches.filter(
    F.col("biocombustible").isin("B100", "ETANOL")
)


# ============================================================
# 8. PRODUCER MONTHLY DISPATCHES
# ============================================================

# Producer records represent the dispatches reported by
# biofuel producers.

producer_monthly = (
    producers_bio
    .groupBy(
        "anio",
        "mes",
        "biocombustible"
    )
    .agg(
        F.sum("volumen").alias(
            "volumen_productores"
        ),
        F.count("*").alias(
            "registros_productores"
        )
    )
)


# ============================================================
# 9. WHOLESALE MONTHLY DISPATCHES
# ============================================================

# Wholesale records represent downstream dispatches
# registered by wholesale distributors.

wholesale_monthly = (
    dispatches_bio
    .groupBy(
        "anio",
        "mes",
        "biocombustible"
    )
    .agg(
        F.sum("volumen").alias(
            "volumen_mayoristas"
        ),
        F.count("*").alias(
            "registros_mayoristas"
        )
    )
)


# ============================================================
# 10. NATIONAL MONTHLY COMPARISON
# ============================================================

national_comparison = (
    producer_monthly
    .join(
        wholesale_monthly,
        [
            "anio",
            "mes",
            "biocombustible"
        ],
        "full"
    )
    .withColumn(
        "diferencia_volumen",
        F.when(
            F.col("volumen_productores").isNotNull() &
            F.col("volumen_mayoristas").isNotNull(),
            F.col("volumen_mayoristas") -
            F.col("volumen_productores")
        )
    )
    .withColumn(
        "razon_mayoristas_productores",
        F.when(
            F.col("volumen_productores") > 0,
            F.col("volumen_mayoristas") /
            F.col("volumen_productores")
        )
    )
    .withColumn(
        "diferencia_porcentual",
        F.when(
            F.col("volumen_productores") > 0,
            (
                F.col("diferencia_volumen") /
                F.col("volumen_productores")
            ) * 100
        )
    )
    .withColumn(
        "anio_mes",
        F.concat_ws(
            "-",
            F.col("anio").cast("string"),
            F.lpad(
                F.col("mes").cast("string"),
                2,
                "0"
            )
        )
    )
)


# ============================================================
# 11. MONTHLY TRENDS
# ============================================================

bio_window = (
    Window
    .partitionBy("biocombustible")
    .orderBy("anio", "mes")
)

national_comparison = (
    national_comparison
    .withColumn(
        "productores_mes_anterior",
        F.lag("volumen_productores").over(bio_window)
    )
    .withColumn(
        "mayoristas_mes_anterior",
        F.lag("volumen_mayoristas").over(bio_window)
    )
    .withColumn(
        "crecimiento_productores_pct",
        F.when(
            F.col("productores_mes_anterior") > 0,
            (
                (
                    F.col("volumen_productores") -
                    F.col("productores_mes_anterior")
                ) /
                F.col("productores_mes_anterior")
            ) * 100
        )
    )
    .withColumn(
        "crecimiento_mayoristas_pct",
        F.when(
            F.col("mayoristas_mes_anterior") > 0,
            (
                (
                    F.col("volumen_mayoristas") -
                    F.col("mayoristas_mes_anterior")
                ) /
                F.col("mayoristas_mes_anterior")
            ) * 100
        )
    )
)


# ============================================================
# 12. DEPARTMENT-LEVEL COMPARISON
# ============================================================

# Both datasets use departamento as destination department.
# This is a comparison of reported dispatch volumes by
# destination department, not a physical batch-level trace.

producer_department = (
    producers_bio
    .groupBy(
        "anio",
        "mes",
        "biocombustible",
        "departamento"
    )
    .agg(
        F.sum("volumen").alias(
            "volumen_productores"
        ),
        F.count("*").alias(
            "registros_productores"
        )
    )
)

wholesale_department = (
    dispatches_bio
    .groupBy(
        "anio",
        "mes",
        "biocombustible",
        "departamento"
    )
    .agg(
        F.sum("volumen").alias(
            "volumen_mayoristas"
        ),
        F.count("*").alias(
            "registros_mayoristas"
        )
    )
)

department_comparison = (
    producer_department
    .join(
        wholesale_department,
        [
            "anio",
            "mes",
            "biocombustible",
            "departamento"
        ],
        "full"
    )
    .withColumn(
        "diferencia_volumen",
        F.when(
            F.col("volumen_productores").isNotNull() &
            F.col("volumen_mayoristas").isNotNull(),
            F.col("volumen_mayoristas") -
            F.col("volumen_productores")
        )
    )
    .withColumn(
        "razon_mayoristas_productores",
        F.when(
            F.col("volumen_productores") > 0,
            F.col("volumen_mayoristas") /
            F.col("volumen_productores")
        )
    )
    .withColumn(
        "diferencia_porcentual",
        F.when(
            F.col("volumen_productores") > 0,
            (
                F.col("diferencia_volumen") /
                F.col("volumen_productores")
            ) * 100
        )
    )
)


# ============================================================
# 13. PRODUCER ORIGIN ANALYSIS
# ============================================================

# Use the new origin DANE code specified for productores_b100e.
# This provides an origin-level view of reported producer
# dispatches, separate from the destination comparison.

producer_origin = (
    producers_bio
    .withColumn(
        "codigo_municipio_dane_proveedor",
        F.col(
            "codigo_municipio_dane_proveedor"
        ).cast("long")
    )
    .withColumn(
        "departamento_proveedor",
        F.upper(
            F.trim(
                F.col("departamento_proveedor").cast("string")
            )
        )
    )
    .groupBy(
        "anio",
        "mes",
        "biocombustible",
        "departamento_proveedor",
        "codigo_municipio_dane_proveedor"
    )
    .agg(
        F.sum("volumen").alias(
            "volumen_origen_productores"
        ),
        F.count("*").alias(
            "registros_origen"
        )
    )
)


# ============================================================
# 14. DEPARTMENTAL CONTRIBUTION
# ============================================================

department_window = Window.partitionBy(
    "anio",
    "mes",
    "biocombustible"
)

producer_geographic_contribution = (
    producer_department
    .withColumn(
        "total_departamental_periodo",
        F.sum("volumen_productores").over(
            department_window
        )
    )
    .withColumn(
        "participacion_departamental_pct",
        F.when(
            F.col("total_departamental_periodo") > 0,
            (
                F.col("volumen_productores") /
                F.col("total_departamental_periodo")
            ) * 100
        )
    )
)


# ============================================================
# 15. PRODUCT SUMMARY
# ============================================================

product_summary = (
    national_comparison
    .groupBy("biocombustible")
    .agg(
        F.sum("volumen_productores").alias(
            "total_volumen_productores"
        ),
        F.sum("volumen_mayoristas").alias(
            "total_volumen_mayoristas"
        ),
        F.avg("diferencia_porcentual").alias(
            "diferencia_porcentual_promedio"
        ),
        F.count("*").alias(
            "periodos_registrados"
        ),
        F.sum(
            F.when(
                F.col("volumen_productores").isNotNull() &
                F.col("volumen_mayoristas").isNotNull(),
                1
            ).otherwise(0)
        ).alias(
            "periodos_con_datos_en_ambas_fuentes"
        )
    )
)


# ============================================================
# 16. SAVE OUTPUTS TO S3
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
    national_comparison,
    f"{OUTPUT_BASE}/national_monthly_comparison/"
)

save_parquet(
    department_comparison,
    f"{OUTPUT_BASE}/department_comparison/"
)

save_parquet(
    producer_monthly,
    f"{OUTPUT_BASE}/producer_monthly/"
)

save_parquet(
    wholesale_monthly,
    f"{OUTPUT_BASE}/wholesale_monthly/"
)

save_parquet(
    producer_origin,
    f"{OUTPUT_BASE}/producer_origin/"
)

save_parquet(
    producer_geographic_contribution,
    f"{OUTPUT_BASE}/geographic_contribution/"
)

save_parquet(
    product_summary,
    f"{OUTPUT_BASE}/product_summary/"
)

save_parquet(
    product_classification_audit,
    f"{OUTPUT_BASE}/product_classification_audit/"
)


# ============================================================
# 17. COMPLETION
# ============================================================

print("============================================")
print("Q4 BIOFUELS TRACEABILITY COMPLETED")
print("============================================")
print(f"Output location: {OUTPUT_BASE}")

job.commit()
