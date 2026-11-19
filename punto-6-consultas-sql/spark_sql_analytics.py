#!/usr/bin/env python3
"""
==============================================================================
PUNTO 6: PROCESAMIENTO ANALÍTICO DESCRIPTIVO CON SPARK SQL
Proyecto: Data Lakehouse Sector Energético Colombiano (EnergiaCol)
Módulo: Consultas Analíticas Distribuidas con Apache Spark SQL / AWS EMR
==============================================================================

Este script permite ejecutar las 5 consultas de negocio analíticas sobre la
capa Trusted (almacenada en formato Apache Parquet particionado en S3)
utilizando el motor distribuido Spark SQL.

Puede ejecutarse:
  1. En AWS EMR (spark-submit o notebook EMR Studio / Zeppelin)
  2. En local / contenedor Docker con Apache Spark

Uso:
  python spark_sql_analytics.py [--pregunta {1,2,3,4,5,all}] [--source {s3,catalog}] [--save]
==============================================================================
"""

import sys
import os
import argparse
import logging
from typing import Dict

# Configuración de logging estructurado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SparkSQL-Analytics")

# Definición de tablas y rutas en S3
DEFAULT_BUCKET = "big-data-lake-energiacol"
DATABASE_NAME = "energia_col_trusted"

TABLES_S3_PATHS = {
    "distribuidores_mayoristas": f"s3://{DEFAULT_BUCKET}/trusted/distribuidores_mayoristas/",
    "estaciones_servicio": f"s3://{DEFAULT_BUCKET}/trusted/estaciones_servicio/",
    "beneficios_frontera": f"s3://{DEFAULT_BUCKET}/trusted/beneficios_frontera/",
    "ventas_gncv": f"s3://{DEFAULT_BUCKET}/trusted/ventas_gncv/",
    "productores_b100e": f"s3://{DEFAULT_BUCKET}/trusted/productores_b100e/",
    "agentes_gncv": f"s3://{DEFAULT_BUCKET}/trusted/agentes_gncv/",
    "iea_electricity": f"s3://{DEFAULT_BUCKET}/trusted/iea_electricity/"
}

# ==============================================================================
# DEFINICIÓN DE CONSULTAS ANALÍTICAS (PREGUNTAS DE NEGOCIO 1 A 5)
# ==============================================================================

QUERIES: Dict[int, Dict[str, str]] = {
    1: {
        "title": "Pregunta 1: Concentración del Mercado Mayorista de Combustibles",
        "description": "Ranking y porcentaje de participación de los principales departamentos en el volumen nacional de combustibles líquidos (2022-2024).",
        "sql": """
            WITH despacho_por_departamento AS (
                SELECT
                    departamento,
                    producto,
                    SUM(volumen_despachado) AS volumen_total_despachado,
                    COUNT(*) AS total_transacciones_despacho
                FROM distribuidores_mayoristas
                WHERE CAST(anio_despacho AS INT) >= 2022
                GROUP BY departamento, producto
            ),
            totales_nacionales AS (
                SELECT
                    departamento,
                    producto,
                    volumen_total_despachado,
                    total_transacciones_despacho,
                    SUM(volumen_total_despachado) OVER (PARTITION BY producto) AS volumen_nacional_producto,
                    ROUND((volumen_total_despachado / SUM(volumen_total_despachado) OVER (PARTITION BY producto)) * 100, 2) AS pct_participacion_nacional,
                    DENSE_RANK() OVER (PARTITION BY producto ORDER BY volumen_total_despachado DESC) AS ranking_departamento
                FROM despacho_por_departamento
            )
            SELECT
                ranking_departamento,
                departamento,
                producto,
                ROUND(volumen_total_despachado, 2) AS volumen_despachado_galones,
                ROUND(volumen_nacional_producto, 2) AS volumen_nacional_galones,
                pct_participacion_nacional,
                total_transacciones_despacho
            FROM totales_nacionales
            WHERE ranking_departamento <= 5
            ORDER BY producto, ranking_departamento
        """
    },
    2: {
        "title": "Pregunta 2: Impacto y Ejecución de Subsidios en Zonas de Frontera",
        "description": "Comparación del volumen de combustible despachado hacia estaciones de frontera frente a los cupos asignados por la UPME.",
        "sql": """
            WITH consumo_estaciones AS (
                SELECT
                    codigo_municipio_dane_destino,
                    departamento,
                    municipio,
                    anio_despacho,
                    SUM(CASE WHEN zona_frontera = 'SI' THEN volumen_despachado ELSE 0.0 END) AS volumen_frontera,
                    SUM(CASE WHEN zona_frontera = 'NO' THEN volumen_despachado ELSE 0.0 END) AS volumen_no_frontera,
                    SUM(volumen_despachado) AS volumen_total,
                    COUNT(DISTINCT CASE WHEN zona_frontera = 'SI' THEN subtipo_comprador END) AS eds_frontera_activas
                FROM estaciones_servicio
                WHERE CAST(anio_despacho AS INT) >= 2022
                GROUP BY codigo_municipio_dane_destino, departamento, municipio, anio_despacho
            ),
            cupos_asignados AS (
                SELECT
                    codigo_municipio_dane_destino,
                    anio_beneficio,
                    SUM(beneficio) AS cupo_total_upme_asignado,
                    COUNT(*) AS total_asignaciones_cupo
                FROM beneficios_frontera
                WHERE CAST(anio_beneficio AS INT) >= 2022
                GROUP BY codigo_municipio_dane_destino, anio_beneficio
            )
            SELECT
                c.anio_despacho AS anio,
                c.departamento,
                c.municipio,
                c.codigo_municipio_dane_destino,
                ROUND(c.volumen_frontera, 2) AS volumen_despachado_frontera,
                ROUND(COALESCE(b.cupo_total_upme_asignado, 0.0), 2) AS cupo_upme_asignado,
                CASE 
                    WHEN COALESCE(b.cupo_total_upme_asignado, 0.0) > 0 
                    THEN ROUND((c.volumen_frontera / b.cupo_total_upme_asignado) * 100, 2)
                    ELSE NULL 
                END AS pct_ejecucion_cupo,
                ROUND(c.volumen_no_frontera, 2) AS volumen_despachado_ordinario,
                c.eds_frontera_activas
            FROM consumo_estaciones c
            LEFT JOIN cupos_asignados b 
                ON c.codigo_municipio_dane_destino = b.codigo_municipio_dane_destino 
               AND c.anio_despacho = b.anio_beneficio
            WHERE c.volumen_frontera > 0
            ORDER BY pct_ejecucion_cupo DESC
            LIMIT 25
        """
    },
    3: {
        "title": "Pregunta 3: Correlación entre Infraestructura y Ventas de GNCV",
        "description": "Relación entre densidad de agentes/talleres autorizados y volumen total de ventas de Gas Natural Comprimido Vehicular por municipio.",
        "sql": """
            WITH agentes_municipio AS (
                SELECT
                    codigo_municipio_dane,
                    departamento,
                    municipio,
                    COUNT(*) AS total_agentes_autorizados,
                    COUNT(CASE WHEN tipo_agente LIKE '%TALLER%' THEN 1 END) AS talleres_conversion,
                    COUNT(CASE WHEN tipo_agente LIKE '%ESTACION%' THEN 1 END) AS estaciones_gncv
                FROM agentes_gncv
                WHERE estado_agente = 'ACTIVO'
                GROUP BY codigo_municipio_dane, departamento, municipio
            ),
            ventas_municipio AS (
                SELECT
                    codigo_municipio_dane,
                    SUM(cantidad_volumen_suministrado) AS volumen_total_m3,
                    SUM(numero_de_ventas) AS transacciones_totales,
                    SUM(vehiculos_atendidos) AS vehiculos_totales,
                    AVG(eds_activas) AS promedio_eds_operativas
                FROM ventas_gncv
                GROUP BY codigo_municipio_dane
            )
            SELECT
                a.departamento,
                a.municipio,
                a.codigo_municipio_dane,
                a.total_agentes_autorizados,
                a.talleres_conversion,
                a.estaciones_gncv,
                ROUND(COALESCE(v.volumen_total_m3, 0.0), 2) AS volumen_gncv_suministrado_m3,
                COALESCE(v.vehiculos_totales, 0) AS total_vehiculos_atendidos,
                CASE 
                    WHEN a.total_agentes_autorizados > 0 
                    THEN ROUND(COALESCE(v.volumen_total_m3, 0.0) / a.total_agentes_autorizados, 2)
                    ELSE 0.0 
                END AS volumen_promedio_por_agente_m3
            FROM agentes_municipio a
            LEFT JOIN ventas_municipio v 
                ON a.codigo_municipio_dane = v.codigo_municipio_dane
            ORDER BY volumen_gncv_suministrado_m3 DESC
            LIMIT 25
        """
    },
    4: {
        "title": "Pregunta 4: Trazabilidad y Cumplimiento de Mezcla de Biocombustibles (B100 / Etanol)",
        "description": "Capacidad de producción nacional de B100 y Etanol frente a la cuota regulatoria obligatoria de mezcla del 10% en combustibles fósiles.",
        "sql": """
            WITH produccion_biocombustibles AS (
                SELECT
                    anio_despacho,
                    mes_despacho,
                    SUM(CASE WHEN producto LIKE '%BIODIESEL%' OR producto LIKE '%B100%' THEN volumen_despachado ELSE 0.0 END) AS produccion_b100_galones,
                    SUM(CASE WHEN producto LIKE '%ETANOL%' OR producto LIKE '%ALCOHOL%' THEN volumen_despachado ELSE 0.0 END) AS produccion_etanol_galones,
                    SUM(volumen_despachado) AS produccion_total_bio_galones
                FROM productores_b100e
                GROUP BY anio_despacho, mes_despacho
            ),
            demanda_despacho_mayorista AS (
                SELECT
                    anio_despacho,
                    mes_despacho,
                    SUM(CASE WHEN producto LIKE '%BIODIESEL%' THEN volumen_despachado ELSE 0.0 END) AS despacho_diesel_mezcla,
                    SUM(CASE WHEN producto LIKE '%GASOLINA%' OR producto LIKE '%CORRIENTE%' THEN volumen_despachado ELSE 0.0 END) AS despacho_gasolina_mezcla,
                    SUM(volumen_despachado) AS despacho_total_combustibles
                FROM distribuidores_mayoristas
                GROUP BY anio_despacho, mes_despacho
            )
            SELECT
                d.anio_despacho,
                d.mes_despacho,
                ROUND(d.despacho_diesel_mezcla * 0.10, 2) AS demanda_teorica_b100_10pct,
                ROUND(COALESCE(p.produccion_b100_galones, 0.0), 2) AS produccion_real_b100,
                CASE 
                    WHEN (d.despacho_diesel_mezcla * 0.10) > 0 
                    THEN ROUND((COALESCE(p.produccion_b100_galones, 0.0) / (d.despacho_diesel_mezcla * 0.10)) * 100, 2)
                    ELSE NULL 
                END AS pct_cobertura_b100,
                ROUND(d.despacho_gasolina_mezcla * 0.10, 2) AS demanda_teorica_etanol_10pct,
                ROUND(COALESCE(p.produccion_etanol_galones, 0.0), 2) AS produccion_real_etanol,
                CASE 
                    WHEN (d.despacho_gasolina_mezcla * 0.10) > 0 
                    THEN ROUND((COALESCE(p.produccion_etanol_galones, 0.0) / (d.despacho_gasolina_mezcla * 0.10)) * 100, 2)
                    ELSE NULL 
                END AS pct_cobertura_etanol
            FROM demanda_despacho_mayorista d
            LEFT JOIN produccion_biocombustibles p 
                ON d.anio_despacho = p.anio_despacho 
               AND d.mes_despacho = p.mes_despacho
            WHERE CAST(d.anio_despacho AS INT) >= 2022
            ORDER BY d.anio_despacho DESC, d.mes_despacho DESC
            LIMIT 25
        """
    },
    5: {
        "title": "Pregunta 5: Macro-tendencias Energéticas Cruzadas (Electricidad vs Combustibles Líquidos y Gas)",
        "description": "Comportamiento estacional mensual comparado entre generación eléctrica (hidráulica vs térmica) y demanda de combustibles y GNCV.",
        "sql": """
            WITH generacion_electrica AS (
                SELECT
                    year,
                    month,
                    SUM(CASE WHEN product = 'Hydro' THEN value ELSE 0.0 END) AS gen_hidro_gwh,
                    SUM(CASE WHEN product IN ('Solar', 'Wind', 'Other renewables') THEN value ELSE 0.0 END) AS gen_renovable_no_convencional_gwh,
                    SUM(CASE WHEN product IN ('Coal', 'Natural gas', 'Oil') THEN value ELSE 0.0 END) AS gen_termica_fosil_gwh,
                    SUM(value) AS gen_total_gwh
                FROM iea_electricity
                WHERE CAST(year AS INT) BETWEEN 2018 AND 2022
                GROUP BY year, month
            ),
            consumo_gas_mensual AS (
                SELECT
                    anio_venta AS year,
                    mes_venta AS month,
                    SUM(cantidad_volumen_suministrado) AS gncv_suministrado_m3,
                    SUM(numero_de_ventas) AS transacciones_gncv
                FROM ventas_gncv
                GROUP BY anio_venta, mes_venta
            ),
            despacho_combustibles_mensual AS (
                SELECT
                    anio_despacho AS year,
                    mes_despacho AS month,
                    SUM(volumen_despachado) AS combustible_liquido_galones
                FROM distribuidores_mayoristas
                GROUP BY anio_despacho, mes_despacho
            )
            SELECT
                e.year,
                e.month,
                ROUND(e.gen_hidro_gwh, 2) AS generacion_hidroelectrica_gwh,
                ROUND(e.gen_termica_fosil_gwh, 2) AS generacion_termica_gwh,
                ROUND(e.gen_total_gwh, 2) AS generacion_total_gwh,
                ROUND(COALESCE(g.gncv_suministrado_m3, 0.0), 2) AS demanda_gncv_m3,
                ROUND(COALESCE(c.combustible_liquido_galones, 0.0), 2) AS despacho_combustibles_galones
            FROM generacion_electrica e
            LEFT JOIN consumo_gas_mensual g 
                ON CAST(e.year AS INT) = CAST(g.year AS INT)
               AND CAST(e.month AS INT) = CAST(g.month AS INT)
            LEFT JOIN despacho_combustibles_mensual c 
                ON CAST(e.year AS INT) = CAST(c.year AS INT)
               AND CAST(e.month AS INT) = CAST(c.month AS INT)
            ORDER BY e.year DESC, e.month DESC
            LIMIT 30
        """
    }
}


# ==============================================================================
# GESTIÓN DE SPARK SESSION
# ==============================================================================

def get_spark_session(app_name: str = "EnergiaCol-SparkSQL-Analytics"):
    """
    Crea o recupera la SparkSession configurada para AWS EMR o ejecución local.
    Intenta habilitar Hive metastore si está disponible.
    """
    try:
        from pyspark.sql import SparkSession #type: ignore
    except ImportError:
        logger.error("PySpark no está instalado en este entorno de Python.")
        logger.error("Ejecute: pip install pyspark o invoque via spark-submit en un cluster EMR.")
        sys.exit(1)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.broadcastTimeout", "600")
    )

    # Intento de habilitar soporte Hive (estándar en EMR)
    try:
        spark = builder.enableHiveSupport().getOrCreate()
        logger.info("SparkSession inicializada con Hive metastore habilitado.")
    except Exception as e:
        logger.warning(f"No se pudo habilitar Hive Support ({e}). Inicializando Spark estándar.")
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==============================================================================
# CARGA Y REGISTRO DE VISTAS TEMPORALES
# ==============================================================================

def register_tables(spark, source: str = "s3", bucket: str = DEFAULT_BUCKET):
    """
    Registra las 7 tablas de la capa Trusted como vistas temporales de Spark SQL.
    
    Si source == 'catalog':
        Utiliza el catálogo de AWS Glue / Hive (USE energia_col_trusted).
    Si source == 's3':
        Lee directamente los datos Parquet desde S3 y registra DataFrames como vistas.
    """
    logger.info(f"Registrando tablas fuente con estrategia: '{source}'...")

    if source == "catalog":
        try:
            spark.sql(f"USE {DATABASE_NAME}")
            logger.info(f"Catálogo '{DATABASE_NAME}' seleccionado exitosamente.")
            return
        except Exception as e:
            logger.warning(f"Fallo al seleccionar base de datos '{DATABASE_NAME}': {e}. Cambiando a lectura directa desde S3.")

    # Registro directo leyendo Parquet de S3
    for table_name, s3_path in TABLES_S3_PATHS.items():
        custom_path = s3_path.replace(DEFAULT_BUCKET, bucket)
        logger.info(f"Cargando vista '{table_name}' desde: {custom_path}")
        try:
            df = spark.read.parquet(custom_path)
            df.createOrReplaceTempView(table_name)
            logger.info(f"  ✓ Vista '{table_name}' registrada ({df.count()} registros aproximados).")
        except Exception as e:
            logger.error(f"  ✗ Error registrando '{table_name}' desde {custom_path}: {e}")
            logger.warning(f"  Creando vista vacía para '{table_name}' para evitar fallos de sintaxis.")
            # Crear vista vacía dummy si aún no está disponible la tabla
            spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {table_name} AS SELECT 1 AS dummy WHERE 1=0")


# ==============================================================================
# EJECUCIÓN DE CONSULTAS ANALÍTICAS
# ==============================================================================

def run_query(spark, query_id: int, save: bool = False, bucket: str = DEFAULT_BUCKET):
    """
    Ejecuta una consulta SQL específica por ID (1-5), imprime los resultados y opcionalmente
    los exporta a la capa analytics en S3.
    """
    if query_id not in QUERIES:
        logger.error(f"Pregunta {query_id} no válida. Las opciones válidas son 1 a 5.")
        return

    q_data = QUERIES[query_id]
    banner = "=" * 80
    print(f"\n{banner}")
    print(f"  {q_data['title'].upper()}")
    print(f"  {q_data['description']}")
    print(f"{banner}\n")

    logger.info(f"Ejecutando Spark SQL para Pregunta {query_id}...")
    try:
        result_df = spark.sql(q_data["sql"])
        
        # Mostrar resultados en consola
        result_df.show(30, truncate=False)
        
        row_count = result_df.count()
        logger.info(f"Pregunta {query_id} completada exitosamente. Filas retornadas: {row_count}")

        # Guardar en S3 si se solicita el flag --save
        if save:
            output_path = f"s3://{bucket}/analytics/pregunta_{query_id}/"
            logger.info(f"Guardando resultados en formato Parquet en: {output_path}")
            result_df.coalesce(1).write.mode("overwrite").parquet(output_path)
            
            # Exportar también copia CSV para fácil visualización
            csv_path = f"s3://{bucket}/analytics/pregunta_{query_id}_csv/"
            logger.info(f"Guardando copia en CSV con cabecera en: {csv_path}")
            result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
            logger.info(f"  ✓ Datos persistidos exitosamente en S3.")

    except Exception as e:
        logger.error(f"Error ejecutando Pregunta {query_id}: {e}", exc_info=True)


# ==============================================================================
# PUNTO DE ENTRADA CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Punto 6: Consultas Analíticas con Spark SQL sobre Data Lakehouse EnergiaCol"
    )
    parser.add_argument(
        "--pregunta", "-p",
        choices=["1", "2", "3", "4", "5", "all"],
        default="all",
        help="Número de la pregunta de negocio a resolver (1 a 5 o 'all'). Por defecto: 'all'."
    )
    parser.add_argument(
        "--source", "-s",
        choices=["s3", "catalog"],
        default="s3",
        help="Fuente de datos: 's3' para leer Parquet directamente, 'catalog' para usar AWS Glue / Hive Metastore."
    )
    parser.add_argument(
        "--bucket", "-b",
        default=DEFAULT_BUCKET,
        help=f"Nombre del bucket de S3. Por defecto: '{DEFAULT_BUCKET}'."
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Exportar los resultados de las consultas a s3://<bucket>/analytics/ en Parquet y CSV."
    )

    args = parser.parse_args()

    logger.info("=== INICIANDO MOTOR SPARK SQL ANALYTICS (PUNTO 6) ===")
    spark = get_spark_session()

    try:
        register_tables(spark, source=args.source, bucket=args.bucket)

        if args.pregunta == "all":
            for q_id in range(1, 6):
                run_query(spark, query_id=q_id, save=args.save, bucket=args.bucket)
        else:
            run_query(spark, query_id=int(args.pregunta), save=args.save, bucket=args.bucket)

        logger.info("=== PROCESAMIENTO ANALÍTICO FINALIZADO CON ÉXITO ===")
    finally:
        spark.stop()
        logger.info("SparkSession cerrada.")


if __name__ == "__main__":
    main()
