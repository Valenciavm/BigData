-- ==============================================================================
-- PUNTO 5: CATALOGACIÓN DE TABLAS EN AWS ATHENA / EMR HIVE
-- Base de Datos: energia_col_trusted
-- Capa: Trusted (Silver) - Formato Apache Parquet con compresión Snappy
-- Bucket: s3://big-data-lake-energiacol/trusted/
-- ==============================================================================

-- 1. CREACIÓN DE LA BASE DE DATOS
CREATE DATABASE IF NOT EXISTS energia_col_trusted
COMMENT 'Catalogo de datos capa Trusted - Sector Energetico Colombia'
LOCATION 's3://big-data-lake-energiacol/trusted/';

USE energia_col_trusted;


-- ==============================================================================
-- 2. TABLAS TRANSACCIONALES DE COMBUSTIBLES Y SUBSIDIOS
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Tabla 1: Distribuidores Mayoristas - Despacho de Combustibles
-- Particiones: anio_despacho (INT), mes_despacho (INT)
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS distribuidores_mayoristas (
    fecha_despacho                  DATE,
    dia_despacho                    INT,
    tipo_proveedor                  STRING,
    codigo_municipio_dane           BIGINT,
    departamento_proveedor          STRING,
    municipio_proveedor             STRING,
    tipo_comprador                  STRING,
    subtipo_comprador               STRING,
    codigo_municipio_dane_destino   BIGINT,
    departamento                    STRING,
    municipio                       STRING,
    producto                        STRING,
    volumen_despachado              DOUBLE,
    latitud                         DOUBLE,
    longitud                        DOUBLE
)
PARTITIONED BY (
    anio_despacho                   INT,
    mes_despacho                    INT
)
STORED AS PARQUET
LOCATION 's3://big-data-lake-energiacol/trusted/distribuidores_mayoristas/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- Cargar particiones descubiertas en S3
MSCK REPAIR TABLE distribuidores_mayoristas;


-- ------------------------------------------------------------------------------
-- Tabla 2: Estaciones de Servicio Automotriz y Fluvial
-- Particiones: anio_despacho (INT), mes_despacho (INT)
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS estaciones_servicio (
    fecha_despacho                  DATE,
    dia_despacho                    INT,
    tipo_proveedor                  STRING,
    codigo_municipio_dane           BIGINT,
    departamento_proveedor          STRING,
    municipio_proveedor             STRING,
    tipo_comprador                  STRING,
    subtipo_comprador               STRING,
    zona_frontera                   STRING,
    codigo_municipio_dane_destino   BIGINT,
    departamento                    STRING,
    municipio                       STRING,
    producto                        STRING,
    volumen_despachado              DOUBLE,
    latitud                         DOUBLE,
    longitud                        DOUBLE
)
PARTITIONED BY (
    anio_despacho                   INT,
    mes_despacho                    INT
)
STORED AS PARQUET
LOCATION 's3://big-data-lake-energiacol/trusted/estaciones_servicio/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK REPAIR TABLE estaciones_servicio;


-- ------------------------------------------------------------------------------
-- Tabla 3: Beneficio Zona de Frontera (Cupos UPME)
-- Particiones: anio_beneficio (INT), mes_beneficio (INT)
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS beneficios_frontera (
    fecha_beneficio                 DATE,
    dia_beneficio                   INT,
    tipo_agente                     STRING,
    tipo_beneficio                  STRING,
    codigo_municipio_dane_destino   BIGINT,
    departamento                    STRING,
    municipio                       STRING,
    beneficio                       DOUBLE,
    latitud                         DOUBLE,
    longitud                        DOUBLE
)
PARTITIONED BY (
    anio_beneficio                  INT,
    mes_beneficio                   INT
)
STORED AS PARQUET
LOCATION 's3://big-data-lake-energiacol/trusted/beneficios_frontera/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK REPAIR TABLE beneficios_frontera;


-- ------------------------------------------------------------------------------
-- Tabla 4: Ventas de Gas Natural Comprimido Vehicular (GNCV)
-- Particiones: anio_venta (INT), mes_venta (INT)
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ventas_gncv (
    fecha_venta                     DATE,
    dia_venta                       INT,
    codigo_municipio_dane           BIGINT,
    departamento                    STRING,
    municipio                       STRING,
    latitud                         DOUBLE,
    longitud                        DOUBLE,
    tipo_agente                     STRING,
    tipo_de_combustible             STRING,
    eds_activas                     INT,
    numero_de_ventas                INT,
    vehiculos_atendidos             INT,
    cantidad_volumen_suministrado   DOUBLE
)
PARTITIONED BY (
    anio_venta                      INT,
    mes_venta                       INT
)
STORED AS PARQUET
LOCATION 's3://big-data-lake-energiacol/trusted/ventas_gncv/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK REPAIR TABLE ventas_gncv;


-- ------------------------------------------------------------------------------
-- Tabla 5: Productores de Biocombustibles (B100 y Etanol)
-- Particiones: anio_despacho (INT), mes_despacho (INT)
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS productores_b100e (
    fecha_despacho                  DATE,
    dia_despacho                    INT,
    tipo_proveedor                  STRING,
    subtipo_proveedor               STRING,
    codigo_municipio_dane_proveedor BIGINT,
    departamento_proveedor          STRING,
    municipio_proveedor             STRING,
    tipo_comprador                  STRING,
    codigo_municipio_dane_destino   BIGINT,
    departamento                    STRING,
    municipio                       STRING,
    producto                        STRING,
    volumen_despachado              DOUBLE,
    latitud                         DOUBLE,
    longitud                        DOUBLE
)
PARTITIONED BY (
    anio_despacho                   INT,
    mes_despacho                    INT
)
STORED AS PARQUET
LOCATION 's3://big-data-lake-energiacol/trusted/productores_b100e/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK REPAIR TABLE productores_b100e;


-- ==============================================================================
-- 3. TABLAS DE DIMENSIÓN Y REFERENCIA INTERNACIONAL
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Tabla 6: Catálogo Maestro de Agentes GNCV
-- Partición: departamento (STRING)
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS agentes_gncv (
    tipo_agente                     STRING,
    nombre_comercial                STRING,
    razon_social                    STRING,
    estado_agente                   STRING,
    municipio                       STRING,
    codigo_municipio_dane           BIGINT,
    latitud                         DOUBLE,
    longitud                        DOUBLE
)
PARTITIONED BY (
    departamento                    STRING
)
STORED AS PARQUET
LOCATION 's3://big-data-lake-energiacol/trusted/agentes_gncv/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK REPAIR TABLE agentes_gncv;


-- ------------------------------------------------------------------------------
-- Tabla 7: Estadísticas Eléctricas Internacionales (IEA Monthly Electricity)
-- Particiones: country (STRING), year (INT)
-- ------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS iea_electricity (
    code_time                       STRING,
    time                            STRING,
    month                           INT,
    month_name                      STRING,
    product                         STRING,
    value                           DOUBLE,
    display_order                   INT,
    year_to_date                    DOUBLE,
    previous_year_to_date           DOUBLE,
    share                           DOUBLE
)
PARTITIONED BY (
    country                         STRING,
    year                            INT
)
STORED AS PARQUET
LOCATION 's3://big-data-lake-energiacol/trusted/iea_electricity/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK REPAIR TABLE iea_electricity;


-- ==============================================================================
-- 4. CONSULTAS DE VALIDACIÓN POST-CATALOGACIÓN
-- ==============================================================================

-- Listar tablas creadas en el catálogo
SHOW TABLES IN energia_col_trusted;

-- Verificar conteo de filas de muestra
SELECT 'distribuidores_mayoristas' AS tabla, count(*) AS total FROM distribuidores_mayoristas
UNION ALL
SELECT 'estaciones_servicio' AS tabla, count(*) AS total FROM estaciones_servicio
UNION ALL
SELECT 'beneficios_frontera' AS tabla, count(*) AS total FROM beneficios_frontera
UNION ALL
SELECT 'ventas_gncv' AS tabla, count(*) AS total FROM ventas_gncv
UNION ALL
SELECT 'productores_b100e' AS tabla, count(*) AS total FROM productores_b100e
UNION ALL
SELECT 'agentes_gncv' AS tabla, count(*) AS total FROM agentes_gncv
UNION ALL
SELECT 'iea_electricity' AS tabla, count(*) AS total FROM iea_electricity;
