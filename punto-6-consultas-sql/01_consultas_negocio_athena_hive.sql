-- ==============================================================================
-- PUNTO 6: CONSULTAS Y PROCESAMIENTO ANALÍTICO DESCRIPTIVO CON SQL
-- Motor: AWS Athena / EMR Hive
-- Base de Datos: energia_col_trusted
-- ==============================================================================

USE energia_col_trusted;


-- ==============================================================================
-- PREGUNTA 1: CONCENTRACIÓN DEL MERCADO MAYORISTA DE COMBUSTIBLES
-- ¿Qué porcentaje del volumen total de despacho de combustibles concentran los
-- principales distribuidores mayoristas frente a la red nacional de estaciones?
-- Conceptos SQL: CTEs, Window Functions (SUM() OVER()), RANK(), Ratios de Concentración (CR4)
-- ==============================================================================
WITH despacho_por_departamento AS (
    SELECT
        departamento,
        producto,
        SUM(volumen_despachado) AS volumen_total_despachado,
        COUNT(*) AS total_transacciones_despacho
    FROM distribuidores_mayoristas
    WHERE TRY_CAST (anio_despacho AS INTEGER) >= 2022
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
ORDER BY producto, ranking_departamento;


-- ==============================================================================
-- PREGUNTA 2: IMPACTO DE SUBSIDIOS EN ZONAS DE FRONTERA
-- ¿Existe una desviación o incremento anómalo en el volumen de combustible despachado
-- hacia estaciones ubicadas en zonas de frontera beneficiadas (cupos UPME) en comparación
-- con el cupo asignado y el comportamiento departamental?
-- Conceptos SQL: JOIN entre fact tables, agregación condicional, ratio de utilización de cupo
-- ==============================================================================
WITH consumo_estaciones AS (
    SELECT
        codigo_municipio_dane_destino,
        departamento,
        municipio,
        anio_despacho,
        SUM(CASE WHEN zona_frontera = 'SI' THEN volumen_despachado ELSE 0 END) AS volumen_frontera,
        SUM(CASE WHEN zona_frontera = 'NO' THEN volumen_despachado ELSE 0 END) AS volumen_no_frontera,
        SUM(volumen_despachado) AS volumen_total,
        COUNT(DISTINCT CASE WHEN zona_frontera = 'SI' THEN subtipo_comprador END) AS eds_frontera_activas
    FROM estaciones_servicio
    WHERE TRY_CAST (anio_despacho AS INTEGER) >= 2022
    GROUP BY codigo_municipio_dane_destino, departamento, municipio, anio_despacho
),
cupos_asignados AS (
    SELECT
        codigo_municipio_dane_destino,
        anio_beneficio,
        SUM(beneficio) AS cupo_total_upme_asignado,
        COUNT(*) AS total_asignaciones_cupo
    FROM beneficios_frontera
    WHERE TRY_CAST (anio_beneficio AS INTEGER) >= 2022
    GROUP BY codigo_municipio_dane_destino, anio_beneficio
)
SELECT
    c.anio_despacho AS anio,
    c.departamento,
    c.municipio,
    c.codigo_municipio_dane_destino,
    ROUND(c.volumen_frontera, 2) AS volumen_despachado_frontera,
    ROUND(COALESCE(b.cupo_total_upme_asignado, 0), 2) AS cupo_upme_asignado,
    CASE 
        WHEN COALESCE(b.cupo_total_upme_asignado, 0) > 0 
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
ORDER BY pct_ejecucion_cupo DESC NULLS LAST
LIMIT 20;


-- ==============================================================================
-- PREGUNTA 3: CORRELACIÓN DE INFRAESTRUCTURA Y VENTAS DE GNCV
-- ¿Cómo correlaciona la densidad geográfica de agentes autorizados y talleres de GNCV
-- con el volumen real de ventas mensuales de gas comprimido vehicular a nivel municipal?
-- Conceptos SQL: LEFT JOIN dimensión-hechos, ratios de abastecimiento por agente, ranking municipal
-- ==============================================================================
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
    ROUND(COALESCE(v.volumen_total_m3, 0), 2) AS volumen_gncv_suministrado_m3,
    COALESCE(v.vehiculos_totales, 0) AS total_vehiculos_atendidos,
    CASE 
        WHEN a.total_agentes_autorizados > 0 
        THEN ROUND(COALESCE(v.volumen_total_m3, 0) / a.total_agentes_autorizados, 2)
        ELSE 0 
    END AS volumen_promedio_por_agente_m3
FROM agentes_municipio a
LEFT JOIN ventas_municipio v 
    ON a.codigo_municipio_dane = v.codigo_municipio_dane
ORDER BY volumen_gncv_suministrado_m3 DESC
LIMIT 25;


-- ==============================================================================
-- PREGUNTA 4: TRAZABILIDAD Y CUMPLIMIENTO DE BIOCOMBUSTIBLES (B100 / ETANOL)
-- ¿La capacidad de producción nacional de B100 y Etanol satisface la cuota de mezcla
-- legal obligatoria (10% mandatorio) exigida en los despachos mayoristas de combustibles?
-- Conceptos SQL: Estimación de mezcla teórica vs producción real, ratio de autosuficiencia
-- ==============================================================================
WITH produccion_biocombustibles AS (
    SELECT
        anio_despacho,
        mes_despacho,
        SUM(CASE WHEN producto LIKE '%BIODIESEL%' OR producto LIKE '%B100%' THEN volumen_despachado ELSE 0 END) AS produccion_b100_galones,
        SUM(CASE WHEN producto LIKE '%ETANOL%' OR producto LIKE '%ALCOHOL%' THEN volumen_despachado ELSE 0 END) AS produccion_etanol_galones,
        SUM(volumen_despachado) AS produccion_total_bio_galones
    FROM productores_b100e
    GROUP BY anio_despacho, mes_despacho
),
demanda_despacho_mayorista AS (
    SELECT
        anio_despacho,
        mes_despacho,
        SUM(CASE WHEN producto LIKE '%BIODIESEL%' THEN volumen_despachado ELSE 0 END) AS despacho_diesel_mezcla,
        SUM(CASE WHEN producto LIKE '%GASOLINA%' OR producto LIKE '%CORRIENTE%' THEN volumen_despachado ELSE 0 END) AS despacho_gasolina_mezcla,
        SUM(volumen_despachado) AS despacho_total_combustibles
    FROM distribuidores_mayoristas
    GROUP BY anio_despacho, mes_despacho
)
SELECT
    d.anio_despacho,
    d.mes_despacho,
    -- Demanda estimada con cuota legal de mezcla del 10% (0.10)
    ROUND(d.despacho_diesel_mezcla * 0.10, 2) AS demanda_teorica_b100_10pct,
    ROUND(COALESCE(p.produccion_b100_galones, 0), 2) AS produccion_real_b100,
    CASE 
        WHEN (d.despacho_diesel_mezcla * 0.10) > 0 
        THEN ROUND((COALESCE(p.produccion_b100_galones, 0) / (d.despacho_diesel_mezcla * 0.10)) * 100, 2)
        ELSE NULL 
    END AS pct_cobertura_b100,
    ROUND(d.despacho_gasolina_mezcla * 0.10, 2) AS demanda_teorica_etanol_10pct,
    ROUND(COALESCE(p.produccion_etanol_galones, 0), 2) AS produccion_real_etanol,
    CASE 
        WHEN (d.despacho_gasolina_mezcla * 0.10) > 0 
        THEN ROUND((COALESCE(p.produccion_etanol_galones, 0) / (d.despacho_gasolina_mezcla * 0.10)) * 100, 2)
        ELSE NULL 
    END AS pct_cobertura_etanol
FROM demanda_despacho_mayorista d
LEFT JOIN produccion_biocombustibles p 
    ON d.anio_despacho = p.anio_despacho 
   AND d.mes_despacho = p.mes_despacho
WHERE TRY_CAST (d.anio_despacho AS INTEGER) >= 2022
ORDER BY d.anio_despacho DESC, d.mes_despacho DESC;


-- ==============================================================================
-- PREGUNTA 5: MACRO-TENDENCIAS ENERGÉTICAS CRUZADAS (ELECTRICIDAD VS COMBUSTIBLES)
-- ¿Cómo se comportan estacionalmente las métricas de generación eléctrica mensual de la IEA
-- en paralelo con la demanda agregada de gas vehicular y combustibles líquidos?
-- Conceptos SQL: Multi-source temporal join, normalización mensual, análisis estacional
-- ==============================================================================
WITH generacion_electrica AS (
    SELECT
        year,
        month,
        SUM(CASE WHEN product = 'Hydro' THEN value ELSE 0 END) AS gen_hidro_gwh,
        SUM(CASE WHEN product IN ('Solar', 'Wind', 'Other renewables') THEN value ELSE 0 END) AS gen_renovable_no_convencional_gwh,
        SUM(CASE WHEN product IN ('Coal', 'Natural gas', 'Oil') THEN value ELSE 0 END) AS gen_termica_fosil_gwh,
        SUM(value) AS gen_total_gwh
    FROM iea_electricity
    WHERE TRY_CAST (year AS INTEGER) BETWEEN 2018 AND 2022
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
    ROUND(COALESCE(g.gncv_suministrado_m3, 0), 2) AS demanda_gncv_m3,
    ROUND(COALESCE(c.combustible_liquido_galones, 0), 2) AS despacho_combustibles_galones
FROM generacion_electrica e
LEFT JOIN consumo_gas_mensual g 
    ON TRY_CAST(e.year AS INTEGER) = TRY_CAST(g.year AS INTEGER)
   AND TRY_CAST(e.month AS INTEGER) = TRY_CAST(g.month AS INTEGER)
LEFT JOIN despacho_combustibles_mensual c 
    ON TRY_CAST(e.year AS INTEGER) = TRY_CAST(c.year AS INTEGER)
   AND TRY_CAST(e.month AS INTEGER) = TRY_CAST(c.month AS INTEGER)
ORDER BY e.year DESC, e.month DESC;
