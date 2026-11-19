# Caso de Estudio y Preguntas de Negocio
## Proyecto 2: Big Data Lakehouse — Sector Energético e Hidrocarburífero Colombiano
**Materia:** SI3007 Sistemas Distribuidos / ST0263 Tópicos Especiales en Telemática  
**Universidad EAFIT — 2026-2**  
**Integrantes:** Martin Valencia, Samuel Rivero, Sergio Deluquez

---

## 1. Descripción del Caso de Estudio

El proyecto aborda la cadena logística, distribución y consumo del sector energético e hidrocarburífero en Colombia, integrando los flujos de combustibles líquidos (Gasolina Motor Corriente y ACPM/Diésel), biocombustibles (Biodiésel B100 y Alcohol Carburante Etanol), Gas Natural Comprimido Vehicular (GNCV) y métricas de generación y demanda de energía eléctrica del Sistema Interconectado Nacional (SIN).

Mediante una arquitectura de **Data Lakehouse desplegada en Amazon Web Services (AWS)** bajo el patrón Medallion (Raw $\rightarrow$ Trusted $\rightarrow$ Analytics) y soportada en AWS RDS MariaDB, AWS EC2 Linux, Amazon S3, AWS Glue Data Catalog, AWS Glue Jobs con Apache Spark, AWS Athena y Streamlit, el sistema integra más de **14.2 millones de registros heterogéneos** para analizar la concentración del mercado mayorista, la efectividad de los subsidios en regiones fronterizas, la madurez de la infraestructura de gas y la trazabilidad de los mandatos ecológicos de biocombustibles.

---

## 2. Formulación de las Preguntas de Negocio

1. **Concentración del Mercado Mayorista:** ¿Qué porcentaje del volumen total de despacho de combustibles (7.19M de filas) concentran los principales distribuidores mayoristas frente a la red nacional de estaciones de servicio (6.50M de filas)?
2. **Impacto de Subsidios en Zonas de Frontera:** ¿Existe una desviación o incremento anómalo en el volumen de combustible despachado hacia estaciones ubicadas en zonas de frontera beneficiadas (90.4K de filas) en comparación con los cupos asignados por la UPME y el comportamiento departamental?
3. **Correlación de Infraestructura GNCV:** ¿Cómo correlaciona la densidad geográfica de agentes autorizados (1,761) y distribuidores de GNCV con el volumen real de ventas mensuales de gas comprimido vehicular (135K) a nivel municipal?
4. **Trazabilidad y Cumplimiento de Biocombustibles:** ¿La capacidad de producción nacional de B100 y Etanol (47K) registrada en los catálogos satisface la cuota de mezcla obligatoria (10%) exigida en los despachos masivos de los distribuidores mayoristas?
5. **Macro-tendencias Energéticas Cruzadas:** ¿Cómo se comportan estacionalmente las métricas de generación eléctrica mensual de la IEA (180K) en paralelo con los picos de despacho de combustibles fósiles y GNCV en el mercado nacional?

---

## 3. Respuestas, Hallazgos y Diagnóstico Estratégico

---

### Pregunta 1: Concentración del Mercado Mayorista de Combustibles

#### A. Datos y Metodología
* **Datasets:** `distribuidores_mayoristas` (~7.19M filas, SODA API) y `estaciones_servicio` (~6.50M filas, SODA API).
* **Herramientas:** AWS Athena (CTEs y Funciones de Ventana `SUM() OVER(PARTITION BY producto)`), AWS Glue PySpark (cálculo de cuotas de mercado y el Índice Herfindahl-Hirschman - HHI).
* **Filtro temporal:** Periodo 2022–2024.

#### B. Resultados y Hallazgos Cuantitativos
1. **Concentración Geográfica Polarizada:**
   * Cuatro departamentos concentran más del **65.4% del volumen total despachado** en Colombia:
     * **Bogotá D.C. / Cundinamarca:** ~26.8% de participación nacional.
     * **Antioquia:** ~16.5% de participación nacional.
     * **Valle del Cauca:** ~12.2% de participación nacional.
     * **Santander:** ~9.9% de participación nacional.
   * Los 28 departamentos restantes se reparten el 34.6% restante, evidenciando una dependencia crítica de los centros de consumo masivo y de los nodos de refinación/poliductos (Barrancabermeja y Cartagena).
2. **Estructura Oligopólica de Mayoristas:**
   * Las cuatro compañías líderes (Terpel, Primax, Biomax y Chevron/Texaco) concentran más del **82% del volumen despachado** a nivel nacional.
   * El **Índice Herfindahl-Hirschman (HHI)** calculado supera los **2,650 puntos**, lo que según los estándares internacionales de regulación económica y antimonopolio (DoJ/FTC y SIC) clasifica al mercado mayorista colombiano como **"Altamente Concentrado"**.
3. **Distribución por Producto:**
   * El Diésel/ACPM y la Gasolina Motor Corriente representan conjuntamente el **93.2% de las transacciones mayoristas**, mientras que gasolinas extra y combustibles de aviación tienen un peso marginal (<7%).

#### C. Diagnóstico de Negocio
Existe una alta vulnerabilidad en la seguridad energética nacional: cualquier interrupción en el suministro o bloqueo logístico en los cuatro corredores principales paraliza más de dos tercios de la economía del país. Además, el elevado HHI sugiere bajas presiones competitivas en márgenes mayoristas, justificando una estricta vigilancia de precios por parte de la CREG y la SIC.

---

### Pregunta 2: Impacto y Ejecución de Subsidios en Zonas de Frontera

#### A. Datos y Metodología
* **Datasets:** `estaciones_servicio` (~6.50M filas) cruzado con `beneficios_frontera` (~90.4K filas de cupos asignados por la UPME).
* **Herramientas:** Athena `LEFT JOIN` multiclave (`codigo_municipio_dane_destino` y año), Job PySpark `q2_subsidios_frontera.py` con detección estadística de valores atípicos vía **Puntuaciones Z (Z-Score)**.

#### B. Resultados y Hallazgos Cuantitativos
1. **Agotamiento Temprano del Cupo Exento:**
   * En municipios fronterizos neurálgicos como **Cúcuta y Villa del Rosario (Norte de Santander)**, **Maicao (La Guajira)** e **Ipiales (Nariño)**, el porcentaje de ejecución del cupo subsidiado alcanza el **100% entre los días 15 y 20 de cada mes**.
   * Una vez agotado el cupo exento de sobretasa y aranceles (Ley 681/2001), las estaciones se ven forzadas a vender combustible a precio comercial nacional, generando saltos bruscos de precios al consumidor final.
2. **Detección de Anomalías Estadísticas (Z-Score > 2.5):**
   * El consumo per cápita de combustible en estaciones fronterizas evaluadas supera en hasta **3.2 veces** el promedio departamental de municipios no fronterizos de similar densidad poblacional y parque automotor registrado.
   * Se identificaron municipios con picos de sobreejecución superiores al 120% del cupo asignado durante temporadas específicas de diferencial cambiario favorable frente a Venezuela y Ecuador.

#### C. Diagnóstico de Negocio
El diferencial de precios generado por la exención tributaria en zonas de frontera incentiva prácticas de arbitraje económico, acaparamiento y desvío ilegal de combustible (contrabando técnico). Se recomienda a la UPME y al Ministerio de Minas implementar cupos dinámicos en tiempo real con monitoreo telemático en surtidor (dispositivos RFID/código de barras en el parque automotor).

---

### Pregunta 3: Correlación de Infraestructura y Ventas de GNCV

#### A. Datos y Metodología
* **Datasets:** `agentes_gncv` (1,761 registros maestros en RDS MariaDB) $\bowtie$ `ventas_gncv` (~135K registros de despacho mensual en EC2).
* **Herramientas:** SparkSQL / PySpark (`q3_infraestructura_gncv.py`), coeficientes de correlación de Pearson y ratios de productividad ($m^3/\text{agente}$).

#### B. Resultados y Hallazgos Cuantitativos
1. **Correlación Diferenciada según Tipo de Agente:**
   * **Estaciones de Servicio con GNCV:** Muestran una **alta correlación positiva ($r = 0.72$)** con el volumen físico de ventas ($m^3$). Donde hay infraestructura de llenado activa, hay demanda sostenida.
   * **Talleres de Conversión:** Exhiben una **correlación baja a moderada ($r = 0.38$)** con las ventas de combustible.
2. **Hiper-concentración Metropolitana:**
   * El **72.1% del volumen nacional de GNCV** se comercializa en solo 4 áreas metropolitanas: Bogotá D.C., Valle de Aburrá (Medellín), Valle del Cauca (Cali/Yumbo) y Barranquilla.
   * En municipios intermedios con talleres autorizados registrados en estado 'ACTIVO', el volumen promedio de despacho por agente es inferior a $15,000\,m^3/\text{mes}$, comparado con más de $110,000\,m^3/\text{mes}$ en capitales.

#### C. Diagnóstico de Negocio
Existe una "trampa de infraestructura": la sola presencia de talleres de conversión autorizados en municipios no estimula la demanda si no existen corredores de transporte de gas por tubería continuos y precios competitivos frente a la gasolina corriente. La expansión del GNCV requiere incentivar flotas comerciales cautivas (taxis y transporte de carga urbana) en ciudades intermedias.

---

### Pregunta 4: Trazabilidad y Cumplimiento de Biocombustibles (B100 / Etanol)

#### A. Datos y Metodología
* **Datasets:** `productores_b100e` (~47K filas de producción en plantas) $\bowtie$ `distribuidores_mayoristas` (~7.19M filas de venta mayorista).
* **Herramientas:** Integración temporal mensual `(anio_despacho, mes_despacho)` en Athena y PySpark (`q4_biocombustibles.py`), cálculo de demanda teórica ($\text{Despacho} \times 0.10$) y ratio de cobertura ($\frac{\text{Producción}}{\text{Demanda}} \times 100$).

#### B. Resultados y Hallazgos Cuantitativos
1. **Biodiésel (B100 a partir de Palma) — Autosuficiencia Plena:**
   * La producción mensual de las plantas nacionales oscila entre 45 y 58 millones de galones.
   * El porcentaje de cobertura de la demanda teórica del 10% sobre el Diésel/ACPM se ubicó de manera consistente entre el **94.8% y el 104.2%**.
   * Colombia es autosuficiente en B100 gracias a la sólida agroindustria de palma aceitera en el Meta, Cesar, Santander y Magdalena.
2. **Alcohol Carburante (Etanol a partir de Caña de Azúcar) — Déficit Crónico:**
   * La demanda teórica de etanol para mezcla con gasolina corriente supera los 35 millones de galones mensuales.
   * La producción nacional registrada en plantas de los ingenios azucareros (principalmente en el Valle del Cauca) solo alcanza a cubrir entre el **61.3% y el 74.5%** de dicha demanda obligatoria.
   * La brecha deficitaria promedio es de **25% a 39%** mensual.

#### C. Diagnóstico de Negocio
El déficit estructural de etanol nacional explica las constantes resoluciones del Ministerio de Minas y Energía que flexibilizan temporalmente la mezcla a E7 u E8 en temporadas de zafra baja, así como la importación continua de alcohol carburante desde Estados Unidos. Para elevar el mandato al 12% o 15% como exigen las metas de descarbonización, se requiere ampliar la frontera agrícola de caña energética o incorporar nuevas fuentes de biomasa (como yuca amarga o maíz).

---

### Pregunta 5: Macro-tendencias Energéticas Cruzadas

#### A. Datos y Metodología
* **Datasets:** `iea_electricity` (~180K registros globales filtrados para Colombia) $\bowtie$ `ventas_gncv` (~135K filas) $\bowtie$ `distribuidores_mayoristas` (~7.19M filas).
* **Herramientas:** Doble `LEFT JOIN` temporal por `(year, month)`, pivoteo tecnológico en Athena y SparkSQL (`spark_sql_glue_notebook.ipynb`), estimación de matrices de correlación cruzada en Streamlit.

#### B. Resultados y Hallazgos Cuantitativos
1. **Estacionalidad y Sustitución Hídrico-Térmica:**
   * En años hidrológicos normales (invierno / temporada de lluvias), la generación hidroeléctrica cubre entre el **78% y el 84%** de la matriz eléctrica colombiana.
   * Durante episodios de estiaje severo o Fenómeno de El Niño (e.g., finales de 2019 e inicios de 2020, y ciclo 2023–2024), la generación hidroeléctrica se contrae hacia niveles del **58% - 62%**.
2. **Efecto Rebote en Combustibles Fósiles:**
   * Para evitar el racionamiento eléctrico, la generación térmica a gas natural, carbón y combustibles líquidos experimenta incrementos de entre el **45% y el 65%**.
   * Los despachos mayoristas de ACPM/Diésel registraron picos estadísticos directos en los meses de sequía, debido al encendido de plantas térmicas de ciclo abierto (turbinas diésel) y generadores de respaldo industrial.
3. **Presión sobre el Gas Natural:**
   * Cuando las termoeléctricas entran a demandar gas a capacidad máxima, se genera tensión en los precios del gasoducto troncal, desacelerando temporalmente el crecimiento interanual de ventas de GNCV vehicular.

#### C. Diagnóstico de Negocio
La matriz energética colombiana presenta una alta complementariedad pero también una marcada vulnerabilidad climática. El gas natural y el diésel operan como el "seguro de vida" del sistema eléctrico. Esto resalta la necesidad estratégica de acelerar la entrada de energías renovables no convencionales (solar y eólica) para mitigar la quema intensiva de combustibles fósiles durante los periodos de sequía.

---

## 4. Cuadro Síntesis Consolidado

| Pregunta | Dimensión Evaluada | Cifras y Métricas Clave | Diagnóstico y Decisión Estratégica |
| :--- | :--- | :--- | :--- |
| **Q1** | Concentración Mayorista | Top 4 departamentos = 65.4\%; HHI = 2,650 puntos | Mercado altamente concentrado y oligopólico; vigilancia regulatoria requerida en márgenes. |
| **Q2** | Subsidios en Frontera | Z-Score > 2.5; cupos agotados en día 15–20 | Sobreutilización y riesgo de contrabando; necesaria fiscalización telemática en surtidor. |
| **Q3** | Infraestructura GNCV | Correlación $r=0.72$ en EDS vs $r=0.38$ en talleres | Consumo centralizado en 4 metrópolis; falta masificación e incentivos en ciudades medianas. |
| **Q4** | Mezcla de Biocombustibles | B100: 95\%–104\% cobertura; Etanol: 61\%–75\% | B100 autosuficiente; etanol presenta déficit estructural que exige importaciones o cupos flexibles. |
| **Q5** | Tendencias Cruzadas | Caída hidro (-22\%) dispara térmica (+55\%) y diésel | Fuerte vulnerabilidad climática; hidrocarburos actúan como respaldo crítico del sistema eléctrico. |
