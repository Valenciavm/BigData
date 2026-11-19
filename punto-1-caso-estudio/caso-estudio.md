# Caso de Estudio y Preguntas de Negocio
## *Descripción del Caso de Estudio*

El proyecto aborda la cadena logística, distribución y consumo del sector energético e hidrocarburífero en Colombia (Gasolina, Diésel/ACPM, Biocombustibles, Gas Natural Vehicular GNCV y métricas de consumo eléctrico).

Mediante el uso de una arquitectura Big Data en AWS (RDS MariaDB, EC2 y almacenamiento masivo vía API/URL hacia S3, AWS Glue, EMR/PySpark y Athena), el sistema integra más de 13.9 millones de registros heterogéneos para analizar la concentración del mercado mayorista, la efectividad de los subsidios en regiones fronterizas y la trazabilidad de los biocombustibles.

## Preguntas de Negocio

+ Concentración del Mercado Mayorista: ¿Qué porcentaje del volumen total de despacho de combustibles (7.18M de filas) concentran los principales distribuidores mayoristas frente a la red nacional de estaciones de servicio (6.48M de filas)?

+ Impacto de Subsidios en Zonas de Frontera: ¿Existe una desviación o incremento anómalo en el volumen de combustible despachado hacia estaciones ubicadas en zonas de frontera beneficiadas (90.4K de filas) en comparación con el comportamiento promedio nacional por departamento?

+ Correlación de Infraestructura GNCV: ¿Cómo correlaciona la densidad geográfica de agentes autorizados (1,761) y distribuidores de GNCV con el volumen real de ventas mensuales de gas comprimido vehicular (135K) a nivel municipal?

+ Trazabilidad y Cumplimiento de Biocombustibles: ¿La capacidad de producción nacional de B100 y Etanol (47K) registrada en los catálogos satisface la cuota de mezcla obligatoria exigida en los despachos masivos de los distribuidores mayoristas?

+ Macro-tendencias Energéticas Cruzadas: ¿Cómo se comportan estacionalmente las métricas de generación eléctrica mensual de la IEA (180K) en paralelo con los picos de despacho de combustibles fósiles y GNCV en el mercado nacional?
