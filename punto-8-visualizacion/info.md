# Visualización de resultados — Energy Colombia Dashboard

## 1. Objetivo

Como complemento de la capa Analytics desarrollada con AWS Glue y PySpark, se implementó una aplicación de visualización utilizando **Python, Streamlit y Plotly**.

El propósito es proporcionar una interfaz interactiva para consultar, explorar y visualizar los resultados de los cinco análisis desarrollados (Q1–Q5), sin depender de Amazon QuickSight.

La aplicación se ejecuta localmente en el equipo del usuario y consume los archivos Parquet almacenados en Amazon S3.

Esta solución permite mantener la arquitectura de procesamiento y almacenamiento en AWS, trasladando únicamente la capa de visualización al entorno local.

## 2. Arquitectura implementada

El flujo de información es el siguiente:

1. **AWS Glue:** ejecuta los trabajos PySpark correspondientes a Q1–Q5.
2. **Amazon S3:** almacena los resultados analíticos en formato Parquet.
3. **AWS IAM:** proporciona las credenciales y permisos necesarios para consultar los archivos almacenados.
4. **Python y s3fs:** permiten establecer la conexión con S3 y recuperar los datos.
5. **Pandas:** carga y organiza los datos para su exploración.
6. **Streamlit:** proporciona la interfaz de usuario.
7. **Plotly:** genera visualizaciones interactivas.

### Flujo de arquitectura

AWS Glue (PySpark Q1–Q5)  
↓  
Amazon S3 — Analytics (Parquet)  
↓  
AWS IAM — Autenticación y autorización  
↓  
Python + s3fs + Pandas  
↓  
Streamlit + Plotly  
↓  
Dashboard interactivo local

## 3. Tecnologías utilizadas

| Tecnología | Función |
|---|---|
| Python | Lenguaje de implementación |
| Streamlit | Desarrollo de la interfaz web |
| Plotly | Visualizaciones interactivas |
| Pandas | Manipulación de datos |
| s3fs | Acceso a archivos almacenados en S3 |
| PyArrow | Lectura de archivos Parquet |
| Boto3 / AWS CLI | Integración y autenticación con AWS |
| Amazon S3 | Almacenamiento de los resultados analíticos |

## 4. Estructura del proyecto

La estructura propuesta para la aplicación es:

```text
energia-dashboard/
│
├── app.py
│
├── requirements.txt
│
├── venv/
│
└── analytics-data/   (opcional)
```

Descripción:

- `app.py`: implementación principal del dashboard.
- `requirements.txt`: dependencias necesarias para ejecutar la aplicación.
- `venv/`: entorno virtual de Python.
- `analytics-data/`: directorio opcional para almacenar copias locales de los resultados descargados desde S3.

La aplicación puede consultar directamente S3, por lo que no es obligatorio descargar los archivos Parquet al equipo local.

## 5. Configuración de AWS

### 5.1. Configuración de IAM

Para permitir que la aplicación consulte los datos de S3, se configuró un usuario IAM dedicado:

`energia-dashboard-user`

Se le asignó una política personalizada denominada:

`EnergiaAnalyticsReadOnly`

La política proporciona permisos de lectura sobre el bucket:

`s3://big-data-lake-energiacol/`

Específicamente:

- `s3:ListBucket`: permite consultar los objetos y directorios disponibles dentro del prefijo Analytics.
- `s3:GetObject`: permite recuperar los archivos almacenados en dicho prefijo.

No se conceden permisos de escritura ni eliminación.

### 5.2. Creación de Access Keys

Desde AWS IAM:

1. Acceder a IAM → Users.
2. Seleccionar `energia-dashboard-user`.
3. Abrir Security credentials.
4. Localizar la sección Access keys.
5. Seleccionar Create access key.
6. Elegir Command Line Interface (CLI).
7. Guardar el Access Key ID y el Secret Access Key.

El Secret Access Key solamente se muestra durante su creación. Debe almacenarse de forma segura.

### 5.3. Instalación de AWS CLI

En macOS, se puede instalar mediante Homebrew:

```bash
brew install awscli
```

Verificar la instalación:

```bash
aws --version
```

### 5.4. Configuración de credenciales

Ejecutar:

```bash
aws configure
```

Ingresar:

```text
AWS Access Key ID: YOUR_ACCESS_KEY_ID
AWS Secret Access Key: YOUR_SECRET_ACCESS_KEY
Default region name: YOUR_BUCKET_REGION
Default output format: json
```

La región debe coincidir con la región donde se encuentra el bucket.

### 5.5. Validación de autenticación

Ejecutar:

```bash
aws sts get-caller-identity
```

Si la configuración es correcta, AWS devuelve información sobre la identidad autenticada, incluyendo el identificador de cuenta y el ARN del usuario IAM.

Posteriormente, verificar el acceso al bucket:

```bash
aws s3 ls s3://big-data-lake-energiacol/analytics/
```

Este comando permite comprobar que el usuario puede consultar el directorio de resultados analíticos.

**Nota:** `get-caller-identity` únicamente valida la autenticación. No ejecuta los trabajos Glue ni genera archivos en S3.

## 6. Instalación y configuración del entorno Python

Se recomienda utilizar un entorno virtual para aislar las dependencias de la aplicación.

### 6.1. Crear el entorno virtual

Desde el directorio del proyecto:

```bash
python3 -m venv venv
```

### 6.2. Activar el entorno

En macOS:

```bash
source venv/bin/activate
```

Cuando se encuentra activo, el terminal muestra `(venv)` al inicio de la línea de comandos.

### 6.3. Instalar dependencias

Archivo `requirements.txt`:

```text
streamlit
pandas
plotly
s3fs
pyarrow
boto3
```

Instalar:

```bash
pip install -r requirements.txt
```

## 7. Implementación del dashboard

El archivo principal `app.py` contiene la lógica de visualización.

Sus principales componentes son:

### 7.1. Conexión con Amazon S3

Se utiliza `s3fs` para establecer una conexión con el bucket:

`s3://big-data-lake-energiacol/`

La aplicación utiliza las credenciales configuradas mediante AWS CLI.

### 7.2. Descubrimiento de datasets

La aplicación identifica automáticamente los archivos Parquet disponibles dentro de:

`s3://big-data-lake-energiacol/analytics/`

Los resultados se agrupan de acuerdo con sus directorios para facilitar la selección de datasets.

### 7.3. Carga de datos

Los archivos Parquet seleccionados se recuperan desde S3 y se cargan mediante Pandas.

Cuando un dataset contiene varios archivos Parquet, estos se concatenan para facilitar su exploración.

### 7.4. Navegación

La interfaz permite seleccionar entre cinco análisis:

- Q1 — Market Concentration.
- Q2 — Border Subsidies.
- Q3 — GNCV Infrastructure.
- Q4 — Biofuels.
- Q5 — Energy Trends.

Dentro de cada análisis se puede seleccionar el dataset que se desea consultar.

### 7.5. Indicadores generales

La aplicación presenta indicadores básicos:

- Total de registros.
- Número de columnas.
- Cantidad de valores nulos.

### 7.6. Exploración y calidad de datos

Se incorporaron componentes para:

- Visualizar registros de los datasets.
- Consultar los tipos de datos.
- Identificar valores nulos.
- Calcular el porcentaje de valores faltantes por columna.

### 7.7. Visualizaciones interactivas

Se implementó un generador de visualizaciones que permite seleccionar variables numéricas y categóricas.

Los tipos de gráficos disponibles son:

- Bar Chart.
- Line Chart.
- Scatter Plot.
- Histogram.

Las visualizaciones se generan mediante Plotly y se integran en la interfaz de Streamlit.

### 7.8. Exportación

Se incorporó una funcionalidad para descargar el dataset seleccionado en formato CSV.

La exportación se realiza desde la interfaz del dashboard hacia el equipo local.

## 8. Ejecución de la aplicación

Una vez configurado el entorno, ejecutar los siguientes comandos desde Terminal.

### Paso 1. Acceder al directorio

```bash
cd energia-dashboard
```

### Paso 2. Activar el entorno virtual

```bash
source venv/bin/activate
```

### Paso 3. Ejecutar Streamlit

Si el archivo principal se llama `app.py`:

```bash
streamlit run app.py
```

Si se utiliza el nombre `dashboard.py`:

```bash
streamlit run dashboard.py
```

**Importante:** no se debe ejecutar mediante `python app.py`, ya que esto no inicia correctamente el servidor de Streamlit.

### Paso 4. Acceder a la interfaz

Streamlit inicia un servidor local y permite consultar la aplicación desde el navegador:

`http://localhost:8501`

El terminal debe permanecer abierto mientras se utiliza el dashboard.

Para detener la aplicación:

```text
Ctrl + C
```

Para desactivar el entorno virtual:

```bash
deactivate
```

## 9. Consideraciones técnicas

### 9.1. Lectura y escritura en S3

La implementación actual está diseñada para consultar los resultados existentes en S3.

No modifica los datasets originales ni genera automáticamente nuevos archivos Parquet.

La generación de resultados analíticos continúa siendo responsabilidad de los trabajos AWS Glue Q1–Q5.

### 9.2. Permisos

La política IAM implementada proporciona acceso de lectura.

Si posteriormente se desea guardar resultados adicionales en S3, será necesario incorporar permisos de escritura específicos, como `s3:PutObject`, preferiblemente sobre un directorio independiente.

### 9.3. Ejecución local

La aplicación se ejecuta en el equipo del usuario y no requiere una instancia EC2.

Por tanto, no depende de permisos de Amazon QuickSight ni de Amazon EMR.

### 9.4. Credenciales

Las credenciales deben mantenerse fuera del código fuente y de los repositorios públicos.

El usuario IAM debe conservar únicamente los permisos necesarios para consultar los datos.

## 10. Estado de implementación

| Componente | Estado |
|---|---|
| Conexión con Amazon S3 | Implementada |
| Autenticación mediante AWS CLI | Configurada |
| Descubrimiento de archivos Parquet | Implementado |
| Carga de datasets con Pandas | Implementada |
| Navegación entre Q1–Q5 | Implementada |
| Indicadores generales | Implementados |
| Reporte básico de calidad | Implementado |
| Gráficos interactivos genéricos | Implementados |
| Exportación CSV local | Implementada |
| Exportación adicional hacia S3 | Opcional, no implementada |
| Visualizaciones especializadas por pregunta | Pendientes de personalización |
| Despliegue remoto del dashboard | No implementado |

## 11. Resultado

Se desarrolló una alternativa de visualización basada en herramientas Python de código abierto, conectada directamente con los resultados analíticos almacenados en Amazon S3.

Esta implementación permite consultar los datasets de Q1–Q5, explorar sus estructuras, revisar indicadores de calidad y generar visualizaciones interactivas sin depender de servicios adicionales de visualización de AWS.

La solución mantiene la separación entre procesamiento, almacenamiento y visualización, conservando AWS Glue como motor de procesamiento analítico y Amazon S3 como repositorio de resultados.
