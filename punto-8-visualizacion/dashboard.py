
import streamlit as st
import pandas as pd
import plotly.express as px
import s3fs
from collections import defaultdict

# ============================================================
# 1. CONFIGURATION
# ============================================================

BUCKET = "big-data-lake-energiacol"
ANALYTICS_PREFIX = "analytics"

ANALYSES = {
    "Q1 — Market Concentration": "q1_concentracion",
    "Q2 — Border Subsidies": "q2_subsidios_frontera",
    "Q3 — GNCV Infrastructure": "q3_infraestructura_gncv",
    "Q4 — Biofuels": "q4_biocombustibles",
    "Q5 — Energy Trends": "q5_tendencias_energeticas"
}

st.set_page_config(
    page_title="Energy Colombia | Analytics",
    page_icon="⚡",
    layout="wide"
)

# ============================================================
# 2. HEADER
# ============================================================

st.title("⚡ Energy Colombia — Analytics Dashboard")
st.caption(
    "AWS Glue · Amazon S3 · Parquet · Python · Streamlit"
)

st.divider()

# ============================================================
# 3. S3 CONNECTION
# ============================================================

@st.cache_resource
def get_s3():
    return s3fs.S3FileSystem()


# ============================================================
# 4. DISCOVER PARQUET DATASETS
# ============================================================

@st.cache_data(ttl=300)
def discover_datasets():

    fs = get_s3()

    root = f"{BUCKET}/{ANALYTICS_PREFIX}"

    if not fs.exists(root):
        raise FileNotFoundError(
            f"S3 path does not exist: s3://{root}"
        )

    files = fs.find(root)

    datasets = defaultdict(list)

    for file in files:

        if file.endswith(".parquet"):

            folder = file.rsplit("/", 1)[0]

            datasets[folder].append(file)

    return dict(datasets)


# ============================================================
# 5. LOAD PARQUET DATA
# ============================================================

@st.cache_data(ttl=300)
def load_dataset(files):

    fs = get_s3()

    frames = []

    for file in files:

        try:

            df = pd.read_parquet(
                f"s3://{file}",
                filesystem=fs,
                engine="pyarrow"
            )

            frames.append(df)

        except Exception as e:

            st.warning(
                f"Could not read {file}: {str(e)}"
            )

    if not frames:
        return pd.DataFrame()

    return pd.concat(
        frames,
        ignore_index=True
    )


# ============================================================
# 6. SIDEBAR NAVIGATION
# ============================================================

st.sidebar.title("Navigation")

selected_analysis = st.sidebar.selectbox(
    "Select analysis",
    list(ANALYSES.keys())
)

analysis_prefix = ANALYSES[selected_analysis]

# ============================================================
# 7. CONNECT AND DISCOVER DATASETS
# ============================================================

try:

    datasets = discover_datasets()

except Exception as e:

    st.error("Unable to connect to Amazon S3.")

    st.exception(e)

    st.info(
        "Check your AWS credentials and S3 permissions. "
        "Run `aws sts get-caller-identity` in Terminal."
    )

    st.stop()


if not datasets:

    st.warning(
        "No Parquet datasets were found in the Analytics bucket."
    )

    st.stop()


# ============================================================
# 8. FILTER DATASETS BY ANALYSIS
# ============================================================

available = {
    path: files
    for path, files in datasets.items()
    if analysis_prefix in path
}


if not available:

    st.warning(
        f"No datasets found for {selected_analysis}."
    )

    st.write("Available S3 datasets:")

    for path in datasets:

        st.write(f"- s3://{path}")

    st.stop()


# ============================================================
# 9. DATASET SELECTION
# ============================================================

dataset_names = {
    path.split("/")[-1]: path
    for path in available
}

selected_dataset = st.sidebar.selectbox(
    "Select dataset",
    sorted(dataset_names.keys())
)

selected_path = dataset_names[selected_dataset]

# ============================================================
# 10. LOAD SELECTED DATASET
# ============================================================

try:

    with st.spinner(
        "Loading analytical data from Amazon S3..."
    ):

        df = load_dataset(
            available[selected_path]
        )

except Exception as e:

    st.error("Error loading the selected dataset.")

    st.exception(e)

    st.stop()


if df.empty:

    st.warning(
        "The selected dataset is empty or could not be read."
    )

    st.stop()


# ============================================================
# 11. DATASET HEADER
# ============================================================

st.header(selected_analysis)

st.subheader(
    selected_dataset.replace("_", " ").title()
)

st.caption(
    f"Source: s3://{selected_path}/"
)

# ============================================================
# 12. KEY PERFORMANCE INDICATORS
# ============================================================

col1, col2, col3 = st.columns(3)

col1.metric(
    "Total records",
    f"{len(df):,}"
)

col2.metric(
    "Total columns",
    f"{len(df.columns):,}"
)

col3.metric(
    "Missing values",
    f"{df.isna().sum().sum():,}"
)

st.divider()

# ============================================================
# 13. DATA PREVIEW
# ============================================================

st.subheader("Dataset Preview")

with st.expander("View records", expanded=True):

    st.dataframe(
        df.head(500),
        use_container_width=True,
        height=350
    )

# ============================================================
# 14. DATA QUALITY
# ============================================================

with st.expander("Data Quality Report"):

    quality = pd.DataFrame({

        "Column": df.columns,

        "Data Type": [
            str(dtype)
            for dtype in df.dtypes
        ],

        "Null Values": [
            int(value)
            for value in df.isna().sum()
        ],

        "Null Percentage": (
            df.isna().mean() * 100
        ).round(2).values

    })

    st.dataframe(
        quality,
        use_container_width=True
    )


# ============================================================
# 15. INTERACTIVE VISUALIZATIONS
# ============================================================

st.divider()

st.header("Interactive Visualization")

numeric_columns = df.select_dtypes(
    include="number"
).columns.tolist()

categorical_columns = df.select_dtypes(
    include=["object", "category", "string"]
).columns.tolist()


if not numeric_columns:

    st.info(
        "No numerical columns available for visualization."
    )

else:

    chart_type = st.selectbox(
        "Visualization type",
        [
            "Bar Chart",
            "Line Chart",
            "Scatter Plot",
            "Histogram"
        ]
    )

    y_column = st.selectbox(
        "Numerical variable",
        numeric_columns
    )

    fig = None

    if chart_type == "Histogram":

        fig = px.histogram(
            df,
            x=y_column,
            title=f"Distribution of {y_column}"
        )

    elif chart_type == "Scatter Plot":

        if len(numeric_columns) < 2:

            st.info(
                "A scatter plot requires at least two numerical variables."
            )

        else:

            x_column = st.selectbox(
                "X variable",
                [
                    col
                    for col in numeric_columns
                    if col != y_column
                ]
            )

            fig = px.scatter(
                df,
                x=x_column,
                y=y_column,
                title=f"{y_column} vs. {x_column}"
            )

    else:

        if not categorical_columns:

            st.info(
                "No categorical columns available for this chart."
            )

        else:

            x_column = st.selectbox(
                "Category / Time variable",
                categorical_columns
            )

            if chart_type == "Bar Chart":

                grouped = (
                    df.groupby(x_column, dropna=False)[y_column]
                    .sum()
                    .reset_index()
                    .sort_values(y_column, ascending=False)
                    .head(100)
                )

                fig = px.bar(
                    grouped,
                    x=x_column,
                    y=y_column,
                    title=f"{y_column} by {x_column}"
                )

            elif chart_type == "Line Chart":

                grouped = (
                    df.groupby(x_column, dropna=False)[y_column]
                    .sum()
                    .reset_index()
                )

                fig = px.line(
                    grouped,
                    x=x_column,
                    y=y_column,
                    title=f"{y_column} over {x_column}",
                    markers=True
                )

    if fig is not None:

        fig.update_layout(
            template="plotly_white",
            xaxis_title=None,
            yaxis_title=y_column
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )


# ============================================================
# 16. DOWNLOAD DATA
# ============================================================

st.divider()

st.header("Export Data")

csv = df.to_csv(
    index=False
).encode("utf-8")

st.download_button(
    label="Download dataset as CSV",
    data=csv,
    file_name=f"{selected_dataset}.csv",
    mime="text/csv"
)


# ============================================================
# 17. FOOTER
# ============================================================

st.divider()

st.caption(
    "Energy Colombia | Big Data Analytics | "
    "AWS Glue + Amazon S3"
)
