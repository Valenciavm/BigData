$urls = @(
    "https://www.datos.gov.co/Minas-y-Energ-a/Consulta-Ventas-de-Gas-Natural-Comprimido-Vehicula/v8jr-kywh/about_data",
    "https://www.datos.gov.co/Minas-y-Energ-a/Distribuidores-Mayoristas-Despacho-de-combustibles/339g-zjac/about_data",
    "https://www.datos.gov.co/Minas-y-Energ-a/Productores-Productores-de-B100-y-Etanol-Alcohol-C/macx-96u2/about_data",
    "https://www.kaggle.com/datasets/ccanb23/iea-monthly-electricity-statistics",
    "https://www.datos.gov.co/Minas-y-Energ-a/Estaciones-de-Servicio-Automotriz-y-Fluvial-Despac/fbht-2fzd/about_data",
    "https://www.datos.gov.co/Minas-y-Energ-a/Beneficio-de-estaciones-de-servicio-en-zona-de-fro/shbt-hqy9/about_data",
    "https://www.datos.gov.co/Minas-y-Energ-a/Consulta-Agentes-de-Gas-Natural-Comprimido-Vehicul/ba2i-v4xx/about_data"
)

foreach ($url in $urls) {
    Start-Process $url
    Start-Sleep -Milliseconds 500
}
