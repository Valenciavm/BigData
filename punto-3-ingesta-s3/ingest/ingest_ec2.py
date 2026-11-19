import boto3
import os
from dotenv import load_dotenv

load_dotenv(override=True)

ssm_client = boto3.client("ssm", region_name="us-east-2")
INSTANCE_ID = os.getenv("EC2_INSTANCE_ID")

# (ec2_local_path, s3_destination)
FILES = [
    (
        "/home/ec2-user/data/beneficio-frontera/Beneficio_de_estaciones_de_servicio_en_zona_de_frontera__Asignación_de_combustibles_líquidos_en_zona_de_frontera_(AUTOMATIZADO)_20261114.csv",
        "s3://big-data-lake-energiacol/raw/ec2/beneficios-frontera/"
    ),
    (
        "/home/ec2-user/data/ventas-gncv/Consulta_Ventas_de_Gas_Natural_Comprimido_Vehicular_(AUTOMATIZADO)_20261114.csv",
        "s3://big-data-lake-energiacol/raw/ec2/ventas-gncv/"
    ),
    (
        "/home/ec2-user/data/iea-monthly-electricity-statistics/file_zip.csv",
        "s3://big-data-lake-energiacol/raw/ec2/iea-monthly-electricity-statistics/"
    ),
]

for ec2_path, s3_path in FILES:
    command = f'aws s3 cp "{ec2_path}" "{s3_path}"'

    response = ssm_client.send_command(
        InstanceIds  = [INSTANCE_ID],
        DocumentName = "AWS-RunShellScript",
        Parameters   = {"commands": [command]}
    )

    command_id = response["Command"]["CommandId"]
    print(f"[EC2] command sent: {command_id}  ({s3_path})")
