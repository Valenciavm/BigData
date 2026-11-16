import boto3
import os
from dotenv import load_dotenv

ssm_client = boto3.client("ssm", region_name="us-east-2")

load_dotenv()

INSTANCE_ID = os.getenv("EC2_INSTANCE_ID")

ec2_path = "/home/ec2-user/data/beneficio-frontera/Beneficio_de_estaciones_de_servicio_en_zona_de_frontera__Asignación_de_combustibles_líquidos_en_zona_de_frontera_(AUTOMATIZADO)_20261114.csv"
s3_path = "s3://big-data-lake/raw/ec2/beneficios-frontera/"
command = f"aws s3 cp {ec2_path} {s3_path}"

response = ssm_client.send_command(
    InstanceIds     = [INSTANCE_ID],
    DocumentName    = "AWS-RunShellScript",
    Parameters      = {"commands": [command]}
)

command_id = response["Command"]["CommandId"]

print(f"ec2 command: {command} sent")

ec2_path = "/home/ec2-user/data/ventas-gncv/Consulta_Ventas_de_Gas_Natural_Comprimido_Vehicular_(AUTOMATIZADO)_20261114.csv"
s3_path = "s3://big-data-lake/raw/ec2/ventas-gncv/"
command =f"aws s3 cp {ec2_path} {s3_path}"

response = ssm_client.send_command(
    InstanceIds     = [INSTANCE_ID],
    DocumentName    = "AWS-RunShellScript",
    Parameters      = {"commands": [command]}
    
)

print(f"ec2 command: {command} sent")


