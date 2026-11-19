import os
import sys
import boto3
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

load_dotenv(override=True)

BUCKET = "big-data-lake-energiacol"
REGION = "us-east-2"

DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT", "3306")
DB_NAME     = os.getenv("DB_NAME", "energia_col")

INSTANCE_ID = os.getenv("EC2_INSTANCE_ID")
GOV_TOKEN   = os.getenv("GOV_TOKEN")

API_URLS = {
    "distribuidores_mayorista": "https://www.datos.gov.co/resource/339g-zjac.json",
    "estaciones_servicio":      "https://www.datos.gov.co/resource/fbht-2fzd.json"
}

def print_result(name: str, passed: bool, detail: str = ""):
    tag = "[PASS]" if passed else "[FAIL]"
    print(f"{tag:<8} {name:<35} {detail}")
    return passed

def run_sanity_checks() -> bool:
    print("\n" + "="*70)
    print("           PRE-FLIGHT SANITY CHECK (BigData Ingest)")
    print("="*70)
    all_ok = True

    # 1. Environment variables
    missing_vars = []
    for var_name, val in [
        ("DB_USER", DB_USER),
        ("DB_PASSWORD", DB_PASSWORD),
        ("DB_HOST", DB_HOST),
        ("EC2_INSTANCE_ID", INSTANCE_ID),
        ("GOV_TOKEN", GOV_TOKEN),
    ]:
        if not val:
            missing_vars.append(var_name)

    if missing_vars:
        all_ok = False
        print_result("Environment Variables", False, f"Missing in .env: {missing_vars}")
    else:
        print_result("Environment Variables", True, "All required variables present")

    # 2. AWS Credentials
    try:
        sts = boto3.client("sts", region_name=REGION)
        caller = sts.get_caller_identity()
        account_id = caller.get("Account")
        print_result("AWS Credentials / IAM", True, f"Account: {account_id}")
    except Exception as e:
        all_ok = False
        print_result("AWS Credentials / IAM", False, str(e))

    # 3. S3 Bucket
    try:
        s3 = boto3.client("s3", region_name=REGION)
        s3.head_bucket(Bucket=BUCKET)
        print_result("S3 Bucket", True, f"Bucket '{BUCKET}' is reachable")
    except Exception as e:
        all_ok = False
        print_result("S3 Bucket", False, f"Bucket '{BUCKET}' unreachable: {e}")

    # 4. RDS MariaDB
    try:
        db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url, connect_args={"connect_timeout": 5})
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        expected_tables = ["productores_b100E", "agentes_gnvc"]
        found = [t for t in expected_tables if t in tables]
        if len(found) == len(expected_tables):
            print_result("RDS MariaDB & Tables", True, f"Connected to '{DB_NAME}', tables: {found}")
        else:
            all_ok = False
            print_result("RDS MariaDB & Tables", False, f"Missing tables. Found: {tables}")
    except Exception as e:
        all_ok = False
        print_result("RDS MariaDB & Tables", False, f"Connection failed: {e}")

    # 5. EC2 & SSM Agent
    try:
        ssm = boto3.client("ssm", region_name=REGION)
        resp = ssm.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": [INSTANCE_ID]}]
        )
        info_list = resp.get("InstanceInformationList", [])
        if info_list:
            ping_status = info_list[0].get("PingStatus")
            if ping_status == "Online":
                print_result("EC2 SSM Status", True, f"{INSTANCE_ID} SSM agent is Online")
            else:
                all_ok = False
                print_result("EC2 SSM Status", False, f"{INSTANCE_ID} SSM PingStatus: {ping_status}")
        else:
            ec2 = boto3.client("ec2", region_name=REGION)
            ec2_resp = ec2.describe_instances(InstanceIds=[INSTANCE_ID])
            state = ec2_resp["Reservations"][0]["Instances"][0]["State"]["Name"]
            all_ok = False
            print_result("EC2 SSM Status", False, f"EC2 state is '{state}', but SSM agent is not registered")
    except Exception as e:
        all_ok = False
        print_result("EC2 SSM Status", False, str(e))

    # 6. Open Data APIs
    headers = {"X-App-Token": GOV_TOKEN}
    for api_name, url in API_URLS.items():
        try:
            r = requests.get(url, params={"$limit": 1}, headers=headers, timeout=20)
            if r.status_code == 200:
                print_result(f"API: {api_name}", True, "Status 200 OK — endpoint alive & token valid")
            else:
                all_ok = False
                print_result(f"API: {api_name}", False, f"HTTP {r.status_code}: {r.text[:80]}")
        except Exception as e:
            all_ok = False
            print_result(f"API: {api_name}", False, str(e))

    print("="*70)
    if all_ok:
        print(">> ALL SANITY CHECKS PASSED. Ready to run ingestion pipelines.\n")
    else:
        print(">> SOME CHECKS FAILED. Please review the errors above before running.\n")
    return all_ok

if __name__ == "__main__":
    passed = run_sanity_checks()
    sys.exit(0 if passed else 1)
