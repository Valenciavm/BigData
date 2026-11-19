import os
import sys
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
load_dotenv(os.path.join(PROJECT_DIR, ".env"), override=True)

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
DB_NAME = "energia_col_trusted"
CRAWLER_NAME = "energia_col_trusted_crawler"
GLUE_ROLE_ARN = os.getenv("GLUE_ROLE_ARN")
TRUSTED_PREFIX = "s3://big-data-lake-energiacol/trusted/"

def ensure_database(glue_client, db_name: str) -> None:
    """Creates the Glue Data Catalog database if it does not already exist."""
    try:
        glue_client.get_database(Name=db_name)
        print(f"[Glue DB] Database '{db_name}' already exists.")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityNotFoundException":
            glue_client.create_database(
                DatabaseInput={
                    "Name": db_name,
                    "Description": "Trusted Parquet catalog for Energiacol data lake"
                }
            )
            print(f"[Glue DB] Database '{db_name}' created successfully.")
        else:
            raise

def upsert_crawler(glue_client, crawler_name: str, db_name: str, role_arn: str, s3_target: str) -> None:
    """Creates or updates the AWS Glue Crawler pointing to the trusted S3 zone."""
    if not role_arn:
        print("[Crawler] WARNING: 'GLUE_ROLE_ARN' is not set in .env. Skipping crawler upsert.")
        print("          Add GLUE_ROLE_ARN=arn:aws:iam::<account>:role/<role-name> to .env to provision it.")
        return

    crawler_config = {
        "DatabaseName": db_name,
        "Role": role_arn,
        "Targets": {
            "S3Targets": [{"Path": s3_target}]
        },
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE"
        },
        "RecrawlPolicy": {
            "RecrawlBehavior": "CRAWL_EVERYTHING"
        },
        "Description": "Crawler for scanning clean Parquet datasets in trusted zone"
    }

    try:
        glue_client.get_crawler(Name=crawler_name)
        glue_client.update_crawler(Name=crawler_name, **crawler_config)
        print(f"[Crawler] Crawler '{crawler_name}' updated successfully.")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityNotFoundException":
            glue_client.create_crawler(Name=crawler_name, **crawler_config)
            print(f"[Crawler] Crawler '{crawler_name}' created successfully.")
        else:
            raise

def start_crawler_if_requested(glue_client, crawler_name: str) -> None:
    """Starts the crawler run if '--run' or '--start' argument is passed."""
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["--run", "--start", "run", "start"]:
        try:
            glue_client.start_crawler(Name=crawler_name)
            print(f"[Crawler] Execution started for '{crawler_name}'.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "CrawlerRunningException":
                print(f"[Crawler] Crawler '{crawler_name}' is already running.")
            else:
                raise

def main() -> None:
    glue = boto3.client("glue", region_name=AWS_REGION)
    print("=" * 60)
    print("      AWS GLUE DATA CATALOG & CRAWLER PROVISIONING")
    print("=" * 60)
    
    ensure_database(glue, DB_NAME)
    upsert_crawler(glue, CRAWLER_NAME, DB_NAME, GLUE_ROLE_ARN, TRUSTED_PREFIX)
    start_crawler_if_requested(glue, CRAWLER_NAME)
    print("=" * 60)
    print("Done. Catalog setup ready.")

if __name__ == "__main__":
    main()
