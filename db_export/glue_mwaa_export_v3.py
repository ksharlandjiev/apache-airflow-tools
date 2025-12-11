import json
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.models import Variable
import boto3

## AWS Glue Settings

# AWS Region where connection and job will be executed
AWS_REGION= Variable.get("aws_region", default_var="us-east-1")

# Glue Execution Role
AWS_GLUE_ROLE_NAME = Variable.get("glue_role_name", "your-iam-role-name")

# Target S3 bucket
S3_BUCKET = Variable.get("export_s3_bucket", "your-s3-bucket-name")

# Script location
SCRIPT_LOCATION = f's3://{S3_BUCKET}/scripts/mwaa_metadb_export.py'

# Define data window
MAX_AGE_IN_DAYS = 30

# Define the tables and date fields to export from Airflow 3.0 metadata database
# Prioritized by importance and likelihood to exist in MWAA environments
OBJECTS_TO_EXPORT = [
    # Core execution tables (highest priority)
    {"table": "dag_run", "date_field": "execution_date"},
    {"table": "task_instance", "date_field": "start_date"},
    {"table": "xcom", "date_field": "execution_date"},
    
    # Historical and logging tables
    {"table": "task_instance_history", "date_field": "start_date"},
    {"table": "log", "date_field": "dttm"},
    {"table": "import_error", "date_field": "timestamp"},
    
    # DAG metadata tables
    {"table": "dag", "date_field": "last_parsed_time"},
    {"table": "dag_code", "date_field": "last_updated"},
    {"table": "dag_version", "date_field": "created_at"},
    
    # Configuration tables (no date filter - export all)
    {"table": "variable", "date_field": None},
    {"table": "connection", "date_field": None},
    {"table": "slot_pool", "date_field": None},
    
    # Notes and annotations (if they exist)
    {"table": "dag_run_note", "date_field": "created_at"},
    {"table": "task_instance_note", "date_field": "created_at"},
    
    # Asset/Dataset tables (new in 3.0, may not exist in all environments)
    {"table": "asset_event", "date_field": "timestamp"},
    {"table": "asset", "date_field": "created_at"},
    
    # Job and trigger tables
    {"table": "job", "date_field": "start_date"},
    {"table": "trigger", "date_field": "created_date"},
    
    # Backfill tables (new in 3.0, may not exist)
    {"table": "backfill", "date_field": "created_at"},
    {"table": "backfill_dag_run", "date_field": None},
]

# Do not edit below this line
ENV_NAME = os.getenv("AIRFLOW_ENV_NAME")
DAG_ID = os.path.basename(__file__).replace(".py", "")

# Task to create a Glue connection
@task()
def create_glue_connection():
    
    # Try to get SQL Alchemy connection string first (Airflow 2.x style)
    sql_alchemy_conn = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
    if sql_alchemy_conn is None:
        # Retry with the new environment variable name
        sql_alchemy_conn = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    
    # If SQL Alchemy connection is not available or blocked, try MWAA-specific variables (Airflow 3.x)
    if sql_alchemy_conn is None or sql_alchemy_conn.startswith("airflow-db-not-allowed"):
        print("SQL Alchemy connection not available, trying MWAA-specific environment variables...")
        
        # Get MWAA database credentials
        mwaa_db_credentials = os.getenv("DB_SECRETS")
        mwaa_db_host = os.getenv("POSTGRES_HOST")
        mwaa_db_port = os.getenv("POSTGRES_PORT", "5432")
        mwaa_db_name = os.getenv("POSTGRES_DB", "AirflowMetadata")
        
        if not all([mwaa_db_credentials, mwaa_db_host]):
            raise ValueError("Neither SQL_ALCHEMY_CONN nor MWAA database environment variables are available.")
        
        # Parse MWAA credentials JSON
        try:
            import json
            credentials = json.loads(mwaa_db_credentials)
            username = credentials["username"]
            password = credentials["password"]
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Failed to parse MWAA__DB__CREDENTIALS: {e}")
        
        # Construct JDBC URL from MWAA variables
        jdbc_connection_url = f"jdbc:postgresql://{mwaa_db_host}:{mwaa_db_port}/{mwaa_db_name}"
        
        print(f"Using MWAA database variables:")
        print(f"  Host: {mwaa_db_host}")
        print(f"  Port: {mwaa_db_port}")
        print(f"  Database: {mwaa_db_name}")
        print(f"  Username: {username}")
        print(f"  JDBC URL: {jdbc_connection_url}")
        
    else:
        # Parse traditional SQL Alchemy connection string
        print(f"Using SQL Alchemy Connection: {sql_alchemy_conn}")
        
        # Parse JDBC Connection String
        s = sql_alchemy_conn.split('@')
        s2 = s[1].split('?')[0]
        jdbc_connection_url = f'jdbc:postgresql://{s2}'
        c = s[0].split('//')[1].split(':')
        username = c[0]
        password = c[1]
        
        print(f"Parsed JDBC URL: {jdbc_connection_url}")
        print(f"Username: {username}")
    
    conn_name = f'{ENV_NAME}_conn'
    glue_client = boto3.client('glue', region_name=AWS_REGION)

    # Check if Glue connection name exists
    try:
        glue_client.get_connection(Name=conn_name)
        print(f"Connection {conn_name} already exists.")
        return conn_name
    except glue_client.exceptions.EntityNotFoundException:
        pass
    
    # Export environment network configuration
    mwaa_client = boto3.client('mwaa', region_name=AWS_REGION)
    response = mwaa_client.get_environment(Name=ENV_NAME)
    mwaa_security_group_ids = response['Environment']['NetworkConfiguration']['SecurityGroupIds']
    mwaa_subnet_id = response['Environment']['NetworkConfiguration']['SubnetIds'][0]

    ec2_client = boto3.client('ec2', region_name=AWS_REGION)
    response = ec2_client.describe_subnets(SubnetIds=[mwaa_subnet_id], DryRun=False)
    mwaa_subnet_az = response['Subnets'][0]['AvailabilityZone']

    # Create new AWS Glue connection
    mwaa_metadb_conn = {
        'Name': conn_name,
        'Description': f'{ENV_NAME} MetaDB connection',
        'ConnectionType': 'JDBC',
        'MatchCriteria': ['string'],
        'ConnectionProperties': {
            'JDBC_ENFORCE_SSL': 'false',
            'JDBC_CONNECTION_URL': jdbc_connection_url,
            'PASSWORD': password,
            'USERNAME': username,
            'KAFKA_SSL_ENABLED': 'false'
        },
        'PhysicalConnectionRequirements': {
            'SubnetId': mwaa_subnet_id,
            'SecurityGroupIdList': mwaa_security_group_ids,
            'AvailabilityZone': mwaa_subnet_az
        }
    }

    try:
        glue_client.create_connection(ConnectionInput=mwaa_metadb_conn)
        print(f"Connection {conn_name} created successfully.")
    except Exception as e:
        print(f"Failed to create connection: {e}")
    
    return conn_name

# Define the DAG
@dag(dag_id=DAG_ID, schedule=None, start_date=datetime(2024, 1, 1))
def glue_mwaa_export_dag():
    
    # Task 1: Create Glue Connection
    glue_connection_name = create_glue_connection()

    # Task 2: Run Glue Job and export table data in CSV.
    glue_job_name = f"{ENV_NAME}_export"
    database_name = Variable.get("database_name", default_var="airflow_metadb")

    glue_job_task = GlueJobOperator(
        task_id="run_glue_job",
        job_name=glue_job_name,
        script_location=SCRIPT_LOCATION,
        s3_bucket=S3_BUCKET,
        region_name=AWS_REGION,
        iam_role_name=AWS_GLUE_ROLE_NAME,
        script_args={
            '--S3_OUTPUT_PATH': f"s3://{S3_BUCKET}/exports/{ENV_NAME}",
            '--DATABASE_NAME': database_name,
            '--EXPORT_TABLES': json.dumps(OBJECTS_TO_EXPORT),
            '--GLUE_CONNECTION_NAME': glue_connection_name,
            '--MAX_AGE_IN_DAYS': str(MAX_AGE_IN_DAYS),
        },
        create_job_kwargs={
            'NumberOfWorkers': 1,
            'WorkerType': 'Standard',
            'GlueVersion': '4.0',
            'Connections': {
                'Connections': [glue_connection_name]
            }            
        },
        do_xcom_push=True,
    )

    glue_connection_name >> glue_job_task

# Instantiate the DAG
glue_mwaa_export_dag_d = glue_mwaa_export_dag()
