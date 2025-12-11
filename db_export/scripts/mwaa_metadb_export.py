#!/usr/bin/env python3
"""
AWS Glue script to export MWAA metadata database tables to S3.

This script connects to the Airflow metadata database and exports specified
tables to S3 as compressed CSV files, filtering by date to only include
recent records.

Parameters:
- S3_OUTPUT_PATH: S3 path where exported files will be stored
- DATABASE_NAME: Name of the database (for logging purposes)
- EXPORT_TABLES: JSON string containing table definitions with date fields
- GLUE_CONNECTION_NAME: Name of the Glue connection to the database
- MAX_AGE_IN_DAYS: Maximum age of records to export (filters by date fields)
"""

import sys
import json
import gzip
import csv
from datetime import datetime, timedelta
from io import StringIO
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_OUTPUT_PATH',
    'DATABASE_NAME', 
    'EXPORT_TABLES',
    'GLUE_CONNECTION_NAME',
    'MAX_AGE_IN_DAYS'
])

job.init(args['JOB_NAME'], args)

# Parse parameters
s3_output_path_raw = args['S3_OUTPUT_PATH']
database_name = args['DATABASE_NAME']
export_tables = json.loads(args['EXPORT_TABLES'])
glue_connection_name = args['GLUE_CONNECTION_NAME']
max_age_days = int(args['MAX_AGE_IN_DAYS'])

# Create date-organized path structure: exports/<env_name>/<year>/<month>/<day>/
export_date = datetime.now()
year = export_date.strftime('%Y')
month = export_date.strftime('%m')
day = export_date.strftime('%d')

# Ensure backups are stored in <env_name>-backups folder with date structure
if s3_output_path_raw.endswith('/'):
    s3_output_path_raw = s3_output_path_raw[:-1]

# Split the path to get the environment name (last part)
path_parts = s3_output_path_raw.split('/')
if len(path_parts) >= 2:
    env_name = path_parts[-1]  # Get the last part (environment name)
    # Replace the last part with env_name-backups and add date structure
    path_parts[-1] = f"{env_name}-backups"
    s3_output_path = '/'.join(path_parts) + f"/{year}/{month}/{day}"
else:
    # Fallback: just append -backups and date structure to the entire path
    s3_output_path = f"{s3_output_path_raw}-backups/{year}/{month}/{day}"

print(f"Original S3 path: {s3_output_path_raw}")
print(f"Date-organized S3 path: {s3_output_path}")
print(f"Export date: {export_date.strftime('%Y-%m-%d')}")

# Calculate cutoff date
cutoff_date = datetime.now() - timedelta(days=max_age_days)
cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')

print(f"Starting MWAA metadata export job")
print(f"S3 Output Path: {s3_output_path}")
print(f"Database: {database_name}")
print(f"Connection: {glue_connection_name}")
print(f"Max Age: {max_age_days} days")
print(f"Cutoff Date: {cutoff_date_str}")
print(f"Tables to export: {len(export_tables)}")

# Initialize S3 client
s3_client = boto3.client('s3')

def parse_s3_path(s3_path):
    """Parse S3 path into bucket and key."""
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    parts = s3_path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    return bucket, key

def export_table_to_s3(table_name, date_field, s3_path):
    """Export a single table to S3 as compressed CSV."""
    try:
        print(f"Exporting table: {table_name}")
        
        # Try different table name variations (with and without schema)
        table_variations = [
            table_name,                    # Original name
            f"public.{table_name}",       # With public schema
            f"airflow.{table_name}",      # With airflow schema
        ]
        
        df = None
        successful_table_name = None
        
        for table_variant in table_variations:
            try:
                print(f"Trying to read table: {table_variant}")
                df = glueContext.create_dynamic_frame.from_options(
                    connection_type="postgresql",
                    connection_options={
                        "useConnectionProperties": "true",
                        "connectionName": glue_connection_name,
                        "dbtable": table_variant
                    }
                ).toDF()
                successful_table_name = table_variant
                print(f"Successfully connected to table: {table_variant}")
                break
            except Exception as table_error:
                print(f"Failed to read {table_variant}: {str(table_error)}")
                continue
        
        if df is None:
            print(f"ERROR: Could not find table {table_name} in any schema. Skipping...")
            return 0
        
        # Apply date filter in Spark
        print(f"Applying date filter: {date_field} >= '{cutoff_date_str}'")
        
        # Convert date field to date type and filter
        from pyspark.sql.functions import col, to_date, to_timestamp
        
        # Try different date conversion approaches based on the field type
        try:
            # First try as timestamp
            filtered_df = df.filter(col(date_field) >= cutoff_date_str)
        except Exception as e1:
            try:
                # Try converting to date first
                filtered_df = df.filter(to_date(col(date_field)) >= cutoff_date_str)
            except Exception as e2:
                try:
                    # Try converting to timestamp
                    filtered_df = df.filter(to_timestamp(col(date_field)) >= cutoff_date_str)
                except Exception as e3:
                    print(f"Warning: Could not apply date filter, exporting all records. Errors: {e1}, {e2}, {e3}")
                    filtered_df = df
        
        # Sort by date field (descending)
        try:
            df = filtered_df.orderBy(col(date_field).desc())
        except Exception as e:
            print(f"Warning: Could not sort by {date_field}, using original order. Error: {e}")
            df = filtered_df
        
        # Check if we have data
        row_count = df.count()
        print(f"Found {row_count} rows in {table_name}")
        
        if row_count == 0:
            print(f"No data found for {table_name}, skipping...")
            return 0
        
        # Convert to Pandas for CSV export
        pandas_df = df.toPandas()
        
        # Create CSV content
        csv_buffer = StringIO()
        pandas_df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)
        csv_content = csv_buffer.getvalue()
        
        # Compress the CSV content
        compressed_content = gzip.compress(csv_content.encode('utf-8'))
        
        # Generate S3 key (timestamp now in path structure)
        timestamp = datetime.now().strftime('%H%M%S')  # Just time for uniqueness within the day
        bucket, base_key = parse_s3_path(s3_path)
        s3_key = f"{base_key}/{table_name}_{timestamp}.csv.gz"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=compressed_content,
            ContentType='application/gzip',
            ContentEncoding='gzip'
        )
        
        print(f"Successfully exported {table_name}: {row_count} rows -> s3://{bucket}/{s3_key}")
        return row_count
        
    except Exception as e:
        print(f"Error exporting table {table_name}: {str(e)}")
        raise e

def create_export_summary(export_results, s3_path):
    """Create a summary file with export statistics."""
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        
        summary = {
            "export_timestamp": timestamp,
            "database_name": database_name,
            "cutoff_date": cutoff_date_str,
            "max_age_days": max_age_days,
            "total_tables": len(export_results),
            "total_rows": sum(export_results.values()),
            "tables": export_results
        }
        
        # Create summary JSON
        summary_json = json.dumps(summary, indent=2)
        
        # Compress summary
        compressed_summary = gzip.compress(summary_json.encode('utf-8'))
        
        # Upload summary to S3
        bucket, base_key = parse_s3_path(s3_path)
        summary_key = f"{base_key}/export_summary_{datetime.now().strftime('%H%M%S')}.json.gz"
        
        s3_client.put_object(
            Bucket=bucket,
            Key=summary_key,
            Body=compressed_summary,
            ContentType='application/gzip',
            ContentEncoding='gzip'
        )
        
        print(f"Export summary created: s3://{bucket}/{summary_key}")
        return summary
        
    except Exception as e:
        print(f"Error creating export summary: {str(e)}")
        raise e

def list_available_tables():
    """List available tables in the database for debugging."""
    try:
        print("Attempting to list available tables...")
        
        # Try to query information_schema to list tables
        tables_query = """
        (SELECT table_schema, table_name 
         FROM information_schema.tables 
         WHERE table_type = 'BASE TABLE' 
         AND table_schema NOT IN ('information_schema', 'pg_catalog')
         ORDER BY table_schema, table_name) AS available_tables
        """
        
        df = glueContext.create_dynamic_frame.from_options(
            connection_type="postgresql",
            connection_options={
                "useConnectionProperties": "true",
                "connectionName": glue_connection_name,
                "dbtable": tables_query
            }
        ).toDF()
        
        tables_list = df.collect()
        print(f"Found {len(tables_list)} tables:")
        for row in tables_list:
            print(f"  {row.table_schema}.{row.table_name}")
            
    except Exception as e:
        print(f"Could not list tables: {str(e)}")

# Main export process
try:
    export_results = {}
    
    # First, try to list available tables for debugging
    list_available_tables()
    
    # Export each table
    for table_config in export_tables:
        table_name = table_config['table']
        date_field = table_config['date_field']
        
        row_count = export_table_to_s3(table_name, date_field, s3_output_path)
        export_results[table_name] = row_count
    
    # Create export summary
    summary = create_export_summary(export_results, s3_output_path)
    
    # Print final results
    print("=" * 60)
    print("EXPORT COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(f"Total tables exported: {len(export_results)}")
    print(f"Total rows exported: {sum(export_results.values())}")
    print(f"Export location: {s3_output_path}")
    
    for table_name, row_count in export_results.items():
        print(f"  {table_name}: {row_count:,} rows")
    
    print("=" * 60)

except Exception as e:
    print(f"EXPORT FAILED: {str(e)}")
    raise e

finally:
    # Commit the job
    job.commit()
    print("Glue job completed.")