# AWS MWAA Custom RBAC Solution

A comprehensive solution for implementing custom Role-Based Access Control (RBAC) in Amazon Managed Workflows for Apache Airflow (MWAA) using CloudFormation, Lambda authorizers, and Azure AD integration.

## Overview

This solution enables fine-grained access control for MWAA environments by:
- Integrating with Azure AD (Entra ID) for authentication
- Using Application Load Balancer (ALB) with Lambda authorizers
- Creating custom MWAA roles with specific DAG permissions
- Supporting both Airflow 2.x and 3.x with direct database access

## Key Features

### V2 (Current - Direct Database Access)
- **Fast Login**: 3-5 seconds (vs 60+ seconds with DAG-based approach)
- **More Reliable**: No dependency on DAG scheduler
- **Simpler Architecture**: Fewer moving parts, easier to maintain
- **Auto-Configuration**: Reads credentials from Glue connection
- **User Management**: Automatically sets first_name and last_name to username
- **Platform-Specific Build**: Lambda layer built with Docker for proper psycopg2 binary
- **Proper VPC Configuration**: Lambda in subnet with NAT Gateway and MWAA security group
- **Email Extraction**: Handles long usernames by extracting email from roleSessionName

### Recent Updates (January 2026)
- ✅ **Lambda Layer Automation**: Integrated psycopg2 layer build into deploy-stack.sh
- ✅ **VPC Configuration Fix**: Lambda now uses MWAA security group for database access
- ✅ **NAT Gateway Routing**: Lambda in subnet-3 for internet access (ALB public keys)
- ✅ **User Name Population**: Automatically sets first_name and last_name in database
- ✅ **Airflow2 Backport**: All airflow3 improvements backported to airflow2
- ✅ **Consolidated Documentation**: Single README with all information
- ✅ **File Naming**: Renamed v1 files to `*_with_dag.py` for clarity

### Architecture

```
Azure AD → ALB → Lambda Authorizer → MWAA Environment
                      ↓
              IAM Role Assumption
                      ↓
         Direct Database Access (V2)
                      ↓
              MWAA Role Assignment
```

## Directory Structure

```
mwaa-rbac-custom-roles/
├── airflow2/              # Airflow 2.x compatible implementation
│   ├── 01-vpc-mwaa.yml    # VPC and MWAA environment
│   ├── 02-alb.yml         # ALB and authentication
│   ├── deploy-stack.sh    # Deployment script with Lambda layer support
│   ├── cleanup-stack.sh   # Cleanup script
│   ├── lambda_auth/       # Lambda authorizer code
│   │   ├── lambda_mwaa-authorizer.py                    # V2 - Direct DB (current)
│   │   ├── lambda_mwaa-authorizer_with_dag.py           # V1 - DAG-based (backup)
│   │   ├── lambda_mwaa-authorizer_with_dag_original.py  # Original airflow2 version
│   │   ├── mwaa_database_manager.py                     # Database helper class
│   │   ├── Dockerfile                                   # Lambda layer build
│   │   └── requirements_layer.txt                       # Layer dependencies
│   ├── role_creation_dag/ # DAGs for role management
│   └── sample_dags/       # Example DAGs
│
├── airflow3/              # Airflow 3.x compatible implementation
│   ├── 01-vpc-mwaa.yml    # VPC and MWAA environment
│   ├── 02-alb.yml         # ALB and authentication (updated for AF3)
│   ├── deploy-stack.sh    # Deployment script with Lambda layer support
│   ├── cleanup-stack.sh   # Cleanup script
│   ├── lambda_auth/       # Lambda authorizer code
│   │   ├── lambda_mwaa-authorizer.py                    # V2 - Direct DB (current)
│   │   ├── lambda_mwaa-authorizer_with_dag.py           # V1 - DAG-based (backup)
│   │   ├── mwaa_database_manager.py                     # Database helper class
│   │   ├── Dockerfile                                   # Lambda layer build
│   │   └── requirements_layer.txt                       # Layer dependencies
│   ├── role_creation_dag/ # DAGs for role management (updated for AF3)
│   └── sample_dags/       # Example DAGs (updated for AF3)
│
└── README.md              # This file
```

## Prerequisites

- AWS CLI configured with appropriate credentials
- Docker (for building Lambda layer)
- jq (JSON processor)
- Azure AD tenant with SAML application configured
- ACM certificate for ALB HTTPS
- Sufficient AWS service limits (VPCs, MWAA environments, etc.)

## Quick Start

### 1. Configure Deployment Parameters

Copy the template and fill in your values:

```bash
cd airflow2  # or airflow3
cp deployment-config.template.json deployment-config.json
```

Edit `deployment-config.json` with your:
- VPC CIDR ranges
- MWAA environment name
- ALB certificate ARN
- Azure AD group IDs
- Azure AD tenant URLs

### 2. Deploy Infrastructure

**Full deployment (recommended):**
```bash
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb
```

This automatically:
1. Deploys VPC and MWAA environment
2. Uploads files to S3
3. **Builds and deploys Lambda layer** (psycopg2 for database access)
4. Deploys ALB and authentication
5. Updates Lambda environment variables

**Step-by-step deployment:**
```bash
# 1. Deploy VPC and MWAA
./deploy-stack.sh --vpc-stack my-mwaa --vpc

# 2. Upload files to S3
./deploy-stack.sh --vpc-stack my-mwaa --upload

# 3. Build and deploy Lambda layer (requires Docker)
./deploy-stack.sh --vpc-stack my-mwaa --lambda-layer

# 4. Deploy ALB and authentication
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --alb

# 5. Update Lambda code (if needed)
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --update-lambda-code
```

### 3. Create Custom Roles

**Prerequisites**: Run the `create_role_glue_job` DAG once to create the Glue connection:

```json
{
  "aws_region": "us-east-1",
  "stack_name": "my-mwaa",
  "source_role": "User",
  "target_role": "Restricted",
  "specific_dags": ["hello_world_advanced", "hello_world_simple"]
}
```

This creates:
- Glue connection to MWAA metadata database
- Custom role with specific DAG permissions
- Database credentials for Lambda to use

### 4. Access MWAA

Navigate to the ALB URL (from CloudFormation outputs):
```
https://<alb-dns>/aws_mwaa/aws-console-sso
```

## Lambda Authorizer Versions

### V2 (Current - Direct Database Access)

**How It Works:**
1. User authenticates via Azure AD
2. Lambda retrieves database credentials from Glue connection
3. Lambda directly updates user role in MWAA metadata database
4. User is redirected to Airflow UI

**Benefits:**
- **20x faster**: 3-5 seconds vs 60-90 seconds
- **More reliable**: No DAG scheduler dependency
- **Simpler**: Single Lambda function
- **Auto-configuration**: Reads credentials from Glue connection

### V1 (DAG-Based - Backup)

**How It Works:**
1. User authenticates via Azure AD
2. Lambda triggers `update_user_role` DAG
3. DAG updates user role in database
4. Lambda waits for DAG completion
5. User is redirected to Airflow UI

**When to Use:**
- Complex role assignment logic
- Need audit trail via DAG runs
- Prefer Airflow-native approach

**Rollback to V1:**
```bash
cd airflow2/lambda_auth  # or airflow3/lambda_auth
mv lambda_mwaa-authorizer.py lambda_mwaa-authorizer_v2_backup.py
mv lambda_mwaa-authorizer_with_dag_original.py lambda_mwaa-authorizer.py

cd ../..
./deploy-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --update-lambda-code
```

## Airflow Version Compatibility

### Airflow 2.x (airflow2/)
- Tested with Airflow 2.5.x through 2.10.x
- Uses standard authentication endpoints
- Compatible with `schedule_interval` and `DummyOperator`
- Direct database access via psycopg2 (V2)

### Airflow 3.x (airflow3/)
- Tested with Airflow 3.0.x
- **Updated authentication**: New `/auth/login/` endpoint
- **Updated DAG syntax**: Uses `schedule` instead of `schedule_interval`
- **Updated operators**: Uses `EmptyOperator` instead of `DummyOperator`
- **Database access**: Direct PostgreSQL via psycopg2 (bypasses `airflow-db-not-allowed`)
- **REST API changes**: Requires `logical_date` field when triggering DAGs
- Direct database access via psycopg2 (V2)

## Key Differences: Airflow 2.x vs 3.x

| Feature | Airflow 2.x | Airflow 3.x |
|---------|-------------|-------------|
| **Authentication Endpoints** | `/login/` | `/auth/login/` |
| **Plugin Paths** | `/aws_mwaa/aws-console-sso` | `/pluginsv2/aws_mwaa/aws-console-sso` |
| **DB Connection Env Var** | `AIRFLOW__CORE__SQL_ALCHEMY_CONN` | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` |
| **MWAA DB Access** | Direct SQL Alchemy | Blocked, use `DB_SECRETS` |
| **Database Access Method** | Direct PostgreSQL (V2) | Direct PostgreSQL (V2) |
| **REST API - Trigger DAG** | No `logical_date` required | Requires `logical_date` field |
| **Schedule Parameter** | `schedule_interval` | `schedule` (required) |
| **Dummy Operator** | `DummyOperator` | `EmptyOperator` (required) |
| **PythonOperator Context** | `provide_context=True` | Always provided (parameter removed) |

## Lambda Layer (psycopg2)

The solution uses a Lambda layer for the psycopg2 PostgreSQL driver to enable direct database access.

### Why a Lambda Layer?

psycopg2 requires compiled C extensions that must match the Lambda runtime environment (Amazon Linux 2). Building it in a Docker container ensures compatibility and resolves the common `No module named 'psycopg2._psycopg'` error.

### Dockerfile Configuration

The Dockerfile includes system dependencies for proper compilation:

```dockerfile
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.11

# Install system dependencies for psycopg2
RUN yum install -y postgresql-devel gcc python3-devel

# Install psycopg2-binary for x86_64
RUN pip install --upgrade pip && \
    pip install psycopg2-binary==2.9.9 -t /python --no-cache-dir

# Create the layer structure
RUN mkdir -p /layer/python && cp -r /python/* /layer/python/
```

### Automatic Build Process

The `deploy-stack.sh` script automatically:
1. Checks for Docker availability
2. Builds Docker image with Lambda runtime and system dependencies
3. Installs psycopg2-binary with proper C extension compilation
4. Extracts layer and creates zip file
5. Uploads to S3 bucket
6. Publishes Lambda layer with version tracking
7. Attaches layer to Lambda function

**Command:**
```bash
./deploy-stack.sh --vpc-stack my-mwaa --lambda-layer
```

### Manual Build (if needed)

If you need to rebuild the layer manually:

```bash
cd airflow2/lambda_auth  # or airflow3/lambda_auth

# Build layer using Docker
docker build --platform linux/amd64 -t psycopg2-layer .
docker create --name psycopg2-container psycopg2-layer
docker cp psycopg2-container:/layer/. ./lambda-layer/
docker rm psycopg2-container
docker rmi psycopg2-layer

# Create zip
cd lambda-layer && zip -r ../psycopg2-layer.zip . -q && cd ..

# Upload to S3
aws s3 cp psycopg2-layer.zip s3://YOUR-BUCKET/lambda-layers/

# Publish layer
aws lambda publish-layer-version \
    --layer-name mwaa-psycopg2 \
    --description 'PostgreSQL adapter for MWAA authorizer' \
    --content S3Bucket=YOUR-BUCKET,S3Key=lambda-layers/psycopg2-layer.zip \
    --compatible-runtimes python3.11 python3.12

# Attach to Lambda
aws lambda update-function-configuration \
    --function-name YOUR-LAMBDA-FUNCTION \
    --layers arn:aws:lambda:REGION:ACCOUNT:layer:mwaa-psycopg2:VERSION
```

### Layer Structure

```
psycopg2-layer.zip
└── python/
    └── psycopg2/
        ├── __init__.py
        ├── _psycopg.cpython-311-x86_64-linux-gnu.so  # C extension
        └── ... (other files)
```

## Database Manager Helper Class

The `mwaa_database_manager.py` provides a clean, maintainable interface for database operations:

### Features

- **Context Manager**: Automatic connection and transaction handling
- **User Management**: Create or update users with automatic name population
- **Role Assignment**: Clean role assignment with automatic cleanup
- **Error Handling**: Comprehensive error handling and logging
- **Transaction Safety**: Automatic commit on success, rollback on error

### Usage Example

```python
from mwaa_database_manager import MWAADatabaseManager

# Context manager handles connection lifecycle
with MWAADatabaseManager(host, port, db, user, password) as db:
    # Update user role (creates user if doesn't exist)
    success = db.update_user_role(
        username='john.doe',
        role_name='Admin',
        email='john.doe@example.com'
    )
    # first_name and last_name automatically set to 'john.doe'
```

### Methods

**`get_or_create_user(username, email=None, first_name='', last_name='')`**
- Creates new user or returns existing user ID
- Updates email, first_name, last_name if provided
- Defaults first_name and last_name to username if not provided

**`get_role_id(role_name)`**
- Retrieves role ID by name
- Returns None if role doesn't exist

**`remove_all_user_roles(user_id)`**
- Removes all role assignments from user
- Ensures clean state before new assignment

**`assign_role_to_user(user_id, role_id)`**
- Assigns specific role to user
- Creates new role assignment record

**`update_user_role(username, role_name, email=None, first_name=None, last_name=None)`**
- Main method combining all operations
- Removes all existing roles
- Assigns only the specified role
- Defaults first_name and last_name to username

### Automatic Name Population

The database manager automatically populates user names:

```python
# If first_name and last_name not provided
db.update_user_role(username='john.doe', role_name='Admin')
# Result: first_name='john.doe', last_name='john.doe'

# If explicitly provided
db.update_user_role(
    username='john.doe', 
    role_name='Admin',
    first_name='John',
    last_name='Doe'
)
# Result: first_name='John', last_name='Doe'
```

### Transaction Management

The context manager ensures proper transaction handling:

```python
with MWAADatabaseManager(...) as db:
    db.update_user_role(...)  # Operations in transaction
    # Automatic commit on success
# Automatic rollback on exception
# Automatic connection close
```

### Error Handling

All methods include comprehensive error handling:

```python
try:
    with MWAADatabaseManager(...) as db:
        success = db.update_user_role(...)
        if not success:
            logger.error('Failed to update user role')
except Exception as e:
    logger.error(f'Database error: {e}')
    # Connection automatically closed
    # Transaction automatically rolled back
```

## VPC Configuration

### Network Requirements

The Lambda function requires specific VPC configuration for proper operation:

**Security Groups:**
- **MWAA Security Group**: Required for database access to MWAA metadata database
- **ALB Security Group**: Required for ALB communication

**Subnet Requirements:**
- Must be in a subnet with NAT Gateway for internet access
- Needs to reach ALB public keys endpoint: `public-keys.auth.elb.us-east-1.amazonaws.com`
- Must have route to MWAA VPC endpoint for database access

### Subnet Configuration

The VPC template creates three private subnets with different routing:

- **Subnet 1 & 2**: Private subnets without NAT Gateway
  - Used by MWAA workers
  - Route table: Local VPC + S3 VPC Endpoint only
  - No internet access

- **Subnet 3**: Private subnet with NAT Gateway
  - Used by Lambda function
  - Route table: Local VPC + S3 VPC Endpoint + NAT Gateway (0.0.0.0/0)
  - Internet access for ALB JWT verification

### CloudFormation Configuration

The ALB template configures Lambda VPC settings:

```yaml
VpcConfig:
  SecurityGroupIds: 
    - !ImportValue '${MWAAVPCStackName}-mwaa-sg'      # Database access
    - !ImportValue '${MWAAVPCStackName}-alb-sg'       # ALB communication
  SubnetIds: 
    - !ImportValue '${MWAAVPCStackName}-private-subnet-3'  # NAT Gateway access
```

### Why This Configuration?

The Lambda needs to access two different endpoints:

1. **MWAA Metadata Database** (via VPC endpoint)
   - Requires MWAA security group
   - Private communication within VPC
   - No internet required

2. **ALB Public Keys** (via internet)
   - Requires NAT Gateway for outbound internet
   - Validates JWT tokens from ALB
   - Must reach `public-keys.auth.elb.us-east-1.amazonaws.com`

### Common VPC Issues

**Issue:** `connection to server at "..." failed: timeout expired`
- **Cause**: Lambda missing MWAA security group
- **Solution**: Verify Lambda has both MWAA and ALB security groups

**Issue:** `Connection to public-keys.auth.elb.us-east-1.amazonaws.com timed out`
- **Cause**: Lambda not in subnet with NAT Gateway
- **Solution**: Ensure Lambda is in subnet-3 with NAT Gateway route

**Verification Commands:**
```bash
# Check Lambda VPC configuration
aws lambda get-function-configuration \
    --function-name YOUR-LAMBDA-NAME \
    --query 'VpcConfig.{SecurityGroups: SecurityGroupIds, Subnets: SubnetIds}'

# Check subnet route table
aws ec2 describe-route-tables \
    --filters "Name=association.subnet-id,Values=YOUR-SUBNET-ID" \
    --query 'RouteTables[0].Routes'

# Should show: 0.0.0.0/0 → NAT Gateway
```

## Role Mapping

The solution maps Azure AD groups to IAM roles and MWAA roles:

```
Azure AD Group → IAM Role → MWAA Role
─────────────────────────────────────
Admin Group    → MwaaAdminRole    → Admin
User Group     → MwaaUserRole     → User
Viewer Group   → MwaaViewerRole   → Viewer
Custom Group   → MwaaCustomRole   → Restricted (custom)
```

### Standard MWAA Roles

- **Admin**: Full access to all DAGs and Airflow UI
- **Op**: Operational access (trigger, clear, etc.)
- **User**: Can view and trigger DAGs
- **Viewer**: Read-only access
- **Public**: Minimal access

### Custom Roles

Custom roles are created using the `create_role_glue_job` DAG, which:
1. Connects to MWAA metadata database via AWS Glue
2. Copies permissions from a source role (e.g., User)
3. Restricts access to specific DAGs only
4. Creates the new role in the RBAC tables
5. Preserves `menu access on DAGs` permission (required for UI visibility)

## Deployment Options

### Command-Line Flags

- `--vpc` - Deploy VPC stack only
- `--upload` - Upload files to S3 only
- `--lambda-layer` - Build and deploy Lambda layer only
- `--alb` - Deploy ALB stack only
- `--update-lambda` - Update Lambda environment variables only
- `--update-lambda-code` - Update Lambda function code only
- `--vpc-stack NAME` - Specify VPC stack name (default: mwaa-vpc)
- `--alb-stack NAME` - Specify ALB stack name (default: <vpc-stack>-alb)

### Examples

```bash
# Deploy with custom stack names
./deploy-stack.sh --vpc-stack prod-mwaa --alb-stack prod-mwaa-alb

# Deploy only VPC
./deploy-stack.sh --vpc-stack dev-mwaa --vpc

# Rebuild Lambda layer
./deploy-stack.sh --vpc-stack prod-mwaa --lambda-layer

# Update Lambda configuration after role changes
./deploy-stack.sh --vpc-stack prod-mwaa --alb-stack prod-mwaa-alb --update-lambda
```

## Cleanup

To remove all resources:

```bash
# Full cleanup
./cleanup-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb

# Step-by-step cleanup
./cleanup-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --alb
./cleanup-stack.sh --vpc-stack my-mwaa --s3
./cleanup-stack.sh --vpc-stack my-mwaa --vpc

# Force mode (skip confirmations)
./cleanup-stack.sh --vpc-stack my-mwaa --alb-stack my-mwaa-alb --force
```

## Troubleshooting

### Database Connection Issues

**Error:** `connection to server at "..." failed: timeout expired`

**Solution:**
1. Verify Lambda has MWAA security group
2. Check Lambda is in subnet-3 (with NAT Gateway)
3. Verify Glue connection exists and has correct network configuration

```bash
# Check Lambda VPC configuration
aws lambda get-function-configuration \
    --function-name YOUR-LAMBDA-NAME \
    --query 'VpcConfig.{SecurityGroups: SecurityGroupIds, Subnets: SubnetIds}'

# Verify Glue connection
aws glue get-connection --name {ENV}_metadata_conn
```

### psycopg2 Module Not Found

**Error:** `No module named 'psycopg2._psycopg'`

**Solution:** Rebuild and deploy Lambda layer:
```bash
./deploy-stack.sh --vpc-stack my-mwaa --lambda-layer
```

**Verify layer is attached:**
```bash
aws lambda get-function-configuration \
    --function-name YOUR-LAMBDA-NAME \
    --query 'Layers[*].Arn'
```

### ALB Public Keys Timeout

**Error:** `Connection to public-keys.auth.elb.us-east-1.amazonaws.com timed out`

**Solution:**
- Ensure Lambda is in subnet-3 (has NAT Gateway)
- Verify route table has 0.0.0.0/0 → NAT Gateway

```bash
# Check subnet route table
aws ec2 describe-route-tables \
    --filters "Name=association.subnet-id,Values=YOUR-SUBNET-ID" \
    --query 'RouteTables[0].Routes'
```

### Glue Connection Not Found

**Error:** `Glue connection not found`

**Solution:**
1. Run `create_role_glue_job` DAG once
2. Verify connection exists:
```bash
aws glue get-connection --name {ENV}_metadata_conn
```

### Role Creation Fails

**Error:** Glue job fails to create role

**Solutions:**
1. Check Glue job logs in CloudWatch
2. Verify Glue connection to MWAA database
3. Ensure source role exists
4. Check GlueRoleCreatorRole permissions
5. Verify target role doesn't already exist (delete it first)

### Custom Role Users Can't See DAGs (Airflow 3.x)

**Error:** User has custom role but DAGs page is empty

**Root Cause:** Role missing `menu access on DAGs` permission

**Solution:**
1. Delete the custom role in Airflow UI (Security → List Roles)
2. Ensure you have the latest `create_role_glue_job_dag.py`
3. Re-run the `create_role_glue_job` DAG
4. Verify role has `menu access on DAGs` permission

### Users Have Multiple Roles

**Error:** User has both Public and custom roles

**Root Cause:** `update_user_role` not removing all existing roles

**Solution:**
1. Ensure you have the latest `update_user_role_dag.py` or use V2 Lambda
2. Manually remove extra roles from user (Security → List Users → Edit)
3. Or trigger role assignment again

## Migration from Airflow 2.x to 3.x

### Checklist

- [ ] Update MWAA environment to Airflow 3.x
- [ ] Deploy updated ALB configuration (handles new endpoints)
- [ ] Replace `schedule_interval` with `schedule` in all DAGs
- [ ] Remove `provide_context=True` from all PythonOperator instances
- [ ] Replace `DummyOperator` with `EmptyOperator`
- [ ] Update Lambda authorizer (already compatible)
- [ ] Test authentication flow
- [ ] Verify role creation DAG works
- [ ] Test custom role users can see DAGs page

### Deployment

```bash
cd airflow3
./deploy-stack.sh --vpc-stack mwaa-af3 --alb-stack mwaa-af3-alb
```

## Security Considerations

1. **Secrets Management**: Deployment config contains sensitive data - keep it secure
2. **IAM Permissions**: Follow principle of least privilege
3. **Network Security**: MWAA uses private subnets with VPC endpoints
4. **ALB Security**: HTTPS only with valid ACM certificate
5. **Database Access**: Credentials from Glue connection (encrypted)
6. **Lambda Layer**: Built with platform-specific binaries for security

## Cost Optimization

- Use appropriate MWAA environment size
- Configure auto-scaling for workers
- Use S3 lifecycle policies for logs
- Delete unused Glue connections and jobs
- Monitor CloudWatch logs retention
- Use Lambda layers to reduce deployment package size

## Performance Comparison

| Metric | V1 (DAG-based) | V2 (Direct DB) |
|--------|----------------|----------------|
| Login Time | 60-90 seconds | 3-5 seconds |
| Database Queries | Via DAG | Direct |
| Failure Points | 3 (Lambda, DAG, Glue) | 1 (Lambda) |
| Dependencies | DAG scheduler | Database only |
| Complexity | High | Low |
| Maintenance | Multiple files | Centralized |

## Contributing

When contributing:
1. Test changes in both Airflow 2.x and 3.x
2. Update both airflow2/ and airflow3/ folders
3. Update main README
4. Follow existing code style
5. Test deployment and cleanup scripts
6. Ensure Lambda layer builds correctly

## References

- [AWS MWAA Documentation](https://docs.aws.amazon.com/mwaa/)
- [Integrate Amazon MWAA with Microsoft Entra ID using SAML authentication](https://aws.amazon.com/blogs/big-data/integrate-amazon-mwaa-with-microsoft-entra-id-using-saml-authentication/)
- [Limit access to Apache Airflow DAGs in Amazon MWAA](https://docs.aws.amazon.com/mwaa/latest/userguide/limit-access-to-dags.html)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Azure AD SAML Configuration](https://docs.microsoft.com/en-us/azure/active-directory/saas-apps/)

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review CloudFormation stack events
3. Check CloudWatch logs for Lambda and Glue jobs
4. Verify Azure AD configuration
5. Ensure Docker is installed for Lambda layer builds

## License

This solution is provided as-is for use with AWS services.
