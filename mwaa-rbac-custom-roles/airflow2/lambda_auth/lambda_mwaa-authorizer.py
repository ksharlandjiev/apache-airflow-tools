"""
Lambda Authorizer for MWAA with Direct Database Access (Airflow 3.x)

This version directly manages users in the MWAA metadata database instead of
triggering a DAG. This provides faster login times and more reliable user management.

Key Features:
- Direct PostgreSQL database access via psycopg2
- Helper class for database operations (MWAADatabaseManager)
- Retrieves database credentials from AWS Secrets Manager
- Creates/updates users and assigns roles in a single transaction
- No dependency on DAG execution during login
"""

import os
import json
import base64
import logging
import requests
from jose import jwt
import botocore
import boto3
from urllib.parse import quote
from mwaa_database_manager import MWAADatabaseManager

# Environment variables
Amazon_MWAA_ENV_NAME = os.environ.get('Amazon_MWAA_ENV_NAME', '').strip()
AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID', '').strip()
COGNITO_CLIENT_ID = os.environ.get('COGNITO_CLIENT_ID', '').strip()
COGNITO_DOMAIN = os.environ.get('COGNITO_DOMAIN').strip()
AWS_REGION = os.environ.get('AWS_REGION')
IDP_LOGIN_URI = os.environ.get('IDP_LOGIN_URI').strip()
GROUP_TO_ROLE_MAP = json.loads(os.environ.get('GROUP_TO_ROLE_MAP', '{}'))
ALB_COOKIE_NAME = os.environ.get('ALB_COOKIE_NAME', 'AWSELBAuthSessionCookie').strip()
LOGOUT_REDIRECT_DELAY = 10  # seconds

# AWS clients
sts = boto3.client('sts')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_database_credentials_from_glue(mwaa_env_name, aws_region):
    """
    Retrieve database credentials from Glue connection.
    
    The Glue connection is created by the create_role_glue_job DAG and contains
    the MWAA metadata database credentials.
    
    Args:
        mwaa_env_name: MWAA environment name
        aws_region: AWS region
    
    Returns:
        dict: Database credentials
    """
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        conn_name = f'{mwaa_env_name}_metadata_conn'
        
        logger.info(f'Retrieving database credentials from Glue connection: {conn_name}')
        
        response = glue_client.get_connection(Name=conn_name)
        connection = response['Connection']
        
        # Extract connection properties
        jdbc_url = connection['ConnectionProperties']['JDBC_CONNECTION_URL']
        username = connection['ConnectionProperties']['USERNAME']
        password = connection['ConnectionProperties']['PASSWORD']
        
        # Parse JDBC URL: jdbc:postgresql://host:port/database
        # Example: jdbc:postgresql://mwaa-db.rds.amazonaws.com:5432/AirflowMetadata
        jdbc_parts = jdbc_url.replace('jdbc:postgresql://', '').split('/')
        host_port = jdbc_parts[0]
        dbname = jdbc_parts[1] if len(jdbc_parts) > 1 else 'AirflowMetadata'
        
        if ':' in host_port:
            host, port = host_port.split(':')
        else:
            host = host_port
            port = 5432
        
        logger.info(f'✓ Retrieved database credentials from Glue connection')
        logger.info(f'  Host: {host}')
        logger.info(f'  Port: {port}')
        logger.info(f'  Database: {dbname}')
        
        return {
            'host': host,
            'port': int(port),
            'dbname': dbname,
            'username': username,
            'password': password
        }
        
    except glue_client.exceptions.EntityNotFoundException:
        logger.error(f'✗ Glue connection "{conn_name}" not found')
        logger.error('  The connection is created by the create_role_glue_job DAG')
        logger.error('  Please run the create_role_glue_job DAG at least once to create the connection')
        raise
    except Exception as e:
        logger.error(f'✗ Failed to retrieve database credentials from Glue: {e}')
        raise


def get_database_credentials():
    """
    Retrieve database credentials from Glue connection.
    
    This function retrieves the MWAA metadata database credentials from the
    Glue connection that was created by the create_role_glue_job DAG.
    
    Returns:
        dict: Database credentials
    """
    return get_database_credentials_from_glue(
        mwaa_env_name=Amazon_MWAA_ENV_NAME,
        aws_region=AWS_REGION
    )


def lambda_handler(event, context):
    """
    Lambda handler
    """
    path = event['path']
    headers = event['multiValueHeaders']
    
    logger.info(f'Received request for path: {path}')
    
    if 'x-amzn-oidc-data' in headers:
        encoded_jwt = headers['x-amzn-oidc-data'][0]
        token_payload = decode_jwt(encoded_jwt)
    else:
        # There is no session, close
        logger.info('No OIDC data in headers, closing session')
        return close(headers)
    
    # Airflow 3.x redirects to /auth/login/ - redirect to our SSO endpoint
    if path.startswith('/auth/login'):
        logger.info('Airflow 3.x /auth/login detected, redirecting to /aws_mwaa/aws-console-sso')
        redirect = login(headers, token_payload)
    elif path == '/aws_mwaa/aws-console-sso':
        logger.info('Processing /aws_mwaa/aws-console-sso request')
        redirect = login(headers, token_payload)
    elif path == '/logout/':
        logger.info('Processing logout request')
        redirect = logout(headers, 'Logged out successfully')
    else:
        logger.info(f'Unknown path: {path}, logging out')
        redirect = logout(headers, '')
    
    logger.info(json.dumps(redirect))
    return redirect


def login(headers, jwt_payload):
    """
    Function that returns a redirection to an appropriate
    URL that includes a web login token.
    """
    # Role to be determined using claims in JWT token
    role_arn = get_iam_role_arn(jwt_payload)
    mwaa_role = get_mwaa_role(jwt_payload)  # Get the MWAA role from the group mapping
    user_name = jwt_payload.get('username', role_arn)
    
    # Extract just the email address part, removing any prefix
    user_email = extract_email(user_name)
    
    # Use email for role session name (must be <= 64 characters)
    # AWS limits: roleSessionName and sourceIdentity must be <= 64 characters
    role_session_name = user_email[:64] if len(user_email) <= 64 else user_email.split('@')[0][:32] + '@' + user_email.split('@')[1][:31]
    
    logger.info('Here is role: ' + role_arn)
    logger.info('Here is mwaa_role: ' + mwaa_role)
    logger.info('Here is user_name: ' + user_name)
    logger.info('Here is user_email: ' + user_email)
    logger.info('Here is role_session_name: ' + role_session_name)
    
    host = headers['host'][0]
    
    if role_arn:
        mwaa = get_mwaa_client(role_arn, role_session_name)
        if mwaa:
            # Obtain web login token for the configured environment
            try:
                mwaa_web_token = mwaa.create_web_login_token(Name=Amazon_MWAA_ENV_NAME)["WebToken"]
                logger.info('Obtained Amazon MWAA WEB TOKEN')
                
                # Decode the JWT token to get the actual username
                airflow_username = extract_username_from_token(mwaa_web_token, role_arn)
                
                # Update user role in database for non-standard roles
                standard_roles = ['Admin', 'Op', 'User', 'Viewer']
                
                if mwaa_role not in standard_roles:
                    logger.info(f'Updating user role in database for user: {airflow_username} with role: {mwaa_role}')
                    
                    try:
                        # Get database credentials
                        db_creds = get_database_credentials()
                        
                        # Update user role in database
                        with MWAADatabaseManager(
                            db_host=db_creds['host'],
                            db_port=db_creds['port'],
                            db_name=db_creds['dbname'],
                            db_username=db_creds['username'],
                            db_password=db_creds['password']
                        ) as db:
                            success = db.update_user_role(
                                username=airflow_username,
                                role_name=mwaa_role,
                                email=user_email
                            )
                            
                            if not success:
                                logger.error('Failed to update user role in database')
                                redirect = logout(headers, 'Failed to configure user access. Please try again or contact your administrator.')
                                return redirect
                        
                        logger.info('✓ User role updated successfully in database')
                        
                    except Exception as e:
                        logger.error(f'Error updating user role in database: {e}')
                        import traceback
                        logger.error(f'Traceback: {traceback.format_exc()}')
                        redirect = logout(headers, 'Failed to configure user access. Please try again or contact your administrator.')
                        return redirect
                else:
                    logger.info(f'User has standard role "{mwaa_role}", skipping database update')
                
                # Proceed with login
                logger.info('Redirecting to MWAA UI with web token')
                redirect = {
                    'statusCode': 302,
                    'statusDescription': '302 Found',
                    'multiValueHeaders': {
                        'Location': [f'https://{host}/aws_mwaa/aws-console-sso?token=true#{mwaa_web_token}']
                    }
                }
                
            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'AccessDeniedException':
                    print("here is the role " + role_arn)
                    print("here is the role_session_name " + role_session_name)
                    redirect = logout(headers, f'The role {role_arn} assigned to {role_session_name} does not have access to the environment {Amazon_MWAA_ENV_NAME}.')
                elif error.response['Error']['Code'] == 'ResourceNotFoundException':
                    redirect = logout(headers, f'Environment {Amazon_MWAA_ENV_NAME} was not found.')
                else:
                    redirect = logout(headers, error)
        else:
            redirect = logout(headers, 'There was an error while logging in, please contact your administrator.')
    else:
        redirect = logout(headers, 'There is no valid role associated with your user.')
    
    return redirect


def extract_email(user_name):
    """
    Extract email address from username.
    
    Args:
        user_name: Username that may contain email
    
    Returns:
        str: Extracted email or generated email
    """
    if '@' in user_name:
        # Split by underscore and take the last part which should be the email
        email_part = user_name.split('_')[-1]
        if '@' in email_part:
            return email_part
        else:
            # Fallback: look for @ symbol and extract from there
            at_index = user_name.find('@')
            if at_index > 0:
                # Find the start of the email by looking backwards for non-alphanumeric chars
                start_index = at_index
                while start_index > 0 and (user_name[start_index - 1].isalnum() or user_name[start_index - 1] in '.-_'):
                    start_index -= 1
                return user_name[start_index:]
    
    # No email found, generate one
    return f'{user_name}@example.com'


def extract_username_from_token(mwaa_web_token, fallback_role_arn):
    """
    Extract username from MWAA web token.
    
    Args:
        mwaa_web_token: MWAA web token
        fallback_role_arn: Fallback role ARN if extraction fails
    
    Returns:
        str: Extracted username
    """
    try:
        # python-jose: disable all validation when verify_signature is False
        decoded_token = jwt.decode(
            mwaa_web_token,
            None,
            options={
                "verify_signature": False,
                "verify_aud": False,
                "verify_exp": False
            }
        )
        logger.info(f'Decoded JWT token: {decoded_token}')
        
        # The username is in the 'user' field
        # Format: assumed-role/sso-mwaa-alb-MWAACustomRole-gcSM1l1QKqZ7TmGqeL25oGX
        airflow_username = decoded_token.get('user', decoded_token.get('sub', fallback_role_arn))
        logger.info(f'Extracted username from JWT: {airflow_username}')
        
        # Validate the username format
        if not airflow_username.startswith('assumed-role/'):
            logger.warning(f'Username does not start with assumed-role/: {airflow_username}')
            logger.warning(f'Using role_arn as fallback: {fallback_role_arn}')
            airflow_username = fallback_role_arn
        
        return airflow_username
        
    except Exception as e:
        logger.error(f'Could not decode JWT token: {e}. Using role ARN as username.')
        return fallback_role_arn


def logout(headers, message):
    """
    Logs out from Airflow and expires the ALB cookies.
    If a message is present, it displays it for a few
    seconds and redirects to Cognito logout.
    """
    logger.info('LOGGING OUT')
    logger.info(headers)
    host = headers['host'][0]
    redirect_uri = quote(f'https://{host}/logout/close', safe="")
    cognito_logout_uri = \
        f'https://{COGNITO_DOMAIN}.auth.{AWS_REGION}.amazoncognito.com/logout?client_id=' + \
        f'{COGNITO_CLIENT_ID}&response_type=code&logout_uri={redirect_uri}&scope=openid'
    
    headers['Location'] = [cognito_logout_uri]
    expire_alb_cookies(headers)
    
    if message:
        logger.info('inside message')
        body = error_redirection_body(message, cognito_logout_uri)
        logger.info('back inside message')
        headers['Content-Type'] = ['text/html']
        redirect = {
            'statusCode': 200,
            'multiValueHeaders': headers,
            'body': body,
            'isBase64Encoded': False
        }
    else:
        redirect = {
            'statusCode': 302,
            'statusDescription': '302 Found',
            'multiValueHeaders': headers
        }
    
    return redirect


def get_mwaa_client(role_arn, user_name):
    """
    Returns an Amazon MWAA client under the given IAM role
    """
    logger.info('inside get mwaa client function')
    
    mwaa = None
    try:
        logger.info(f'Assuming role "{role_arn}" with source identity "{user_name}"...')
        logger.info('Here is role: ' + role_arn)
        logger.info('Here is user_name: ' + user_name)
        
        credentials = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=user_name,
            DurationSeconds=900,  # This is the minimum allowed
            SourceIdentity=user_name
        )['Credentials']
        
        access_key = credentials['AccessKeyId']
        secret_key = credentials['SecretAccessKey']
        session_token = credentials['SessionToken']
        
        # create service client using the assumed role credentials
        mwaa = boto3.client(
            'mwaa',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token
        )
        
    except botocore.exceptions.ClientError as error:
        logger.error(f'Error while assuming role {role_arn}.{error}')
    except Exception as error:
        logger.error(f'Unknown error while assuming role {role_arn}. {error}')
    
    return mwaa


def get_iam_role_arn(jwt_payload):
    """
    Returns the name of an IAM role based on the
    'custom:idp-groups' contained in the JWT token
    """
    role_arn = ''
    logger.info(f'JWT payload: {jwt_payload}')
    
    if 'custom:idp-groups' in jwt_payload:
        user_groups = parse_groups(jwt_payload['custom:idp-groups'])
        for mapping in GROUP_TO_ROLE_MAP:
            if mapping['idp-group'] in user_groups:
                role_name = mapping['iam-role']
                role_arn = f'arn:aws:iam::{AWS_ACCOUNT_ID}:role/{role_name}'
                logger.info('here is IAM role ' + role_arn)
                break
    
    return role_arn


def get_mwaa_role(jwt_payload):
    """
    Returns the MWAA role based on the
    'custom:idp-groups' contained in the JWT token
    """
    mwaa_role = 'Public'  # Default role if no mapping found
    logger.info(f'JWT payload: {jwt_payload}')
    
    if 'custom:idp-groups' in jwt_payload:
        user_groups = parse_groups(jwt_payload['custom:idp-groups'])
        for mapping in GROUP_TO_ROLE_MAP:
            if mapping['idp-group'] in user_groups:
                mwaa_role = mapping.get('mwaa-role', 'Public')
                logger.info(f'Found MWAA role: {mwaa_role} for group: {mapping["idp-group"]}')
                break
    
    return mwaa_role


def parse_groups(groups):
    """
    Converts the groups SAML claim content to a list of strings
    """
    # The groups SAML claim comes in a string
    # When there is more than one group id, the string starts and ends with square brackets
    # There might also be spaces between the group ids
    groups = groups.replace('[', '').replace(']', '').replace(' ', '')
    return groups.split(',')


def decode_jwt(encoded_jwt):
    """
    Decodes a JSON Web Token issued by the ALB after
    successful authentication against an OIDC IdP (e.g.: Cognito).
    """
    # Step 1: Get the key id from JWT headers (the kid field)
    jwt_headers = encoded_jwt.split('.')[0]
    decoded_jwt_headers = base64.b64decode(jwt_headers)
    decoded_jwt_headers = decoded_jwt_headers.decode("utf-8")
    decoded_json = json.loads(decoded_jwt_headers)
    kid = decoded_json['kid']
    
    # Step 2: Get the public key from regional endpoint
    url = f'https://public-keys.auth.elb.{AWS_REGION}.amazonaws.com/{kid}'
    req = requests.get(url)
    pub_key = req.text
    
    # Step 3: Get the payload using python-jose
    payload = jwt.decode(encoded_jwt, pub_key, algorithms=[decoded_json['alg']])
    return payload


def expire_alb_cookies(headers):
    """
    Sets ALB session cookies to expire
    """
    alb_cookies = [
        f'{ALB_COOKIE_NAME}-1=del;Max-Age=-1;Path=/;',
        f'{ALB_COOKIE_NAME}-0=del;Max-Age=-1;Path=/;'
    ]
    if 'Set-Cookie' in headers:
        headers['Set-Cookie'] += alb_cookies
    else:
        headers['Set-Cookie'] = alb_cookies


def error_redirection_body(message, logout_uri):
    """
    Returns an HTML string that displays an error message
    and redirects the browser to the logout_uri
    """
    body = f'<html><body><h3>{message}</h3><br><br>Closing session in ' + \
           f'<span id="countdown">{LOGOUT_REDIRECT_DELAY}</span> seconds' + \
           '</body></html><script type="text/javascript">' + \
           f'var seconds = {LOGOUT_REDIRECT_DELAY};' + \
           'function countdown() {' + \
           ' seconds -= 1;' + \
           ' if (seconds < 0) {' + \
           f' window.location = "{logout_uri}";' + \
           ' } else {' + \
           ' document.getElementById("countdown").innerHTML = seconds;' + \
           ' window.setTimeout("countdown()", 1000);' + \
           ' }' + \
           '}' + \
           'countdown();' + \
           '</script>'
    return body


def close(headers):
    """
    Requests user to close the current tab
    """
    body = '<html><body><h3>You can now close this tab.</h3></body></html>'
    headers['Content-Type'] = ['text/html']
    return {
        'statusCode': 200,
        'multiValueHeaders': headers,
        'body': body,
        'isBase64Encoded': False
    }
