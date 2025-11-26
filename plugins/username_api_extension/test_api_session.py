#!/usr/bin/env python3
"""
Test script for Username API Extension Plugin with Session Authentication
Tests CREATE (standard API), GET, PATCH, DELETE (extended API)

Supports both local Airflow and AWS MWAA environments.

Usage:
    # Local Airflow
    python3 test_api_session.py
    
    # AWS MWAA
    python3 test_api_session.py --mwaa --env-name my-mwaa-env --region us-east-1
"""

import requests
import json
import sys
import argparse
import logging
from urllib.parse import quote

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

# Configuration (can be overridden by command line args)
BASE_URL = "http://localhost:8080"
USERNAME = "admin"
PASSWORD = "test"
TEST_USERNAME = "assumed-role/TestUser123"
USE_MWAA = False
MWAA_ENV_NAME = None
MWAA_REGION = None
# Note: requests library auto-encodes params, so we don't manually encode

# Colors
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
NC = '\033[0m'

def print_header(text):
    print(f"\n{YELLOW}=== {text} ==={NC}")

def print_success(text):
    print(f"{GREEN}✓ {text}{NC}")

def print_error(text):
    print(f"{RED}✗ {text}{NC}")

def print_info(text):
    print(f"{YELLOW}{text}{NC}")

def pretty_print(data):
    print(json.dumps(data, indent=2))

def get_csrf_token(session, base_url=None):
    """Get CSRF token from the home page or cookies"""
    url = base_url or BASE_URL
    try:
        # First, check all cookies for CSRF token
        for cookie_name in ['csrf_token', '_csrf_token', 'csrftoken', 'CSRF-TOKEN']:
            csrf_token = session.cookies.get(cookie_name)
            if csrf_token:
                print_info(f"Found CSRF token in cookie: {cookie_name}")
                return csrf_token
        
        # Try to get from home page
        response = session.get(f"{url}/home")
        if response.status_code == 200:
            # Check cookies again after home page load
            for cookie_name in ['csrf_token', '_csrf_token', 'csrftoken', 'CSRF-TOKEN']:
                csrf_token = session.cookies.get(cookie_name)
                if csrf_token:
                    print_info(f"Found CSRF token in cookie after /home: {cookie_name}")
                    return csrf_token
            
            # Try to extract from response headers
            csrf_token = response.headers.get('X-CSRFToken') or response.headers.get('X-CSRF-Token')
            if csrf_token:
                print_info("Found CSRF token in response header")
                return csrf_token
                
            # Try to parse from HTML (look for various patterns)
            import re
            
            # Pattern 1: csrf_token = "value"
            match = re.search(r'csrf_token["\s:=]+["\']([a-zA-Z0-9\-_.]+)["\']', response.text)
            if match:
                print_info("Found CSRF token in HTML (pattern 1)")
                return match.group(1)
            
            # Pattern 2: name="csrf_token" value="value"
            match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([a-zA-Z0-9\-_.]+)["\']', response.text)
            if match:
                print_info("Found CSRF token in HTML (pattern 2)")
                return match.group(1)
            
            # Pattern 3: data-csrf="value"
            match = re.search(r'data-csrf=["\']([a-zA-Z0-9\-_.]+)["\']', response.text)
            if match:
                print_info("Found CSRF token in HTML (pattern 3)")
                return match.group(1)
            
            # Pattern 4: meta tag
            match = re.search(r'<meta[^>]*name=["\']csrf-token["\'][^>]*content=["\']([a-zA-Z0-9\-_.]+)["\']', response.text, re.IGNORECASE)
            if match:
                print_info("Found CSRF token in meta tag")
                return match.group(1)
        
        print_info("No CSRF token found")
        return None
    except Exception as e:
        print_error(f"Exception getting CSRF token: {e}")
        logging.exception("Detailed error:")
        return None

def get_mwaa_session(region, env_name):
    """
    Get session for AWS MWAA environment using web login token.
    
    Args:
        region (str): AWS region where MWAA environment is located
        env_name (str): Name of the MWAA environment
        
    Returns:
        tuple: (session, csrf_token, base_url) or (None, None, None) on failure
    """
    try:
        import boto3
    except ImportError:
        print_error("boto3 is required for MWAA authentication. Install it with: pip install boto3")
        return None, None, None
    
    print_header("0. MWAA LOGIN - Get Web Login Token")
    
    try:
        # Create MWAA client
        mwaa = boto3.client('mwaa', region_name=region)
        
        # Request web login token
        print_info(f"Requesting web login token for environment: {env_name}")
        response = mwaa.create_web_login_token(Name=env_name)
        
        web_server_host_name = response["WebServerHostname"]
        web_token = response["WebToken"]
        
        print_info(f"Web server: {web_server_host_name}")
        
        # Construct login URL
        base_url = f"https://{web_server_host_name}"
        login_url = f"{base_url}/aws_mwaa/login"
        login_payload = {"token": web_token}
        
        # Create session and authenticate
        session = requests.Session()
        response = session.post(login_url, data=login_payload, timeout=10)
        
        if response.status_code == 200:
            print_success("MWAA login successful")
            session_cookie = session.cookies.get("session")
            if session_cookie:
                print_info(f"Session cookie obtained: {session_cookie[:20]}...")
            
            # Get CSRF token for MWAA
            csrf_token = get_csrf_token(session, base_url)
            if csrf_token:
                print_info(f"CSRF token obtained: {csrf_token[:20]}...")
            else:
                print_info("Warning: No CSRF token found (may cause issues with PATCH/DELETE)")
            
            return session, csrf_token, base_url
        else:
            print_error(f"Failed to log in: HTTP {response.status_code}")
            return None, None, None
            
    except Exception as e:
        print_error(f"MWAA login failed: {e}")
        logging.exception("Detailed error:")
        return None, None, None

def login():
    """Login to get session cookie (local Airflow)"""
    print_header("0. LOGIN - Get Session Cookie")
    
    session = requests.Session()
    
    # Get CSRF token first
    try:
        response = session.get(f"{BASE_URL}/login/")
        if response.status_code != 200:
            print_error(f"Failed to access login page (HTTP {response.status_code})")
            return None, None
    except Exception as e:
        print_error(f"Exception accessing login page: {e}")
        return None, None
    
    # Login
    login_data = {
        "username": USERNAME,
        "password": PASSWORD
    }
    
    try:
        response = session.post(
            f"{BASE_URL}/login/",
            data=login_data,
            allow_redirects=True
        )
        
        if response.status_code == 200 and "login" not in response.url.lower():
            print_success("Login successful")
            print_info(f"Session cookies: {list(session.cookies.keys())}")
            
            # Get CSRF token
            csrf_token = get_csrf_token(session, BASE_URL)
            if csrf_token:
                print_info(f"CSRF token obtained: {csrf_token[:20]}...")
            else:
                print_info("No CSRF token found (may not be required)")
            
            return session, csrf_token
        else:
            print_error(f"Login failed (HTTP {response.status_code})")
            print_error(f"Final URL: {response.url}")
            return None, None
    except Exception as e:
        print_error(f"Exception during login: {e}")
        return None, None

def test_create_user(session):
    """Test 1: Create user using standard Airflow API"""
    print_header("1. CREATE USER (Standard API)")
    print(f"POST {BASE_URL}/api/v1/users")
    
    payload = {
        "username": TEST_USERNAME,
        "email": "testuser@example.com",
        "first_name": "Test",
        "last_name": "User",
        "roles": [{"name": "Viewer"}]
    }
    
    try:
        response = session.post(
            f"{BASE_URL}/api/v1/users",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code in [200, 201]:
            print_success(f"User created successfully (HTTP {response.status_code})")
            pretty_print(response.json())
            return True
        else:
            print_error(f"Failed to create user (HTTP {response.status_code})")
            print(response.text)
            return False
    except Exception as e:
        print_error(f"Exception: {e}")
        return False

def test_list_users(session):
    """Test 2: List all users to verify creation"""
    print_header("2. LIST USERS - Verify user exists (Standard API)")
    print(f"GET {BASE_URL}/api/v1/users")
    
    try:
        response = session.get(
            f"{BASE_URL}/api/v1/users",
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            data = response.json()
            users = data.get('users', [])
            print_success(f"Retrieved {len(users)} users")
            
            # Find our test user
            test_user = None
            for user in users:
                if user.get('username') == TEST_USERNAME:
                    test_user = user
                    break
            
            if test_user:
                print_success(f"Test user '{TEST_USERNAME}' found in user list")
                print_info("User details:")
                pretty_print(test_user)
                return True
            else:
                print_error(f"Test user '{TEST_USERNAME}' NOT found in user list")
                print_info(f"Available users: {[u.get('username') for u in users[:5]]}")
                return False
        else:
            print_error(f"Failed to list users (HTTP {response.status_code})")
            print(response.text)
            return False
    except Exception as e:
        print_error(f"Exception: {e}")
        return False

def test_get_user(session):
    """Test 3: Get user using extended API"""
    print_header("3. GET USER (Extended API)")
    print(f"GET {BASE_URL}/api/v1/users-ext/?username={quote(TEST_USERNAME, safe='')}")
    
    try:
        response = session.get(
            f"{BASE_URL}/api/v1/users-ext/",
            params={"username": TEST_USERNAME},
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            print_success("User retrieved successfully")
            pretty_print(response.json())
            return True
        else:
            print_error(f"Failed to get user (HTTP {response.status_code})")
            print(response.text)
            return False
    except Exception as e:
        print_error(f"Exception: {e}")
        return False

def test_patch_user(session, csrf_token=None):
    """Test 4: Update user email and name using extended API"""
    print_header("4. PATCH USER - Update email and name (Extended API)")
    print(f"PATCH {BASE_URL}/api/v1/users-ext/?username={quote(TEST_USERNAME, safe='')}")
    
    payload = {
        "email": "updated@example.com",
        "first_name": "Updated"
    }
    
    headers = {
        "Content-Type": "application/json",
        "Referer": f"{BASE_URL}/home",
        "Origin": BASE_URL
    }
    if csrf_token:
        headers["X-CSRFToken"] = csrf_token
    
    try:
        response = session.patch(
            f"{BASE_URL}/api/v1/users-ext/",
            params={"username": TEST_USERNAME},
            json=payload,
            headers=headers
        )
        
        if response.status_code == 200:
            print_success("User updated successfully")
            pretty_print(response.json())
            return True
        else:
            print_error(f"Failed to update user (HTTP {response.status_code})")
            print(response.text)
            return False
    except Exception as e:
        print_error(f"Exception: {e}")
        return False

def test_patch_roles(session, csrf_token=None):
    """Test 5: Update user roles with update_mask"""
    print_header("5. PATCH USER - Update roles with update_mask (Extended API)")
    print(f"PATCH {BASE_URL}/api/v1/users-ext/?username={quote(TEST_USERNAME, safe='')}&update_mask=roles")
    
    payload = {
        "roles": [{"name": "Admin"}]
    }
    
    headers = {
        "Content-Type": "application/json",
        "Referer": f"{BASE_URL}/home",
        "Origin": BASE_URL
    }
    if csrf_token:
        headers["X-CSRFToken"] = csrf_token
    
    try:
        response = session.patch(
            f"{BASE_URL}/api/v1/users-ext/",
            params={"username": TEST_USERNAME, "update_mask": "roles"},
            json=payload,
            headers=headers
        )
        
        if response.status_code == 200:
            print_success("User roles updated successfully")
            pretty_print(response.json())
            return True
        else:
            print_error(f"Failed to update user roles (HTTP {response.status_code})")
            print(response.text)
            return False
    except Exception as e:
        print_error(f"Exception: {e}")
        return False

def test_verify_updates(session):
    """Test 6: Verify all updates"""
    print_header("6. GET USER - Verify updates (Extended API)")
    print(f"GET {BASE_URL}/api/v1/users-ext/?username={quote(TEST_USERNAME, safe='')}")
    
    try:
        response = session.get(
            f"{BASE_URL}/api/v1/users-ext/",
            params={"username": TEST_USERNAME},
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            print_success("User retrieved successfully")
            data = response.json()
            pretty_print(data)
            
            # Verify changes
            print_info("\nVerifying changes:")
            
            email = data.get('email')
            if email == "updated@example.com":
                print_success("Email updated correctly")
            else:
                print_error(f"Email not updated (expected: updated@example.com, got: {email})")
            
            first_name = data.get('first_name')
            if first_name == "Updated":
                print_success("First name updated correctly")
            else:
                print_error(f"First name not updated (expected: Updated, got: {first_name})")
            
            roles = data.get('roles', [])
            role_name = roles[0].get('name') if roles else None
            if role_name == "Admin":
                print_success("Role updated correctly")
            else:
                print_error(f"Role not updated (expected: Admin, got: {role_name})")
            
            return True
        else:
            print_error(f"Failed to get user (HTTP {response.status_code})")
            print(response.text)
            return False
    except Exception as e:
        print_error(f"Exception: {e}")
        return False

def test_delete_user(session, csrf_token=None):
    """Test 7: Delete user using extended API"""
    print_header("7. DELETE USER (Extended API)")
    print(f"DELETE {BASE_URL}/api/v1/users-ext/?username={quote(TEST_USERNAME, safe='')}")
    
    headers = {
        "Referer": f"{BASE_URL}/home",
        "Origin": BASE_URL
    }
    if csrf_token:
        headers["X-CSRFToken"] = csrf_token
    
    try:
        response = session.delete(
            f"{BASE_URL}/api/v1/users-ext/",
            params={"username": TEST_USERNAME},
            headers=headers
        )
        
        if response.status_code == 204:
            print_success("User deleted successfully (HTTP 204 No Content)")
            return True
        else:
            print_error(f"Failed to delete user (HTTP {response.status_code})")
            print(response.text)
            return False
    except Exception as e:
        print_error(f"Exception: {e}")
        return False

def test_verify_deletion(session):
    """Test 8: Verify user was deleted"""
    print_header("8. GET USER - Verify deletion (Extended API)")
    print(f"GET {BASE_URL}/api/v1/users-ext/?username={quote(TEST_USERNAME, safe='')}")
    
    try:
        response = session.get(
            f"{BASE_URL}/api/v1/users-ext/",
            params={"username": TEST_USERNAME},
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 404:
            print_success("User not found (correctly deleted)")
            pretty_print(response.json())
            return True
        else:
            print_error(f"User still exists (HTTP {response.status_code})")
            print(response.text)
            return False
    except Exception as e:
        print_error(f"Exception: {e}")
        return False

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Test Username API Extension Plugin',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test local Airflow
  python3 test_api_session.py
  
  # Test AWS MWAA
  python3 test_api_session.py --mwaa --env-name my-mwaa-env --region us-east-1
  
  # Test with custom username
  python3 test_api_session.py --username "domain/testuser"
        """
    )
    
    parser.add_argument('--mwaa', action='store_true',
                        help='Use AWS MWAA authentication')
    parser.add_argument('--env-name', type=str,
                        help='MWAA environment name (required if --mwaa is set)')
    parser.add_argument('--region', type=str, default='us-east-1',
                        help='AWS region (default: us-east-1)')
    parser.add_argument('--base-url', type=str,
                        help='Base URL for local Airflow (default: http://localhost:8080)')
    parser.add_argument('--username', type=str,
                        help='Test username to create (default: assumed-role/TestUser123)')
    parser.add_argument('--local-username', type=str, default='admin',
                        help='Local Airflow username (default: admin)')
    parser.add_argument('--local-password', type=str, default='test',
                        help='Local Airflow password (default: test)')
    
    return parser.parse_args()

def main():
    global BASE_URL, USERNAME, PASSWORD, TEST_USERNAME, USE_MWAA, MWAA_ENV_NAME, MWAA_REGION
    
    # Parse arguments
    args = parse_args()
    
    # Validate MWAA arguments
    if args.mwaa and not args.env_name:
        print_error("--env-name is required when using --mwaa")
        sys.exit(1)
    
    # Set configuration from arguments
    USE_MWAA = args.mwaa
    MWAA_ENV_NAME = args.env_name
    MWAA_REGION = args.region
    
    if args.base_url:
        BASE_URL = args.base_url
    if args.username:
        TEST_USERNAME = args.username
    if args.local_username:
        USERNAME = args.local_username
    if args.local_password:
        PASSWORD = args.local_password
    
    print("=" * 50)
    print("Username API Extension - Full CRUD Test")
    if USE_MWAA:
        print("(AWS MWAA Authentication)")
    else:
        print("(Session Authentication)")
    print("=" * 50)
    
    if USE_MWAA:
        print_info(f"MWAA Environment: {MWAA_ENV_NAME}")
        print_info(f"AWS Region: {MWAA_REGION}")
    else:
        print_info(f"Base URL: {BASE_URL}")
    
    print_info(f"Test User: {TEST_USERNAME}")
    print_info(f"URL Encoded: {quote(TEST_USERNAME, safe='')}")
    
    # Login
    if USE_MWAA:
        session, csrf_token, base_url = get_mwaa_session(MWAA_REGION, MWAA_ENV_NAME)
        if not session:
            print_error("Failed to login to MWAA. Cannot proceed with tests.")
            sys.exit(1)
        # Update BASE_URL for MWAA
        BASE_URL = base_url
    else:
        session, csrf_token = login()
        if not session:
            print_error("Failed to login. Cannot proceed with tests.")
            sys.exit(1)
    
    tests = [
        ("Create User", lambda: test_create_user(session)),
        ("List Users", lambda: test_list_users(session)),
        ("Get User", lambda: test_get_user(session)),
        ("Patch User", lambda: test_patch_user(session, csrf_token)),
        ("Patch Roles", lambda: test_patch_roles(session, csrf_token)),
        ("Verify Updates", lambda: test_verify_updates(session)),
        ("Delete User", lambda: test_delete_user(session, csrf_token)),
        ("Verify Deletion", lambda: test_verify_deletion(session)),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print_error(f"Test '{test_name}' failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("Test Summary")
    print("=" * 50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = f"{GREEN}PASS{NC}" if result else f"{RED}FAIL{NC}"
        print(f"{test_name}: {status}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print_success("All tests passed!")
        sys.exit(0)
    else:
        print_error(f"{total - passed} test(s) failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
