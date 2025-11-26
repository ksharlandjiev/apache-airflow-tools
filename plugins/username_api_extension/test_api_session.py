#!/usr/bin/env python3
"""
Test script for Username API Extension Plugin with Session Authentication
Tests CREATE (standard API), GET, PATCH, DELETE (extended API)
"""

import requests
import json
import sys
from urllib.parse import quote

# Configuration
BASE_URL = "http://localhost:8080"
USERNAME = "admin"
PASSWORD = "test"
TEST_USERNAME = "assumed-role/TestUser123"
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

def get_csrf_token(session):
    """Get CSRF token from the home page"""
    try:
        response = session.get(f"{BASE_URL}/home")
        if response.status_code == 200:
            # Try to extract CSRF token from cookies
            csrf_token = session.cookies.get('csrf_token') or session.cookies.get('_csrf_token')
            if csrf_token:
                return csrf_token
            
            # Try to extract from response headers
            csrf_token = response.headers.get('X-CSRFToken')
            if csrf_token:
                return csrf_token
                
            # Try to parse from HTML
            import re
            match = re.search(r'csrf_token["\s:]+([a-zA-Z0-9\-_]+)', response.text)
            if match:
                return match.group(1)
        
        return None
    except Exception as e:
        print_error(f"Exception getting CSRF token: {e}")
        return None

def login():
    """Login to get session cookie"""
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
            csrf_token = get_csrf_token(session)
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
    
    headers = {"Content-Type": "application/json"}
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
    
    headers = {"Content-Type": "application/json"}
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
    
    headers = {}
    if csrf_token:
        headers["X-CSRFToken"] = csrf_token
    
    try:
        response = session.delete(
            f"{BASE_URL}/api/v1/users-ext/",
            params={"username": TEST_USERNAME},
            headers=headers if headers else None
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

def main():
    print("=" * 50)
    print("Username API Extension - Full CRUD Test")
    print("(Session Authentication)")
    print("=" * 50)
    print_info(f"Test User: {TEST_USERNAME}")
    print_info(f"URL Encoded: {quote(TEST_USERNAME, safe='')}")
    
    # Login first
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
