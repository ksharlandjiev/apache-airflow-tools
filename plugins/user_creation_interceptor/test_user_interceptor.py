#!/usr/bin/env python3
"""
Test script for User Creation Interceptor Plugin.

This script tests the plugin functionality by creating test users
and verifying that usernames with slashes are properly sanitized.

Usage:
    python test_user_interceptor.py
"""
from __future__ import annotations

import sys
from datetime import datetime

# Add Airflow to path if needed
try:
    from airflow import settings
    from airflow.providers.fab.auth_manager.models import User
    from airflow.www.extensions.init_auth_manager import get_auth_manager
    from sqlalchemy import select
except ImportError as e:
    print(f"Error importing Airflow modules: {e}")
    print("Make sure you're running this from an environment with Airflow installed.")
    sys.exit(1)


def test_user_creation_with_slash():
    """Test creating a user with a slash in the username."""
    print("\n" + "="*70)
    print("TEST: Creating user with slash in username")
    print("="*70)
    
    test_username = f"test/user_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    expected_username = test_username.replace('/', '_')
    test_email = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}@example.com"
    
    print(f"\nOriginal username: {test_username}")
    print(f"Expected username: {expected_username}")
    print(f"Email: {test_email}")
    
    try:
        # Get security manager
        security_manager = get_auth_manager().security_manager
        
        # Find or create a role
        role = security_manager.find_role("Viewer")
        if not role:
            print("\nError: 'Viewer' role not found. Creating it...")
            role = security_manager.add_role("Viewer")
        
        # Create user
        print(f"\nCreating user with username: {test_username}")
        user = security_manager.add_user(
            username=test_username,
            first_name="Test",
            last_name="User",
            email=test_email,
            role=[role],
            password="test123"
        )
        
        if user:
            print(f"✓ User created successfully!")
            print(f"  - Actual username in DB: {user.username}")
            print(f"  - Email: {user.email}")
            print(f"  - ID: {user.id}")
            
            # Verify the username was sanitized
            if user.username == expected_username:
                print(f"\n✓ SUCCESS: Username was correctly sanitized!")
                print(f"  Original: {test_username}")
                print(f"  Sanitized: {user.username}")
                return True, user.id
            else:
                print(f"\n✗ FAILURE: Username was not sanitized correctly!")
                print(f"  Expected: {expected_username}")
                print(f"  Got: {user.username}")
                return False, user.id if user else None
        else:
            print("✗ Failed to create user")
            return False, None
            
    except Exception as e:
        print(f"\n✗ Error creating user: {e}")
        import traceback
        traceback.print_exc()
        return False, None


def test_user_retrieval(user_id):
    """Test retrieving the created user."""
    print("\n" + "="*70)
    print("TEST: Retrieving created user")
    print("="*70)
    
    try:
        session = settings.Session()
        user = session.get(User, user_id)
        
        if user:
            print(f"\n✓ User retrieved successfully!")
            print(f"  - Username: {user.username}")
            print(f"  - Email: {user.email}")
            print(f"  - Active: {user.active}")
            print(f"  - Roles: {[role.name for role in user.roles]}")
            
            # Verify no slash in username
            if '/' not in user.username:
                print(f"\n✓ SUCCESS: Username contains no slashes")
                return True
            else:
                print(f"\n✗ FAILURE: Username still contains slashes: {user.username}")
                return False
        else:
            print(f"\n✗ User with ID {user_id} not found")
            return False
            
    except Exception as e:
        print(f"\n✗ Error retrieving user: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_user_without_slash():
    """Test creating a user without a slash (should work normally)."""
    print("\n" + "="*70)
    print("TEST: Creating user WITHOUT slash in username")
    print("="*70)
    
    test_username = f"normaluser_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    test_email = f"normal_{datetime.now().strftime('%Y%m%d%H%M%S')}@example.com"
    
    print(f"\nUsername: {test_username}")
    print(f"Email: {test_email}")
    
    try:
        security_manager = get_auth_manager().security_manager
        role = security_manager.find_role("Viewer")
        
        user = security_manager.add_user(
            username=test_username,
            first_name="Normal",
            last_name="User",
            email=test_email,
            role=[role],
            password="test123"
        )
        
        if user and user.username == test_username:
            print(f"\n✓ SUCCESS: Normal username unchanged")
            print(f"  Username: {user.username}")
            return True, user.id
        else:
            print(f"\n✗ FAILURE: Normal username was modified")
            return False, user.id if user else None
            
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False, None


def cleanup_test_users(user_ids):
    """Clean up test users."""
    print("\n" + "="*70)
    print("CLEANUP: Removing test users")
    print("="*70)
    
    try:
        security_manager = get_auth_manager().security_manager
        session = security_manager.get_session
        
        for user_id in user_ids:
            if user_id:
                user = session.get(User, user_id)
                if user:
                    print(f"\nDeleting user: {user.username} (ID: {user_id})")
                    user.roles = []  # Clear foreign keys
                    session.delete(user)
        
        session.commit()
        print("\n✓ Cleanup completed")
        
    except Exception as e:
        print(f"\n✗ Cleanup error: {e}")


def main():
    """Run all tests."""
    print("\n" + "="*70)
    print("USER CREATION INTERCEPTOR PLUGIN - TEST SUITE")
    print("="*70)
    
    user_ids = []
    results = []
    
    # Test 1: User with slash
    success, user_id = test_user_creation_with_slash()
    results.append(("User with slash", success))
    if user_id:
        user_ids.append(user_id)
        
        # Test 2: Retrieve user
        success = test_user_retrieval(user_id)
        results.append(("User retrieval", success))
    
    # Test 3: User without slash
    success, user_id = test_user_without_slash()
    results.append(("User without slash", success))
    if user_id:
        user_ids.append(user_id)
    
    # Cleanup
    cleanup_test_users(user_ids)
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    for test_name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    total_tests = len(results)
    passed_tests = sum(1 for _, success in results if success)
    
    print(f"\nTotal: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("\n🎉 All tests passed! Plugin is working correctly.")
        return 0
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
