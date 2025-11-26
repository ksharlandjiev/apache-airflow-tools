#!/usr/bin/env bash

# Test script for Username API Extension Plugin using AWS MWAA CLI
# Tests CREATE (standard API), GET, PATCH, DELETE (extended API)
#
# ⚠️  IMPORTANT LIMITATIONS:
# The AWS MWAA CLI cannot add custom headers (Referer, Origin, X-CSRFToken)
# which are required by MWAA for PATCH/DELETE operations.
#
# This script will show:
# - ✓ GET operations work
# - ✓ POST operations work (for user creation)
# - ✗ PATCH operations fail (missing headers)
# - ✗ DELETE operations fail (missing headers)
#
# For full CRUD testing on MWAA, use the Python script instead:
#   python3 test_api_session.py --mwaa --env-name your-env --region us-east-1

# Configuration
MWAA_ENV_NAME="${MWAA_ENV_NAME:-MyAirflowEnvironment}"
TEST_USERNAME="assumed-role/TestUser123"
ENCODED_USERNAME="assumed-role%2FTestUser123"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Username API Extension - MWAA CLI Test"
echo "=========================================="
printf "${YELLOW}MWAA Environment:${NC} %s\n" "$MWAA_ENV_NAME"
printf "${YELLOW}Test User:${NC} %s\n" "$TEST_USERNAME"
printf "${YELLOW}Encoded:${NC} %s\n" "$ENCODED_USERNAME"

# Function to print test headers
print_header() {
    printf "\n${YELLOW}=== %s ===${NC}\n" "$1"
}

# Function to print success
print_success() {
    printf "${GREEN}✓ %s${NC}\n" "$1"
}

# Function to print error
print_error() {
    printf "${RED}✗ %s${NC}\n" "$1"
}

# Function to check HTTP status from MWAA response
check_status() {
    local response="$1"
    local expected="$2"
    local status=$(echo "$response" | jq -r '.ResponseMetadata.HTTPStatusCode' 2>/dev/null)
    
    if [ "$status" = "$expected" ]; then
        return 0
    else
        return 1
    fi
}

# 1. CREATE USER (Standard API)
print_header "1. CREATE USER (Standard API)"
echo "POST /users"

CREATE_RESPONSE=$(aws mwaa invoke-rest-api \
  --name "$MWAA_ENV_NAME" \
  --method POST \
  --path "/users" \
  --body '{
    "username": "'"$TEST_USERNAME"'",
    "email": "testuser@example.com",
    "first_name": "Test",
    "last_name": "User",
    "roles": [{"name": "Admin"}]
  }' 2>&1)

if echo "$CREATE_RESPONSE" | jq -e '.RestApiResponse' > /dev/null 2>&1; then
    BODY=$(echo "$CREATE_RESPONSE" | jq -r '.RestApiResponse')
    HTTP_STATUS=$(echo "$CREATE_RESPONSE" | jq -r '.ResponseMetadata.HTTPStatusCode // empty')
    
    # MWAA CLI doesn't always return HTTPStatusCode, check response content
    if echo "$BODY" | jq -e '.username' > /dev/null 2>&1; then
        print_success "User created successfully"
        echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
    elif echo "$BODY" | jq -e '.detail' > /dev/null 2>&1; then
        DETAIL=$(echo "$BODY" | jq -r '.detail')
        if echo "$DETAIL" | grep -q "already exists"; then
            print_success "User already exists"
        else
            print_error "Failed to create user"
        fi
        echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
    else
        print_error "Unexpected response"
        echo "$BODY"
    fi
else
    print_error "Failed to create user"
    echo "$CREATE_RESPONSE"
fi

sleep 2

# 2. LIST USERS (Standard API)
print_header "2. LIST USERS - Verify user exists (Standard API)"
echo "GET /users"

LIST_RESPONSE=$(aws mwaa invoke-rest-api \
  --name "$MWAA_ENV_NAME" \
  --method GET \
  --path "/users" 2>&1)

if echo "$LIST_RESPONSE" | jq -e '.RestApiResponse' > /dev/null 2>&1; then
    BODY=$(echo "$LIST_RESPONSE" | jq -r '.RestApiResponse')
    
    if echo "$BODY" | jq -e '.users' > /dev/null 2>&1; then
        USER_COUNT=$(echo "$BODY" | jq '.users | length' 2>/dev/null)
        print_success "Retrieved $USER_COUNT users"
        
        # Check if our test user exists
        USER_EXISTS=$(echo "$BODY" | jq --arg username "$TEST_USERNAME" '.users[] | select(.username == $username)' 2>/dev/null)
        
        if [ -n "$USER_EXISTS" ]; then
            print_success "Test user '$TEST_USERNAME' found in user list"
            printf "${YELLOW}User details:${NC}\n"
            echo "$USER_EXISTS" | jq .
        else
            print_error "Test user '$TEST_USERNAME' NOT found in user list"
        fi
    else
        print_error "Failed to list users"
        echo "$BODY"
    fi
else
    print_error "Failed to list users"
    echo "$LIST_RESPONSE"
fi

sleep 2

# 3. GET USER (Extended API)
print_header "3. GET USER (Extended API)"
echo "GET /users-ext/?username=$ENCODED_USERNAME"

GET_RESPONSE=$(aws mwaa invoke-rest-api \
  --name "$MWAA_ENV_NAME" \
  --method GET \
  --path "/users-ext/?username=$ENCODED_USERNAME" 2>&1)

if echo "$GET_RESPONSE" | jq -e '.RestApiResponse' > /dev/null 2>&1; then
    BODY=$(echo "$GET_RESPONSE" | jq -r '.RestApiResponse')
    
    if echo "$BODY" | jq -e '.username' > /dev/null 2>&1; then
        print_success "User retrieved successfully"
        echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
    else
        print_error "Failed to get user"
        echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
    fi
else
    print_error "Failed to get user"
    echo "$GET_RESPONSE"
fi

sleep 2

# 4. PATCH USER - Update email and name (Extended API)
print_header "4. PATCH USER - Update email and name (Extended API)"
echo "PATCH /users-ext/?username=$ENCODED_USERNAME"

PATCH_RESPONSE=$(aws mwaa invoke-rest-api \
  --name "$MWAA_ENV_NAME" \
  --method PATCH \
  --path "/users-ext/?username=$ENCODED_USERNAME" \
  --body '{
    "email": "updated@example.com",
    "first_name": "Updated"
  }' 2>&1)

if echo "$PATCH_RESPONSE" | jq -e '.RestApiResponse' > /dev/null 2>&1; then
    BODY=$(echo "$PATCH_RESPONSE" | jq -r '.RestApiResponse')
    HTTP_STATUS=$(echo "$PATCH_RESPONSE" | jq -r '.ResponseMetadata.HTTPStatusCode')
    
    if [ "$HTTP_STATUS" = "200" ]; then
        print_success "User updated successfully"
        echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
    else
        print_error "Failed to update user (HTTP $HTTP_STATUS)"
        echo "$BODY"
    fi
else
    print_error "Failed to update user"
    echo "$PATCH_RESPONSE"
fi

sleep 2

# 5. PATCH USER - Update roles with update_mask (Extended API)
print_header "5. PATCH USER - Update roles with update_mask (Extended API)"
echo "PATCH /users-ext/?username=$ENCODED_USERNAME&update_mask=roles"

PATCH_ROLES_RESPONSE=$(aws mwaa invoke-rest-api \
  --name "$MWAA_ENV_NAME" \
  --method PATCH \
  --path "/users-ext/?username=$ENCODED_USERNAME&update_mask=roles" \
  --body '{
    "roles": [{"name": "Admin"}]
  }' 2>&1)

if echo "$PATCH_ROLES_RESPONSE" | jq -e '.RestApiResponse' > /dev/null 2>&1; then
    BODY=$(echo "$PATCH_ROLES_RESPONSE" | jq -r '.RestApiResponse')
    HTTP_STATUS=$(echo "$PATCH_ROLES_RESPONSE" | jq -r '.ResponseMetadata.HTTPStatusCode')
    
    if [ "$HTTP_STATUS" = "200" ]; then
        print_success "User roles updated successfully"
        echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
    else
        print_error "Failed to update user roles (HTTP $HTTP_STATUS)"
        echo "$BODY"
    fi
else
    print_error "Failed to update user roles"
    echo "$PATCH_ROLES_RESPONSE"
fi

sleep 2

# 6. GET USER - Verify updates (Extended API)
print_header "6. GET USER - Verify updates (Extended API)"
echo "GET /users-ext/?username=$ENCODED_USERNAME"

GET_VERIFY_RESPONSE=$(aws mwaa invoke-rest-api \
  --name "$MWAA_ENV_NAME" \
  --method GET \
  --path "/users-ext/?username=$ENCODED_USERNAME" 2>&1)

if echo "$GET_VERIFY_RESPONSE" | jq -e '.RestApiResponse' > /dev/null 2>&1; then
    BODY=$(echo "$GET_VERIFY_RESPONSE" | jq -r '.RestApiResponse')
    HTTP_STATUS=$(echo "$GET_VERIFY_RESPONSE" | jq -r '.ResponseMetadata.HTTPStatusCode')
    
    if [ "$HTTP_STATUS" = "200" ]; then
        print_success "User retrieved successfully"
        echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
        
        # Verify changes
        printf "\n${YELLOW}Verifying changes:${NC}\n"
        
        EMAIL=$(echo "$BODY" | jq -r '.email' 2>/dev/null)
        FIRST_NAME=$(echo "$BODY" | jq -r '.first_name' 2>/dev/null)
        ROLE=$(echo "$BODY" | jq -r '.roles[0].name' 2>/dev/null)
        
        if [ "$EMAIL" = "updated@example.com" ]; then
            print_success "Email updated correctly"
        else
            print_error "Email not updated (expected: updated@example.com, got: $EMAIL)"
        fi
        
        if [ "$FIRST_NAME" = "Updated" ]; then
            print_success "First name updated correctly"
        else
            print_error "First name not updated (expected: Updated, got: $FIRST_NAME)"
        fi
        
        if [ "$ROLE" = "Admin" ]; then
            print_success "Role updated correctly"
        else
            print_error "Role not updated (expected: Admin, got: $ROLE)"
        fi
    else
        print_error "Failed to get user (HTTP $HTTP_STATUS)"
        echo "$BODY"
    fi
else
    print_error "Failed to get user"
    echo "$GET_VERIFY_RESPONSE"
fi

sleep 2

# 7. DELETE USER (Extended API)
print_header "7. DELETE USER (Extended API)"
echo "DELETE /users-ext/?username=$ENCODED_USERNAME"

DELETE_RESPONSE=$(aws mwaa invoke-rest-api \
  --name "$MWAA_ENV_NAME" \
  --method DELETE \
  --path "/users-ext/?username=$ENCODED_USERNAME" 2>&1)

if echo "$DELETE_RESPONSE" | jq -e '.ResponseMetadata' > /dev/null 2>&1; then
    HTTP_STATUS=$(echo "$DELETE_RESPONSE" | jq -r '.ResponseMetadata.HTTPStatusCode')
    
    if [ "$HTTP_STATUS" = "204" ]; then
        print_success "User deleted successfully (HTTP 204 No Content)"
    else
        BODY=$(echo "$DELETE_RESPONSE" | jq -r '.RestApiResponse' 2>/dev/null)
        print_error "Failed to delete user (HTTP $HTTP_STATUS)"
        echo "$BODY"
    fi
else
    print_error "Failed to delete user"
    echo "$DELETE_RESPONSE"
fi

sleep 2

# 8. GET USER - Verify deletion (Extended API)
print_header "8. GET USER - Verify deletion (Extended API)"
echo "GET /users-ext/?username=$ENCODED_USERNAME"

GET_DELETED_RESPONSE=$(aws mwaa invoke-rest-api \
  --name "$MWAA_ENV_NAME" \
  --method GET \
  --path "/users-ext/?username=$ENCODED_USERNAME" 2>&1)

if echo "$GET_DELETED_RESPONSE" | jq -e '.RestApiResponse' > /dev/null 2>&1; then
    BODY=$(echo "$GET_DELETED_RESPONSE" | jq -r '.RestApiResponse')
    HTTP_STATUS=$(echo "$GET_DELETED_RESPONSE" | jq -r '.ResponseMetadata.HTTPStatusCode')
    
    if [ "$HTTP_STATUS" = "404" ]; then
        print_success "User not found (correctly deleted)"
        echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
    else
        print_error "User still exists (HTTP $HTTP_STATUS)"
        echo "$BODY"
    fi
else
    print_error "Unexpected response"
    echo "$GET_DELETED_RESPONSE"
fi

printf "\n==========================================\n"
printf "${GREEN}Test Complete!${NC}\n"
echo "=========================================="
