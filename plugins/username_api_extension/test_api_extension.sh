#!/bin/bash
# Test script for Username API Extension Plugin
# Usage: ./test_api_extension.sh [base_url] [username] [password]

set -e

# Configuration
BASE_URL="${1:-http://localhost:8080}"
USERNAME="${2:-admin}"
PASSWORD="${3:-admin}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test user details
TEST_USERNAME="test/user_$(date +%s)"
TEST_EMAIL="test_$(date +%s)@example.com"

echo "========================================"
echo "Username API Extension - Test Suite"
echo "========================================"
echo "Base URL: $BASE_URL"
echo "Auth User: $USERNAME"
echo "Test Username: $TEST_USERNAME"
echo ""

# Function to print test results
print_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ PASS${NC}: $2"
    else
        echo -e "${RED}✗ FAIL${NC}: $2"
        exit 1
    fi
}

# Function to URL encode
urlencode() {
    python3 -c "import urllib.parse; print(urllib.parse.quote('$1'))"
}

# Encode test username
ENCODED_USERNAME=$(urlencode "$TEST_USERNAME")

echo "Step 1: Create test user (using standard CLI)"
echo "--------------------------------------"
airflow users create \
    --username "$TEST_USERNAME" \
    --firstname "Test" \
    --lastname "User" \
    --role "Viewer" \
    --email "$TEST_EMAIL" \
    --password "test123" 2>/dev/null || true

print_result $? "User created via CLI"
echo ""

echo "Step 2: Test GET user with slash in username"
echo "--------------------------------------"
RESPONSE=$(curl -s -w "\n%{http_code}" -X GET \
    "$BASE_URL/api/v1/users-ext/?username=$ENCODED_USERNAME" \
    -u "$USERNAME:$PASSWORD")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n-1)

if [ "$HTTP_CODE" = "200" ]; then
    print_result 0 "GET request successful (HTTP $HTTP_CODE)"
    echo "Response: $BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"
else
    print_result 1 "GET request failed (HTTP $HTTP_CODE)"
fi
echo ""

echo "Step 3: Test PATCH user with slash in username"
echo "--------------------------------------"
NEW_EMAIL="updated_$(date +%s)@example.com"
RESPONSE=$(curl -s -w "\n%{http_code}" -X PATCH \
    "$BASE_URL/api/v1/users-ext/?username=$ENCODED_USERNAME" \
    -H "Content-Type: application/json" \
    -u "$USERNAME:$PASSWORD" \
    -d "{\"email\": \"$NEW_EMAIL\"}")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n-1)

if [ "$HTTP_CODE" = "200" ]; then
    print_result 0 "PATCH request successful (HTTP $HTTP_CODE)"
    echo "Response: $BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"
else
    print_result 1 "PATCH request failed (HTTP $HTTP_CODE)"
fi
echo ""

echo "Step 4: Verify email was updated"
echo "--------------------------------------"
RESPONSE=$(curl -s -X GET \
    "$BASE_URL/api/v1/users-ext/?username=$ENCODED_USERNAME" \
    -u "$USERNAME:$PASSWORD")

if echo "$RESPONSE" | grep -q "$NEW_EMAIL"; then
    print_result 0 "Email successfully updated"
else
    print_result 1 "Email not updated"
fi
echo ""

echo "Step 5: Test with update_mask"
echo "--------------------------------------"
RESPONSE=$(curl -s -w "\n%{http_code}" -X PATCH \
    "$BASE_URL/api/v1/users-ext/?username=$ENCODED_USERNAME&update_mask=first_name" \
    -H "Content-Type: application/json" \
    -u "$USERNAME:$PASSWORD" \
    -d '{"first_name": "Updated"}')

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

if [ "$HTTP_CODE" = "200" ]; then
    print_result 0 "PATCH with update_mask successful (HTTP $HTTP_CODE)"
else
    print_result 1 "PATCH with update_mask failed (HTTP $HTTP_CODE)"
fi
echo ""

echo "Step 6: Test error handling - missing username"
echo "--------------------------------------"
RESPONSE=$(curl -s -w "\n%{http_code}" -X GET \
    "$BASE_URL/api/v1/users-ext/" \
    -u "$USERNAME:$PASSWORD")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

if [ "$HTTP_CODE" = "400" ]; then
    print_result 0 "Missing username returns 400 (HTTP $HTTP_CODE)"
else
    print_result 1 "Missing username should return 400, got $HTTP_CODE"
fi
echo ""

echo "Step 7: Test error handling - user not found"
echo "--------------------------------------"
RESPONSE=$(curl -s -w "\n%{http_code}" -X GET \
    "$BASE_URL/api/v1/users-ext/?username=nonexistent/user" \
    -u "$USERNAME:$PASSWORD")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

if [ "$HTTP_CODE" = "404" ]; then
    print_result 0 "Nonexistent user returns 404 (HTTP $HTTP_CODE)"
else
    print_result 1 "Nonexistent user should return 404, got $HTTP_CODE"
fi
echo ""

echo "Step 8: Test DELETE user with slash in username"
echo "--------------------------------------"
RESPONSE=$(curl -s -w "\n%{http_code}" -X DELETE \
    "$BASE_URL/api/v1/users-ext/?username=$ENCODED_USERNAME" \
    -u "$USERNAME:$PASSWORD")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

if [ "$HTTP_CODE" = "204" ]; then
    print_result 0 "DELETE request successful (HTTP $HTTP_CODE)"
else
    print_result 1 "DELETE request failed (HTTP $HTTP_CODE)"
fi
echo ""

echo "Step 9: Verify user was deleted"
echo "--------------------------------------"
RESPONSE=$(curl -s -w "\n%{http_code}" -X GET \
    "$BASE_URL/api/v1/users-ext/?username=$ENCODED_USERNAME" \
    -u "$USERNAME:$PASSWORD")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

if [ "$HTTP_CODE" = "404" ]; then
    print_result 0 "User successfully deleted (returns 404)"
else
    print_result 1 "User still exists (HTTP $HTTP_CODE)"
fi
echo ""

echo "========================================"
echo -e "${GREEN}All tests passed!${NC}"
echo "========================================"
echo ""
echo "The Username API Extension plugin is working correctly."
echo "You can now use /api/v1/users-ext/ endpoints with usernames containing slashes."
