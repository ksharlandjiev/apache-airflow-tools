# User Creation Interceptor Plugin

Demonstrates how to intercept user creation events in Apache Airflow to trigger custom logic such as username sanitization, role assignment, or other automated workflows.

## Overview

Apache Airflow provides limited hooks for customizing user creation behavior. This plugin demonstrates how to use SQLAlchemy event listeners to intercept user creation events and execute custom logic before or after users are created in the database.

**Common use cases:**
- Automatic role assignment based on user attributes
- Username validation and policy enforcement
- Integration with external identity management systems
- Audit logging and compliance tracking
- User provisioning workflows

## Example Implementation: Username Sanitization

The included implementation demonstrates username sanitization by replacing forward slashes with underscores. This addresses a specific limitation where Flask/Connexion treats `/` as a URL path separator, making usernames like `domain/username` inaccessible via API endpoints such as `/api/v1/users/{username}`.

**Example behavior:**
- User tries to create: `domain/username`
- Actually created: `domain_username`
- API access: `/api/v1/users/domain_username` (works correctly)

**Note:** This is just one example implementation. The plugin architecture can be adapted for many other use cases (see Use Cases section below).

## ⚠️ IMPORTANT NOTE FOR MWAA USERS
**This username sanitization approach will NOT work for AWS MWAA environments.** MWAA relies on exact username matching to map users to IAM identities. Modifying usernames (e.g., changing `assumed-role/roleName` to `assumed-role_roleName`) will break IAM authentication mapping, preventing users from logging in.

**For MWAA environments:**
- Use the [**Username API Extension**](../username_api_extension/) plugin instead to preserve exact usernames
- Or use this plugin as a template for other user creation workflows (see Use Cases below)

## Installation

### 1. Copy Plugin File
```bash
cp user_creation_interceptor.py $AIRFLOW_HOME/plugins/
```

### 2. Restart Airflow
```bash
# Stop services
pkill -f "airflow webserver"
pkill -f "airflow scheduler"

# Start services
airflow webserver -D
airflow scheduler -D
```

### 3. Verify Installation
```bash
airflow plugins | grep user_creation_interceptor
```

You should see:
```
user_creation_interceptor     |
```

## Usage

The plugin works automatically once installed. No configuration needed!

### Creating Users

**Via CLI:**
```bash
airflow users create \
    --username "domain/username" \
    --firstname "John" \
    --lastname "Doe" \
    --role "Admin" \
    --email "john@example.com" \
    --password "password123"

# Username will be created as: domain_username
```

**Via API:**
```bash
curl -X POST "http://localhost:8080/api/v1/users" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
  -d '{
    "username": "domain/username",
    "first_name": "John",
    "last_name": "Doe",
    "email": "john@example.com",
    "roles": [{"name": "Admin"}]
  }'

# Username will be created as: domain_username
```

**Via UI:**
Navigate to Security → List Users → Add User, enter `domain/username` as username.

### Accessing Users

After creation, use the sanitized username:

```bash
# Get user
curl -X GET "http://localhost:8080/api/v1/users/domain_username" \
  -u "admin:admin"

# Update user
curl -X PATCH "http://localhost:8080/api/v1/users/domain_username" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
  -d '{"email": "newemail@example.com"}'

# Delete user
curl -X DELETE "http://localhost:8080/api/v1/users/domain_username" \
  -u "admin:admin"
```

## How It Works

The plugin uses SQLAlchemy event listeners to intercept user creation:

1. **Before Insert**: Detects `/` in username and replaces with `_`
2. **Logs Change**: Records the transformation for audit
3. **After Insert**: Confirms user creation

```python
# Pseudocode
if '/' in username:
    username = username.replace('/', '_')
    log.warning(f"Changed {original} to {username}")
```

## Logging

All username transformations are logged:

```
WARNING - Username sanitization: Changed username from 'domain/username' to 'domain_username' 
          because forward slashes are not supported in API endpoints.
INFO - User created successfully: username='domain_username', email='john@example.com'
```

Check logs at:
- `$AIRFLOW_HOME/logs/scheduler/latest/*.log`
- `$AIRFLOW_HOME/logs/webserver/*.log`

## Testing

### Automated Test
```bash
cd $AIRFLOW_HOME/plugins
python test_user_interceptor.py
```

### Manual Test
```bash
# Create test user
airflow users create \
    --username "test/user" \
    --firstname "Test" \
    --lastname "User" \
    --role "Viewer" \
    --email "test@example.com" \
    --password "test123"

# Verify sanitization
airflow users list | grep test_user

# Should show: test_user (not test/user)
```

## Customization

### Change Replacement Character

Edit the plugin to use a different character:

```python
# Replace this line:
sanitized_username = original_username.replace('/', '_')

# With (for example, dash):
sanitized_username = original_username.replace('/', '-')
```

### Handle Multiple Characters

```python
# Replace multiple special characters
replacements = {
    '/': '_',
    '\\': '_',
    ' ': '_',
    ':': '_'
}

for old, new in replacements.items():
    if old in sanitized_username:
        sanitized_username = sanitized_username.replace(old, new)
```

### Reject Instead of Sanitize

```python
# Prevent creation instead of sanitizing
if '/' in target.username:
    raise ValueError(
        f"Username '{target.username}' contains invalid character '/'"
    )
```

See `CUSTOMIZATION_EXAMPLES.md` for 12 more examples.

## Troubleshooting

### Plugin Not Loading

**Check file location:**
```bash
ls $AIRFLOW_HOME/plugins/user_creation_interceptor.py
```

**Check for syntax errors:**
```bash
python -m py_compile $AIRFLOW_HOME/plugins/user_creation_interceptor.py
```

**Check logs:**
```bash
grep "User Creation Interceptor" $AIRFLOW_HOME/logs/scheduler/latest/*.log
```

### Users Still Created with Slashes

1. Verify plugin is loaded: `airflow plugins`
2. Ensure you're using FAB auth manager (not custom)
3. Restart **all** Airflow services
4. Check for errors in logs

### Import Errors

Ensure you're using:
- Airflow 2.8.0+
- FAB auth manager
- Python 3.8+

Check auth manager in `airflow.cfg`:
```ini
[core]
auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

## Features

- **Automatic** - Works without configuration
- **Non-invasive** - No Airflow code changes
- **Logged** - All transformations recorded
- **Tested** - Includes test suite
- **Customizable** - Easy to modify
- **Production-ready** - Handles edge cases  

## Compatibility

- **Airflow:** 2.8.0+
- **Python:** 3.8+
- **Auth Manager:** FAB (Flask-AppBuilder)
- **Database:** PostgreSQL, MySQL, SQLite
- **⚠️ MWAA:** Username sanitization NOT compatible (breaks IAM mapping). Use for other workflows only.

## Performance

- **Impact:** <1ms per user creation
- **No impact** on existing users
- **No impact** on authentication
- **No impact** on API performance

## Security

- All changes logged for audit
- Original username preserved in logs
- No data loss
- Idempotent operation
- Non-breaking for existing users

## Use Cases

This plugin demonstrates the user creation interception pattern, which can be adapted for various scenarios:

### Username Sanitization (Demonstrated)
- Automatically clean special characters from usernames
- Enforce username conventions
- **Note:** Not suitable for MWAA (breaks IAM mapping)

### Automatic Role Assignment
```python
@event.listens_for(User, 'before_insert')
def auto_assign_roles(mapper, connection, target):
    """Assign roles based on email domain or username pattern"""
    if target.email.endswith('@admin.company.com'):
        # Assign admin role automatically
        admin_role = security_manager.find_role('Admin')
        target.roles = [admin_role]
    elif target.email.endswith('@company.com'):
        # Assign viewer role for regular employees
        viewer_role = security_manager.find_role('Viewer')
        target.roles = [viewer_role]
```

### User Provisioning Workflows
```python
@event.listens_for(User, 'after_insert')
def trigger_provisioning(mapper, connection, target):
    """Trigger external provisioning systems"""
    # Send notification to Slack
    # Create home directory in S3
    # Add user to monitoring systems
    # Send welcome email
```

### Audit and Compliance
```python
@event.listens_for(User, 'before_insert')
def audit_user_creation(mapper, connection, target):
    """Log user creation for compliance"""
    audit_log.info(f"New user created: {target.username} by {current_user}")
    # Send to SIEM system
    # Update compliance database
```

### Username Validation
```python
@event.listens_for(User, 'before_insert')
def validate_username(mapper, connection, target):
    """Enforce username policies"""
    if not re.match(r'^[a-zA-Z0-9_-]+$', target.username):
        raise ValueError("Username must contain only alphanumeric, underscore, or dash")
    if len(target.username) < 3:
        raise ValueError("Username must be at least 3 characters")
```

### Integration with External Systems
```python
@event.listens_for(User, 'after_insert')
def sync_to_external_systems(mapper, connection, target):
    """Sync user to external identity providers"""
    # Update LDAP
    # Sync to Active Directory
    # Update SSO provider
    # Create Jira account
```

## When to Use This Plugin

**Use this plugin pattern for:**
- Automatic role assignment based on user attributes
- Triggering provisioning workflows
- Enforcing username policies and validation
- Audit logging and compliance tracking
- Integration with external identity systems
- Custom user creation workflows

**Use the username sanitization example if:**
- You're setting up a new self-hosted Airflow instance (not MWAA)
- You control user creation process
- You want automatic sanitization
- You're okay with modified usernames

**Don't use username sanitization if:**
- You're using AWS MWAA (breaks IAM authentication)
- You must preserve exact usernames for SSO/LDAP
- You're using a custom auth manager
- You have existing users with special characters that can't be renamed

## Alternative Solutions

### For MWAA or Exact Username Preservation
If you need to preserve exact usernames with slashes (especially for MWAA), see the [**Username API Extension**](../username_api_extension/) plugin which provides alternative API endpoints using query parameters.

### For Other Use Cases
Adapt the event listener pattern shown in this plugin to implement your specific user creation workflow.

## Files

- `user_creation_interceptor.py` - Main plugin file
- `README.md` - This file
- `test_user_interceptor.py` - Test suite
- `CUSTOMIZATION_EXAMPLES.md` - Customization examples

## Support

For issues:
1. Run test suite: `python test_user_interceptor.py`
2. Check logs: `$AIRFLOW_HOME/logs/`
3. Verify plugin loaded: `airflow plugins`
4. Check auth manager configuration

## License

Apache License 2.0 - Same as Apache Airflow

---

**Quick Start:**
```bash
cp user_creation_interceptor.py $AIRFLOW_HOME/plugins/
airflow webserver -D && airflow scheduler -D
airflow plugins | grep user_creation_interceptor
```

The plugin is now active and will execute your custom logic on user creation.
