# Customization Examples

This document provides examples of how to customize the User Creation Interceptor plugin for different use cases.

## Example 1: Use Different Replacement Character

Replace slashes with dashes instead of underscores:

```python
@event.listens_for(User, 'before_insert')
def sanitize_username_before_insert(mapper, connection, target):
    original_username = target.username
    
    if '/' in original_username:
        # Use dash instead of underscore
        sanitized_username = original_username.replace('/', '-')
        target.username = sanitized_username
        
        log.warning(
            "Username sanitization: Changed username from '%s' to '%s'",
            original_username,
            sanitized_username
        )
```

## Example 2: Reject Instead of Sanitize

Prevent user creation if username contains slashes:

```python
@event.listens_for(User, 'before_insert')
def validate_username_before_insert(mapper, connection, target):
    if '/' in target.username:
        raise ValueError(
            f"Username '{target.username}' contains invalid character '/'. "
            "Usernames cannot contain forward slashes."
        )
```

## Example 3: Multiple Character Sanitization

Handle multiple special characters:

```python
@event.listens_for(User, 'before_insert')
def sanitize_username_before_insert(mapper, connection, target):
    original_username = target.username
    sanitized_username = original_username
    
    # Define character replacements
    replacements = {
        '/': '_',   # Forward slash
        '\\': '_',  # Backslash
        ' ': '_',   # Space
        ':': '_',   # Colon
        '*': '_',   # Asterisk
        '?': '_',   # Question mark
        '"': '_',   # Quote
        '<': '_',   # Less than
        '>': '_',   # Greater than
        '|': '_',   # Pipe
    }
    
    # Apply all replacements
    for old_char, new_char in replacements.items():
        if old_char in sanitized_username:
            sanitized_username = sanitized_username.replace(old_char, new_char)
    
    # Update if changed
    if sanitized_username != original_username:
        target.username = sanitized_username
        log.warning(
            "Username sanitization: Changed username from '%s' to '%s'",
            original_username,
            sanitized_username
        )
```

## Example 4: Lowercase Enforcement

Force all usernames to lowercase:

```python
@event.listens_for(User, 'before_insert')
def sanitize_username_before_insert(mapper, connection, target):
    original_username = target.username
    sanitized_username = original_username
    
    # Replace slashes
    if '/' in sanitized_username:
        sanitized_username = sanitized_username.replace('/', '_')
    
    # Force lowercase
    sanitized_username = sanitized_username.lower()
    
    if sanitized_username != original_username:
        target.username = sanitized_username
        log.warning(
            "Username sanitization: Changed username from '%s' to '%s'",
            original_username,
            sanitized_username
        )
```

## Example 5: Length Limit Enforcement

Enforce maximum username length:

```python
@event.listens_for(User, 'before_insert')
def sanitize_username_before_insert(mapper, connection, target):
    original_username = target.username
    sanitized_username = original_username
    
    # Replace slashes
    if '/' in sanitized_username:
        sanitized_username = sanitized_username.replace('/', '_')
    
    # Enforce max length (e.g., 50 characters)
    MAX_LENGTH = 50
    if len(sanitized_username) > MAX_LENGTH:
        sanitized_username = sanitized_username[:MAX_LENGTH]
        log.warning(
            "Username truncated from %d to %d characters",
            len(original_username),
            MAX_LENGTH
        )
    
    if sanitized_username != original_username:
        target.username = sanitized_username
        log.warning(
            "Username sanitization: Changed username from '%s' to '%s'",
            original_username,
            sanitized_username
        )
```

## Example 6: Domain Prefix Handling

Special handling for domain-prefixed usernames:

```python
@event.listens_for(User, 'before_insert')
def sanitize_username_before_insert(mapper, connection, target):
    original_username = target.username
    
    # Check if username has domain prefix (e.g., "DOMAIN/username")
    if '/' in original_username:
        parts = original_username.split('/', 1)
        if len(parts) == 2:
            domain, username = parts
            # Keep domain but use @ separator
            sanitized_username = f"{username}@{domain}"
        else:
            # Fallback to underscore replacement
            sanitized_username = original_username.replace('/', '_')
        
        target.username = sanitized_username
        log.warning(
            "Username sanitization: Changed username from '%s' to '%s'",
            original_username,
            sanitized_username
        )
```

## Example 7: Whitelist Approach

Only allow specific characters:

```python
import re

@event.listens_for(User, 'before_insert')
def sanitize_username_before_insert(mapper, connection, target):
    original_username = target.username
    
    # Only allow alphanumeric, underscore, dash, and dot
    sanitized_username = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', original_username)
    
    if sanitized_username != original_username:
        target.username = sanitized_username
        log.warning(
            "Username sanitization: Changed username from '%s' to '%s'",
            original_username,
            sanitized_username
        )
```

## Example 8: Store Original Username in Metadata

Keep track of original username for reference:

```python
@event.listens_for(User, 'before_insert')
def sanitize_username_before_insert(mapper, connection, target):
    original_username = target.username
    
    if '/' in original_username:
        sanitized_username = original_username.replace('/', '_')
        target.username = sanitized_username
        
        # Store original in first_name or a custom field if available
        # Note: This is just an example - you might want to use a custom field
        if not target.first_name or target.first_name == "":
            target.first_name = f"Original: {original_username}"
        
        log.warning(
            "Username sanitization: Changed username from '%s' to '%s'",
            original_username,
            sanitized_username
        )
```

## Example 9: Email-Based Username Generation

Generate username from email if it contains slashes:

```python
@event.listens_for(User, 'before_insert')
def sanitize_username_before_insert(mapper, connection, target):
    original_username = target.username
    
    if '/' in original_username:
        # Use email prefix as username
        if target.email and '@' in target.email:
            email_prefix = target.email.split('@')[0]
            sanitized_username = email_prefix.replace('.', '_')
        else:
            # Fallback to underscore replacement
            sanitized_username = original_username.replace('/', '_')
        
        target.username = sanitized_username
        log.warning(
            "Username sanitization: Changed username from '%s' to '%s' (derived from email)",
            original_username,
            sanitized_username
        )
```

## Example 10: Conditional Sanitization Based on Role

Different rules for different roles:

```python
@event.listens_for(User, 'before_insert')
def sanitize_username_before_insert(mapper, connection, target):
    original_username = target.username
    
    if '/' in original_username:
        # Check if user has admin role
        is_admin = any(role.name == 'Admin' for role in target.roles)
        
        if is_admin:
            # For admins, use @ separator
            sanitized_username = original_username.replace('/', '@')
        else:
            # For regular users, use underscore
            sanitized_username = original_username.replace('/', '_')
        
        target.username = sanitized_username
        log.warning(
            "Username sanitization: Changed username from '%s' to '%s' (role-based)",
            original_username,
            sanitized_username
        )
```

## Example 11: Notification on Sanitization

Send notification when username is sanitized:

```python
@event.listens_for(User, 'before_insert')
def sanitize_username_before_insert(mapper, connection, target):
    original_username = target.username
    
    if '/' in original_username:
        sanitized_username = original_username.replace('/', '_')
        target.username = sanitized_username
        
        log.warning(
            "Username sanitization: Changed username from '%s' to '%s'",
            original_username,
            sanitized_username
        )
        
        # Send notification (example - implement your notification logic)
        try:
            send_notification(
                email=target.email,
                subject="Username Modified",
                message=f"Your username was changed from '{original_username}' "
                        f"to '{sanitized_username}' due to system requirements."
            )
        except Exception as e:
            log.error("Failed to send notification: %s", e)
```

## Example 12: Audit Log Entry

Create detailed audit log:

```python
@event.listens_for(User, 'after_insert')
def log_user_creation_audit(mapper, connection, target):
    """Create detailed audit log entry."""
    from datetime import datetime
    
    audit_entry = {
        'timestamp': datetime.utcnow().isoformat(),
        'action': 'user_created',
        'username': target.username,
        'email': target.email,
        'roles': [role.name for role in target.roles],
        'created_by': target.created_by.username if target.created_by else 'system'
    }
    
    log.info("User creation audit: %s", audit_entry)
    
    # Optionally write to separate audit file
    # with open('/var/log/airflow/user_audit.log', 'a') as f:
    #     f.write(json.dumps(audit_entry) + '\n')
```

## How to Apply Customizations

1. Open `user_creation_interceptor.py`
2. Find the `sanitize_username_before_insert` function
3. Replace it with your customized version
4. Save the file
5. Restart Airflow services
6. Test with: `python test_user_interceptor.py`

## Testing Your Customizations

Always test your customizations:

```bash
# Test with the test script
python test_user_interceptor.py

# Or manually test
airflow users create \
    --username "test/user" \
    --firstname "Test" \
    --lastname "User" \
    --role "Viewer" \
    --email "test@example.com" \
    --password "test123"

# Verify the result
airflow users list | grep test
```

## Best Practices

1. **Always log changes** - Keep audit trail of username modifications
2. **Test thoroughly** - Test with various username patterns
3. **Document your rules** - Comment your sanitization logic
4. **Handle edge cases** - Consider empty strings, very long names, etc.
5. **Be consistent** - Use the same rules across all entry points
6. **Communicate changes** - Notify users when their username is modified
