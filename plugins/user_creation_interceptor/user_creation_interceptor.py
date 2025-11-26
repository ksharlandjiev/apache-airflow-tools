# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
User Creation Interceptor Plugin.

This plugin intercepts user creation in Airflow and handles usernames containing
forward slashes by replacing them with underscores. This is necessary because
Flask/Connexion routing treats '/' as a path separator in API endpoints like
/api/v1/users/{username}, making it impossible to access users with slashes in
their usernames via the REST API.
"""
from __future__ import annotations

import logging

from sqlalchemy import event

from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)


# Register event listeners at module load time
try:
    from airflow.providers.fab.auth_manager.models import User
    
    @event.listens_for(User, 'before_insert')
    def sanitize_username_before_insert(mapper, connection, target):
        """
        Sanitize username before inserting into database.
        
        Replaces forward slashes with underscores to ensure the username
        can be used in REST API endpoints.
        
        Args:
            mapper: The mapper which is the target of this event
            connection: The database connection being used
            target: The User instance being inserted
        """
        original_username = target.username
        
        if '/' in original_username:
            # Replace forward slashes with underscores
            sanitized_username = original_username.replace('/', '_')
            target.username = sanitized_username
            
            log.warning(
                "Username sanitization: Changed username from '%s' to '%s' "
                "because forward slashes are not supported in API endpoints.",
                original_username,
                sanitized_username
            )
    
    @event.listens_for(User, 'after_insert')
    def log_user_creation(mapper, connection, target):
        """
        Log user creation after successful insert.
        
        Args:
            mapper: The mapper which is the target of this event
            connection: The database connection being used
            target: The User instance that was inserted
        """
        log.info(
            "User created successfully: username='%s', email='%s'",
            target.username,
            target.email
        )
    
    log.info("User Creation Interceptor: Event listeners registered successfully")
    
except ImportError as e:
    log.error(
        "Failed to register User Creation Interceptor event listeners: %s. "
        "Make sure you're using FAB auth manager.",
        e
    )
except Exception as e:
    log.error(
        "Unexpected error registering User Creation Interceptor event listeners: %s",
        e,
        exc_info=True
    )


class UserCreationInterceptorPlugin(AirflowPlugin):
    """
    Plugin to intercept and sanitize usernames during user creation.
    
    This plugin registers SQLAlchemy event listeners on the User model to:
    1. Detect usernames containing forward slashes
    2. Replace forward slashes with underscores
    3. Log the transformation for audit purposes
    
    Note: Event listeners are registered at module load time (above),
    not in on_load(), to ensure they're active before any user operations.
    """

    name = "user_creation_interceptor"
