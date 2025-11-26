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
Username API Extension Plugin.

This plugin provides alternative API endpoints that accept usernames with
forward slashes via query parameters instead of path parameters. This works
around Flask/Connexion's limitation where path parameters cannot contain slashes.

Endpoints:
- GET  /api/v1/users-ext/?username=domain/username
- PATCH /api/v1/users-ext/?username=domain/username
- DELETE /api/v1/users-ext/?username=domain/username
"""
from __future__ import annotations

import logging
from http import HTTPStatus
from typing import TYPE_CHECKING

from flask import Blueprint, jsonify, request
from marshmallow import ValidationError
from werkzeug.security import generate_password_hash

from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound
from airflow.api_connexion.schemas.user_schema import user_collection_item_schema, user_schema
from airflow.api_connexion.security import requires_access_custom_view
from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from flask import Response

log = logging.getLogger(__name__)

# Create Blueprint for extended user API
bp = Blueprint('username_api_ext', __name__, url_prefix='/api/v1/users-ext')


@bp.route('/', methods=['GET'])
@requires_access_custom_view("GET", permissions.RESOURCE_USER)
def get_user() -> Response:
    """
    Get a user by username (query parameter).
    
    This endpoint accepts usernames with forward slashes via query parameter.
    
    Query Parameters:
        username (str): The username to retrieve
    
    Returns:
        JSON response with user details
        
    Example:
        GET /api/v1/users-ext/?username=domain/username
    """
    username = request.args.get('username')
    
    if not username:
        return jsonify({
            'detail': 'Missing required query parameter: username',
            'status': 400,
            'title': 'Bad Request',
            'type': 'about:blank'
        }), 400
    
    try:
        security_manager = get_auth_manager().security_manager
        user = security_manager.find_user(username=username)
        
        if not user:
            return jsonify({
                'detail': f"The User with username `{username}` was not found",
                'status': 404,
                'title': 'User not found',
                'type': 'about:blank'
            }), 404
        
        return jsonify(user_collection_item_schema.dump(user)), 200
        
    except Exception as e:
        log.error("Error retrieving user %s: %s", username, e, exc_info=True)
        return jsonify({
            'detail': str(e),
            'status': 500,
            'title': 'Internal Server Error',
            'type': 'about:blank'
        }), 500


@bp.route('/', methods=['PATCH'])
@requires_access_custom_view("PUT", permissions.RESOURCE_USER)
def patch_user() -> Response:
    """
    Update a user by username (query parameter).
    
    This endpoint accepts usernames with forward slashes via query parameter.
    
    Query Parameters:
        username (str): The username to update
        update_mask (str, optional): Comma-separated list of fields to update
    
    Request Body:
        JSON object with user fields to update
        
    Returns:
        JSON response with updated user details
        
    Example:
        PATCH /api/v1/users-ext/?username=domain/username
        Body: {"email": "newemail@example.com"}
    """
    username = request.args.get('username')
    
    if not username:
        return jsonify({
            'detail': 'Missing required query parameter: username',
            'status': 400,
            'title': 'Bad Request',
            'type': 'about:blank'
        }), 400
    
    try:
        # Use partial=True to allow partial updates (only validate provided fields)
        data = user_schema.load(request.json, partial=True)
    except ValidationError as e:
        return jsonify({
            'detail': str(e.messages),
            'status': 400,
            'title': 'Bad Request',
            'type': 'about:blank'
        }), 400
    
    try:
        security_manager = get_auth_manager().security_manager
        user = security_manager.find_user(username=username)
        
        if user is None:
            return jsonify({
                'detail': f"The User with username `{username}` was not found",
                'status': 404,
                'title': 'User not found',
                'type': 'about:blank'
            }), 404
        
        # Check unique username if changing
        new_username = data.get('username')
        if new_username and new_username != username:
            if security_manager.find_user(username=new_username):
                return jsonify({
                    'detail': f"The username `{new_username}` already exists",
                    'status': 409,
                    'title': 'Already Exists',
                    'type': 'about:blank'
                }), 409
        
        # Check unique email if changing
        email = data.get('email')
        if email and email != user.email:
            if security_manager.find_user(email=email):
                return jsonify({
                    'detail': f"The email `{email}` already exists",
                    'status': 409,
                    'title': 'Already Exists',
                    'type': 'about:blank'
                }), 409
        
        # Handle update_mask if provided
        update_mask = request.args.get('update_mask')
        if update_mask:
            update_fields = [field.strip() for field in update_mask.split(',')]
            masked_data = {}
            missing_fields = []
            
            for field in update_fields:
                if field in data:
                    masked_data[field] = data[field]
                else:
                    missing_fields.append(field)
            
            if missing_fields:
                return jsonify({
                    'detail': f"Unknown update masks: {', '.join(repr(f) for f in missing_fields)}",
                    'status': 400,
                    'title': 'Bad Request',
                    'type': 'about:blank'
                }), 400
            
            data = masked_data
        
        # Handle roles if provided
        roles_to_update = None
        if 'roles' in data:
            roles_to_update = []
            missing_role_names = []
            
            for role_data in data.pop('roles', ()):
                role_name = role_data['name']
                role = security_manager.find_role(role_name)
                if role is None:
                    missing_role_names.append(role_name)
                else:
                    roles_to_update.append(role)
            
            if missing_role_names:
                return jsonify({
                    'detail': f"Unknown roles: {', '.join(repr(n) for n in missing_role_names)}",
                    'status': 400,
                    'title': 'Bad Request',
                    'type': 'about:blank'
                }), 400
        
        # Update password if provided
        if 'password' in data:
            user.password = generate_password_hash(data.pop('password'))
        
        # Update roles if provided
        if roles_to_update is not None:
            user.roles = roles_to_update
        
        # Update other fields
        for key, value in data.items():
            setattr(user, key, value)
        
        security_manager.update_user(user)
        
        return jsonify(user_schema.dump(user)), 200
        
    except Exception as e:
        log.error("Error updating user %s: %s", username, e, exc_info=True)
        return jsonify({
            'detail': str(e),
            'status': 500,
            'title': 'Internal Server Error',
            'type': 'about:blank'
        }), 500


@bp.route('/', methods=['DELETE'])
@requires_access_custom_view("DELETE", permissions.RESOURCE_USER)
def delete_user() -> Response:
    """
    Delete a user by username (query parameter).
    
    This endpoint accepts usernames with forward slashes via query parameter.
    
    Query Parameters:
        username (str): The username to delete
    
    Returns:
        204 No Content on success
        
    Example:
        DELETE /api/v1/users-ext/?username=domain/username
    """
    username = request.args.get('username')
    
    if not username:
        return jsonify({
            'detail': 'Missing required query parameter: username',
            'status': 400,
            'title': 'Bad Request',
            'type': 'about:blank'
        }), 400
    
    try:
        security_manager = get_auth_manager().security_manager
        user = security_manager.find_user(username=username)
        
        if user is None:
            return jsonify({
                'detail': f"The User with username `{username}` was not found",
                'status': 404,
                'title': 'User not found',
                'type': 'about:blank'
            }), 404
        
        # Clear foreign keys first
        user.roles = []
        security_manager.get_session.delete(user)
        security_manager.get_session.commit()
        
        return '', HTTPStatus.NO_CONTENT
        
    except Exception as e:
        log.error("Error deleting user %s: %s", username, e, exc_info=True)
        return jsonify({
            'detail': str(e),
            'status': 500,
            'title': 'Internal Server Error',
            'type': 'about:blank'
        }), 500


class UsernameApiExtensionPlugin(AirflowPlugin):
    """
    Plugin that adds extended user API endpoints.
    
    Provides alternative endpoints that accept usernames via query parameters,
    allowing usernames with forward slashes to be used.
    """
    
    name = "username_api_extension"
    flask_blueprints = [bp]


# Log when plugin is loaded
log.info("Username API Extension Plugin loaded - endpoints available at /api/v1/users-ext/")
