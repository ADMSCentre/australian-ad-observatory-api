from datetime import datetime

from pydantic import ValidationError
from routes import route
from middlewares.authorise import Role, authorise
from middlewares.authenticate import authenticate
from utils import use, jwt, Response
import hashlib
import boto3
import json
import utils.metadata_sub_bucket as metadata
from models.project import Project, ProjectMemberRole, TeamMember, Cell

from config import config

session = boto3.Session(region_name=config.aws.region)

PROJECTS_FOLDER_PREFIX = 'projects'

def get_project_member(project, username):
    for member in project.get('team', []):
        if member.get('username') == username:
            return member
    return None

# Must be used after authenticate middleware
# Requires a project_id in the path parameters
def authorise_member(*roles: list[ProjectMemberRole]):
    def decorator(func):
        def wrapper(event, response, context):
            project_id = event['pathParameters']['project_id']
            project_data = metadata.get_object(f"{PROJECTS_FOLDER_PREFIX}/{project_id}.json")
            if not project_data:
                return response.status(404).json({'message': 'Project not found'})
            try:
                project = json.loads(project_data)
            except Exception as e:
                return response.status(404).json({'message': f'Failed to parse project: {e}'})
            
            member = get_project_member(project, event['identity'].provider_user_id)
            if not member or member.get("role") not in [role.value for role in roles]:
                return response.status(403).json({'message': 'You do not have permission to perform this action'})
            return func(event, response, context)
        return wrapper
    return decorator


def get_all_projects():
    projects = {}
    for project_id in metadata.list_objects(PROJECTS_FOLDER_PREFIX):
        project_data = metadata.get_object(f"{PROJECTS_FOLDER_PREFIX}/{project_id}")
        project = json.loads(project_data)
        projects[project_id] = project
    return projects

@route('/projects', 'POST')
@use(authenticate)
def create_project(event, response, context):
    """Create a new project.
    ---
    tags:
      - projects
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            properties:
              name:
                type: string
              description:
                type: string
    responses:
      201:
        description: Project created successfully
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Project'
    """
    data = event['body']
    project_id = hashlib.sha256(f"{data['name']}{datetime.now()}".encode()).hexdigest()
    username = event['identity'].provider_user_id
    project = Project(
        id=project_id,
        name=data['name'],
        description=data['description'],
        ownerId=username,
        team=[{
                'username': username,
                'role': 'admin'
            }],
        cells=[]
    )
    metadata.put_object(f"{PROJECTS_FOLDER_PREFIX}/{project_id}.json", json.dumps(project.model_dump()))
    response.status(201)
    return project.model_dump()

@route('/projects', 'GET')
@use(authenticate)
def list_projects(event, response, context):
    """List all projects where the user is a part of (either owner, or if the user is an admin, all projects).
    ---
    tags:
      - projects
    responses:
      200:
        description: List of projects
        content:
          application/json:
            schema:
              type: array
              items:
                $ref: '#/components/schemas/Project'
    """
    user = event['user']
    username = event['identity'].provider_user_id
    user_projects = []
    for project_id in metadata.list_objects(PROJECTS_FOLDER_PREFIX):
        print(project_id)
        project_data = metadata.get_object(project_id)
        project = json.loads(project_data)
        if project['ownerId'] == username or user.role == 'admin' or any(member['username'] == username for member in project['team']):
            user_projects.append(project)
    return user_projects

@route('/projects/{project_id}', 'GET')
@use(authenticate)
@authorise_member(ProjectMemberRole.ADMIN, ProjectMemberRole.EDITOR, ProjectMemberRole.VIEWER)
def get_project(event, response, context):
    """Get a project by ID.
    ---
    tags:
      - projects
    responses:
      200:
        description: Project retrieved successfully
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Project'
      404:
        description: Project not found
    """
    project_id = event['pathParameters']['project_id']
    project_data = metadata.get_object(f"{PROJECTS_FOLDER_PREFIX}/{project_id}.json")
    if project_data:
        project = json.loads(project_data)
        return project
    else:
        response.status(404).json({'message': 'Project not found'})

@route('/projects/{project_id}', 'PUT')
@use(authenticate)
@authorise_member(ProjectMemberRole.ADMIN, ProjectMemberRole.EDITOR)
def update_project(event, response, context):
    """Update a project by ID.
    ---
    tags:
      - projects
    requestBody:
      required: true
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/Project'
    responses:
      200:
        description: Project updated successfully
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Project'
      404:
        description: Project not found
    """
    project_id = event['pathParameters']['project_id']
    data = event['body']
    try:
        project = Project(**data)
        metadata.update_object(f"{PROJECTS_FOLDER_PREFIX}/{project_id}.json", json.dumps(project.model_dump()))
        return project.model_dump()
    except:
        response.status(404).json({'message': 'Project not found'})
    # if project_id in get_all_projects():
    #     project = Project(**data)
    #     metadata.update_object(f"{PROJECTS_FOLDER_PREFIX}/{project_id}.json", json.dumps(project.__dict__))
    #     return project.__dict__
    # else:
    #     response.status(404).json({'message': 'Project not found'})

@route('/projects/{project_id}', 'DELETE')
@use(authenticate)
@authorise_member(ProjectMemberRole.ADMIN)
def delete_project(event, response, context):
    """Delete a project by ID.
    ---
    tags:
      - projects
    responses:
      204:
        description: Project deleted successfully
      404:
        description: Project not found
    """
    project_id = event['pathParameters']['project_id']
    try:
        metadata.delete_object(f"{PROJECTS_FOLDER_PREFIX}/{project_id}.json")
        return response.status(204).json({
            'success': True
        })
    except:
        response.status(404).json({'message': 'Project not found'})