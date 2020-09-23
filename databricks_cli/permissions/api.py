# Databricks CLI
# Copyright 2017 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"), except
# that the use of services to which certain application programming
# interfaces (each, an "API") connect requires that the user first obtain
# a license for the use of the APIs from Databricks, Inc. ("Databricks"),
# by creating an account at www.databricks.com and agreeing to either (a)
# the Community Edition Terms of Service, (b) the Databricks Terms of
# Service, or (c) another written agreement between Licensee and Databricks
# for the use of the APIs.
#
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from enum import Enum

from databricks_cli.sdk.permissions_service import PermissionsService
from .exceptions import PermissionsError


class PermissionTargets(Enum):
    clusters = 'clusters'
    cluster = clusters
    directories = 'directories'
    directory = directories
    instance_pools = 'instance-pools'
    instance_pool = instance_pools
    jobs = 'jobs'
    job = jobs
    notebooks = 'notebooks'
    notebook = notebooks
    registered_models = 'registered-models'
    registered_model = registered_models
    model = registered_models
    models = registered_models

    def to_name(self):
        return self[self.value]

    @classmethod
    def values(cls):
        return [e.value for e in PermissionTargets]

    @classmethod
    def help_values(cls):
        return ', '.join([e.value for e in PermissionTargets])

    @classmethod
    def get(cls, item):
        if '-' in item:
            item = item.replace('-', '_')
        return PermissionTargets[item]


class PermissionLevel(Enum):
    manage = 'CAN_MANAGE'
    restart = 'CAN_RESTART'
    attach = 'CAN_ATTACH_TO'
    manage_run = 'CAN_MANAGE_RUN'
    owner = 'IS_OWNER'
    view = 'CAN_VIEW'
    read = 'CAN_READ'
    run = 'CAN_RUN'
    edit = 'CAN_EDIT'

    def to_name(self):
        return self[self.value]

    @classmethod
    def values(cls):
        return [e.value for e in PermissionLevel]

    @classmethod
    def help_values(cls):
        return ', '.join([e.value for e in PermissionLevel])


class PermissionType(Enum):
    user = 'user_name'
    group = 'group_name'
    service = 'service_principal_name'

    def to_name(self):
        return self[self.value]

    @classmethod
    def values(cls):
        return [e.value for e in PermissionType]

    @classmethod
    def help_values(cls):
        return ', '.join([e.value for e in PermissionType])


class PermissionsLookup(object):
    items = {
        'CAN_MANAGE': PermissionLevel.manage,
        'CAN_RESTART': PermissionLevel.restart,
        'CAN_ATTACH_TO': PermissionLevel.attach,
        'CAN_MANAGE_RUN': PermissionLevel.manage_run,
        'IS_OWNER': PermissionLevel.owner,
        'CAN_VIEW': PermissionLevel.view,
        'CAN_READ': PermissionLevel.read,
        'CAN_RUN': PermissionLevel.run,
        'CAN_EDIT': PermissionLevel.edit,
        'user_name': PermissionType.user,
        'group_name': PermissionType.group,
        'service_principal_name': PermissionType.service,

        'clusters': PermissionTargets.clusters,
        'cluster': PermissionTargets.cluster,
        'directories': PermissionTargets.directories,
        'directory': PermissionTargets.directory,
        'instance-pools': PermissionTargets.instance_pools,
        'instance_pools': PermissionTargets.instance_pool,
        'jobs': PermissionTargets.jobs,
        'job': PermissionTargets.job,
        'notebooks': PermissionTargets.notebooks,
        'notebook': PermissionTargets.notebook,
        'registered-models': PermissionTargets.registered_models,
        'registered_models': PermissionTargets.registered_model,
        'model': PermissionTargets.model,
        'models': PermissionTargets.models,

    }

    @classmethod
    def from_name(cls, name):
        rv = cls.items.get(name)
        if not rv:
            raise PermissionsError('Unable to find value for name {}'.format(name))
        return rv


class Permission(object):
    def __init__(self, permission_type, value, permission_level):
        # type: (PermissionType,  str, PermissionLevel) -> None
        self.permission_type = permission_type
        self.permission_level = permission_level
        if value is None:
            value = ''

        self.value = value

    def to_dict(self):
        # type: () -> dict
        if not self.permission_type or not self.permission_level:
            return {}

        return {
            self.permission_type.value: self.value,
            'permission_level': self.permission_level.value
        }


class PermissionsObject(object):
    def __init__(self):
        self.permissions = []

    def add(self, permission):
        # type: (Permission) -> None
        self.permissions.append(permission)

    def user(self, name, level):
        # type: (str, PermissionLevel) -> None
        self.add(Permission(PermissionType.user, value=name, permission_level=level))

    def group(self, name, level):
        # type: (str, PermissionLevel) -> None
        self.add(Permission(PermissionType.group, value=name, permission_level=level))

    def service(self, name, level):
        # type: (str, PermissionLevel) -> None
        self.add(Permission(PermissionType.service, value=name, permission_level=level))

    def to_dict(self):
        # type: () -> dict
        if not self.permissions:
            return {}

        return {
            'access_control_list': [entry.to_dict() for entry in self.permissions]
        }


# FIXME: add set/update permissions, right now this is read only.
class PermissionsApi(object):
    def __init__(self, api_client):
        self.api_client = api_client
        self.client = PermissionsService(api_client)

    def get_permissions(self, object_type, object_id):
        # type: (str, str) -> dict
        if not object_type:
            raise PermissionsError('object_type is invalid')
        if not object_id:
            object_id = ''
            # raise PermissionsError('object_id is invalid')

        return self.client.get_permissions(object_type=PermissionTargets.get(object_type).value,
                                           object_id=object_id)

    def get_possible_permissions(self, object_type, object_id):
        # type: (str, str) -> dict
        if not object_type:
            raise PermissionsError('object_type is invalid')
        if not object_id:
            raise PermissionsError('object_id is invalid')

        return self.client.get_possible_permissions(
            object_type=PermissionTargets.get(object_type).value,
            object_id=object_id)

    def add_permissions(self, object_type, object_id, permissions):
        # type: (str, str, PermissionsObject) -> dict
        if not object_type:
            raise PermissionsError('object_type is invalid')
        if not object_id:
            raise PermissionsError('object_id is invalid')

        return self.client.add_permissions(object_type=PermissionTargets.get(object_type).value,
                                           object_id=object_id, data=permissions.to_dict())

    def update_permissions(self, object_type, object_id, permissions):
        # type: (str, str, PermissionsObject) -> dict
        if not object_type:
            raise PermissionsError('object_type is invalid')
        if not object_id:
            raise PermissionsError('object_id is invalid')

        return self.client.update_permissions(object_type=PermissionTargets.get(object_type).value,
                                              object_id=object_id, data=permissions.to_dict())
