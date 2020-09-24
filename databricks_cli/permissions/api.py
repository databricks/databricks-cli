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
    no_permissions = 'NONE'
    manage = 'CAN_MANAGE'
    manage_staging_versions = 'CAN_MANAGE_STAGING_VERSIONS'
    manage_production_versions = 'CAN_MANAGE_PRODUCTION_VERSIONS'
    restart = 'CAN_RESTART'
    attach = 'CAN_ATTACH_TO'
    manage_run = 'CAN_MANAGE_RUN'
    owner = 'IS_OWNER'
    view = 'CAN_VIEW'
    read = 'CAN_READ'
    run = 'CAN_RUN'
    edit = 'CAN_EDIT'
    use = 'CAN_USE'

    @classmethod
    def names(cls):
        return [e.name for e in PermissionLevel]

    @classmethod
    def values(cls):
        return [e.value for e in PermissionLevel]

    @classmethod
    def help_values(cls):
        return ', '.join([e.value for e in PermissionLevel])


class BasicPermissions(object):
    def __init__(self, object_type, valid_permissions):
        self.object_type = object_type
        self.valid_permissions = valid_permissions

    def is_valid_target(self, permission):
        # type: (str) -> bool
        return permission in self.valid_permissions

    def valid_targets(self):
        return [s.name for s in self.valid_permissions]


class TokenPermissions(BasicPermissions):
    def __init__(self):
        super().__init__(PermissionTargets.token, {
            PermissionLevel.no_permissions,
            PermissionLevel.use,
            PermissionLevel.manage,
        })


class PasswordPermissions(BasicPermissions):
    def __init__(self):
        super().__init__(PermissionTargets.password, {
            PermissionLevel.no_permissions,
            PermissionLevel.use,
        })


class ClusterPermissions(BasicPermissions):
    def __init__(self):
        super().__init__(PermissionTargets.clusters, {
            PermissionLevel.no_permissions,
            PermissionLevel.attach,
            PermissionLevel.restart,
            PermissionLevel.manage,
        })


class InstancePoolPermissions(BasicPermissions):
    def __init__(self):
        super().__init__(PermissionTargets.instance_pools, {
            PermissionLevel.no_permissions,
            PermissionLevel.attach,
            PermissionLevel.manage,
        })


class JobPermissions(BasicPermissions):
    def __init__(self):
        super().__init__(PermissionTargets.jobs, {
            PermissionLevel.no_permissions,
            PermissionLevel.view,
            PermissionLevel.manage_run,
            PermissionLevel.owner,
            PermissionLevel.manage,
        })


class NotebookPermissions(BasicPermissions):
    def __init__(self):
        super().__init__(PermissionTargets.notebook, {
            PermissionLevel.no_permissions,
            PermissionLevel.read,
            PermissionLevel.run,
            PermissionLevel.edit,
            PermissionLevel.manage,
        })


class DirectoryPermissions(BasicPermissions):
    def __init__(self):
        super().__init__(PermissionTargets.directory, {
            PermissionLevel.no_permissions,
            PermissionLevel.read,
            PermissionLevel.run,
            PermissionLevel.edit,
            PermissionLevel.manage,
        })


class MlFlowPermissions(BasicPermissions):
    def __init__(self):
        super().__init__(PermissionTargets.models, {
            PermissionLevel.no_permissions,
            PermissionLevel.read,
            PermissionLevel.edit,
            PermissionLevel.manage_staging_versions,
            PermissionLevel.manage_production_versions,
            PermissionLevel.manage,
        })


class PermissionType(Enum):
    user = 'user_name'
    group = 'group_name'
    service = 'service_principal_name'

    @classmethod
    def values(cls):
        return [e.value for e in PermissionType]


class PermissionsLookup(object):
    """
    static lookup table for permissions
    """

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

        'clusters': ClusterPermissions(),
        'cluster': ClusterPermissions(),
        'directories': DirectoryPermissions(),
        'directory': DirectoryPermissions(),
        'instance-pools': InstancePoolPermissions(),
        'instance_pools': InstancePoolPermissions(),
        'jobs': JobPermissions(),
        'job': JobPermissions(),
        'notebooks': NotebookPermissions(),
        'notebook': NotebookPermissions(),
        'registered-models': MlFlowPermissions(),
        'registered_models': MlFlowPermissions(),
        'model': MlFlowPermissions(),
        'models': MlFlowPermissions(),
    }


class Permission(object):
    def __init__(self, object_type, permission_type, permission_level, permission_value):
        # type: (str, PermissionType,  str, str) -> None
        self.validator = PermissionsLookup.items[object_type]

        if not self.validator.is_valid_target(permission_level):
            raise PermissionsError(
                '{} is not a valid target for {}\n'.format(permission_level,
                                                           self.validator.object_type) +

                'Valid values are {}'.format(self.validator.valid_targets()))

        self.permission_type = permission_type
        self.permission_level = PermissionLevel[permission_level]
        self.value = permission_value

    def to_dict(self):
        # type: () -> dict
        if not self.permission_type or not self.permission_level:
            return {}

        return {
            self.permission_type.value: self.value,
            'permission_level': self.permission_level.value
        }


class PermissionsObject(object):
    def __init__(self, permissions=None):
        if not permissions:
            permissions = []
        self.permissions = permissions

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

    def check_if_valid_for(self, object_type):
        """
        Check if the permissions are valid for this object type.
        """
        pass


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
