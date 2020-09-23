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

# pylint: disable=redefined-outer-name

import re

import databricks_cli.permissions.cli as cli
import mock
import pytest
from click.testing import CliRunner
from databricks_cli.permissions.api import PermissionTargets
from databricks_cli.utils import pretty_format
from tests.test_data import TEST_CLUSTER_ID
from tests.utils import provide_conf


def strip_margin(text):
    # type: (str) -> str
    return re.sub('\n[ \t]*\\|', '\n', text)


PERMISSIONS_RETURNS = {
    'get': {
        'clusters': {
            TEST_CLUSTER_ID: {
                'object_id': '/clusters/{}'.format(TEST_CLUSTER_ID),
                'object_type': 'cluster',
                'access_control_list': [
                    {
                        'group_name': 'admins',
                        'all_permissions': [
                            {
                                'permission_level': 'CAN_MANAGE',
                                'inherited': True,
                                'inherited_from_object': [
                                    '/clusters/'
                                ]
                            }
                        ]
                    }
                ]
            }
        }
    },
    'list-permissions': {
        'clusters': {
            'permission_levels': [
                {
                    'permission_level': 'CAN_MANAGE',
                    'description': 'Can Manage permission on cluster'
                },
                {
                    'permission_level': 'CAN_RESTART',
                    'description': 'Can Restart permission on cluster'
                },
                {
                    'permission_level': 'CAN_ATTACH_TO',
                    'description': 'Can Attach To permission on cluster'
                }
            ]
        },
        'directories': {
            'permission_levels': [
                {
                    'permission_level': 'CAN_READ',
                    'description': 'Can view and comment on notebooks in the directory'
                },
                {
                    'permission_level': 'CAN_RUN',
                    'description': 'Can view, comment, attach/detach, and run commands in ' +
                                   'notebooks in the directory'
                },
                {
                    'permission_level': 'CAN_EDIT',
                    'description': 'Can view, comment, attach/detach, run commands, and edit ' +
                                   'notebooks in the directory'
                },
                {
                    'permission_level': 'CAN_MANAGE',
                    'description': 'Can view, comment, attach/detach, run commands, and edit ' +
                                   'notebooks in the folder, and can create, delete, and change ' +
                                   'permissions of items in the directory'
                }
            ]
        },
        'instance-pools': {
            'permission_levels': [
                {
                    "permission_level": "CAN_MANAGE",
                    "description": "Can Manage permission on a pool"
                },
                {
                    "permission_level": "CAN_ATTACH_TO",
                    "description": "Can Attach To permission on a pool"
                }
            ]
        },
        'jobs': {
            'permission_levels': [
                {
                    'permission_level': 'IS_OWNER',
                    'description': 'Is Owner permission on a job'
                },
                {
                    'permission_level': 'CAN_MANAGE_RUN',
                    'description': 'Can Manage Run permission to trigger or cancel job runs'
                },
                {
                    'permission_level': 'CAN_VIEW',
                    'description': 'Can View permission to view job run results'
                }
            ]
        },
        'notebooks': {
            'permission_levels': [
                {
                    'permission_level': 'CAN_READ',
                    'description': 'Can view and comment on the notebook'
                },
                {
                    'permission_level': 'CAN_RUN',
                    'description': 'Can view, comment, attach/detach, and run commands in the' +
                                   'notebook'
                },
                {
                    'permission_level': 'CAN_EDIT',
                    'description': 'Can view, comment, attach/detach, run commands, and edit ' +
                                   'the notebook'
                },
                {
                    'permission_level': 'CAN_MANAGE',
                    'description': 'Can view, comment, attach/detach, run commands, edit, and ' +
                                   'change permissions of the notebook'
                }
            ]
        },
        'registered-models': {
            'permission_levels': [
                {
                    'permission_level': 'CAN_READ',
                    'description': 'Can view the details of the registered model and its model ' +
                                   'versions, and use the model versions.'
                },
                {
                    'permission_level': 'CAN_EDIT',
                    'description': 'Can view and edit the details of a registered model and ' +
                                   'its model versions (except stage changes), and add ' +
                                   'new model versions.'
                },
                {
                    'permission_level': 'CAN_MANAGE_STAGING_VERSIONS',
                    'description': 'Can view and edit the details of a registered model and its ' +
                                   'model versions, add new model versions, and manage stage ' +
                                   'transitions between non-Production stages.'
                },
                {
                    'permission_level': 'CAN_MANAGE_PRODUCTION_VERSIONS',
                    'description': 'Can view and edit the details of a registered model and its ' +
                                   'model versions, add new model versions, and manage stage ' +
                                   'transitions between any stages.'
                },
                {
                    'permission_level': 'CAN_MANAGE',
                    'description': 'Can manage permissions on, view all details of, and perform ' +
                                   'all actions on the registered model and its model versions.'
                }
            ]
        }
    },
    'add': {
        'clusters': {
            'add-manage-group-name': {
                'object_id': TEST_CLUSTER_ID,
                'object_type': 'cluster',
                'access_control_list': [
                    {
                        'group_name': 'admins',
                        'all_permissions': [
                            {
                                'permission_level': 'CAN_MANAGE',
                                'inherited': True,
                                'inherited_from_object': [
                                    '/clusters/'
                                ]
                            }
                        ]
                    },
                    {
                        'group_name': 'sam',
                        'all_permissions': [
                            {
                                'permission_level': 'CAN_MANAGE',
                                'inherited': False
                            }
                        ]
                    }
                ]
            },
            'add-manage-user-name': {
                'object_id': TEST_CLUSTER_ID,
                'object_type': 'cluster',
                'access_control_list': [
                    {
                        'group_name': 'admins',
                        'all_permissions': [
                            {
                                'permission_level': 'CAN_MANAGE',
                                'inherited': True,
                                'inherited_from_object': [
                                    '/clusters/'
                                ]
                            }
                        ]
                    },
                    {
                        'user_name': 'sam',
                        'all_permissions': [
                            {
                                'permission_level': 'CAN_MANAGE',
                                'inherited': False
                            }
                        ]
                    }
                ]
            }
        }
    }
}


@pytest.fixture()
def permissions_sdk_mock():
    with mock.patch('databricks_cli.permissions.api.PermissionsService') as SdkMock:
        _permissions_sdk_mock = mock.MagicMock()
        SdkMock.return_value = _permissions_sdk_mock
        # _permissions_sdk_mock.get_cluster = mock.MagicMock(return_value={})

        yield _permissions_sdk_mock


def help_test(cli_function, service_function=None, rv=None, args=None, format_result=True):
    """
    This function makes testing the cli functions that just pass data through simpler
    """

    if args is None:
        args = []

    with mock.patch('databricks_cli.permissions.cli.click.echo') as echo_mock:
        if service_function:
            service_function.return_value = rv
        runner = CliRunner()
        result = runner.invoke(cli_function, args)
        if result.exit_code != 0:
            print(result.output)
        if format_result:
            expected = pretty_format(rv)
        else:
            expected = rv
        assert echo_mock.call_args[0][0] == expected


@pytest.fixture()
def perms_api_mock():
    with mock.patch('databricks_cli.permissions.cli.PermissionsApi') as PermissionsApiMock:
        _perms_api_mock = mock.MagicMock()
        PermissionsApiMock.return_value = _perms_api_mock
        yield _perms_api_mock


@provide_conf
def test_get_cli(perms_api_mock):
    return_value = PERMISSIONS_RETURNS['get']['clusters']
    perms_api_mock.get_permissions.return_value = return_value
    help_test(cli.get_cli, args=[
        '--object-type',
        'clusters',
        '--object-id',
        TEST_CLUSTER_ID
    ], rv=return_value)

    assert perms_api_mock.get_permissions.call_args[0][0] == 'clusters'
    assert perms_api_mock.get_permissions.call_args[0][1] == TEST_CLUSTER_ID


@provide_conf
def test_list_permissions_types_cli(permissions_sdk_mock):
    for perm_type in PermissionTargets.values():
        return_value = PERMISSIONS_RETURNS['list-permissions'][perm_type]
        permissions_sdk_mock.get_possible_permissions.return_value = return_value
        help_test(cli.list_permissions_types_cli, args=[
            '--object-type',
            perm_type,
            '--object-id',
            TEST_CLUSTER_ID
        ], rv=return_value)


@provide_conf
def test_add_cluster_manage(permissions_sdk_mock):
    perm_type = 'clusters'

    for add_type in ['user-name', 'group-name']:
        return_value = PERMISSIONS_RETURNS['add'][perm_type]['add-manage-{}'.format(add_type)]
        permissions_sdk_mock.add_permissions.return_value = return_value
        help_test(cli.add_cli, args=[
            '--object-type',
            perm_type,
            '--object-id',
            TEST_CLUSTER_ID,
            '--{}'.format(add_type),
            'sam',
            '--permission-level',
            'manage'
        ], rv=return_value)


@provide_conf
def test_list_permissions_targets_cli():
    help_test(cli.list_permissions_targets_cli, args=None, rv=cli.POSSIBLE_OBJECT_TYPES,
              format_result=False)


@provide_conf
def test_list_permissions_level_cli():
    help_test(cli.list_permissions_level_cli, args=None, rv=cli.POSSIBLE_PERMISSION_LEVELS,
              format_result=False)


@pytest.fixture()
def workspace_api_mock():
    with mock.patch('databricks_cli.permissions.cli.WorkspaceApi') as WorkspaceApiMock:
        workspace_api_mock = mock.MagicMock()
        WorkspaceApiMock.return_value = workspace_api_mock
        yield workspace_api_mock


@provide_conf
def test_directory_cli_missing_path(permissions_sdk_mock, workspace_api_mock):
    workspace_api_mock.get_id_for_directory.return_value = None
    help_test(cli.directory_cli,
              args=[
                  '--path',
                  '/workspace/missing.txt'
              ],
              rv='Failed to find id for /workspace/missing.txt',
              format_result=False)
