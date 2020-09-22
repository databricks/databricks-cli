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

# pylint:disable=redefined-outer-name

import re

import mock
import pytest
from click.testing import CliRunner

import databricks_cli.permissions.cli as cli
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
    }
}


def help_test(cli_function, service_function=None, rv=None, args=None):
    """
    This function makes testing the cli functions that just pass data through simpler
    """

    if args is None:
        args = []

    with mock.patch('databricks_cli.permissions.cli.click.echo') as echo_mock:
        if service_function:
            service_function.return_value = rv
        runner = CliRunner()
        runner.invoke(cli_function, args)
        assert echo_mock.call_args[0][0] == pretty_format(rv)


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
        '1234-567890-kens4'
    ], rv=return_value)

    assert perms_api_mock.get_permissions.call_args[0][0] == 'clusters'
    assert perms_api_mock.get_permissions.call_args[0][1] == '1234-567890-kens4'
