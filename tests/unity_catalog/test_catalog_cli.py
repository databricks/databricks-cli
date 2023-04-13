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

import mock
import pytest
from click.testing import CliRunner
from databricks_cli.unity_catalog.utils import mc_pretty_format

from databricks_cli.unity_catalog import catalog_cli
from tests.utils import provide_conf

CATALOG_NAME = 'test_catalog_name'

WORKSPACE_BINDINGS = {
    "workspaces": [6051921418418893]
}

EMPTY_WORKSPACE_BINDINGS = {}

@pytest.fixture()
def api_mock():
    with mock.patch(
            'databricks_cli.unity_catalog.catalog_cli.UnityCatalogApi') as uc_api_mock:
        _cred_api_mock = mock.MagicMock()
        uc_api_mock.return_value = _cred_api_mock
        yield _cred_api_mock


@pytest.fixture()
def echo_mock():
    with mock.patch('databricks_cli.unity_catalog.catalog_cli.click.echo') as echo_mock:
        yield echo_mock


@provide_conf
def test_get_catalog_bindings_cli(api_mock, echo_mock):
    api_mock.get_catalog_bindings.return_value = WORKSPACE_BINDINGS
    runner = CliRunner()
    runner.invoke(
        catalog_cli.get_catalog_bindings_cli,
        args=['--name', CATALOG_NAME])
    api_mock.get_catalog_bindings.assert_called_once()
    echo_mock.assert_called_once_with(mc_pretty_format(WORKSPACE_BINDINGS))

@provide_conf
def test_update_catalog_bindings_cli_with_json(api_mock, echo_mock):
    api_mock.update_catalog_bindings.return_value = EMPTY_WORKSPACE_BINDINGS
    runner = CliRunner()
    runner.invoke(
        catalog_cli.update_catalog_bindings_cli,
        args=[
            '--name', CATALOG_NAME,
            '--json', '{ "unassign_workspaces": [6051921418418893] }'
        ])
    api_mock.update_catalog_bindings.assert_called_once_with(
        CATALOG_NAME,
        {
            'unassign_workspaces': [6051921418418893]
        })
    echo_mock.assert_called_once_with(mc_pretty_format(EMPTY_WORKSPACE_BINDINGS))
