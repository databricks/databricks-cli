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

from databricks_cli.unity_catalog import metastore_cli
from tests.utils import provide_conf

METASTORE_NAME = 'test_metastore'
METASTORE_ID = 'test_metastore_id'
STORAGE_ROOT = 's3://some-root'
REGION = 'us-west-2'
METASTORES = {
    'metastores': [
        {
            'id': METASTORE_ID
        }
    ]
}
METASTORE = {
    'name': METASTORE_NAME,
    'storage_root': STORAGE_ROOT,
    'region': REGION
}

WORKSPACE_ID = 12345
DEFAULT_CATALOG_NAME = 'catalog_name'
METASTORE_ASSIGNMENT = {
    'workspace_id': WORKSPACE_ID,
    'metastore_id': METASTORE_ID,
    'default_catalog_name': DEFAULT_CATALOG_NAME
}


@pytest.fixture()
def api_mock():
    with mock.patch(
            'databricks_cli.unity_catalog.metastore_cli.UnityCatalogApi') as uc_api_mock:
        _metastore_api_mock = mock.MagicMock()
        uc_api_mock.return_value = _metastore_api_mock
        yield _metastore_api_mock


@pytest.fixture()
def echo_mock():
    with mock.patch('databricks_cli.unity_catalog.metastore_cli.click.echo') as echo_mock:
        yield echo_mock


@provide_conf
def test_create_metastore_cli(api_mock, echo_mock):
    api_mock.create_metastore.return_value = METASTORE
    runner = CliRunner()
    runner.invoke(
        metastore_cli.create_metastore_cli,
        args=[
            '--name', METASTORE_NAME,
            '--storage-root', STORAGE_ROOT,
            '--region', REGION
        ])
    api_mock.create_metastore.assert_called_once_with(
        METASTORE_NAME, STORAGE_ROOT, REGION)
    echo_mock.assert_called_once_with(mc_pretty_format(METASTORE))


@provide_conf
def test_list_metastores_cli(api_mock, echo_mock):
    api_mock.list_metastores.return_value = METASTORES
    runner = CliRunner()
    runner.invoke(metastore_cli.list_metastores_cli)
    api_mock.list_metastores.assert_called_once()
    echo_mock.assert_called_once_with(mc_pretty_format(METASTORES))


@provide_conf
def test_get_metastore_cli(api_mock, echo_mock):
    api_mock.get_metastore.return_value = METASTORE
    runner = CliRunner()
    runner.invoke(
        metastore_cli.get_metastore_cli,
        args=['--id', METASTORE_ID])
    api_mock.get_metastore.assert_called_once_with(METASTORE_ID)
    echo_mock.assert_called_once_with(mc_pretty_format(METASTORE))


@provide_conf
def test_update_metastore_cli(api_mock, echo_mock):
    api_mock.update_metastore.return_value = METASTORE
    runner = CliRunner()
    runner.invoke(
        metastore_cli.update_metastore_cli,
        args=[
            '--id', METASTORE_ID,
            '--new-name', 'new_metastore_name',
            '--storage-root-credential-id', 'new_storage_root_credential_id',
            '--delta-sharing-scope', 'INTERNAL_AND_EXTERNAL',
            '--delta-sharing-recipient-token-lifetime-in-seconds', '123',
            '--delta-sharing-organization-name', 'new_organization_name',
            '--owner', 'owner'
        ])
    expected_data = {
        'name': 'new_metastore_name',
        'storage_root_credential_id': 'new_storage_root_credential_id',
        'delta_sharing_scope': 'INTERNAL_AND_EXTERNAL',
        'delta_sharing_recipient_token_lifetime_in_seconds': 123,
        'delta_sharing_organization_name': 'new_organization_name',
        'owner': 'owner'
    }
    api_mock.update_metastore.assert_called_once_with(METASTORE_ID, expected_data)
    echo_mock.assert_called_once_with(mc_pretty_format(METASTORE))


@provide_conf
def test_update_metastore_cli_with_json(api_mock, echo_mock):
    api_mock.update_metastore.return_value = METASTORE
    runner = CliRunner()
    runner.invoke(
        metastore_cli.update_metastore_cli,
        args=[
            '--id', METASTORE_ID,
            '--json', '{ "name": "new_metastore_name" }'
        ])
    api_mock.update_metastore.assert_called_once_with(
        METASTORE_ID,
        {
            'name': 'new_metastore_name'
        })
    echo_mock.assert_called_once_with(mc_pretty_format(METASTORE))
    

@provide_conf
def test_delete_metastore_cli(api_mock):
    runner = CliRunner()
    runner.invoke(
        metastore_cli.delete_metastore_cli,
        args=[
            '--id', METASTORE_ID,
            '--force'
        ])
    api_mock.delete_metastore.assert_called_once_with(METASTORE_ID, True)


@provide_conf
def test_metastore_summary_cli(api_mock, echo_mock):
    api_mock.get_metastore_summary.return_value = METASTORE
    runner = CliRunner()
    runner.invoke(
        metastore_cli.metastore_summary_cli,
        args=[])
    api_mock.get_metastore_summary.assert_called_once_with()
    echo_mock.assert_called_once_with(mc_pretty_format(METASTORE))


@provide_conf
def test_get_metastore_assignment_cli(api_mock, echo_mock):
    api_mock.get_current_metastore_assignment.return_value = METASTORE_ASSIGNMENT
    runner = CliRunner()
    runner.invoke(
        metastore_cli.get_metastore_assignment_cli,
        args=[])
    api_mock.get_current_metastore_assignment.assert_called_once_with()
    echo_mock.assert_called_once_with(mc_pretty_format(METASTORE_ASSIGNMENT))


@provide_conf
def test_assign_metastore_assignment_cli(api_mock, echo_mock):
    api_mock.create_metastore_assignment.return_value = {}
    runner = CliRunner()
    runner.invoke(
        metastore_cli.assign_metastore_cli,
        args=[
            '--workspace-id', WORKSPACE_ID,
            '--metastore-id', METASTORE_ID,
            '--default-catalog-name', DEFAULT_CATALOG_NAME
        ])
    api_mock.create_metastore_assignment.assert_called_once_with(
        WORKSPACE_ID, METASTORE_ID, DEFAULT_CATALOG_NAME
    )
    echo_mock.assert_called_once_with(mc_pretty_format({}))


@provide_conf
def test_unassign_metastore_assignment_cli(api_mock, echo_mock):
    api_mock.delete_metastore_assignment.return_value = {}
    runner = CliRunner()
    runner.invoke(
        metastore_cli.unassign_metastore_cli,
        args=[
            '--workspace-id', WORKSPACE_ID,
            '--metastore-id', METASTORE_ID
        ])
    api_mock.delete_metastore_assignment.assert_called_once_with(WORKSPACE_ID, METASTORE_ID)
    echo_mock.assert_called_once_with(mc_pretty_format({}))
    