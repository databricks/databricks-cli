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

from databricks_cli.unity_catalog import ext_loc_cli
from tests.utils import provide_conf

EXTERNAL_LOCATION_NAME = 'test_external_location_name'
URL = 'some_url'
CREDENTIAL_NAME = 'some_storage_credential_name'
COMMENT = 'some_comment'
EXTERNAL_LOCATIONS = {
    'external_locations': [
        {
            'name': EXTERNAL_LOCATION_NAME
        }
    ]
}
EXTERNAL_LOCATION = {
    'name': EXTERNAL_LOCATION_NAME,
    'url': URL,
    'credential_name': CREDENTIAL_NAME,
    'read_only': True,
    'comment': COMMENT
}


@pytest.fixture()
def api_mock():
    with mock.patch(
            'databricks_cli.unity_catalog.ext_loc_cli.UnityCatalogApi') as uc_api_mock:
        _ext_loc_api_mock = mock.MagicMock()
        uc_api_mock.return_value = _ext_loc_api_mock
        yield _ext_loc_api_mock


@pytest.fixture()
def echo_mock():
    with mock.patch('databricks_cli.unity_catalog.ext_loc_cli.click.echo') as echo_mock:
        yield echo_mock


@provide_conf
def test_create_location_cli(api_mock, echo_mock):
    api_mock.create_external_location.return_value = EXTERNAL_LOCATION
    runner = CliRunner()
    runner.invoke(
        ext_loc_cli.create_location_cli,
        args=[
            '--name', EXTERNAL_LOCATION_NAME,
            '--url', URL,
            '--storage-credential-name', CREDENTIAL_NAME,
            '--read-only',
            '--comment', COMMENT,
            '--skip-validation'
        ])
    api_mock.create_external_location.assert_called_once_with(EXTERNAL_LOCATION, True)
    echo_mock.assert_called_once_with(mc_pretty_format(EXTERNAL_LOCATION))
    

@provide_conf
def test_create_location_cli_with_json(api_mock, echo_mock):
    api_mock.create_external_location.return_value = EXTERNAL_LOCATION
    runner = CliRunner()
    runner.invoke(
        ext_loc_cli.create_location_cli,
        args=[
            '--json', '{ "name": "test_location_name" }'
        ])
    api_mock.create_external_location.assert_called_once_with(
        {
            'name': 'test_location_name'
        },
        False)
    echo_mock.assert_called_once_with(mc_pretty_format(EXTERNAL_LOCATION))


@provide_conf
def test_list_locations_cli(api_mock, echo_mock):
    api_mock.list_external_locations.return_value = EXTERNAL_LOCATIONS
    runner = CliRunner()
    runner.invoke(ext_loc_cli.list_locations_cli)
    api_mock.list_external_locations.assert_called_once()
    echo_mock.assert_called_once_with(mc_pretty_format(EXTERNAL_LOCATIONS))


@provide_conf
def test_get_location_cli(api_mock, echo_mock):
    api_mock.get_external_location.return_value = EXTERNAL_LOCATION
    runner = CliRunner()
    runner.invoke(
        ext_loc_cli.get_location_cli,
        args=['--name', EXTERNAL_LOCATION_NAME])
    api_mock.get_external_location.assert_called_once_with(EXTERNAL_LOCATION_NAME)
    echo_mock.assert_called_once_with(mc_pretty_format(EXTERNAL_LOCATION))


@provide_conf
def test_update_location_cli(api_mock, echo_mock):
    api_mock.update_external_location.return_value = EXTERNAL_LOCATION
    runner = CliRunner()
    runner.invoke(
        ext_loc_cli.update_location_cli,
        args=[
            '--name', EXTERNAL_LOCATION_NAME,
            '--new-name', 'new_location_name',
            '--url', URL,
            '--storage-credential-name', CREDENTIAL_NAME,
            '--no-read-only',
            '--comment', COMMENT,
            '--owner', 'owner',
            '--force',
            '--skip-validation'
        ])
    expected_data = {
        'name': 'new_location_name',
        'url': URL,
        'credential_name': CREDENTIAL_NAME,
        'read_only': False,
        'comment': COMMENT,
        'owner': 'owner'
    }
    api_mock.update_external_location.assert_called_once_with(
        EXTERNAL_LOCATION_NAME, expected_data, True, True)
    echo_mock.assert_called_once_with(mc_pretty_format(EXTERNAL_LOCATION))


@provide_conf
def test_update_location_cli_null_read_only(api_mock, echo_mock):
    api_mock.update_external_location.return_value = EXTERNAL_LOCATION
    runner = CliRunner()
    runner.invoke(
        ext_loc_cli.update_location_cli,
        args=[
            '--name', EXTERNAL_LOCATION_NAME,
            '--new-name', 'new_location_name'
        ])
    expected_data = {
        'name': 'new_location_name',
        'url': None,
        'credential_name': None,
        'read_only': None,
        'comment': None,
        'owner': None
    }
    api_mock.update_external_location.assert_called_once_with(
        EXTERNAL_LOCATION_NAME, expected_data, False, False)
    echo_mock.assert_called_once_with(mc_pretty_format(EXTERNAL_LOCATION))


@provide_conf
def test_update_location_cli_with_json(api_mock, echo_mock):
    api_mock.update_external_location.return_value = EXTERNAL_LOCATION
    runner = CliRunner()
    runner.invoke(
        ext_loc_cli.update_location_cli,
        args=[
            '--name', EXTERNAL_LOCATION_NAME,
            '--json', '{ "name": "new_location_name" }'
        ])
    api_mock.update_external_location.assert_called_once_with(
        EXTERNAL_LOCATION_NAME,
        {
            'name': 'new_location_name'
        },
        False,
        False)
    echo_mock.assert_called_once_with(mc_pretty_format(EXTERNAL_LOCATION))


@provide_conf
def test_delete_location_cli(api_mock):
    runner = CliRunner()
    runner.invoke(
        ext_loc_cli.delete_location_cli,
        args=[
            '--name', EXTERNAL_LOCATION_NAME,
            '--force'
        ])
    api_mock.delete_external_location.assert_called_once_with(EXTERNAL_LOCATION_NAME, True)


@provide_conf
def test_validate_location_cli(api_mock, echo_mock):
    api_mock.validate_external_location.return_value = {}
    runner = CliRunner()
    runner.invoke(
        ext_loc_cli.validate_location_cli,
        args=[
            '--name', EXTERNAL_LOCATION_NAME,
            '--url', URL,
            '--cred-name', CREDENTIAL_NAME,
            '--cred-aws-iam-role', 'aws-iam-role',
            '--cred-az-directory-id', 'az-directory-id',
            '--cred-az-application-id', 'az-application-id',
            '--cred-az-client-secret', 'az-client-secret',
            '--cred-az-mi-access-connector-id', 'az-mi-access-connector-id',
            '--cred-az-mi-id', 'az-mi-id',
            '--cred-gcp-sak-email', 'gcp-sak-email',
            '--cred-gcp-sak-private-key-id', 'gcp-sak-private-key-id',
            '--cred-gcp-sak-private-key', 'gcp-sak-private-key'
        ])
    api_mock.validate_external_location.assert_called_once_with(
        {
            'external_location_name': EXTERNAL_LOCATION_NAME,
            'url': URL,
            'storage_credential_name': CREDENTIAL_NAME,
            'aws_iam_role': {
                'role_arn': 'aws-iam-role'
            },
            'azure_service_principal': {
                'directory_id': 'az-directory-id',
                'application_id': 'az-application-id',
                'client_secret': 'az-client-secret',
            },
            'azure_managed_identity': {
                'access_connector_id': 'az-mi-access-connector-id',
                'managed_identity_id': 'az-mi-id',
            },
            'gcp_service_account_key': {
                'email': 'gcp-sak-email',
                'private_key_id': 'gcp-sak-private-key-id',
                'private_key': 'gcp-sak-private-key'
            }
        })
    echo_mock.assert_called_once_with(mc_pretty_format({}))
    