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

from databricks_cli.unity_catalog import cred_cli
from tests.utils import provide_conf

STORAGE_CREDENTIAL_NAME = 'test_storage_credential_name'
AWS_IAM_ROLE_ARN = 'test_aws_iam_role_arn'
AZ_SP_DIRECTORY_ID = 'test_az_sp_directory_id'
AZ_SP_APPLICATION_ID = 'test_az_sp_application_id'
AZ_SP_CLIENT_SEC = 'test_az_sp_client_secret'  # Named as such to suppress dodgy lint warnings
AZ_MI_ACCESS_CONNECTOR_ID = 'test_mi_access_connector_id'
AZ_MI_ID = 'test_mi_mid'
GCP_SAK_EMAIL = 'test_sak_email'
GCP_SAK_PRIVATE_KEY_ID = 'test_sak_private_key_id'
GCP_SAK_PRIVATE_KEY = 'test_sak_private_key'
COMMENT = 'some_comment'
STORAGE_CREDENTIALS = {
    'storage_credentials': [
        {
            'name': STORAGE_CREDENTIAL_NAME
        }
    ]
}
STORAGE_CREDENTIAL = {
    'name': STORAGE_CREDENTIAL_NAME,
    'aws_iam_role': {
        'role_arn': AWS_IAM_ROLE_ARN
    },
    'azure_service_principal': {
        'directory_id': AZ_SP_DIRECTORY_ID,
        'application_id': AZ_SP_APPLICATION_ID,
        'client_secret': AZ_SP_CLIENT_SEC
    },
    'azure_managed_identity': {
        'access_connector_id': AZ_MI_ACCESS_CONNECTOR_ID,
        'managed_identity_id': AZ_MI_ID
    },
    'gcp_service_account_key': {
        'email': GCP_SAK_EMAIL,
        'private_key_id': GCP_SAK_PRIVATE_KEY_ID,
        'private_key': GCP_SAK_PRIVATE_KEY
    },
    'comment': COMMENT
}


@pytest.fixture()
def api_mock():
    with mock.patch(
            'databricks_cli.unity_catalog.cred_cli.UnityCatalogApi') as uc_api_mock:
        _cred_api_mock = mock.MagicMock()
        uc_api_mock.return_value = _cred_api_mock
        yield _cred_api_mock


@pytest.fixture()
def echo_mock():
    with mock.patch('databricks_cli.unity_catalog.cred_cli.click.echo') as echo_mock:
        yield echo_mock


@provide_conf
def test_create_credential_cli(api_mock, echo_mock):
    api_mock.create_storage_credential.return_value = STORAGE_CREDENTIAL
    runner = CliRunner()
    runner.invoke(
        cred_cli.create_credential_cli,
        args=[
            '--name', STORAGE_CREDENTIAL_NAME,
            '--aws-iam-role-arn', AWS_IAM_ROLE_ARN,
            '--az-sp-directory-id', AZ_SP_DIRECTORY_ID,
            '--az-sp-application-id', AZ_SP_APPLICATION_ID,
            '--az-sp-client-secret', AZ_SP_CLIENT_SEC,
            '--az-mi-access-connector-id', AZ_MI_ACCESS_CONNECTOR_ID,
            '--az-mi-id', AZ_MI_ID,
            '--gcp-sak-email', GCP_SAK_EMAIL,
            '--gcp-sak-private-key-id', GCP_SAK_PRIVATE_KEY_ID,
            '--gcp-sak-private-key', GCP_SAK_PRIVATE_KEY,
            '--comment', COMMENT,
            '--skip-validation'
        ])
    api_mock.create_storage_credential.assert_called_once_with(STORAGE_CREDENTIAL, True)
    echo_mock.assert_called_once_with(mc_pretty_format(STORAGE_CREDENTIAL))


@provide_conf
def test_create_credential_cli_with_json(api_mock, echo_mock):
    api_mock.create_storage_credential.return_value = STORAGE_CREDENTIAL
    runner = CliRunner()
    runner.invoke(
        cred_cli.create_credential_cli,
        args=[
            '--json', '{ "name": "test_credential_name" }'
        ])
    api_mock.create_storage_credential.assert_called_once_with(
        {
            'name': 'test_credential_name'
        },
        False)
    echo_mock.assert_called_once_with(mc_pretty_format(STORAGE_CREDENTIAL))


@provide_conf
def test_list_credentials_cli(api_mock, echo_mock):
    api_mock.list_storage_credentials.return_value = STORAGE_CREDENTIALS
    runner = CliRunner()
    runner.invoke(cred_cli.list_credentials_cli)
    api_mock.list_storage_credentials.assert_called_once()
    echo_mock.assert_called_once_with(mc_pretty_format(STORAGE_CREDENTIALS))


@provide_conf
def test_get_credential_cli(api_mock, echo_mock):
    api_mock.get_storage_credential.return_value = STORAGE_CREDENTIAL
    runner = CliRunner()
    runner.invoke(
        cred_cli.get_credential_cli,
        args=['--name', STORAGE_CREDENTIAL_NAME])
    api_mock.get_storage_credential.assert_called_once_with(STORAGE_CREDENTIAL_NAME)
    echo_mock.assert_called_once_with(mc_pretty_format(STORAGE_CREDENTIAL))


@provide_conf
def test_update_credential_cli(api_mock, echo_mock):
    api_mock.update_storage_credential.return_value = STORAGE_CREDENTIAL
    runner = CliRunner()
    runner.invoke(
        cred_cli.update_credential_cli,
        args=[
            '--name', STORAGE_CREDENTIAL_NAME,
            '--new-name', 'new_credential_name',
            '--aws-iam-role-arn', AWS_IAM_ROLE_ARN,
            '--az-sp-directory-id', AZ_SP_DIRECTORY_ID,
            '--az-sp-application-id', AZ_SP_APPLICATION_ID,
            '--az-sp-client-secret', AZ_SP_CLIENT_SEC,
            '--az-mi-access-connector-id', AZ_MI_ACCESS_CONNECTOR_ID,
            '--az-mi-id', AZ_MI_ID,
            '--gcp-sak-email', GCP_SAK_EMAIL,
            '--gcp-sak-private-key-id', GCP_SAK_PRIVATE_KEY_ID,
            '--gcp-sak-private-key', GCP_SAK_PRIVATE_KEY,
            '--comment', COMMENT,
            '--owner', 'owner',
            '--skip-validation'
        ])
    expected_data = {
        'name': 'new_credential_name',
        'aws_iam_role': {
            'role_arn': AWS_IAM_ROLE_ARN
        },
        'azure_service_principal': {
            'directory_id': AZ_SP_DIRECTORY_ID,
            'application_id': AZ_SP_APPLICATION_ID,
            'client_secret': AZ_SP_CLIENT_SEC
        },
        'azure_managed_identity': {
            'access_connector_id': AZ_MI_ACCESS_CONNECTOR_ID,
            'managed_identity_id': AZ_MI_ID
        },
        'gcp_service_account_key': {
            'email': GCP_SAK_EMAIL,
            'private_key_id': GCP_SAK_PRIVATE_KEY_ID,
            'private_key': GCP_SAK_PRIVATE_KEY
        },
        'comment': COMMENT,
        'owner': 'owner'
    }
    api_mock.update_storage_credential.assert_called_once_with(
        STORAGE_CREDENTIAL_NAME, expected_data, True)
    echo_mock.assert_called_once_with(mc_pretty_format(STORAGE_CREDENTIAL))


@provide_conf
def test_update_credential_cli_with_json(api_mock, echo_mock):
    api_mock.update_storage_credential.return_value = STORAGE_CREDENTIAL
    runner = CliRunner()
    runner.invoke(
        cred_cli.update_credential_cli,
        args=[
            '--name', STORAGE_CREDENTIAL_NAME,
            '--json', '{ "name": "new_credential_name" }'
        ])
    api_mock.update_storage_credential.assert_called_once_with(
        STORAGE_CREDENTIAL_NAME,
        {
            'name': 'new_credential_name'
        },
        False)
    echo_mock.assert_called_once_with(mc_pretty_format(STORAGE_CREDENTIAL))
    