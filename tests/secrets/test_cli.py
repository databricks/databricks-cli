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
#
# pylint:disable=redefined-outer-name

import mock
import pytest
from tabulate import tabulate
from click.testing import CliRunner

import databricks_cli.secrets.cli as cli
from databricks_cli.secrets.cli import SCOPE_HEADER, SECRET_HEADER, ACL_HEADER, DASH_MARKER
from tests.utils import provide_conf


SCOPE = 'test_scope'


@pytest.fixture()
def secrets_api_mock():
    with mock.patch('databricks_cli.secrets.cli.SecretApi') as SecretApiMock:
        _secrets_api_mock = mock.MagicMock()
        SecretApiMock.return_value = _secrets_api_mock
        yield _secrets_api_mock


@provide_conf
def test_create_scope(secrets_api_mock):
    runner = CliRunner()
    runner.invoke(cli.create_scope, ['--scope', SCOPE, '--initial-manage-principal', 'users'])
    assert secrets_api_mock.create_scope.call_args[0][0] == SCOPE
    assert secrets_api_mock.create_scope.call_args[0][1] == 'users'


@provide_conf
def test_delete_scope(secrets_api_mock):
    runner = CliRunner()
    runner.invoke(cli.delete_scope, ['--scope', SCOPE])
    assert secrets_api_mock.delete_scope.call_args[0][0] == SCOPE


LIST_SCOPES_RETURN = {
    "scopes": [{
        "name": "my-scope",
        "backend_type": "DATABRICKS"
    }, {
        "name": "akv-scope",
        "backend_type": "AZURE_KEYVAULT",
        "keyvault_metadata": {
            "resource_id": "/subscriptions/.../resource_id",
            "dns_name": "https://akvscope.vault.azure.net/"
        }
    }]
}


@provide_conf
def test_list_scope(secrets_api_mock):
    with mock.patch('databricks_cli.secrets.cli.click.echo') as echo_mock:
        secrets_api_mock.list_scopes.return_value = LIST_SCOPES_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_scopes)
        assert echo_mock.call_args[0][0] == \
            tabulate([
                ('my-scope', 'DATABRICKS', 'N/A'),
                ('akv-scope', 'AZURE_KEYVAULT', 'https://akvscope.vault.azure.net/')],
                headers=SCOPE_HEADER)


KEY = 'test_key'
VALUE = 'test_value'


@provide_conf
def test_put_secret_string_value(secrets_api_mock):
    runner = CliRunner()
    runner.invoke(cli.put_secret, ['--scope', SCOPE, '--key', KEY, '--string-value', VALUE])
    assert secrets_api_mock.put_secret.call_args[0] == (SCOPE, KEY, VALUE, None)


@provide_conf
def test_put_secret_multiple_value(secrets_api_mock):
    runner = CliRunner()
    result = runner.invoke(cli.put_secret,
                           ['--scope', SCOPE, '--key', KEY,
                            '--string-value', VALUE, '--binary-file', VALUE])
    assert result.exit_code != 0
    assert secrets_api_mock.put_secret.call_count == 0


@provide_conf
def test_put_secret_editor_input_correct_split(secrets_api_mock):
    with mock.patch('databricks_cli.secrets.cli.click.edit') as edit_mock:
        # normal input with trailing new lines
        edit_mock.return_value = VALUE + '\n' + DASH_MARKER
        runner = CliRunner()
        runner.invoke(cli.put_secret, ['--scope', SCOPE, '--key', KEY])
        assert secrets_api_mock.put_secret.call_args[0] == (SCOPE, KEY, VALUE, None)


@provide_conf
def test_put_secret_editor_input_no_value(secrets_api_mock):
    with mock.patch('databricks_cli.secrets.cli.click.edit') as edit_mock:
        # file not saved
        edit_mock.return_value = None
        runner = CliRunner()
        result = runner.invoke(cli.put_secret, ['--scope', SCOPE, '--key', KEY])
        assert result.exit_code != 0
        assert secrets_api_mock.put_secret.call_count == 0


@provide_conf
def test_put_secret_editor_input_edited_marker(secrets_api_mock):
    with mock.patch('databricks_cli.secrets.cli.click.edit') as edit_mock:
        # input with marker line edited
        edit_mock.return_value = VALUE + '\n' + '# ----------\n'
        runner = CliRunner()
        result = runner.invoke(cli.put_secret, ['--scope', SCOPE, '--key', KEY])
        assert result.exit_code != 0
        assert secrets_api_mock.put_secret.call_count == 0


@provide_conf
def test_put_secrets_binary_file(secrets_api_mock):
    import os
    import tempfile

    # using tempfile to make sure passing click.Path check
    fd, name = tempfile.mkstemp(prefix='test-cli', suffix='.tmp')
    try:
        f = os.fdopen(fd, 'wb')
        f.write((VALUE + '\n\n').encode())
        f.close()

        runner = CliRunner()
        runner.invoke(cli.put_secret,
                      ['--scope', SCOPE, '--key', KEY, '--binary-file', name])
        assert secrets_api_mock.put_secret.call_args[0] == (SCOPE, KEY, None, 'dGVzdF92YWx1ZQoK')
    finally:
        os.unlink(name)


LIST_SECRETS_RETURN = {
    "secrets": [
        {
            "key": "key-1",
            "last_updated_timestamp": "1520467595000"
        },
        {
            "key": "key-2",
        },
    ]
}


@provide_conf
def test_list_secrets(secrets_api_mock):
    with mock.patch('databricks_cli.secrets.cli.click.echo') as echo_mock:
        secrets_api_mock.list_secrets.return_value = LIST_SECRETS_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_secrets, ['--scope', SCOPE])
        assert secrets_api_mock.list_secrets.call_args[0][0] == SCOPE
        assert echo_mock.call_args[0][0] == \
            tabulate([('key-1', '1520467595000'), ('key-2', 'Not Available')],
                     headers=SECRET_HEADER)


@provide_conf
def test_delete_secret(secrets_api_mock):
    runner = CliRunner()
    runner.invoke(cli.delete_secret, ['--scope', SCOPE, '--key', KEY])
    assert secrets_api_mock.delete_secret.call_args[0][0] == SCOPE
    assert secrets_api_mock.delete_secret.call_args[0][1] == KEY


PRINCIPAL = "admins"
PERMISSION = 'MANAGE'
LIST_ACLS_RETURN = {
    "items": [{
        "principal": PRINCIPAL,
        "permission": PERMISSION
    }]
}


@provide_conf
def test_list_acls(secrets_api_mock):
    with mock.patch('databricks_cli.secrets.cli.click.echo') as echo_mock:
        secrets_api_mock.list_acls.return_value = LIST_ACLS_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_acls, ['--scope', SCOPE])
        assert secrets_api_mock.list_acls.call_args[0][0] == SCOPE
        assert echo_mock.call_args[0][0] == \
            tabulate([(PRINCIPAL, PERMISSION)], headers=ACL_HEADER)


GET_ACL_RETURN = {
    "principal": PRINCIPAL,
    "permission": PERMISSION
}


@provide_conf
def test_get_acl(secrets_api_mock):
    with mock.patch('databricks_cli.secrets.cli.click.echo') as echo_mock:
        secrets_api_mock.get_acl.return_value = GET_ACL_RETURN
        runner = CliRunner()
        runner.invoke(cli.get_acl, ['--scope', SCOPE, '--principal', PRINCIPAL])
        assert secrets_api_mock.get_acl.call_args[0][0] == SCOPE
        assert secrets_api_mock.get_acl.call_args[0][1] == PRINCIPAL
        assert echo_mock.call_args[0][0] == \
            tabulate([(PRINCIPAL, 'MANAGE')], headers=ACL_HEADER)


@provide_conf
def test_delete_acl(secrets_api_mock):
    runner = CliRunner()
    runner.invoke(cli.delete_acl, ['--scope', SCOPE, '--principal', PRINCIPAL])
    assert secrets_api_mock.delete_acl.call_args[0][0] == SCOPE
    assert secrets_api_mock.delete_acl.call_args[0][1] == PRINCIPAL


@provide_conf
def test_put_acl(secrets_api_mock):
    runner = CliRunner()
    runner.invoke(cli.put_acl,
                  ['--scope', SCOPE, '--principal', PRINCIPAL, '--permission', PERMISSION])
    assert secrets_api_mock.put_acl.call_args[0][0] == SCOPE
    assert secrets_api_mock.put_acl.call_args[0][1] == PRINCIPAL
    assert secrets_api_mock.put_acl.call_args[0][2] == PERMISSION
