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

import base64
import click
from tabulate import tabulate

from databricks_cli.click_types import OutputClickType, OneOfOption
from databricks_cli.secrets.api import SecretApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, truncate_string, \
    translate_value
from databricks_cli.configure.config import provide_api_client, profile_option
from databricks_cli.version import print_version_callback, version


SCOPE_HEADER = ('Scope', 'Backend')
SECRET_HEADER = ('Key name', 'Last updated')
ACL_HEADER = ('Principal', 'Permission')
VALUE_OPTIONS = ['string-value', 'bytes-value']


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Creates a secret scope.")
@click.option('--scope', required=True)
@click.option('--initial-manage-acl',
              type=click.Choice(['CREATOR_ONLY', 'ALL_USERS']), default='CREATOR_ONLY',
              help='The initial ACL applied to the secret scope. Must be either "CREATOR_ONLY"'
                   ' or "ALL_USERS". Defaults to "CREATOR_ONLY".')
@profile_option
@eat_exceptions
@provide_api_client
def create_scope(api_client, scope, initial_manage_acl):
    """
    Creates a new secret scope with given name.

    "initial_manage_acl" controls the initial ACL applied to the secret scope.
    If "CREATOR_ONLY", the initial ACL applied to scope is MANAGE permission, assigned to
    the request issuer's user id.
    If "ALL_USERS", the initial ACL applied to scope is MANAGE permission, assigned to
    the group "all-users".
    """
    SecretApi(api_client).create_scope(scope, initial_manage_acl)


def _scopes_to_table(scopes_json):
    ret = []
    for s in scopes_json.get('scopes', []):
        ret.append((truncate_string(s['name']), s['backend_type']))
    return ret


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Lists all the secret scopes.')
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@profile_option
@eat_exceptions
@provide_api_client
def list_scopes(api_client, output):
    """
    Lists all secret scopes available in the workspace.
    """
    scopes_json = SecretApi(api_client).list_scopes()
    if OutputClickType.is_json(output):
        click.echo(pretty_format(scopes_json))
    else:
        click.echo(tabulate(_scopes_to_table(scopes_json), headers=SCOPE_HEADER))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deletes a secret scope.')
@click.option('--scope', required=True)
@profile_option
@eat_exceptions
@provide_api_client
def delete_scope(api_client, scope):
    """
    Deletes a secret scope.
    """
    SecretApi(api_client).delete_scope(scope)


def _read_value(string_value, bytes_value, value, no_strip):
    """
    Translates value to actual secret value
    """
    if string_value:
        return translate_value(value, no_strip, read_bytes=False), None
    if bytes_value:
        secret_content = translate_value(value, no_strip, read_bytes=True)
        base64_bytes = base64.b64encode(secret_content)
        base64_str = base64_bytes.decode('utf-8')
        return None, base64_str


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Writes a secret to a scope.')
@click.option('--scope', required=True)
@click.option('--key', required=True)
@click.option('--string-value', is_flag=True, cls=OneOfOption, one_of=VALUE_OPTIONS, default=False,
              help='A flag indicates the value will be stored in UTF-8 (MB4) form')
@click.option('--bytes-value', is_flag=True, cls=OneOfOption, one_of=VALUE_OPTIONS, default=False,
              help='A flag indicates the value will be store as bytes')
@click.option('--no-strip', is_flag=True, default=False,
              help='Indicate the trailing \'\\n\' should not be stripped from input value')
@click.argument('VALUE', required=False)
@profile_option
@eat_exceptions
@provide_api_client
def write_secret(api_client, scope, key, string_value, bytes_value, no_strip, value):
    """
    Writes a secret to the provided scope with the given name. Overwrites if the name exists.

    You should specify exactly one flag to indicate if the value is "string-value" or "bytes-value".

    If VALUE is an empty string or not provided, an editor will be opened for you to enter your
    secret value.
    If VALUE starts with '@', the rest of the string will be seen as a path to a file, the content
    of file will be read as secret value.
    Otherwise, the VALUE itself will be seen as secret value.
    """
    string_value, bytes_value = _read_value(string_value, bytes_value, value, no_strip)
    SecretApi(api_client).write_secret(scope, key, string_value, bytes_value)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deletes a secret.')
@click.option('--scope', required=True)
@click.option('--key', required=True)
@profile_option
@eat_exceptions
@provide_api_client
def delete_secret(api_client, scope, key):
    """
    Deletes the secret stored in this scope.
    """
    SecretApi(api_client).delete_secret(scope, key)


def _secrets_to_table(secrets_json):
    ret = []
    for s in secrets_json.get('secrets', []):
        ret.append((s['key'], s.get('last_updated_timestamp', 'Not Available')))
    return ret


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Lists all the secrets in a scope.')
@click.option('--scope', required=True)
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@profile_option
@eat_exceptions
@provide_api_client
def list_secrets(api_client, scope, output):
    """
    Lists the secret keys that are stored at this scope. Also lists the last updated timestamp
    if available.
    """
    secrets_json = SecretApi(api_client).list_secrets(scope)
    if OutputClickType.is_json(output):
        click.echo(pretty_format(secrets_json))
    else:
        click.echo(tabulate(_secrets_to_table(secrets_json), headers=SECRET_HEADER))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Writes an access control rule for a principal applied to '
                          ' a given secret scope.')
@click.option('--scope', required=True)
@click.option('--principal', required=True)
@click.option('--permission', type=click.Choice(['MANAGE', 'WRITE', 'READ']),
              required=True)
@profile_option
@eat_exceptions
@provide_api_client
def write_acl(api_client, scope, principal, permission):
    """
    Creates or overwrites the ACL associated with the given principal (user or group) on the
    specified secret scope.
    """
    SecretApi(api_client).write_acl(scope, principal, permission)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deletes an access control rule for a principal.')
@click.option('--scope', required=True)
@click.option('--principal', required=True)
@profile_option
@eat_exceptions
@provide_api_client
def delete_acl(api_client, scope, principal):
    """
    Deletes the given ACL on the given secret scope.
    """
    SecretApi(api_client).delete_acl(scope, principal)


def _acls_to_table(acls_json):
    ret = []
    for s in acls_json.get('items', []):
        ret.append((s['principal'], s['permission'].upper()))
    return ret


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Lists all access control rules for a given secret scope.')
@click.option('--scope', required=True)
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@profile_option
@eat_exceptions
@provide_api_client
def list_acls(api_client, scope, output):
    """
    Lists the ACLs set on the given secret scope.
    """
    acls_json = SecretApi(api_client).list_acls(scope)
    if OutputClickType.is_json(output):
        click.echo(pretty_format(acls_json))
    else:
        click.echo(tabulate(_acls_to_table(acls_json), headers=ACL_HEADER))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Gets the details for an access control rule.')
@click.option('--scope', required=True)
@click.option('--principal', required=True)
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@profile_option
@eat_exceptions
@provide_api_client
def get_acl(api_client, scope, principal, output):
    """
    Describes the details about the given ACL for the principal and secret scope.
    """
    acl_json = SecretApi(api_client).get_acl(scope, principal)
    if OutputClickType.is_json(output):
        click.echo(pretty_format(acl_json))
    else:
        acl_list = _acls_to_table({'items': [acl_json]})
        click.echo(tabulate(acl_list, headers=ACL_HEADER))


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Databricks secret API.'
                        ' (Available only in Azure Databricks)')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@profile_option
@eat_exceptions
def secrets_group():
    """
    Utility to interact with secret API. As of Apr 13, 2018, secrets API is only
    available in Azure Databricks.
    """
    pass


secrets_group.add_command(create_scope, name='create-scope')
secrets_group.add_command(list_scopes, name='list-scopes')
secrets_group.add_command(delete_scope, name='delete-scope')
secrets_group.add_command(write_secret, name='write')
secrets_group.add_command(delete_secret, name='delete')
secrets_group.add_command(list_secrets, name='list')
secrets_group.add_command(write_acl, name='write-acl')
secrets_group.add_command(delete_acl, name='delete-acl')
secrets_group.add_command(list_acls, name='list-acls')
secrets_group.add_command(get_acl, name='get-acl')
