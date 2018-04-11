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

from databricks_cli.click_types import OutputClickType
from databricks_cli.secrets.api import SecretApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, truncate_string, \
    translate_value, error_and_quit
from databricks_cli.configure.config import provide_api_client, profile_option
from databricks_cli.version import print_version_callback, version


SCOPE_HEADER = ('Scope', 'Backend')
SECRET_HEADER = ('Key name', 'Last updated')
ACL_HEADER = ('Principal', 'Permission')


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Creates a scope")
@click.option('--scope', required=True)
@click.option('--initial-manage-acl',
              type=click.Choice(['creator-only', 'all-users']), default='creator-only',
              help='The initial ACL applied to the scope. Must be either "creator-only"'
                   ' or "all-users". Defaults to "creator-only".')
@profile_option
@eat_exceptions
@provide_api_client
def create_scope(api_client, scope, initial_manage_acl):
    """
    Creates a new scope.

    The specification for the scope name and initial_manage_acl can be found at:
    https://docs.azuredatabricks.net/api/latest/secrets.html#create-secret-scope
    """
    initial_manage_acl = initial_manage_acl.replace('-', '_')
    SecretApi(api_client).create_scope(scope, initial_manage_acl)


def _scopes_to_table(scopes_json):
    ret = []
    for s in scopes_json.get('scopes', []):
        ret.append((truncate_string(s['name']), s['backend_type']))
    return ret


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Lists all the scopes')
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
               short_help='Deletes a scope')
@click.option('--scope', required=True)
@profile_option
@eat_exceptions
@provide_api_client
def delete_scope(api_client, scope):
    """
    Deletes a secret scope.
    """
    SecretApi(api_client).delete_scope(scope)


def verify_and_read_value(string_value, bytes_value, value, no_strip):
    """
    Verify exactly one among the options is set to True and translate the value
    """
    valid_count = 0
    for v in [string_value, bytes_value]:
        if v:
            valid_count += 1

    if valid_count != 1:
        error_and_quit("You must specify exactly one in options"
                       " [--string-value, --bytes-value]")

    if string_value:
        return translate_value(value, no_strip), None
    if bytes_value:
        return None, base64.encodestring(translate_value(value, no_strip))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Writes a secret')
@click.option('--scope', required=True)
@click.option('--key', required=True)
@click.option('--string-value', is_flag=True, default=False,
              help='A flag indicates the value will be stored in UTF-8 (MB4) form')
@click.option('--bytes-value', is_flag=True, default=False,
              help='A flag indicates the value will be store as bytes')
@click.option('--no-strip', is_flag=True, default=False,
              help='Indicate the trailing \'\\n\' should not be stripped from input value')
@click.argument('VALUE', required=False)
@profile_option
@eat_exceptions
@provide_api_client
def write_secret(api_client, scope, key, string_value, bytes_value, no_strip, value):
    """
    Inserts a secret under the provided scope with the given name. Overwrite if the name exists.

    You should specify exactly one flag to indicate the value is "string-value" or "bytes-value".

    If VALUE is an empty string or not provided, an editor will be opened for you to enter your
    secret value.
    If VALUE starts with '@', the rest of the string will be seen as a path to a file, the content
    of file will be read as content.
    Otherwise, the VALUE itself will be seen as secret value.

    The specification for input can be found at:
    https://docs.azuredatabricks.net/api/latest/secrets.html#write-secret
    """
    string_value, bytes_value = verify_and_read_value(string_value, bytes_value, value, no_strip)
    SecretApi(api_client).write_secret(scope, key, string_value, bytes_value)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deletes a secret')
@click.option('--scope', required=True)
@click.option('--key', required=True)
@profile_option
@eat_exceptions
@provide_api_client
def delete_secret(api_client, scope, key):
    """
    Deletes the secret stored in this secret scope. You must have WRITE or MANAGE permission on
    the Secret Scope.
    """
    SecretApi(api_client).delete_secret(scope, key)


def _secrets_to_table(secrets_json):
    ret = []
    for s in secrets_json.get('secrets', []):
        ret.append((s['key'], s.get('last_updated_timestamp', 'Not Available')))
    return ret


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Lists all the secrets in a scope')
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
               short_help='Writes a ACL for a principal')
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
    specified scope.
    """
    SecretApi(api_client).write_acl(scope, principal, permission)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deletes a ACL for a principal')
@click.option('--scope', required=True)
@click.option('--principal', required=True)
@profile_option
@eat_exceptions
@provide_api_client
def delete_acl(api_client, scope, principal):
    """
    Deletes the given ACL on the given scope.
    """
    SecretApi(api_client).delete_acl(scope, principal)


def _acls_to_table(acls_json):
    ret = []
    for s in acls_json.get('items', []):
        ret.append((s['principal'], s['permission'].upper()))
    return ret


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Lists ACLs in a scope')
@click.option('--scope', required=True)
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@profile_option
@eat_exceptions
@provide_api_client
def list_acls(api_client, scope, output):
    """
    Lists the ACLs set on the given scope.
    """
    acls_json = SecretApi(api_client).list_acls(scope)
    if OutputClickType.is_json(output):
        click.echo(pretty_format(acls_json))
    else:
        click.echo(tabulate(_acls_to_table(acls_json), headers=ACL_HEADER))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Gets details of ACL')
@click.option('--scope', required=True)
@click.option('--principal', required=True)
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@profile_option
@eat_exceptions
@provide_api_client
def get_acl(api_client, scope, principal, output):
    """
    Describes the details about the given ACL.
    """
    acl_json = SecretApi(api_client).get_acl(scope, principal)
    if OutputClickType.is_json(output):
        click.echo(pretty_format(acl_json))
    else:
        acl_list = _acls_to_table({'items': [acl_json]})
        click.echo(tabulate(acl_list, headers=ACL_HEADER))


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Databricks secret manager.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@profile_option
@eat_exceptions
def secrets_group():
    """
    Utility to interact with Databricks secret manager. For now, secret manager is only
    available on Azure workspace.
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
