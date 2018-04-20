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

import click
from tabulate import tabulate

from databricks_cli.click_types import OutputClickType
from databricks_cli.secrets.api import SecretApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, truncate_string, \
    error_and_quit, is_base64_str
from databricks_cli.configure.config import provide_api_client, profile_option
from databricks_cli.version import print_version_callback, version


SCOPE_HEADER = ('Scope', 'Backend')
SECRET_HEADER = ('Key name', 'Last updated')
ACL_HEADER = ('Principal', 'Permission')
VALUE_OPTIONS = ['string-value', 'bytes-value']


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Creates a secret scope.")
@click.option('--scope', required=True)
@click.option('--initial-manage-principal',
              help='The initial principal that can manage the created secret scope.')
@profile_option
@eat_exceptions
@provide_api_client
def create_scope(api_client, scope, initial_manage_principal):
    """
    Creates a new secret scope with given name.

    If "initial_manage_principal" is specified, the initial ACL applied to the scope is
    applied to the supplied principal (user or group) with MANAGE permissions.
    The only supported principal for this option is the group "users", which
    contains all users in the workspace. If "initial_manage_principal" is not specified,
    the initial ACL with MANAGE permission applied to the scope is assigned to the
    API request issuer's user identity.
    """
    if initial_manage_principal is not None and initial_manage_principal != "users":
        error_and_quit("The only supported principal for --initial-manage-principal is users")
    SecretApi(api_client).create_scope(scope, initial_manage_principal)


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


def _verify_and_translate_options(string_value, binary_file):
    """
    Translates options into actual parameters for API call.
    Return tuple with two values representing (string_value, bytes_value).
    """
    if string_value and binary_file:
        error_and_quit("At most one of {} should be provided."
                       .format(['string-value', 'binary-file']))

    if string_value is None and binary_file is None:
        prompt = '# Remove this line and input your secret value.' + \
            ' Any trailing new line will be stripped and text will be stored in' + \
            ' UTF-8 (MB4) form. Exit without saving to abort writing secret.'

        # underlying edit function made sure using a temporary file for editing
        content = click.edit(prompt)
        # return None means editor is closed without changes
        if content is None:
            error_and_quit('No changes made, write secret aborted.'
                           ' Please follow the instruction to input secret value.')

        return content.rstrip('\n'), None

    if string_value is not None:
        return string_value, None

    if binary_file is not None:
        with open(binary_file, 'rb') as f:
            binary_content = f.read().rstrip('\n')

        if not is_base64_str(binary_content):
            error_and_quit("Content in file {} is probably not base64 encoded.".format(binary_file))

        return None, binary_content


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Writes a secret to a scope.')
@click.option('--scope', required=True)
@click.option('--key', required=True)
@click.option('--string-value', default=None,
              help='Read value from string and stored in UTF-8 (MB4) form')
@click.option('--binary-file', default=None, type=click.Path(exists=True, readable=True),
              help='Read value from binary-file and stored as bytes.')
@profile_option
@eat_exceptions
@provide_api_client
def write_secret(api_client, scope, key, string_value, binary_file):
    """
    Writes a secret to the provided scope with the given name. Overwrites if the name exists.

    You should specify at most one option in "string-value" and "binary-file".

    If "string-value", the argument will be stored in UTF-8 (MB4) form.

    If "binary-file", the argument should be a path to file. The file should store
    base64 encoded data. File content will be read as secret value and stored as bytes.

    If none of "string-value" and "binary-file" specified, an editor will be opened for
    inputting secret value. The value will be stored in UTF-8 (MB4) form.
    """
    string_param, bytes_param = _verify_and_translate_options(string_value, binary_file)
    SecretApi(api_client).write_secret(scope, key, string_param, bytes_param)


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
