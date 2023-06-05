# Databricks CLI
# Copyright 2022 Databricks, Inc.
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

import functools

import click

from databricks_cli.click_types import JsonClickType
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.unity_catalog.api import UnityCatalogApi
from databricks_cli.unity_catalog.utils import del_none, hide, json_file_help, json_string_help, \
    mc_pretty_format
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, json_cli_base

# These two options are shared among create and updates, so they are very common
def create_update_common_options(f):
    @click.option('--read-only/--no-read-only', is_flag=True, default=None,
                help='Whether the location is read-only')
    @click.option('--comment', default=None,
                help='Free-form text description.')
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        f(*args, **kwargs)
    return wrapper

# These args show up in most create operations
def common_create_args(f):
    @click.option('--name', default=None,
                 help='Name of new connection')
    @click.option('--host', default=None,
                 help='Host of new connection')
    @click.option('--port', default=None,
                 help='Port of new connection')
    @click.option('--user', default=None,
                help='Username for authorization of new connection')
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        f(*args, **kwargs)
    return wrapper

def json_options(f):
    @click.option('--json-file', default=None, type=click.Path(),
                  help=json_file_help(method='POST', path='/connections'), 
    )
    @click.option('--json', default=None, type=JsonClickType(),
                  help=json_string_help(method='POST', path='/connections'), 
    )
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        f(*args, **kwargs)
    return wrapper

# Workaround to prompt for password if user does not specifiy inline JSON or JSON file
# See https://stackoverflow.com/questions/32656571/

def deactivate_prompts(ctx, _, value):
    if not value:
        for p in ctx.command.params:
            if isinstance(p, click.Option) and p.prompt is not None:
                p.prompt = None
    return value


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create mysql connection with CLI flags.')
@common_create_args
@create_update_common_options
@click.option(
    "--password", prompt=True, hide_input=True,
    confirmation_prompt=True
)
@debug_option
@profile_option
@provide_api_client
def create_mysql_cli(api_client, name, host, port, user,
                        read_only, comment, password):
    """
    Create new mysql connection.
    """
    if (name is None) or (host is None) or (port is None) or (user is None):
        raise ValueError('Must provide all required connection parameters')
    data = {
        'name': name,
        'connection_type': 'MYSQL',
        'options_kvpairs': {'host': host, 'port': port, 'user': user, 'password': password},
        'read_only': read_only,
        'comment': comment,
    }
    con_json = UnityCatalogApi(api_client).create_connection(data)
    click.echo(mc_pretty_format(con_json))

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create mysql connection with a JSON input.')
@json_options
@debug_option
@profile_option
@provide_api_client
def create_json(api_client, json_file, json):
    '''
    Create new mysql connection with JSON.
    '''
    if (json is None) and (json_file is None):
        raise ValueError('Must either provide inline JSON or JSON file.')
    json_cli_base(json_file, json,
                  lambda json:
                  UnityCatalogApi(api_client).create_connection(json),
                  encode_utf8=True)
        
@click.group()
def create_group():  # pragma: no cover
    pass

@click.group()
def connections_group():  # pragma: no cover
    pass


def register_connection_commands(cmd_group):
    # Register deprecated "verb-noun" commands for backward compatibility.
    cmd_group.add_command(hide(create_mysql_cli), name='create-mysql-connection')

    # Register command group.
    create_group.add_command(create_mysql_cli, name='mysql')
    connections_group.add_command(create_group, name='create')
    connections_group.add_command(create_json, name='create-json')
    cmd_group.add_command(connections_group, name='connection')
