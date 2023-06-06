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
from databricks_cli.unity_catalog.utils import hide, json_file_help, json_string_help, \
    mc_pretty_format
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, json_cli_base

# These two options are shared among create and updates, so they are very common
def create_update_common_options(f):
    @click.option('--read-only/--no-read-only', is_flag=True, default=None,
                help='Whether the connection is read-only')
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

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create mysql connection with CLI flags.')
@common_create_args
@create_update_common_options
@click.option(
    "--password", prompt=True, hide_input=True,
    confirmation_prompt=True
)
@debug_option
@eat_exceptions
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
        'options': {'host': host, 'port': port, 'user': user, 'password': password},
        'read_only': read_only,
        'comment': comment,
    }
    con_json = UnityCatalogApi(api_client).create_connection(data)
    click.echo(mc_pretty_format(con_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create postgresql connection with CLI flags.')
@common_create_args
@create_update_common_options
@click.option(
    "--password", prompt=True, hide_input=True,
    confirmation_prompt=True
)
@debug_option
@eat_exceptions
@profile_option
@provide_api_client
def create_postgresql_cli(api_client, name, host, port, user,
                        read_only, comment, password):
    """
    Create new postgresql connection.
    """
    if (name is None) or (host is None) or (port is None) or (user is None):
        raise ValueError('Must provide all required connection parameters')
    data = {
        'name': name,
        'connection_type': 'POSTGRESQL',
        'options': {'host': host, 'port': port, 'user': user, 'password': password},
        'read_only': read_only,
        'comment': comment,
    }
    con_json = UnityCatalogApi(api_client).create_connection(data)
    click.echo(mc_pretty_format(con_json))

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create snowflake connection with CLI flags.')
@common_create_args
@create_update_common_options
@click.option('--sfwarehouse', default=None,
                help='Snowflake warehouse name of new connection')
@click.option(
    "--password", prompt=True, hide_input=True,
    confirmation_prompt=True
)
@debug_option
@eat_exceptions
@profile_option
@provide_api_client
def create_snowflake_cli(api_client, name, host, port, user, sfwarehouse,
                        read_only, comment, password):
    """
    Create new snowflake connection.
    """
    if (name is None) or (host is None) or (port is None) or (user is None) or \
        (sfwarehouse is None):
        raise ValueError('Must provide all required connection parameters')
    data = {
        'name': name,
        'connection_type': 'SNOWFLAKE',
        'options': {'host': host, 'port': port, 'user': user, 'password': password, 
                    'sfWarehouse': sfwarehouse},
        'read_only': read_only,
        'comment': comment,
    }
    con_json = UnityCatalogApi(api_client).create_connection(data)
    click.echo(mc_pretty_format(con_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create redshift connection with CLI flags.')
@common_create_args
@create_update_common_options
@click.option(
    "--password", prompt=True, hide_input=True,
    confirmation_prompt=True
)
@debug_option
@eat_exceptions
@profile_option
@provide_api_client
def create_redshift_cli(api_client, name, host, port, user,
                        read_only, comment, password):
    """
    Create new redshift connection.
    """
    if (name is None) or (host is None) or (port is None) or (user is None):
        raise ValueError('Must provide all required connection parameters')
    data = {
        'name': name,
        'connection_type': 'REDSHIFT',
        'options': {'host': host, 'port': port, 'user': user, 'password': password},
        'read_only': read_only,
        'comment': comment,
    }
    con_json = UnityCatalogApi(api_client).create_connection(data)
    click.echo(mc_pretty_format(con_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create sqldw connection with CLI flags.')
@common_create_args
@create_update_common_options
@click.option('--trustservercert', is_flag=True, default=None,
                help='Trust the server provided certificate')
@click.option(
    "--password", prompt=True, hide_input=True,
    confirmation_prompt=True
)
@debug_option
@eat_exceptions
@profile_option
@provide_api_client
def create_sqldw_cli(api_client, name, host, port, user, trustservercert,
                        read_only, comment, password):
    """
    Create new sqldw connection.
    """
    if (name is None) or (host is None) or (port is None) or (user is None):
        raise ValueError('Must provide all required connection parameters')
    data = {
        'name': name,
        'connection_type': 'SQLDW',
        'options': {'host': host, 'port': port, 'user': user, 'password': password},
        'read_only': read_only,
        'comment': comment,
    }
    if trustservercert is not None:
        data['options']['trustServerCertificate'] = trustservercert
    con_json = UnityCatalogApi(api_client).create_connection(data)
    click.echo(mc_pretty_format(con_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create sqlserver connection with CLI flags.')
@common_create_args
@create_update_common_options
@click.option('--trustservercert', is_flag=True, default=None,
                help='Trust the server provided certificate')
@click.option(
    "--password", prompt=True, hide_input=True,
    confirmation_prompt=True
)
@debug_option
@eat_exceptions
@profile_option
@provide_api_client
def create_sqlserver_cli(api_client, name, host, port, user, trustservercert,
                        read_only, comment, password):
    """
    Create new sqlserver connection.
    """
    if (name is None) or (host is None) or (port is None) or (user is None):
        raise ValueError('Must provide all required connection parameters')
    data = {
        'name': name,
        'connection_type': 'SQLSERVER',
        'options': {'host': host, 'port': port, 'user': user, 'password': password},
        'read_only': read_only,
        'comment': comment,
    }
    if trustservercert is not None:
        data['options']['trustServerCertificate'] = trustservercert
    con_json = UnityCatalogApi(api_client).create_connection(data)
    click.echo(mc_pretty_format(con_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create databricks connection with CLI flags.')
@create_update_common_options
@click.option('--name', default=None,
             help='Name of new connection')
@click.option('--host', default=None,
             help='Host of new connection')
@click.option('--httppath', default=None,
             help='HTTP path of new connection')
@click.option(
    "--token", prompt=True, hide_input=True,
    confirmation_prompt=True
)
@debug_option
@eat_exceptions
@profile_option
@provide_api_client
def create_databricks_cli(api_client, name, host, httppath, token,
                        read_only, comment):
    """
    Create new databricks connection.
    """
    if (name is None) or (host is None) or (httppath is None) or (token is None):
        raise ValueError('Must provide all required connection parameters')
    data = {
        'name': name,
        'connection_type': 'DATABRICKS',
        'options': {'host': host, 'httpPath': httppath, 'personalAccessToken': token},
        'read_only': read_only,
        'comment': comment,
    }
    con_json = UnityCatalogApi(api_client).create_connection(data)
    click.echo(mc_pretty_format(con_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create online catalog connection with CLI flags.')
@common_create_args
@create_update_common_options
@debug_option
@eat_exceptions
@profile_option
@provide_api_client
def create_online_catalog_cli(api_client, name, host, port, user,
                        read_only, comment):
    """
    Create new online catalog connection.
    """
    if (name is None) or (host is None) or (port is None) or (user is None):
        raise ValueError('Must provide all required connection parameters')
    data = {
        'name': name,
        'connection_type': 'ONLINE_CATALOG',
        'options': {'host': host, 'port': port, 'user': user},
        'read_only': read_only,
        'comment': comment,
    }
    con_json = UnityCatalogApi(api_client).create_connection(data)
    click.echo(mc_pretty_format(con_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create connection with a JSON input.')
@json_options
@debug_option
@eat_exceptions
@profile_option
@provide_api_client
def create_json(api_client, json_file, json):
    '''
    Create new connection with an inline JSON or JSON file input.
    '''
    if (json is None) and (json_file is None):
        raise ValueError('Must either provide inline JSON or JSON file.')
    json_cli_base(json_file, json,
                  lambda json:
                  UnityCatalogApi(api_client).create_connection(json),
                  encode_utf8=True)
        


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List connections.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_connections_cli(api_client, ):
    """
    List connections.
    """
    locs_json = UnityCatalogApi(api_client).list_connections()
    click.echo(mc_pretty_format(locs_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get a connection.')
@click.option('--name', required=True,
              help='Name of the connection to get.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_connection_cli(api_client, name):
    """
    Get a connection.
    """
    loc_json = UnityCatalogApi(api_client).get_connection(name)
    click.echo(mc_pretty_format(loc_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a connection.')
@click.option('--name', required=True,
              help='Name of the connection to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_connection_cli(api_client, name):
    """
    Delete a connection.
    """
    UnityCatalogApi(api_client).delete_connection(name)


@click.group()
def create_group():  # pragma: no cover
    pass

@click.group()
def connections_group():  # pragma: no cover
    pass


def register_connection_commands(cmd_group):
    # Register deprecated "verb-noun" commands for backward compatibility.
    cmd_group.add_command(hide(create_json), name='create-connection-json')
    cmd_group.add_command(hide(create_mysql_cli), name='create-mysql-connection')
    cmd_group.add_command(hide(create_postgresql_cli), name='create-postgresql-connection')
    cmd_group.add_command(hide(create_snowflake_cli), name='create-snowflake-connection')
    cmd_group.add_command(hide(create_redshift_cli), name='create-redshift-connection')
    cmd_group.add_command(hide(create_sqldw_cli), name='create-sqldw-connection')
    cmd_group.add_command(hide(create_sqlserver_cli), name='create-sqlserver-connection')
    cmd_group.add_command(hide(create_databricks_cli), name='create-databricks-connection')
    cmd_group.add_command(hide(create_online_catalog_cli), name='create-online-catalog-connection')
    cmd_group.add_command(hide(list_connections_cli), name='list-connections')
    cmd_group.add_command(hide(get_connection_cli), name='get-connection')
    cmd_group.add_command(hide(delete_connection_cli), name='delete-connection')




    # Register command group.
    create_group.add_command(create_mysql_cli, name='mysql')
    create_group.add_command(create_postgresql_cli, name='postgresql')
    create_group.add_command(create_snowflake_cli, name='snowflake')
    create_group.add_command(create_redshift_cli, name='redshift')
    create_group.add_command(create_sqldw_cli, name='sqldw')
    create_group.add_command(create_sqlserver_cli, name='sqlserver')
    create_group.add_command(create_databricks_cli, name='databricks')
    create_group.add_command(create_online_catalog_cli, name='online-catalog')

    connections_group.add_command(create_group, name='create')
    connections_group.add_command(create_json, name='create-json')
    connections_group.add_command(list_connections_cli, name='list')
    connections_group.add_command(get_connection_cli, name='get')
    connections_group.add_command(delete_connection_cli, name='delete')
    cmd_group.add_command(connections_group, name='connection')
