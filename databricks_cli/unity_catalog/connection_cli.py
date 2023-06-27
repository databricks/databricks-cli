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
import http.server
import logging
import os
import socketserver
import ssl
import webbrowser
import oauthlib.oauth2
from oauthlib.common import generate_token


import click

from databricks_cli.click_types import JsonClickType
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.unity_catalog.api import UnityCatalogApi
from databricks_cli.unity_catalog.utils import hide, json_file_help, json_string_help, \
    mc_pretty_format
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, json_cli_base

# These two options are shared among create and updates, so they are very common

logging.getLogger("http.server").setLevel(logging.WARNING)

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


redirect_uri = 'https://localhost:8771'
return_query_res = ""

class AccessCodeRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        script_path = os.path.abspath(__file__)
        html_file_path = os.path.join(os.path.dirname(script_path), 'response.html')

        with open(html_file_path, 'rb') as file:
            self.wfile.write(file.read())

        global return_query_res
        return_query_res = self.path

def run_oauth_response_server():
    server_address = ('', 8771)
    httpd = socketserver.TCPServer(server_address, AccessCodeRequestHandler)

    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(certfile="localhost.pem", keyfile="localhost-key.pem")
    httpd.socket = ssl_context.wrap_socket(httpd.socket, server_side=True)
    
    try:
        httpd.handle_request()
    except KeyboardInterrupt:
        pass

def get_auth_code(host, client_id, scope):
    oauth = oauthlib.oauth2.WebApplicationClient(client_id)
    state = generate_token()
    verifier = oauth.create_code_verifier(96)
    challenge = oauth.create_code_challenge(verifier, 'S256')

    authorization_url = oauth.prepare_request_uri(
            'https://' + host + '/oauth/authorize', redirect_uri = redirect_uri, 
            scope = scope, state = state, code_challenge = challenge, 
            code_challenge_method = 'S256')
    
    webbrowser.open_new(authorization_url)
    run_oauth_response_server()
    parsed_result =  oauth.parse_request_uri_response(redirect_uri + return_query_res, 
                                                      state = state)
    parsed_result['code_verifier'] = verifier
    return parsed_result


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create mysql connection with CLI flags.')
@common_create_args
@create_update_common_options
@click.option('--user', default=None,
                help='Username for authorization of new connection')
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
@click.option('--user', default=None,
                help='Username for authorization of new connection')
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
@click.option('--user', default=None,
                help='Username for authorization of new connection')
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
@click.option('--user', default=None,
                help='Username for authorization of new connection')
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
@click.option('--user', default=None,
                help='Username for authorization of new connection')
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
@click.option('--user', default=None,
                help='Username for authorization of new connection')
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

#OAuth CLIs below

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create snowflake oauth connection with CLI flags.')
@common_create_args
@create_update_common_options
@click.option('--sfwarehouse', default=None,
                help='Snowflake warehouse name of new connection')
@click.option('--scope', default=None,
                help='Scope of new OAuth connection. Should be a single \
                quoted string with separate options separated by spaces')
@click.option('--client-id', default=None,
                help='Client ID for new connection')
@click.option(
    "--client-secret", prompt=True, hide_input=True)
@debug_option
@eat_exceptions
@profile_option
@provide_api_client
def create_snowflake_oauth_cli(api_client, name, host, port, client_id, sfwarehouse,
                        read_only, comment, scope, client_secret):
    """
    Create new snowflake U2M oauth connection.
    """
    missing_args = (name is None) or (host is None) or (port is None) or (client_id is None) or \
        (client_secret is None) or (sfwarehouse is None)

    if missing_args:
        raise ValueError('Must provide all required oauth connection parameters')
    
    code_dict = get_auth_code(host, client_id, scope)
    data = {
        'name': name,
        'connection_type': 'SNOWFLAKE',
        'options': {'host': host, 'port': port, 'client_id': client_id, 
                    'client_secret': client_secret, 'state': code_dict['state'], 
                    'code': code_dict['code'], 'sfWarehouse': sfwarehouse, 
                    'redirect_uri': redirect_uri, 'code_verifier': code_dict['code_verifier']},
        'read_only': read_only,
        'comment': comment
    }
    click.echo(mc_pretty_format(data))
    UnityCatalogApi(api_client).create_connection(data)
    #click.echo(mc_pretty_format(con_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create connection with a JSON input.')
@json_options
@debug_option
@eat_exceptions
@profile_option
@provide_api_client
def create_json(api_client, json_file, json):
    '''
    Create new connection with an inline JSON or JSON file path.
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
def list_connections_cli(api_client):
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


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update a connection.')
@click.option('--name', required=True,
              help='Name of the connection to update.')
@click.option('--new-name', default=None, help='New name of the connection.')
@create_update_common_options
@click.option('--owner', default=None,
              help='Owner of the connection.')

@click.option('--json-file', default=None, type=click.Path(),
              help=json_file_help(method='PATCH', path='/connections/{name}'))
@click.option('--json', default=None, type=JsonClickType(),
              help=json_string_help(method='PATCH', path='/connections/{name}'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_connection_cli(api_client, name, new_name, read_only,
                        comment, owner, json_file, json):
    """
    Update an connection.

    The public specification for the JSON request is in development.
    """
    if ((new_name is not None) or
        (read_only is not None) or (comment is not None)):
        if (json_file is not None) or (json is not None):
            raise ValueError('Cannot specify JSON if any other update flags are specified')
        data = {
            'name': new_name,
            'read_only': read_only,
            'comment': comment,
            'owner': owner
        }
        loc_json = UnityCatalogApi(api_client).update_connnection(
            name, data)
        click.echo(mc_pretty_format(loc_json))
    else:
        json_cli_base(json_file, json,
                      lambda json: UnityCatalogApi(api_client).update_connnection(name, json),
                      encode_utf8=True)
        
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
    cmd_group.add_command(hide(create_snowflake_oauth_cli), 
                          name='create-snowflake-oauth-connection')
    cmd_group.add_command(hide(create_redshift_cli), name='create-redshift-connection')
    cmd_group.add_command(hide(create_sqldw_cli), name='create-sqldw-connection')
    cmd_group.add_command(hide(create_sqlserver_cli), name='create-sqlserver-connection')
    cmd_group.add_command(hide(create_databricks_cli), name='create-databricks-connection')
    cmd_group.add_command(hide(list_connections_cli), name='list-connections')
    cmd_group.add_command(hide(get_connection_cli), name='get-connection')
    cmd_group.add_command(hide(delete_connection_cli), name='delete-connection')
    cmd_group.add_command(hide(update_connection_cli), name='update-connection')
    




    # Register command group.
    create_group.add_command(create_mysql_cli, name='mysql')
    create_group.add_command(create_postgresql_cli, name='postgresql')
    create_group.add_command(create_snowflake_cli, name='snowflake')
    create_group.add_command(create_snowflake_oauth_cli, name='snowflake-oauth')
    create_group.add_command(create_redshift_cli, name='redshift')
    create_group.add_command(create_sqldw_cli, name='sqldw')
    create_group.add_command(create_sqlserver_cli, name='sqlserver')
    create_group.add_command(create_databricks_cli, name='databricks')

    connections_group.add_command(create_group, name='create')
    connections_group.add_command(create_json, name='create-json')
    connections_group.add_command(list_connections_cli, name='list')
    connections_group.add_command(get_connection_cli, name='get')
    connections_group.add_command(delete_connection_cli, name='delete')
    connections_group.add_command(update_connection_cli, name='update')
    cmd_group.add_command(connections_group, name='connection')
