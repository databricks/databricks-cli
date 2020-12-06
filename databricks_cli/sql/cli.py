# Databricks CLI
# Copyright 2020 Databricks, Inc.
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

from json import loads as json_loads

import click
from tabulate import tabulate
import uuid

from databricks_cli.click_types import OutputClickType, JsonClickType, OneOfOption, \
    SqlQueryClickType, SqlEndpointNameClickType, ContextObject, MissingParameter
from databricks_cli.sql.api import SqlApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, json_cli_base, \
    truncate_string
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.version import print_version_callback, version

def print_query_results(results, output_type):
    if OutputClickType.is_json(output_type):
        click.echo(pretty_format(results))
        return

    cols = results['columns']
    col_names = [ c['friendly_name'] for c in cols ]
    rows = []
    for r in results['rows']:
        newrow = [ r[c['name']] for c in cols ]
        rows += [ newrow ]

    click.echo(' ')  # blank line before table
    click.echo(tabulate(rows, headers=col_names, tablefmt='simple', disable_numparse=True))

QUERY_OPTIONS = ['query', 'query-file']

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Perform an SQL query.')
@click.option('--endpoint', '-e', default=None, required=True, type=SqlEndpointNameClickType(),
              help='Name of the SQL endpoint.')
@click.option('--query', cls=OneOfOption, one_of=QUERY_OPTIONS, type=SqlQueryClickType(),
              help='Text of the SQL query to perform.')
@click.option('--query-file', cls=OneOfOption, one_of=QUERY_OPTIONS, type=SqlQueryClickType(),
              help='File containing the text of the SQL query to perform.')
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def query_cli(api_client, endpoint, query, query_file, output):
    """
    Perform an SQL Query.

    """
    if query_file:
        with open(query_file, 'r') as f:
            query = f.read()
        click.echo("query: %s" % query)

    ctx = click.get_current_context()
    context_object = ctx.ensure_object(ContextObject)

    api_client.do_login()

    endpoints = SqlApi(api_client).list_data_sources()
    endpoint_list = [ e for e in endpoints if e["name"] == endpoint ]
    if len(endpoint_list) == 0:
        raise RuntimeError("Did not find endpoint with name: %s" % (endpoint))
    chosen_endpoint = endpoint_list[0]

    if context_object.debug_mode:
        click.echo('Endpoint:\n' + pretty_format(chosen_endpoint))

    data_source_id = chosen_endpoint['id']

    query_output_json = SqlApi(api_client).do_query(data_source_id, query)
    results_data = query_output_json['query_result']['data']

    print_query_results(results_data, output)


def print_endpoints(endpoints, all_fields, output_type):
    if not all_fields:   # Names only
        names = [ e['name'] for e in endpoints ]
        if OutputClickType.is_json(output_type):
            click.echo(pretty_format(names))
        else:
            table = [ [name] for name in names ]
            click.echo(' ')  # blank line before results
            click.echo(tabulate(table, headers=['name'], tablefmt='simple', disable_numparse=True))
        return

    if OutputClickType.is_json(output_type):
        click.echo(pretty_format(endpoints))
        return

    col_names = sorted(endpoints[0].keys())
    rows = []
    for r in endpoints:
        newrow = []
        for c in col_names:
            newrow.append(r.get(c, '--'))   # Some endpoints don't have all col values
        rows += [ newrow ]

    click.echo(' ')  # blank line before results
    click.echo(tabulate(rows, headers=col_names, tablefmt='simple', disable_numparse=True))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List SQL Endpoints.')
@click.option('--all-fields', '-a', is_flag=True, default=False,
              help='Show all endpont fields (not just names)')
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_endpoints_cli(api_client, all_fields, output):
    """
    List SQL Endpoints

    """
    api_client.do_login()

    endpoints = SqlApi(api_client).list_data_sources()
    print_endpoints(endpoints, all_fields, output)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to run SQL queries.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def sql_group():  # pragma: no cover
    """
    Utility to interact with SQL Analytics.

    """
    pass


sql_group.add_command(query_cli, name='query')
sql_group.add_command(list_endpoints_cli, name='list-endpoints')
