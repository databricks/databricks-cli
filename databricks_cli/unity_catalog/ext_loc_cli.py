# Databricks CLI
# Copyright 2021 Databricks, Inc.
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

from databricks_cli.click_types import JsonClickType
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.unity_catalog.api import UnityCatalogApi
from databricks_cli.unity_catalog.utils import mc_pretty_format
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, json_cli_base


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create External Location.')
@click.option('--name', default=None,
              help='Name of new external location')
@click.option('--url', default=None,
              help='Path URL for the new external location')
@click.option('--storage-credential-name', default=None,
              help='Name of storage credential to use with new external location')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/external-locations'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_location_cli(api_client, name, url, storage_credential_name, json_file, json):
    """
    Create new external location.

    Calls the 'createExternalLocation' RPC endpoint of the Unity Catalog service.
    The specification for the request json can be found at <insert doc link here>.
    Returns the properties of the newly-created Storage Credential.

    """
    if (name is not None) and (url is not None) and (storage_credential_name is not None):
        if (json_file is not None) or (json is not None):
            raise ValueError('Cannot specify JSON if both name and url are given')
        data = {"name": name, "url": url, "credential_name": storage_credential_name}
        loc_json = UnityCatalogApi(api_client).create_external_location(data)
        click.echo(mc_pretty_format(loc_json))
    elif (json is None) and (json_file is None):
        raise ValueError('Must provide name, url and storage-credential-name' +
                         ' or use JSON specification')
    else:
        json_cli_base(json_file, json,
                      lambda json:
                      UnityCatalogApi(api_client).create_external_location(json), encode_utf8=True)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List external locations.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_locations_cli(api_client, ):
    """
    List external locations.

    Calls the 'listExternalLocations' RPC endpoint of the Unity Catalog service.
    Returns array of ExternalLocations.

    """
    locs_json = UnityCatalogApi(api_client).list_external_locations()
    click.echo(mc_pretty_format(locs_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get an external location.')
@click.option('--name', required=True,
              help='Name of the external location to get.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_location_cli(api_client, name):
    """
    Get an external location.

    Calls the 'getExternalLocation' RPC endpoint of the Unity Catalog service.
    Returns an ExternalLocation object.

    """
    loc_json = UnityCatalogApi(api_client).get_external_location(name)
    click.echo(mc_pretty_format(loc_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update an external location.')
@click.option('--name', required=True,
              help='Name of the external location to update.')
@click.option('--force', '-f', is_flag=True, default=False,
              help='Force update even if location has dependent tables/mounts')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/external-locations'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_location_cli(api_client, name, force, json_file, json):
    """
    Update an external location.

    Calls the 'updateExternalLocation' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_external_location(name, force,
                                                                                    json),
                  encode_utf8=True)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete an external location.')
@click.option('--name', required=True,
              help='Name of the external location to delete.')
@click.option('--force', '-f', is_flag=True, default=False,
              help='Force deletion even if location has dependent tables/mounts')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_location_cli(api_client, name, force):
    """
    Delete an external location.

    Calls the 'deleteExternalLocation' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_external_location(name, force)


def register_ext_loc_commands(cmd_group):
    cmd_group.add_command(create_location_cli, name='create-external-location')
    cmd_group.add_command(list_locations_cli, name='list-external-locations')
    cmd_group.add_command(get_location_cli, name='get-external-location')
    cmd_group.add_command(update_location_cli, name='update-external-location')
    cmd_group.add_command(delete_location_cli, name='delete-external-location')
