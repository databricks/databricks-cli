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

import click

from databricks_cli.click_types import JsonClickType
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.unity_catalog.api import UnityCatalogApi
from databricks_cli.unity_catalog.utils import del_none, mc_pretty_format, hide
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, json_cli_base


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create External Location.')
@click.option('--name', default=None,
              help='Name of new external location')
@click.option('--url', default=None,
              help='Path URL for the new external location')
@click.option('--storage-credential-name', default=None,
              help='Name of storage credential to use with new external location')
@click.option('--skip-validation', '-s', 'skip_val', is_flag=True, default=False,
              help='Skip the validation of location\'s storage credential before creation')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/external-locations'))
@debug_option
@profile_option
# UC's createExternalLocation returns a 401 when the validation of the external location's
# storage credential fails; that translates to a misleading error when eat_exceptions is enabled:
#   Your authentication information may be incorrect. Please reconfigure with ``dbfs configure``
# Until that is fixed (should return a 400), show full error trace.
#@eat_exceptions
@provide_api_client
def create_location_cli(api_client, name, url, storage_credential_name, skip_val, json_file, json):
    """
    Create new external location.

    Calls the 'createExternalLocation' RPC endpoint of the Unity Catalog service.
    The public specification for the JSON request is in development.
    Returns the properties of the newly-created Storage Credential.

    """
    if (name is not None) and (url is not None) and (storage_credential_name is not None):
        if (json_file is not None) or (json is not None):
            raise ValueError('Cannot specify JSON if both name and url are given')
        data = {"name": name, "url": url, "credential_name": storage_credential_name}
        loc_json = UnityCatalogApi(api_client).create_external_location(data, skip_val)
        click.echo(mc_pretty_format(loc_json))
    elif (json is None) and (json_file is None):
        raise ValueError('Must provide name, url and storage-credential-name' +
                         ' or use JSON specification')
    else:
        json_cli_base(json_file, json,
                      lambda json:
                      UnityCatalogApi(api_client).create_external_location(json, skip_val),
                      encode_utf8=True)


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
@click.option('--skip-validation', '-s', 'skip_val', is_flag=True, default=False,
              help='Skip the validation of location\'s storage credential before creation')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/external-locations'))
@debug_option
@profile_option
# See comment for create_location_cli
#@eat_exceptions
@provide_api_client
def update_location_cli(api_client, name, force, skip_val, json_file, json):
    """
    Update an external location.

    Calls the 'updateExternalLocation' RPC endpoint of the Unity Catalog service.
    The public specification for the JSON request is in development.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_external_location(name, json,
                                                                                    force,
                                                                                    skip_val),
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


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Validate a external location/credential pair')
@click.option('--name', default=None,
              help='Name of the external location to validate.')
@click.option('--url', default=None,
              help='A storage URL to validate.')
@click.option('--cred-name', default=None,
              help='Name of the storage credential to use for validation.')
@click.option('--cred-aws-iam-role', default=None,
              help='An aws role to validate')
@click.option('--cred-az-directory-id', default=None,
              help='An Azure directory id to validate')
@click.option('--cred-az-application-id', default=None,
              help='An Azure application id to validate')
@click.option('--cred-az-client-secret', default=None,
              help='An Azure directory id to validate')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def validate_location_cli(api_client, name, url, cred_name, cred_aws_iam_role, cred_az_directory_id,
                          cred_az_application_id, cred_az_client_secret):
    """
    Validate an external location/credential combination.

    Calls the 'validateExternalLocation' RPC endpoint of the Unity Catalog service.
    This call will attempt to read/list/write/delete with the given credentials and
    external location.

    One of name/url must be provided. If both are specified, the given credential
    name will be excluded from path overlap checks (used to validate a potential
    update of that credential).

    One of cred-name, or cloud provider specific credential parameters must be
    provided.
    """
    validation_spec = {
        "external_location_name": name,
        "url": url,
        "storage_credential_name": cred_name,
    }
    if cred_aws_iam_role is not None:
        validation_spec["aws_iam_role"] = {
            "role_arn": cred_aws_iam_role
        }

    if cred_az_directory_id is not None:
        validation_spec["azure_service_principal"] = {
            "directory_id": cred_az_directory_id,
            "application_id": cred_az_application_id,
            "client_secret": cred_az_client_secret
        }
    del_none(validation_spec)
    validation_json = UnityCatalogApi(api_client).validate_external_location(validation_spec)
    click.echo(mc_pretty_format(validation_json))


@click.group()
def external_locations_group():  # pragma: no cover
    pass


def register_ext_loc_commands(cmd_group):
    # Register deprecated "verb-noun" commands for backward compatibility.
    cmd_group.add_command(hide(create_location_cli), name='create-external-location')
    cmd_group.add_command(hide(list_locations_cli), name='list-external-locations')
    cmd_group.add_command(hide(get_location_cli), name='get-external-location')
    cmd_group.add_command(hide(update_location_cli), name='update-external-location')
    cmd_group.add_command(hide(delete_location_cli), name='delete-external-location')
    cmd_group.add_command(hide(validate_location_cli), name='validate-external-location')

    # Register command group.
    external_locations_group.add_command(create_location_cli, name='create')
    external_locations_group.add_command(list_locations_cli, name='lists')
    external_locations_group.add_command(get_location_cli, name='get')
    external_locations_group.add_command(update_location_cli, name='update')
    external_locations_group.add_command(delete_location_cli, name='delete')
    external_locations_group.add_command(validate_location_cli, name='validate')
    cmd_group.add_command(external_locations_group, name='external-locations')
