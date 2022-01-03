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
import subprocess
import uuid
import click
import requests
import six

from databricks_cli.click_types import ContextObject
from databricks_cli.configure.provider import get_config, ProfileConfigProvider, \
    AZURE_ENVIRONMENTS, AZURE_DEFAULT_AD_ENDPOINT
from databricks_cli.utils import InvalidConfigurationError
from databricks_cli.sdk import ApiClient

AZURE_METADATA_SERVICE_TOKEN_URL = "http://169.254.169.254/metadata/identity/oauth2/token"
AZURE_METADATA_SERVICE_INSTANCE_URL = "http://169.254.169.254/metadata/instance"
AZURE_MANAGEMENT_ENDPOINT = "https://management.core.windows.net/"
AZURE_TOKEN_SERVICE_URL = "{}/{}/oauth2/token"
DEFAULT_DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
AAD_TIMEOUT_SECONDS = 10


def provide_api_client(function):
    """
    Injects the api_client keyword argument to the wrapped function.
    All callbacks wrapped by provide_api_client expect the argument ``profile`` to be passed in.
    """
    @six.wraps(function)
    def decorator(*args, **kwargs):
        ctx = click.get_current_context()
        command_name = "-".join(ctx.command_path.split(" ")[1:])
        command_name += "-" + str(uuid.uuid1())
        profile = get_profile_from_context()
        if profile:
            # If we request a specific profile, only get credentials from tere.
            config = ProfileConfigProvider(profile).get_config()
        else:
            # If unspecified, use the default provider, or allow for user overrides.
            config = get_config()
        if not config or not config.is_valid:
            raise InvalidConfigurationError.for_profile(profile)
        kwargs['api_client'] = _get_api_client(config, command_name)

        return function(*args, **kwargs)
    decorator.__doc__ = function.__doc__
    return decorator


def get_profile_from_context():
    ctx = click.get_current_context()
    context_object = ctx.ensure_object(ContextObject)
    return context_object.get_profile()


def debug_option(f):
    def callback(ctx, param, value): #  NOQA
        context_object = ctx.ensure_object(ContextObject)
        context_object.set_debug(value)
    return click.option('--debug', is_flag=True, callback=callback,
                        expose_value=False, help="Debug Mode. Shows full stack trace on error.")(f)


def profile_option(f):
    def callback(ctx, param, value): #  NOQA
        if value is not None:
            context_object = ctx.ensure_object(ContextObject)
            context_object.set_profile(value)
    return click.option('--profile', required=False, default=None, callback=callback,
                        expose_value=False,
                        help='CLI connection profile to use. The default profile is "DEFAULT".')(f)


def _get_aad_token_az_cli():
    cmd_line = ["az", "account", "get-access-token", "-o", "tsv", "--query", "accessToken",
                "--resource", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"]

    proc = subprocess.Popen(cmd_line, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        if six.PY2:
            outs, errs = proc.communicate()
        else:
            outs, errs = proc.communicate(timeout=15)  # NOQA
        if proc.returncode != 0:
            raise RuntimeError('[ERROR] Error executing az-cli. Code: %d. Message: \'%s\'\n' %
                               proc.returncode, errs.decode())
        return outs.decode().strip()
    except subprocess.TimeoutExpired:
        proc.kill()
        raise RuntimeError('[ERROR] Timeout executing az-cli\n')


def _create_aad_token(config, resource):
    if config.use_azure_msi_auth:
        params = {
            "api-version": "2018-02-01",
            "resource": resource,
        }
        response = requests.get(
            AZURE_METADATA_SERVICE_TOKEN_URL,
            params=params,
            headers={"Metadata": "true"},
            timeout=AAD_TIMEOUT_SECONDS)
    else:
        params = {
            "grant_type": "client_credentials",
            "client_id": config.azure_client_id,
            "resource": resource,
            "client_secret": config.azure_client_secret,
        }
        azure_host = AZURE_ENVIRONMENTS.get((config.azure_environment or "").lower(),
                                            AZURE_DEFAULT_AD_ENDPOINT)
        response = requests.post(
            AZURE_TOKEN_SERVICE_URL.format(azure_host, config.azure_tenant_id),
            data=params,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=AAD_TIMEOUT_SECONDS)

    response.raise_for_status()
    jsn = response.json()
    if 'access_token' not in jsn or jsn.get('token_type') != 'Bearer' or 'expires_on' not in jsn:
        raise RuntimeError("Can't get necessary data from AAD token: " + jsn)

    return jsn['access_token']


def _get_aad_token_and_headers(config):
    headers = {}
    if config.azure_resource_id:
        headers['X-Databricks-Azure-Workspace-Resource-Id'] = config.azure_resource_id
        mgmt_token = _create_aad_token(config, AZURE_MANAGEMENT_ENDPOINT)
        headers['X-Databricks-Azure-SP-Management-Token'] = mgmt_token

    return _create_aad_token(config, DEFAULT_DATABRICKS_SCOPE), headers


def _get_api_client(config, command_name=""):
    verify = config.insecure is None
    if config.is_valid_with_azure_cli_auth:
        # print("Using Azure CLI authentication")
        return ApiClient(host=config.host, token=_get_aad_token_az_cli(), verify=verify,
                         command_name=command_name, jobs_api_version=config.jobs_api_version)
    elif config.is_valid_with_azure_msi_auth:
        # print("Using Azure MSI authentication")
        config.use_azure_msi_auth = True
        try:
            jsn = requests.get(
                AZURE_METADATA_SERVICE_INSTANCE_URL, params={"api-version": "2021-02-01"},
                headers={"Metadata": "true"}, timeout=2).json()
            if 'compute' not in jsn or 'azEnvironment' not in jsn['compute']:
                raise RuntimeError(
                    "Was able to fetch some metadata, but it doesn't look like Azure Metadata: "
                    + str(jsn)
                )
        except (requests.RequestException, ValueError) as e:
            raise RuntimeError("Can't reach Azure Metadata Service: " + str(e))

        aad_token, headers = _get_aad_token_and_headers(config)
        return ApiClient(host=config.host, token=aad_token, verify=verify, default_headers=headers,
                         command_name=command_name, jobs_api_version=config.jobs_api_version)
    elif config.is_valid_with_azure_client_auth:
        # print("Using Azure SPN authentication")
        aad_token, headers = _get_aad_token_and_headers(config)
        return ApiClient(host=config.host, token=aad_token, verify=verify, default_headers=headers,
                         command_name=command_name, jobs_api_version=config.jobs_api_version)
    elif config.is_valid_with_token:
        # print("Using host/token auth")
        return ApiClient(host=config.host, token=config.token, verify=verify,
                         command_name=command_name, jobs_api_version=config.jobs_api_version)
    return ApiClient(user=config.username, password=config.password,
                     host=config.host, verify=verify, command_name=command_name,
                     jobs_api_version=config.jobs_api_version)
