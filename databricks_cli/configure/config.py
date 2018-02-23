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

import sys

import click
import six

from databricks_cli.configure.provider import DEFAULT_SECTION, get_config_for_profile
from databricks_cli.utils import error_and_quit
from databricks_cli.sdk import ApiClient


def require_config(function):
    """
    All callbacks wrapped by require_config expect the argument ``profile`` to be passed in.
    """
    @six.wraps(function)
    def decorator(*args, **kwargs):
        profile = kwargs.pop('profile')
        config = get_config_for_profile(profile)
        if not config.is_valid:
            if profile == DEFAULT_SECTION:
                error_and_quit(('You haven\'t configured the CLI yet! ' +
                                'Please configure by entering `{} configure`').format(sys.argv[0]))
            else:
                error_and_quit(('You haven\'t configured the CLI yet for the ' +
                                'profile {}! '.format(profile) +
                                'Please configure by entering `{} configure --profile {}`').format(sys.argv[0], profile)) # noqa
        kwargs['api_client'] = _get_api_client(config)

        return function(*args, **kwargs)
    decorator.__doc__ = function.__doc__
    return decorator


def profile_option(f):
    return click.option('--profile', required=False, default=DEFAULT_SECTION,
                        help='Connection configuration to use.')(f)


def _get_api_client(config):
    if config.is_valid_with_token:
        return ApiClient(host=config.host, token=config.token)
    return ApiClient(user=config.username, password=config.password,
                     host=config.host)
