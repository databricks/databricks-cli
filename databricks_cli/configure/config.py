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
import six

from databricks_cli.click_types import ContextObject
from databricks_cli.configure.provider import get_config_for_profile
from databricks_cli.utils import InvalidConfigurationError
from databricks_cli.sdk import ApiClient


def provide_api_client(function):
    """
    Injects the api_client keyword argument to the wrapped function.
    All callbacks wrapped by provide_api_client expect the argument ``profile`` to be passed in.
    """
    @six.wraps(function)
    def decorator(*args, **kwargs):
        profile = get_profile_from_context()
        config = get_config_for_profile(profile)
        if not config.is_valid:
            raise InvalidConfigurationError(profile)
        kwargs['api_client'] = _get_api_client(config)

        return function(*args, **kwargs)
    decorator.__doc__ = function.__doc__
    return decorator


def get_profile_from_context():
    ctx = click.get_current_context()
    context_object = ctx.ensure_object(ContextObject)
    return context_object.get_profile()


def profile_option(f):
    def callback(ctx, param, value): #  NOQA
        if value is not None:
            context_object = ctx.ensure_object(ContextObject)
            context_object.set_profile(value)
    return click.option('--profile', required=False, default=None, callback=callback,
                        expose_value=False,
                        help='CLI connection profile to use. The default profile is "DEFAULT".')(f)


def _get_api_client(config):
    if config.is_valid_with_token:
        return ApiClient(host=config.host, token=config.token)
    return ApiClient(user=config.username, password=config.password,
                     host=config.host)
