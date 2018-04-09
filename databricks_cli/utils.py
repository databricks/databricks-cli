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


import os
import subprocess
import sys
import tempfile
from json import dumps as json_dumps, loads as json_loads

import click
import six
from requests.exceptions import HTTPError

from databricks_cli.configure.provider import DEFAULT_SECTION

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
DEBUG_MODE = False


def eat_exceptions(function):
    @six.wraps(function)
    def decorator(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except HTTPError as exception:
            if exception.response.status_code == 401:
                error_and_quit('Your authentication information may be incorrect. Please '
                               'reconfigure with ``dbfs configure``')
            else:
                error_and_quit(exception.response.content)
        except Exception as exception: # noqa
            if not DEBUG_MODE:
                error_and_quit('{}: {}'.format(type(exception).__name__, str(exception)))
    decorator.__doc__ = function.__doc__
    return decorator


def error_and_quit(message):
    click.echo('Error: {}'.format(message))
    sys.exit(1)


def pretty_format(json):
    return json_dumps(json, indent=2)


def json_cli_base(json_file, json, api):
    """
    Takes json_file or json string and calls an function "api" with the json
    deserialized
    """
    if not (json_file is None) ^ (json is None):
        raise RuntimeError('Either --json-file or --json should be provided')
    if json_file:
        with open(json_file, 'r') as f:
            json = f.read()
    res = api(json_loads(json))
    click.echo(pretty_format(res))


def translate_value(value, no_strip):
    """
    Generate content based on format of value. If value is None or empty string, open a temporary
    file for user to input the content. If value starts with a '@', treat the rest of string as
    a path and read content from file. Otherwise, use the value itself as content.
    Strip the trailing '\n' unless a no_strip flag is set to True.
    """
    if value is None or len(value) == 0:
        editor = os.environ.get('EDITOR', 'vim')
        with tempfile.NamedTemporaryFile(suffix=".tmp") as tf:
            tf.write("# Delete this line and paste your secret value."
                     " Any trailing new lines will be stripped (unless --no-strip specified).")
            tf.flush()
            subprocess.call([editor, tf.name])

            # reopen file to ensure we read the content user input
            with open(tf.name) as f:
                content = f.read()
    elif value.startswith('@'):
        filepath = os.path.expanduser(value[1:])
        try:
            with open(filepath, 'r') as f:
                content = f.read()
        except IOError:
            error_and_quit('Failed to read from file "{}"'.format(filepath))
    else:
        content = value

    if not no_strip:
        content = content.rstrip('\n')
    return content


def truncate_string(s, length=100):
    if len(s) <= length:
        return s
    return s[:length] + '...'


class InvalidConfigurationError(RuntimeError):
    def __init__(self, profile=DEFAULT_SECTION):
        if profile == DEFAULT_SECTION:
            message = ('You haven\'t configured the CLI yet! '
                       'Please configure by entering `{} configure`'.format(sys.argv[0]))
        else:
            message = ('You haven\'t configured the CLI yet for the profile {profile}! '
                       'Please configure by entering '
                       '`{argv} configure --profile {profile}`').format(
                profile=profile, argv=sys.argv[0])
        super(InvalidConfigurationError, self).__init__(message)
