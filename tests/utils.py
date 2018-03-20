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
import decorator
from click.testing import CliRunner

from databricks_cli.configure.provider import DatabricksConfig, DEFAULT_SECTION, \
    update_and_persist_config

TEST_PROFILE = 'test-profile'


def provide_conf(test):
    def wrapper(test, *args, **kwargs):
        config = DatabricksConfig.from_token('test-host', 'test-token')
        update_and_persist_config(DEFAULT_SECTION, config)
        config = DatabricksConfig.from_token('test-host-2', 'test-token-2')
        update_and_persist_config(TEST_PROFILE, config)
        return test(*args, **kwargs)
    return decorator.decorator(wrapper, test)


def assert_cli_output(actual, expected):
    """
    Take runner stdout and assert it's value against an expected string. This just means appending
    a newline to the expected string since ``click.echo`` adds a newline to the output.

    >>> runner = CliRunner()
    >>> res = runner.invoke(cli.list_cli, ['--cluster-id', TEST_CLUSTER_ID])
    >>> assert_cli_output(res.output, 'EXPECTED-OUTPUT')
    """
    assert actual == expected + '\n'


def invoke_cli_runner(*args, **kwargs):
    """
    Helper method to invoke the CliRunner while asserting that the exit code is actually 0.
    """
    res = CliRunner().invoke(*args, **kwargs)
    assert res.exit_code == 0, 'Exit code was not 0. Output is: {}'.format(res.output)
    return res
