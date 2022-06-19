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

# pylint:disable=redefined-outer-name

from click.testing import CliRunner

from databricks_cli.configure.config import get_config
import databricks_cli.unity_catalog.configure_cli as cli
from tests.utils import provide_conf


@provide_conf
def test_configure():
    runner = CliRunner()
    runner.invoke(cli.configure_cli, [])
    assert get_config().uc_api_version is None

    runner.invoke(cli.configure_cli, ['--version=2.0'])
    assert get_config().uc_api_version == '2.0'

    runner.invoke(cli.configure_cli, ['--version=2.1'])
    assert get_config().uc_api_version == '2.1'
