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

# pylint:disable=redefined-outer-name

import pytest
from mock import mock

from click.testing import CliRunner
import databricks_cli.tokens.cli as cli
from tests.utils import provide_conf


@pytest.fixture()
def tokens_api_mock():
    with mock.patch('databricks_cli.tokens.cli.TokensApi') as TokensApi:
        _tokens_api_mock = mock.MagicMock()
        TokensApi.return_value = _tokens_api_mock
        yield _tokens_api_mock


@provide_conf
def test_create_token_cli_defaults(tokens_api_mock):
    runner = CliRunner()
    runner.invoke(cli.create_token_cli, ['--comment', 'test'])
    tokens_api_mock.create.assert_called_with(60 * 60 * 24 * 90, 'test')
