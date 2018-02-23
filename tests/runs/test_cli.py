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

import json
import mock
import pytest
from tabulate import tabulate
from click.testing import CliRunner

import databricks_cli.runs.cli as cli
from databricks_cli.utils import pretty_format
from tests.utils import provide_conf

SUBMIT_RETURN = {'run_id': 5}
SUBMIT_JSON = '{"name": "test_run"}'


@pytest.fixture()
def runs_api_mock():
    with mock.patch('databricks_cli.runs.cli.RunsApi') as RunsApiMock:
        _runs_api_mock = mock.MagicMock()
        RunsApiMock.return_value = _runs_api_mock
        yield _runs_api_mock


@provide_conf
def test_submit_cli_json(runs_api_mock):
    with mock.patch('databricks_cli.runs.cli.click.echo') as echo_mock:
        runs_api_mock.submit_run.return_value = SUBMIT_RETURN
        runner = CliRunner()
        runner.invoke(cli.submit_cli, ['--json', SUBMIT_JSON])
        assert runs_api_mock.submit_run.call_args[0][0] == json.loads(SUBMIT_JSON)
        assert echo_mock.call_args[0][0] == pretty_format(SUBMIT_RETURN)


RUN_PAGE_URL = 'https://databricks.com/#job/1/run/1'
LIST_RETURN = {
    'runs': [{
        'run_id': 1,
        'run_name': 'name',
        'state': {
            'life_cycle_state': 'RUNNING'
        },
        'run_page_url': RUN_PAGE_URL
    }]
}


@provide_conf
def test_list_runs(runs_api_mock):
    with mock.patch('databricks_cli.runs.cli.click.echo') as echo_mock:
        runs_api_mock.list_runs.return_value = LIST_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli)
        rows = [(1, 'name', 'RUNNING', 'n/a', RUN_PAGE_URL)]
        assert echo_mock.call_args[0][0] == tabulate(rows, tablefmt='plain')


@provide_conf
def test_list_runs_output_json(runs_api_mock):
    with mock.patch('databricks_cli.runs.cli.click.echo') as echo_mock:
        runs_api_mock.list_runs.return_value = LIST_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli, ['--output', 'json'])
        assert echo_mock.call_args[0][0] == pretty_format(LIST_RETURN)


@provide_conf
def test_get_cli(runs_api_mock):
    with mock.patch('databricks_cli.runs.cli.click.echo') as echo_mock:
        runs_api_mock.get_run.return_value = {}
        runner = CliRunner()
        runner.invoke(cli.get_cli, ['--run-id', 1])
        assert runs_api_mock.get_run.call_args[0][0] == 1
        assert echo_mock.call_args[0][0] == pretty_format({})


@provide_conf
def test_cancel_cli(runs_api_mock):
    with mock.patch('databricks_cli.runs.cli.click.echo') as echo_mock:
        runs_api_mock.cancel_run.return_value = {}
        runner = CliRunner()
        runner.invoke(cli.cancel_cli, ['--run-id', 1])
        assert runs_api_mock.cancel_run.call_args[0][0] == 1
        assert echo_mock.call_args[0][0] == pretty_format({})
