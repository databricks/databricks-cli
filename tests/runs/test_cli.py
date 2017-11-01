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

import json
import mock
from tabulate import tabulate

import databricks_cli.runs.cli as cli
from databricks_cli.utils import pretty_format
from tests.utils import get_callback

SUBMIT_RETURN = {'run_id': 5}
SUBMIT_JSON = '{"name": "test_run"}'


def test_submit_cli_json():
    with mock.patch('databricks_cli.runs.cli.submit_run') as submit_run_mock:
        with mock.patch('databricks_cli.runs.cli.click.echo') as echo_mock:
            submit_run_mock.return_value = SUBMIT_RETURN
            get_callback(cli.submit_cli)(None, SUBMIT_JSON)
            assert submit_run_mock.call_args[0][0] == json.loads(SUBMIT_JSON)
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


def test_list_runs():
    with mock.patch('databricks_cli.runs.cli.list_runs') as list_runs_mock:
        with mock.patch('databricks_cli.runs.cli.click.echo') as echo_mock:
            list_runs_mock.return_value = LIST_RETURN
            get_callback(cli.list_cli)(None, None, None, None, None, None)
            rows = [(1, 'name', 'RUNNING', 'no_result_state', RUN_PAGE_URL)]
            assert echo_mock.call_args[0][0] == tabulate(rows, tablefmt='plain')


def test_list_runs_output_json():
    with mock.patch('databricks_cli.runs.cli.list_runs') as list_runs_mock:
        with mock.patch('databricks_cli.runs.cli.click.echo') as echo_mock:
            list_runs_mock.return_value = LIST_RETURN
            get_callback(cli.list_cli)(None, None, None, None, None, 'json')
            assert echo_mock.call_args[0][0] == pretty_format(LIST_RETURN)


def test_get_cli():
    with mock.patch('databricks_cli.runs.cli.get_run') as get_run_mock:
        with mock.patch('databricks_cli.runs.cli.click.echo') as echo_mock:
            get_run_mock.return_value = {}
            get_callback(cli.get_cli)(1)
            assert get_run_mock.call_args[0][0] == 1
            assert echo_mock.call_args[0][0] == pretty_format({})


def test_cancel_cli():
    with mock.patch('databricks_cli.runs.cli.cancel_run') as cancel_run_mock:
        with mock.patch('databricks_cli.runs.cli.click.echo') as echo_mock:
            cancel_run_mock.return_value = {}
            get_callback(cli.cancel_cli)(1)
            assert cancel_run_mock.call_args[0][0] == 1
            assert echo_mock.call_args[0][0] == pretty_format({})
