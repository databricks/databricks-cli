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

import databricks_cli.jobs.cli as cli
from databricks_cli.utils import pretty_format
from tests.utils import get_callback

CREATE_RETURN = {'job_id': 5}
CREATE_JSON = '{"name": "test_job"}'


def test_create_cli_json():
    with mock.patch('databricks_cli.jobs.cli.create_job') as create_job_mock:
        with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
            create_job_mock.return_value = CREATE_RETURN
            get_callback(cli.create_cli)(None, CREATE_JSON)
            assert create_job_mock.call_args[0][0] == json.loads(CREATE_JSON)
            assert echo_mock.call_args[0][0] == pretty_format(CREATE_RETURN)


def test_create_cli_json_file(tmpdir):
    path = tmpdir.join('job.json').strpath
    with open(path, 'w') as f:
        f.write(CREATE_JSON)
    with mock.patch('databricks_cli.jobs.cli.create_job') as create_job_mock:
        with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
            create_job_mock.return_value = CREATE_RETURN
            get_callback(cli.create_cli)(path, None)
            assert create_job_mock.call_args[0][0] == json.loads(CREATE_JSON)
            assert echo_mock.call_args[0][0] == pretty_format(CREATE_RETURN)


RESET_JSON = '{"job_name": "test_job"}'


def test_reset_cli_json():
    with mock.patch('databricks_cli.jobs.cli.reset_job') as reset_job_mock:
        get_callback(cli.reset_cli)(None, RESET_JSON, 1)
        assert reset_job_mock.call_args[0][0] == {
            'job_id': 1,
            'new_settings': json.loads(RESET_JSON)
        }


LIST_RETURN = {
    'jobs': [{
        'job_id': 1,
        'settings': {
            'name': 'b'
        }
    }, {
        'job_id': 2,
        'settings': {
            'name': 'a'
        }
    }, {
        'job_id': 30,
        'settings': {
            # Normally 'C' < 'a' < 'b' -- we should do case insensitive sorting though.
            'name': 'C'
        }
    }]
}


def test_list_jobs():
    with mock.patch('databricks_cli.jobs.cli.list_jobs') as list_jobs_mock:
        with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
            list_jobs_mock.return_value = LIST_RETURN
            get_callback(cli.list_cli)(None)
            # Output should be sorted here.
            rows = [(2, 'a'), (1, 'b'), (30, 'C')]
            assert echo_mock.call_args[0][0] == \
                tabulate(rows, tablefmt='plain', disable_numparse=True)


def test_list_jobs_output_json():
    with mock.patch('databricks_cli.jobs.cli.list_jobs') as list_jobs_mock:
        with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
            list_jobs_mock.return_value = LIST_RETURN
            get_callback(cli.list_cli)('json')
            assert echo_mock.call_args[0][0] == pretty_format(LIST_RETURN)


RUN_NOW_RETURN = {
    "number_in_job": 1,
    "run_id": 1
}
NOTEBOOK_PARAMS = '{"a": 1}'
JAR_PARAMS = '[1, 2, 3]'
PYTHON_PARAMS = '["python", "params"]'
SPARK_SUBMIT_PARAMS = '["--class", "org.apache.spark.examples.SparkPi"]'


def test_run_now_no_params():
    with mock.patch('databricks_cli.jobs.cli.run_now') as run_now_mock:
        with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
            run_now_mock.return_value = RUN_NOW_RETURN
            get_callback(cli.run_now_cli)(1, None, None, None, None)
            assert run_now_mock.call_args[0][0] == 1
            assert run_now_mock.call_args[0][1] is None
            assert run_now_mock.call_args[0][2] is None
            assert run_now_mock.call_args[0][3] is None
            assert run_now_mock.call_args[0][4] is None
            assert echo_mock.call_args[0][0] == pretty_format(RUN_NOW_RETURN)


def test_run_now_with_params():
    with mock.patch('databricks_cli.jobs.cli.run_now') as run_now_mock:
        with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
            run_now_mock.return_value = RUN_NOW_RETURN
            get_callback(cli.run_now_cli)(1, JAR_PARAMS, NOTEBOOK_PARAMS, PYTHON_PARAMS,
                                          SPARK_SUBMIT_PARAMS)
            assert run_now_mock.call_args[0][0] == 1
            assert run_now_mock.call_args[0][1] == json.loads(JAR_PARAMS)
            assert run_now_mock.call_args[0][2] == json.loads(NOTEBOOK_PARAMS)
            assert run_now_mock.call_args[0][3] == json.loads(PYTHON_PARAMS)
            assert run_now_mock.call_args[0][4] == json.loads(SPARK_SUBMIT_PARAMS)
            assert echo_mock.call_args[0][0] == pretty_format(RUN_NOW_RETURN)
