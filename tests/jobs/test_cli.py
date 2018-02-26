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

import databricks_cli.jobs.cli as cli
from databricks_cli.utils import pretty_format
from tests.utils import provide_conf

CREATE_RETURN = {'job_id': 5}
CREATE_JSON = '{"name": "test_job"}'


@pytest.fixture()
def jobs_api_mock():
    with mock.patch('databricks_cli.jobs.cli.JobsApi') as JobsApiMock:
        _jobs_api_mock = mock.MagicMock()
        JobsApiMock.return_value = _jobs_api_mock
        yield _jobs_api_mock


@provide_conf
def test_create_cli_json(jobs_api_mock):
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        jobs_api_mock.create_job.return_value = CREATE_RETURN
        runner = CliRunner()
        runner.invoke(cli.create_cli, ['--json', CREATE_JSON])
        assert jobs_api_mock.create_job.call_args[0][0] == json.loads(CREATE_JSON)
        assert echo_mock.call_args[0][0] == pretty_format(CREATE_RETURN)


@provide_conf
def test_create_cli_json_file(jobs_api_mock, tmpdir):
    path = tmpdir.join('job.json').strpath
    with open(path, 'w') as f:
        f.write(CREATE_JSON)
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        jobs_api_mock.create_job.return_value = CREATE_RETURN
        runner = CliRunner()
        runner.invoke(cli.create_cli, ['--json-file', path])
        assert jobs_api_mock.create_job.call_args[0][0] == json.loads(CREATE_JSON)
        assert echo_mock.call_args[0][0] == pretty_format(CREATE_RETURN)


RESET_JSON = '{"job_name": "test_job"}'


@provide_conf
def test_reset_cli_json(jobs_api_mock):
    runner = CliRunner()
    runner.invoke(cli.reset_cli, ['--json', RESET_JSON, '--job-id', 1])
    assert jobs_api_mock.reset_job.call_args[0][0] == {
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


@provide_conf
def test_list_jobs(jobs_api_mock):
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        jobs_api_mock.list_jobs.return_value = LIST_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli)
        # Output should be sorted here.
        rows = [(2, 'a'), (1, 'b'), (30, 'C')]
        assert echo_mock.call_args[0][0] == \
            tabulate(rows, tablefmt='plain', disable_numparse=True)


@provide_conf
def test_list_jobs_output_json(jobs_api_mock):
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        jobs_api_mock.list_jobs.return_value = LIST_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli, ['--output', 'json'])
        assert echo_mock.call_args[0][0] == pretty_format(LIST_RETURN)


RUN_NOW_RETURN = {
    "number_in_job": 1,
    "run_id": 1
}
NOTEBOOK_PARAMS = '{"a": 1}'
JAR_PARAMS = '[1, 2, 3]'
PYTHON_PARAMS = '["python", "params"]'
SPARK_SUBMIT_PARAMS = '["--class", "org.apache.spark.examples.SparkPi"]'


@provide_conf
def test_run_now_no_params(jobs_api_mock):
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        jobs_api_mock.run_now.return_value = RUN_NOW_RETURN
        runner = CliRunner()
        runner.invoke(cli.run_now_cli, ['--job-id', 1])
        assert jobs_api_mock.run_now.call_args[0][0] == 1
        assert jobs_api_mock.run_now.call_args[0][1] is None
        assert jobs_api_mock.run_now.call_args[0][2] is None
        assert jobs_api_mock.run_now.call_args[0][3] is None
        assert jobs_api_mock.run_now.call_args[0][4] is None
        assert echo_mock.call_args[0][0] == pretty_format(RUN_NOW_RETURN)


@provide_conf
def test_run_now_with_params(jobs_api_mock):
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        jobs_api_mock.run_now.return_value = RUN_NOW_RETURN
        runner = CliRunner()
        runner.invoke(cli.run_now_cli, ['--job-id', 1,
                                        '--jar-params', JAR_PARAMS,
                                        '--notebook-params', NOTEBOOK_PARAMS,
                                        '--python-params', PYTHON_PARAMS,
                                        '--spark-submit-params', SPARK_SUBMIT_PARAMS])
        assert jobs_api_mock.run_now.call_args[0][0] == 1
        assert jobs_api_mock.run_now.call_args[0][1] == json.loads(JAR_PARAMS)
        assert jobs_api_mock.run_now.call_args[0][2] == json.loads(NOTEBOOK_PARAMS)
        assert jobs_api_mock.run_now.call_args[0][3] == json.loads(PYTHON_PARAMS)
        assert jobs_api_mock.run_now.call_args[0][4] == json.loads(SPARK_SUBMIT_PARAMS)
        assert echo_mock.call_args[0][0] == pretty_format(RUN_NOW_RETURN)
