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
from databricks_cli.configure.config import get_config
from databricks_cli.utils import pretty_format
from databricks_cli.sdk.api_client import ApiClient
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
        assert jobs_api_mock.create_job.call_args[0][0] == json.loads(
            CREATE_JSON)
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
        assert jobs_api_mock.create_job.call_args[0][0] == json.loads(
            CREATE_JSON)
        assert echo_mock.call_args[0][0] == pretty_format(CREATE_RETURN)


RESET_JSON = '{"job_name": "test_job"}'

CORRECT_REQUEST_PAYLOAD = {'job_id': 1, 'new_settings': json.loads(RESET_JSON)}


@provide_conf
def test_reset_cli_json(jobs_api_mock):
    runner = CliRunner()
    runner.invoke(cli.reset_cli, ['--json', RESET_JSON, '--job-id', 1])
    assert jobs_api_mock.reset_job.call_args[0][0] == CORRECT_REQUEST_PAYLOAD


RESET_JSON_NEW_SETTINGS = '{ "new_settings": %s, "job_id": 5 }' % RESET_JSON


@provide_conf
def test_reset_cli_json_new_settings(jobs_api_mock):
    runner = CliRunner()
    runner.invoke(cli.reset_cli, [
                  '--json', RESET_JSON_NEW_SETTINGS, '--job-id', 1])
    assert jobs_api_mock.reset_job.call_args[0][0] == CORRECT_REQUEST_PAYLOAD

CORRECT_PAYLOAD_NEW_ID = {'job_id': 5, 'new_settings': json.loads(RESET_JSON)}

@provide_conf
def test_reset_cli_json_new_settings_no_job_id(jobs_api_mock):
    runner = CliRunner()
    runner.invoke(cli.reset_cli, ['--json', RESET_JSON_NEW_SETTINGS])
    assert jobs_api_mock.reset_job.call_args[0][0] == CORRECT_PAYLOAD_NEW_ID


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


LIST_21_RETURN = {
    'jobs': [{
        'job_id': 1,
        'settings': {
            'name': 'b',
            'tasks': [{'task_key': 'a'}]
        }
    }, {
        'job_id': 2,
        'settings': {
            'name': 'a',
            'tasks': [{'task_key': 'a'}]
        }
    }, {
        'job_id': 30,
        'settings': {
            # Normally 'C' < 'a' < 'b' -- we should do case insensitive sorting though.
            'name': 'C',
            'tasks': [{'task_key': 'a'}]
        }
    }]
}


@provide_conf
def test_list_jobs_api_21(jobs_api_mock):
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        jobs_api_mock.list_jobs.return_value = LIST_21_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli)
        # Output should be sorted here.
        rows = [(2, 'a'), (1, 'b'), (30, 'C')]
        assert echo_mock.call_args[0][0] == \
            tabulate(rows, tablefmt='plain', disable_numparse=True)


@provide_conf
def test_list_jobs_api_21_output_json(jobs_api_mock):
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        jobs_api_mock.list_jobs.return_value = LIST_21_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli, ['--output', 'json'])
        assert echo_mock.call_args[0][0] == pretty_format(LIST_21_RETURN)


@provide_conf
def test_list_jobs_type_pipeline(jobs_api_mock):
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        jobs_api_mock.list_jobs.return_value = LIST_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli, ['--type', 'PIPELINE'])
        assert jobs_api_mock.list_jobs.call_args[1]['job_type'] == 'PIPELINE'
        rows = [(2, 'a'), (1, 'b'), (30, 'C')]
        assert echo_mock.call_args[0][0] == \
            tabulate(rows, tablefmt='plain', disable_numparse=True)


RUN_NOW_RETURN = {
    "number_in_job": 1,
    "run_id": 1
}
NOTEBOOK_PARAMS = '{"a": 1}'
JAR_PARAMS = '[1, 2, 3]'
PYTHON_PARAMS = '["python", "params"]'
PYTHON_NAMED_PARAMS = '{"python": "named", "params": 1}'
SPARK_SUBMIT_PARAMS = '["--class", "org.apache.spark.examples.SparkPi"]'
IDEMPOTENCY_TOKEN = 'idempotent-token'


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
        assert jobs_api_mock.run_now.call_args[0][5] is None
        assert jobs_api_mock.run_now.call_args[0][6] is None
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
                                        '--python-named-params', PYTHON_NAMED_PARAMS,
                                        '--spark-submit-params', SPARK_SUBMIT_PARAMS,
                                        '--idempotency-token', IDEMPOTENCY_TOKEN])
        assert jobs_api_mock.run_now.call_args[0][0] == 1
        assert jobs_api_mock.run_now.call_args[0][1] == json.loads(JAR_PARAMS)
        assert jobs_api_mock.run_now.call_args[0][2] == json.loads(
            NOTEBOOK_PARAMS)
        assert jobs_api_mock.run_now.call_args[0][3] == json.loads(
            PYTHON_PARAMS)
        assert jobs_api_mock.run_now.call_args[0][4] == json.loads(
            SPARK_SUBMIT_PARAMS)
        assert jobs_api_mock.run_now.call_args[0][5] == json.loads(
            PYTHON_NAMED_PARAMS)
        assert jobs_api_mock.run_now.call_args[0][6] == IDEMPOTENCY_TOKEN
        assert echo_mock.call_args[0][0] == pretty_format(RUN_NOW_RETURN)


@provide_conf
def test_configure():
    runner = CliRunner()
    runner.invoke(cli.configure, ['--version=2.1'])
    assert get_config().jobs_api_version == '2.1'


@provide_conf
def test_list_throws_if_invalid_option_for_version_20():
    runner = CliRunner()
    args = [['--all'], ['--expand-tasks'],
            ['--offset', '20'], ['--limit', '20']]

    for arg in args:
        result = runner.invoke(cli.configure, ['--version=2.0'] + arg)
        assert result.exit_code == 2


LIST_RETURN_1 = {
    'jobs': [{'job_id': '1', 'settings': {'name': 'a'}}], 'has_more': True}
LIST_RETURN_2 = {
    'jobs': [{'job_id': '2', 'settings': {'name': 'b'}}], 'has_more': True}
LIST_RETURN_3 = {
    'jobs': [{'job_id': '3', 'settings': {'name': 'c'}}], 'has_more': False}


@provide_conf
def test_list_all(jobs_api_mock):
    jobs_api_mock.list_jobs.side_effect = iter(
        [LIST_RETURN_1, LIST_RETURN_2, LIST_RETURN_3])
    runner = CliRunner()
    result = runner.invoke(cli.list_cli, ['--version=2.1', '--all'])
    rows = [(1, 'a'), (2, 'b'), (3, 'c')]
    assert result.exit_code == 0
    assert result.output == \
        tabulate(rows, tablefmt='plain', disable_numparse=True) + '\n'


@provide_conf
def test_list_expand_tasks(jobs_api_mock):
    jobs_api_mock.list_jobs.return_value = LIST_RETURN_1
    runner = CliRunner()
    result = runner.invoke(cli.list_cli, ['--version=2.1', '--expand-tasks'])
    assert result.exit_code == 0
    assert jobs_api_mock.list_jobs.call_args[1]['expand_tasks']
    assert jobs_api_mock.list_jobs.call_args[1]['version'] == '2.1'


@provide_conf
def test_list_offset(jobs_api_mock):
    jobs_api_mock.list_jobs.return_value = LIST_RETURN_1
    runner = CliRunner()
    result = runner.invoke(cli.list_cli, ['--version=2.1', '--offset', '1'])
    assert result.exit_code == 0
    assert jobs_api_mock.list_jobs.call_args[1]['offset'] == 1
    assert jobs_api_mock.list_jobs.call_args[1]['version'] == '2.1'

@provide_conf
def test_list_name(jobs_api_mock):
    jobs_api_mock.list_jobs.return_value = LIST_RETURN_1
    runner = CliRunner()
    result = runner.invoke(cli.list_cli, ['--version=2.1', '--name', 'foo'])
    assert result.exit_code == 0
    assert jobs_api_mock.list_jobs.call_args[1]['name'] == 'foo'
    assert jobs_api_mock.list_jobs.call_args[1]['version'] == '2.1'

@provide_conf
def test_list_limit(jobs_api_mock):
    jobs_api_mock.list_jobs.return_value = LIST_RETURN_1
    runner = CliRunner()
    result = runner.invoke(cli.list_cli, ['--version=2.1', '--limit', '1'])
    assert result.exit_code == 0
    assert jobs_api_mock.list_jobs.call_args[1]['limit'] == 1
    assert jobs_api_mock.list_jobs.call_args[1]['version'] == '2.1'


@provide_conf
def test_get_job_21(jobs_api_mock):
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        jobs_api_mock.get_job.return_value = LIST_21_RETURN['jobs'][0]
        runner = CliRunner()
        runner.invoke(cli.get_cli, ['--job-id', '1', '--version', '2.1'])
        assert jobs_api_mock.get_job.call_args == mock.call('1', version='2.1')
        assert echo_mock.call_args[0][0] == pretty_format(LIST_21_RETURN['jobs'][0])


@provide_conf
def test_delete_job_21(jobs_api_mock):
    runner = CliRunner()
    runner.invoke(cli.delete_cli, ['--job-id', '1', '--version', '2.1'])
    assert jobs_api_mock.delete_job.call_args == mock.call('1', version='2.1')


@provide_conf
def test_check_version():
    # Without calling `databricks jobs configure --version=2.1`
    api_client = ApiClient(
        user='apple',
        password='banana',
        host='https://databricks.com',
        jobs_api_version=None
    )

    # databricks jobs list
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        cli.check_version(api_client, None)
        assert echo_mock.called
        assert 'Your CLI is configured to use Jobs API 2.0' in echo_mock.call_args[0][0]
        assert echo_mock.call_args_list[0][1]['err'] is True
    # databricks jobs list --version=2.0
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        cli.check_version(api_client, "2.0")
        assert not echo_mock.called
    # databricks jobs list --version=2.1
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        cli.check_version(api_client, "2.1")
        assert not echo_mock.called

    # After calling `databricks jobs configure --version=2.1`
    api_client = ApiClient(
        user='apple',
        password='banana',
        host='https://databricks.com',
        jobs_api_version="2.1"
    )
    # databricks jobs list
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        cli.check_version(api_client, None)
        assert not echo_mock.called
    # databricks jobs list --version=2.0
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        cli.check_version(api_client, "2.0")
        assert not echo_mock.called
    # databricks jobs list --version=2.1
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        cli.check_version(api_client, "2.1")
        assert not echo_mock.called

    # After calling `databricks jobs configure --version=2.0`
    api_client = ApiClient(
        user='apple',
        password='banana',
        host='https://databricks.com',
        jobs_api_version="2.0"
    )
    # databricks jobs list
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        cli.check_version(api_client, None)
        assert echo_mock.called
        assert 'Your CLI is configured to use Jobs API 2.0' in echo_mock.call_args[0][0]
    # databricks jobs list --version=2.0
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        cli.check_version(api_client, "2.0")
        assert not echo_mock.called
    # databricks jobs list --version=2.1
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        cli.check_version(api_client, "2.1")
        assert not echo_mock.called
