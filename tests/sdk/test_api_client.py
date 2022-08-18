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
import os

import pytest
import requests
import requests_mock

from databricks_cli.sdk import ReposService
from databricks_cli.sdk.api_client import ApiClient


def test_api_client_constructor():
    """This used to throw when we converted <user>:<password> to base64 encoded string."""
    client = ApiClient(user='apple', password='banana', host='https://databricks.com')
    # echo -n "apple:banana" | base64
    assert client.default_headers['Authorization'] == 'Basic YXBwbGU6YmFuYW5h'


requests_mock.mock.case_sensitive = True

@pytest.fixture()
def m():
    with requests_mock.Mocker() as m:
        yield m

@pytest.fixture(autouse=False)
def netrc_file(tmp_path, monkeypatch):
    netrc_file_path = os.path.join(str(tmp_path), ".netrc")
    monkeypatch.setenv("NETRC", netrc_file_path)
    return netrc_file_path

def test_simple_request(m):
    data = {'cucumber': 'dade'}
    m.get('https://databricks.com/api/2.0/endpoint', text=json.dumps(data))
    client = ApiClient(user='apple', password='banana', host='https://databricks.com')
    assert client.perform_query('GET', '/endpoint') == data

def test_no_content_from_server_on_error(m):
    m.get('https://databricks.com/api/2.0/endpoint', status_code=400, text='some html message')
    client = ApiClient(user='apple', password='banana', host='https://databricks.com')
    with pytest.raises(requests.exceptions.HTTPError):
        client.perform_query('GET', '/endpoint')

def test_content_from_server_on_error(m):
    data = {'cucumber': 'dade'}
    m.get('https://databricks.com/api/2.0/endpoint', status_code=400, text=json.dumps(data))
    client = ApiClient(user='apple', password='banana', host='https://databricks.com')
    error_message_contains = "{'cucumber': 'dade'}"
    with pytest.raises(requests.exceptions.HTTPError) as e:
        client.perform_query('GET', '/endpoint')
        assert error_message_contains in e.value.message


def test_get_request_with_true_param(m):
    data = {'cucumber': 'dade'}
    m.get('https://databricks.com/api/2.0/endpoint?active_only=true', text=json.dumps(data))
    client = ApiClient(user='apple', password='banana', host='https://databricks.com')
    assert client.perform_query('GET', '/endpoint', {'active_only': True}) == data


def test_get_request_with_false_param(m):
    data = {'cucumber': 'dade'}
    m.get('https://databricks.com/api/2.0/endpoint?active_only=false', text=json.dumps(data))
    client = ApiClient(user='apple', password='banana', host='https://databricks.com')
    assert client.perform_query('GET', '/endpoint', {'active_only': False}) == data


def test_get_request_with_int_param(m):
    data = {'cucumber': 'dade'}
    m.get('https://databricks.com/api/2.0/endpoint?job_id=0', text=json.dumps(data))
    client = ApiClient(user='apple', password='banana', host='https://databricks.com')
    assert client.perform_query('GET', '/endpoint', {'job_id': 0}) == data


def test_get_request_with_float_param(m):
    data = {'cucumber': 'dade'}
    m.get('https://databricks.com/api/2.0/endpoint?job_id=0.25', text=json.dumps(data))
    client = ApiClient(user='apple', password='banana', host='https://databricks.com')
    assert client.perform_query('GET', '/endpoint', {'job_id': 0.25}) == data


def test_get_request_with_list_param(m):
    client = ApiClient(user='apple', password='banana', host='https://databricks.com')
    with pytest.raises(AssertionError) as ex_info:
        client.perform_query('GET', '/endpoint', {'job_id': ['a','b']})
        assert str(ex_info.value) == 'cannot pass list of objects'


def test_get_url():
    client = ApiClient(host='https://databricks.com', jobs_api_version = '2.1')
    assert client.get_url('') == 'https://databricks.com/api/2.0'
    assert client.get_url('/') == 'https://databricks.com/api/2.0/'
    assert client.get_url('/endpoint') == 'https://databricks.com/api/2.0/endpoint'
    assert client.get_url('/jobs/list') == 'https://databricks.com/api/2.1/jobs/list'
    assert client.get_url('/jobs/list', '3.0') == 'https://databricks.com/api/3.0/jobs/list'
    # Check that UC APIs use '2.1'
    assert client.get_url('/unity-catalog/catalogs') == \
        'https://databricks.com/api/2.1/unity-catalog/catalogs'
    # Can override version for UC API
    assert client.get_url('/unity-catalog/catalogs', '2.0') == \
        'https://databricks.com/api/2.0/unity-catalog/catalogs'


def test_api_client_url_parsing():
    client = ApiClient(host='https://databricks.com')
    assert client.get_url('') == 'https://databricks.com/api/2.0'

    client = ApiClient(host='https://databricks.com/?o=123')
    assert client.get_url('') == 'https://databricks.com/api/2.0'

    client = ApiClient(host='https://databricks.com?o=123')
    assert client.get_url('') == 'https://databricks.com/api/2.0'

    # NOTE: this technically is not possible since we validate that the "host" has a prefix of https:// in
    # databricks_cli.configure.cli
    client = ApiClient(host='http://databricks.com')
    assert client.get_url('') == 'http://databricks.com/api/2.0'

def test_api_client_auth_netrc_and_user_password(m, netrc_file):
    with open(netrc_file, "w+") as netrc:
        #generates header Authorization: 'Basic bmV0cmM6cGFzc3dvcmQ='
        netrc.write("machine databricks.com login netrc password password")

    m.get('https://databricks.com/api/2.0/endpoint', text=json.dumps({}))
    client = ApiClient(user="apple", password="banana", host="https://databricks.com")
    client.perform_query("GET", "/endpoint")
    assert m.request_history[0].headers['Authorization'] == "Basic YXBwbGU6YmFuYW5h"

def test_api_client_auth_only_valid_netrc(m, netrc_file):
    with open(netrc_file, "w+") as netrc:
        #generates header Authorization: 'Basic bmV0cmM6cGFzc3dvcmQ='
        netrc.write("machine databricks.com login netrc password password")

    m.get('https://databricks.com/api/2.0/endpoint', text=json.dumps({}))
    client = ApiClient(host="https://databricks.com")
    client.perform_query("GET", "/endpoint")
    assert m.request_history[0].headers['Authorization'] == "Basic bmV0cmM6cGFzc3dvcmQ="

def test_api_client_auth_invalid_netrc(m, netrc_file):
    with open(netrc_file, "w+") as netrc:
        netrc.write("garbage")

    m.get('https://databricks.com/api/2.0/endpoint', text=json.dumps({}))
    client = ApiClient(host="https://databricks.com")
    client.perform_query("GET", "/endpoint")
    assert "Authorization" not in m.request_history[0].headers

def test_api_client_auth_no_netrc(m):
    m.get('https://databricks.com/api/2.0/endpoint', text=json.dumps({}))
    client = ApiClient(host="https://databricks.com")
    client.perform_query("GET", "/endpoint")
    assert "Authorization" not in m.request_history[0].headers
