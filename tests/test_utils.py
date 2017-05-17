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

import mock
from requests import Response
from requests.exceptions import HTTPError

import databricks_cli.utils as utils


def test_eat_exceptions_normal_case():
    """
    If no exceptions, this wrapper should do nothing.
    """
    @utils.eat_exceptions
    def test_function(x):
        return x

    assert test_function(1) == 1


def test_eat_exceptions_401():
    """
    If wrapped function returns 401 HTTPError, then print special error message.
    """
    with mock.patch('databricks_cli.utils.error_and_quit') as error_and_quit_mock:
        @utils.eat_exceptions
        def test_function():
            resp = Response()
            resp.status_code = 401
            raise HTTPError(response=resp)
        test_function()
        assert error_and_quit_mock.call_count == 1
        assert 'Your authentication information' in error_and_quit_mock.call_args[0][0]
