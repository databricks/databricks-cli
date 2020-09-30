#
# Databricks CLI
# Copyright 2020 Databricks, Inc.
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
#
import pytest

from databricks_cli.scim.cli import validate_user_params


@pytest.mark.parametrize('json_file, json, user_name, groups, entitlements, roles, should_fail', [
    # json_file, json, user_name, groups, entitlements, roles, should_fail
    [None, None, None, None, None, None, True],
    [None, None, 'ac', None, None, None, False],
    [None, '{}', None, None, None, None, False],
    [None, '{}', 'ac', None, None, None, True],
    ['so', None, None, None, None, None, False],
    ['so', None, 'ac', None, None, None, True],
    ['so', '{}', None, None, None, None, True],
    ['so', '{}', 'ac', None, None, None, True],

])
def test_user_args(json_file, json, user_name, groups, entitlements, roles, should_fail):
    if should_fail:
        with pytest.raises(RuntimeError):
            validate_user_params(json_file, json, user_name, groups, entitlements, roles)
    else:
        validate_user_params(json_file, json, user_name, groups, entitlements, roles)
