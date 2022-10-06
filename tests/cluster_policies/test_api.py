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
import pytest

from databricks_cli.cluster_policies.api import ClusterPolicyApi


@pytest.mark.parametrize(
    "policy, expected",
    [
        ({"definition": "foo"}, {"definition": "foo"}),
        ({"definition": {"foo": "bar"}}, {"definition": '{"foo": "bar"}'}),
    ],
)
def test_format_policy_for_api(policy, expected):
    result = ClusterPolicyApi.format_policy_for_api(policy)
    assert result == expected


@pytest.mark.parametrize(
    "fct_name, method, action",
    [
        ("create_cluster_policy", "POST", "create"),
        ("edit_cluster_policy", "POST", "edit"),
    ],
)
@mock.patch(
    "databricks_cli.cluster_policies.api.ClusterPolicyApi.format_policy_for_api"
)
def test_create_and_edit_cluster_policy(
    mock_format_policy_for_api, fct_name, method, action, fixture_cluster_policies_api
):
    mock_policy = mock.Mock()
    getattr(fixture_cluster_policies_api, fct_name)(mock_policy)
    mock_format_policy_for_api.assert_called_once_with(mock_policy)
    fixture_cluster_policies_api.client.client.perform_query.assert_called_once_with(
        method,
        "/policies/clusters/{}".format(action),
        data=mock_format_policy_for_api.return_value,
    )
