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

import databricks_cli.clusters.api as api
from tests.test_data import CLUSTER_1_RV, TEST_CLUSTER_NAME, TEST_CLUSTER_ID
from tests.utils import provide_conf


@pytest.fixture(name='cluster_api_mock')
def fixture_cluster_api_mock():
    with mock.patch('databricks_cli.clusters.api.ClusterService') as ClusterServiceMock:
        _cluster_service_mock = mock.MagicMock()
        ClusterServiceMock.return_value = _cluster_service_mock
        yield _cluster_service_mock


@provide_conf
def test_get_cluster_id_for_name(cluster_api_mock):
    cluster_api_mock.list_clusters.return_value = {'clusters': [CLUSTER_1_RV]}
    cluster_api = api.ClusterApi(None)
    assert TEST_CLUSTER_ID == cluster_api.get_cluster_id_for_name(TEST_CLUSTER_NAME)


@provide_conf
def test_get_cluster_id_for_name_multiple_same_name(cluster_api_mock):
    cluster_api_mock.list_clusters.return_value = {'clusters': [CLUSTER_1_RV, CLUSTER_1_RV]}
    cluster_api = api.ClusterApi(None)
    with pytest.raises(RuntimeError,
                       match='More than 1 cluster was named {}'.format(TEST_CLUSTER_NAME)):
        cluster_api.get_cluster_id_for_name(TEST_CLUSTER_NAME)


@provide_conf
def test_get_cluster_id_for_name_none_found(cluster_api_mock):
    cluster_api_mock.list_clusters.return_value = {'clusters': []}
    cluster_api = api.ClusterApi(None)
    with pytest.raises(RuntimeError,
                       match='No clusters with name {} were found'.format(TEST_CLUSTER_NAME)):
        cluster_api.get_cluster_id_for_name(TEST_CLUSTER_NAME)
