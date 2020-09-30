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

"""
Test data for use with various testing functions.
"""
from copy import deepcopy

TEST_CLUSTER_NAME = 'databricks-cluster-1'
TEST_CLUSTER_ID = '0213-212348-veeps379'
TEST_CLUSTER_ID_2 = '0315-6787348-blah280'
ALL_CLUSTER_STATUSES_RETURN = {
    'statuses': [{
        'library_statuses': [{
            'status': 'INSTALLED',
            'is_library_for_all_clusters': False,
            'library': {
                'jar': 'dbfs:/test.jar'
            }
        }],
        'cluster_id': TEST_CLUSTER_ID
    }]
}

CLUSTER_1_RV = {
    "cluster_id": TEST_CLUSTER_ID,
    "driver": {
        "public_dns": "",
        "node_id": "123456",
        "node_aws_attributes": {
            "is_spot": False
        },
        "instance_id": "i-0abcddef",
        "start_timestamp": 1598550048136,
        "host_private_ip": "127.0.0.1",
        "private_ip": "127.0.0.1"
    },
    "executors": [
        {
            "public_dns": "",
            "node_id": "1234567",
            "node_aws_attributes": {
                "is_spot": True
            },
            "instance_id": "i-0abcddeff",
            "start_timestamp": 1598567503039,
            "host_private_ip": "127.0.0.1",
            "private_ip": "127.0.0.1"
        },
    ],
    "spark_context_id": 1,
    "jdbc_port": 10000,
    "cluster_name": TEST_CLUSTER_NAME,
    "spark_version": "6.5.x-scala2.11",
    "spark_conf": {
    },
    "node_type_id": "i3.4xlarge",
    "driver_node_type_id": "i3.4xlarge",
    "autotermination_minutes": 0,
    "enable_elastic_disk": True,
    "cluster_source": "UI",
    "init_scripts": [
    ],
    "enable_local_disk_encryption": False,
    "state": "RUNNING",
    "state_message": "",
    "start_time": 1598550047769,
    "terminated_time": 0,
    "last_state_loss_time": 1598550119064,
    "last_activity_time": 1598550164268,
    "autoscale": {
        "min_workers": 1,
        "max_workers": 1
    },
    "cluster_memory_mb": 4,
    "cluster_cores": 1.0,
    "default_tags": {
        "Vendor": "Databricks",
        "Creator": "someone@databricks.com",
        "ClusterName": TEST_CLUSTER_NAME,
        "ClusterId": TEST_CLUSTER_ID
    },
    "creator_user_name": "someone@databricks.com",
    "pinned_by_user_name": "100001",
    "init_scripts_safe_mode": False
}

CLUSTER_2_RV = deepcopy(CLUSTER_1_RV)
# fix the cluster ids for the second cluster
CLUSTER_2_RV['cluster_id'] = TEST_CLUSTER_ID_2
CLUSTER_2_RV['default_tags']['ClusterId'] = TEST_CLUSTER_ID_2
