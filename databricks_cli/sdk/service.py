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

import base64
import StringIO

from . import objects

class JobsService(object):
    def __init__(self, client):
        self.client = client

    def create_job(self, name=None, existing_cluster_id=None, new_cluster=None, libraries=None,
                   email_notifications=None, timeout_seconds=None, max_retries=None,
                   min_retry_interval_millis=None, retry_on_timeout=None, schedule=None,
                   notebook_task=None, spark_jar_task=None, max_concurrent_runs=None):
        _data = {}
        if name is not None:
            _data['name'] = name
        if existing_cluster_id is not None:
            _data['existing_cluster_id'] = existing_cluster_id
        if new_cluster is not None:
            _data['new_cluster'] = new_cluster
            if not isinstance(new_cluster, dict):
                raise TypeError('Expected databricks.NewCluster() or dict for field new_cluster')
        if libraries is not None:
            _data['libraries'] = libraries
        if email_notifications is not None:
            _data['email_notifications'] = email_notifications
            if not isinstance(email_notifications, dict):
                raise TypeError('Expected databricks.JobEmailNotifications() or dict for field email_notifications')
        if timeout_seconds is not None:
            _data['timeout_seconds'] = timeout_seconds
        if max_retries is not None:
            _data['max_retries'] = max_retries
        if min_retry_interval_millis is not None:
            _data['min_retry_interval_millis'] = min_retry_interval_millis
        if retry_on_timeout is not None:
            _data['retry_on_timeout'] = retry_on_timeout
        if schedule is not None:
            _data['schedule'] = schedule
            if not isinstance(schedule, dict):
                raise TypeError('Expected databricks.CronSchedule() or dict for field schedule')
        if notebook_task is not None:
            _data['notebook_task'] = notebook_task
            if not isinstance(notebook_task, dict):
                raise TypeError('Expected databricks.NotebookTask() or dict for field notebook_task')
        if spark_jar_task is not None:
            _data['spark_jar_task'] = spark_jar_task
            if not isinstance(spark_jar_task, dict):
                raise TypeError('Expected databricks.SparkJarTask() or dict for field spark_jar_task')
        if max_concurrent_runs is not None:
            _data['max_concurrent_runs'] = max_concurrent_runs
        return self.client.perform_query('POST', '/jobs/create', data=_data)
    
    def submit_run(self, run_name=None, existing_cluster_id=None, new_cluster=None, libraries=None,
                   notebook_task=None, spark_jar_task=None, timeout_seconds=None):
        _data = {}
        if run_name is not None:
            _data['run_name'] = run_name
        if existing_cluster_id is not None:
            _data['existing_cluster_id'] = existing_cluster_id
        if new_cluster is not None:
            _data['new_cluster'] = new_cluster
            if not isinstance(new_cluster, dict):
                raise TypeError('Expected databricks.NewCluster() or dict for field new_cluster')
        if libraries is not None:
            _data['libraries'] = libraries
        if notebook_task is not None:
            _data['notebook_task'] = notebook_task
            if not isinstance(notebook_task, dict):
                raise TypeError('Expected databricks.NotebookTask() or dict for field notebook_task')
        if spark_jar_task is not None:
            _data['spark_jar_task'] = spark_jar_task
            if not isinstance(spark_jar_task, dict):
                raise TypeError('Expected databricks.SparkJarTask() or dict for field spark_jar_task')
        if timeout_seconds is not None:
            _data['timeout_seconds'] = timeout_seconds
        return self.client.perform_query('POST', '/jobs/runs/submit', data=_data)
    
    def reset_job(self, job_id, new_settings):
        _data = {}
        if job_id is not None:
            _data['job_id'] = job_id
        if new_settings is not None:
            _data['new_settings'] = new_settings
            if not isinstance(new_settings, dict):
                raise TypeError('Expected databricks.JobSettings() or dict for field new_settings')
        return self.client.perform_query('POST', '/jobs/reset', data=_data)
    
    def delete_job(self, job_id):
        _data = {}
        if job_id is not None:
            _data['job_id'] = job_id
        return self.client.perform_query('POST', '/jobs/delete', data=_data)
    
    def get_job(self, job_id):
        _data = {}
        if job_id is not None:
            _data['job_id'] = job_id
        return self.client.perform_query('GET', '/jobs/get', data=_data)
    
    def list_jobs(self):
        _data = {}
    
        return self.client.perform_query('GET', '/jobs/list', data=_data)
    
    def run_now(self, job_id=None, jar_params=None, notebook_params=None):
        _data = {}
        if job_id is not None:
            _data['job_id'] = job_id
        if jar_params is not None:
            _data['jar_params'] = jar_params
        if notebook_params is not None:
            _data['notebook_params'] = notebook_params
        return self.client.perform_query('POST', '/jobs/run-now', data=_data)
    
    def list_runs(self, job_id=None, active_only=None, completed_only=None, offset=None,
                  limit=None):
        _data = {}
        if job_id is not None:
            _data['job_id'] = job_id
        if active_only is not None:
            _data['active_only'] = active_only
        if completed_only is not None:
            _data['completed_only'] = completed_only
        if offset is not None:
            _data['offset'] = offset
        if limit is not None:
            _data['limit'] = limit
        return self.client.perform_query('GET', '/jobs/runs/list', data=_data)
    
    def get_run(self, run_id=None):
        _data = {}
        if run_id is not None:
            _data['run_id'] = run_id
        return self.client.perform_query('GET', '/jobs/runs/get', data=_data)
    
    def cancel_run(self, run_id):
        _data = {}
        if run_id is not None:
            _data['run_id'] = run_id
        return self.client.perform_query('POST', '/jobs/runs/cancel', data=_data)
     

class ClusterService(object):
    def __init__(self, client):
        self.client = client

    def list_clusters(self):
        _data = {}
    
        return self.client.perform_query('GET', '/clusters/list', data=_data)
    
    def create_cluster(self, num_workers=None, autoscale=None, cluster_name=None, spark_version=None,
                       spark_conf=None, aws_attributes=None, node_type_id=None,
                       driver_node_type_id=None, ssh_public_keys=None, custom_tags=None,
                       cluster_log_conf=None, spark_env_vars=None, autotermination_minutes=None,
                       enable_elastic_disk=None):
        _data = {}
        if num_workers is not None:
            _data['num_workers'] = num_workers
        if autoscale is not None:
            _data['autoscale'] = autoscale
            if not isinstance(autoscale, dict):
                raise TypeError('Expected databricks.AutoScale() or dict for field autoscale')
        if cluster_name is not None:
            _data['cluster_name'] = cluster_name
        if spark_version is not None:
            _data['spark_version'] = spark_version
        if spark_conf is not None:
            _data['spark_conf'] = spark_conf
        if aws_attributes is not None:
            _data['aws_attributes'] = aws_attributes
            if not isinstance(aws_attributes, dict):
                raise TypeError('Expected databricks.AwsAttributes() or dict for field aws_attributes')
        if node_type_id is not None:
            _data['node_type_id'] = node_type_id
        if driver_node_type_id is not None:
            _data['driver_node_type_id'] = driver_node_type_id
        if ssh_public_keys is not None:
            _data['ssh_public_keys'] = ssh_public_keys
        if custom_tags is not None:
            _data['custom_tags'] = custom_tags
        if cluster_log_conf is not None:
            _data['cluster_log_conf'] = cluster_log_conf
            if not isinstance(cluster_log_conf, dict):
                raise TypeError('Expected databricks.ClusterLogConf() or dict for field cluster_log_conf')
        if spark_env_vars is not None:
            _data['spark_env_vars'] = spark_env_vars
        if autotermination_minutes is not None:
            _data['autotermination_minutes'] = autotermination_minutes
        if enable_elastic_disk is not None:
            _data['enable_elastic_disk'] = enable_elastic_disk
        return self.client.perform_query('POST', '/clusters/create', data=_data)
    
    def start_cluster(self, cluster_id):
        _data = {}
        if cluster_id is not None:
            _data['cluster_id'] = cluster_id
        return self.client.perform_query('POST', '/clusters/start', data=_data)
    
    def list_spark_versions(self):
        _data = {}
    
        return self.client.perform_query('GET', '/clusters/spark-versions', data=_data)
    
    def delete_cluster(self, cluster_id):
        _data = {}
        if cluster_id is not None:
            _data['cluster_id'] = cluster_id
        return self.client.perform_query('POST', '/clusters/delete', data=_data)
    
    def restart_cluster(self, cluster_id):
        _data = {}
        if cluster_id is not None:
            _data['cluster_id'] = cluster_id
        return self.client.perform_query('POST', '/clusters/restart', data=_data)
    
    def resize_cluster(self, cluster_id, num_workers=None, autoscale=None):
        _data = {}
        if cluster_id is not None:
            _data['cluster_id'] = cluster_id
        if num_workers is not None:
            _data['num_workers'] = num_workers
        if autoscale is not None:
            _data['autoscale'] = autoscale
            if not isinstance(autoscale, dict):
                raise TypeError('Expected databricks.AutoScale() or dict for field autoscale')
        return self.client.perform_query('POST', '/clusters/resize', data=_data)
    
    def get_cluster(self, cluster_id):
        _data = {}
        if cluster_id is not None:
            _data['cluster_id'] = cluster_id
        return self.client.perform_query('GET', '/clusters/get', data=_data)
    
    def list_node_types(self):
        _data = {}
    
        return self.client.perform_query('GET', '/clusters/list-node-types', data=_data)
    
    def list_available_zones(self):
        _data = {}
    
        return self.client.perform_query('GET', '/clusters/list-zones', data=_data)
     

class LibraryService(object):
    def __init__(self, client):
        self.client = client

    def list_libraries(self):
        _data = {}
    
        return self.client.perform_query('GET', '/libraries/list', data=_data)
    
    def get_library_cluster_status(self, library_id, cluster_id):
        _data = {}
        if library_id is not None:
            _data['library_id'] = library_id
        if cluster_id is not None:
            _data['cluster_id'] = cluster_id
        return self.client.perform_query('GET', '/libraries/get-cluster-status', data=_data)
    
    def create_library(self, path, jar_specification=None, egg_specification=None,
                       pip_specification=None, maven_specification=None):
        _data = {}
        if path is not None:
            _data['path'] = path
        if jar_specification is not None:
            _data['jar_specification'] = jar_specification
            if not isinstance(jar_specification, dict):
                raise TypeError('Expected databricks.JarSpecification() or dict for field jar_specification')
        if egg_specification is not None:
            _data['egg_specification'] = egg_specification
            if not isinstance(egg_specification, dict):
                raise TypeError('Expected databricks.EggSpecification() or dict for field egg_specification')
        if pip_specification is not None:
            _data['pip_specification'] = pip_specification
            if not isinstance(pip_specification, dict):
                raise TypeError('Expected databricks.PipSpecification() or dict for field pip_specification')
        if maven_specification is not None:
            _data['maven_specification'] = maven_specification
            if not isinstance(maven_specification, dict):
                raise TypeError('Expected databricks.MavenSpecification() or dict for field maven_specification')
        return self.client.perform_query('POST', '/libraries/create', data=_data)
    
    def attach_library(self, library_id, cluster_id):
        _data = {}
        if library_id is not None:
            _data['library_id'] = library_id
        if cluster_id is not None:
            _data['cluster_id'] = cluster_id
        return self.client.perform_query('POST', '/libraries/attach', data=_data)
    
    def detach_library(self, library_id, cluster_id):
        _data = {}
        if library_id is not None:
            _data['library_id'] = library_id
        if cluster_id is not None:
            _data['cluster_id'] = cluster_id
        return self.client.perform_query('POST', '/libraries/detach', data=_data)
    
    def delete_library(self, library_id):
        _data = {}
        if library_id is not None:
            _data['library_id'] = library_id
        return self.client.perform_query('POST', '/libraries/delete', data=_data)
     

class DbfsService(object):
    def __init__(self, client):
        self.client = client

    def read(self, path, offset=None, length=None):
        _data = {}
        if path is not None:
            _data['path'] = path
        if offset is not None:
            _data['offset'] = offset
        if length is not None:
            _data['length'] = length
        return self.client.perform_query('GET', '/dbfs/read', data=_data)
    
    def get_status(self, path):
        _data = {}
        if path is not None:
            _data['path'] = path
        return self.client.perform_query('GET', '/dbfs/get-status', data=_data)
    
    def list(self, path):
        _data = {}
        if path is not None:
            _data['path'] = path
        return self.client.perform_query('GET', '/dbfs/list', data=_data)
    
    def put(self, path, contents=None, overwrite=None):
        _data = {}
        if path is not None:
            _data['path'] = path
        if contents is not None:
            _data['contents'] = contents
        if overwrite is not None:
            _data['overwrite'] = overwrite
        return self.client.perform_query('POST', '/dbfs/put', data=_data)
    
    def mkdirs(self, path):
        _data = {}
        if path is not None:
            _data['path'] = path
        return self.client.perform_query('POST', '/dbfs/mkdirs', data=_data)
    
    def move(self, source_path, destination_path):
        _data = {}
        if source_path is not None:
            _data['source_path'] = source_path
        if destination_path is not None:
            _data['destination_path'] = destination_path
        return self.client.perform_query('POST', '/dbfs/move', data=_data)
    
    def delete(self, path, recursive=None):
        _data = {}
        if path is not None:
            _data['path'] = path
        if recursive is not None:
            _data['recursive'] = recursive
        return self.client.perform_query('POST', '/dbfs/delete', data=_data)
    
    def create(self, path, overwrite=None):
        _data = {}
        if path is not None:
            _data['path'] = path
        if overwrite is not None:
            _data['overwrite'] = overwrite
        return self.client.perform_query('POST', '/dbfs/create', data=_data)
    
    def add_block(self, handle, data):
        _data = {}
        if handle is not None:
            _data['handle'] = handle
        if data is not None:
            _data['data'] = data
        return self.client.perform_query('POST', '/dbfs/add-block', data=_data)
    
    def close(self, handle):
        _data = {}
        if handle is not None:
            _data['handle'] = handle
        return self.client.perform_query('POST', '/dbfs/close', data=_data)
     
    def read_string(self, dbfs_path):
        """Reads an entire file from DBFS and decodes it into a Python string."""
        file_info = self.get_status(dbfs_path)
        buffer = ''
        bytes_read = 0
        while bytes_read < file_info['file_size']:
            result = self.read(dbfs_path, bytes_read, 1024 * 1024)
            bytes_read = bytes_read + result['bytes_read']
            buffer = buffer + base64.decodestring(result['data'])
        return buffer

    def download_file(self, dbfs_path, local_path):
        """Downloads a file from DBFS into a file in the local path provided."""
        file_info = self.get_status(dbfs_path)
        output_file = open(local_path, 'wb')
        bytes_read = 0
        while bytes_read < file_info['file_size']:
            result = self.read(dbfs_path, bytes_read, 1024 * 1024)
            bytes_read = bytes_read + result['bytes_read']
            output_file.write(base64.decodestring(result['data']))
        output_file.close()

    def put_string(self, dbfs_path, string, overwrite=False):
        """Uploads the input string into DBFS to the dbfs path provided."""
        result = self.create(dbfs_path, overwrite)
        handle = result['handle']
        import StringIO
        input_string = StringIO.StringIO(string)
        while True:
            block = input_string.read(1024 * 1024)
            if block == '':
                break
            self.add_block(handle, base64.encodestring(block))
        self.close(handle)
        input_string.close()

    def upload_file(self, dbfs_path, local_path, overwrite=False):
        """Uploads a file into DBFS to the DBFS path provided."""
        result = self.create(dbfs_path, overwrite)
        handle = result['handle']
        input_file = open(local_path, 'rb')
        while True:
            block = input_file.read(1024 * 1024)
            if block == '':
                break
            self.add_block(handle, base64.encodestring(block))
        self.close(handle)
        input_file.close()
