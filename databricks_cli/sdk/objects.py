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

class AutoScale(dict):
    def __init__(self, min_workers=None, max_workers=None):
        super(AutoScale, self).__init__()
        if min_workers is not None:
            self['min_workers'] = min_workers
        if max_workers is not None:
            self['max_workers'] = max_workers

class AwsAttributes(dict):
    def __init__(self, first_on_demand=None, availability=None, zone_id=None,
                 instance_profile_arn=None, spot_bid_price_percent=None, ebs_volume_type=None,
                 ebs_volume_count=None, ebs_volume_size=None):
        super(AwsAttributes, self).__init__()
        if first_on_demand is not None:
            self['first_on_demand'] = first_on_demand
        if availability is not None:
            self['availability'] = availability
        if zone_id is not None:
            self['zone_id'] = zone_id
        if instance_profile_arn is not None:
            self['instance_profile_arn'] = instance_profile_arn
        if spot_bid_price_percent is not None:
            self['spot_bid_price_percent'] = spot_bid_price_percent
        if ebs_volume_type is not None:
            self['ebs_volume_type'] = ebs_volume_type
        if ebs_volume_count is not None:
            self['ebs_volume_count'] = ebs_volume_count
        if ebs_volume_size is not None:
            self['ebs_volume_size'] = ebs_volume_size

class ClusterLogConf(dict):
    def __init__(self, dbfs=None, s3=None):
        super(ClusterLogConf, self).__init__()
        if dbfs is not None:
            self['dbfs'] = dbfs
            if not isinstance(dbfs, dict):
                raise TypeError('Expected databricks.DbfsStorageInfo() or dict for field dbfs')
        if s3 is not None:
            self['s3'] = s3
            if not isinstance(s3, dict):
                raise TypeError('Expected databricks.S3StorageInfo() or dict for field s3')

class DbfsStorageInfo(dict):
    def __init__(self, destination=None):
        super(DbfsStorageInfo, self).__init__()
        if destination is not None:
            self['destination'] = destination

class S3StorageInfo(dict):
    def __init__(self, destination=None, region=None, endpoint=None, enable_encryption=None,
                 encryption_type=None, kms_key=None, canned_acl=None):
        super(S3StorageInfo, self).__init__()
        if destination is not None:
            self['destination'] = destination
        if region is not None:
            self['region'] = region
        if endpoint is not None:
            self['endpoint'] = endpoint
        if enable_encryption is not None:
            self['enable_encryption'] = enable_encryption
        if encryption_type is not None:
            self['encryption_type'] = encryption_type
        if kms_key is not None:
            self['kms_key'] = kms_key
        if canned_acl is not None:
            self['canned_acl'] = canned_acl

class ClusterTag(dict):
    def __init__(self, key=None, value=None):
        super(ClusterTag, self).__init__()
        if key is not None:
            self['key'] = key
        if value is not None:
            self['value'] = value

class SparkConfPair(dict):
    def __init__(self, key=None, value=None):
        super(SparkConfPair, self).__init__()
        if key is not None:
            self['key'] = key
        if value is not None:
            self['value'] = value

class SparkEnvPair(dict):
    def __init__(self, key=None, value=None):
        super(SparkEnvPair, self).__init__()
        if key is not None:
            self['key'] = key
        if value is not None:
            self['value'] = value

class NewCluster(dict):
    def __init__(self, cluster_name=None, spark_version=None, spark_conf=None,
                 aws_attributes=None, node_type_id=None, driver_node_type_id=None,
                 ssh_public_keys=None, custom_tags=None, cluster_log_conf=None,
                 spark_env_vars=None, autotermination_minutes=None, enable_elastic_disk=None,
                 num_workers=None, autoscale=None):
        super(NewCluster, self).__init__()
        if cluster_name is not None:
            self['cluster_name'] = cluster_name
        if spark_version is not None:
            self['spark_version'] = spark_version
        if spark_conf is not None:
            self['spark_conf'] = spark_conf
        if aws_attributes is not None:
            self['aws_attributes'] = aws_attributes
            if not isinstance(aws_attributes, dict):
                raise TypeError('Expected databricks.AwsAttributes() or dict for field aws_attributes')
        if node_type_id is not None:
            self['node_type_id'] = node_type_id
        if driver_node_type_id is not None:
            self['driver_node_type_id'] = driver_node_type_id
        if ssh_public_keys is not None:
            self['ssh_public_keys'] = ssh_public_keys
        if custom_tags is not None:
            self['custom_tags'] = custom_tags
        if cluster_log_conf is not None:
            self['cluster_log_conf'] = cluster_log_conf
            if not isinstance(cluster_log_conf, dict):
                raise TypeError('Expected databricks.ClusterLogConf() or dict for field cluster_log_conf')
        if spark_env_vars is not None:
            self['spark_env_vars'] = spark_env_vars
        if autotermination_minutes is not None:
            self['autotermination_minutes'] = autotermination_minutes
        if enable_elastic_disk is not None:
            self['enable_elastic_disk'] = enable_elastic_disk
        if num_workers is not None:
            self['num_workers'] = num_workers
        if autoscale is not None:
            self['autoscale'] = autoscale
            if not isinstance(autoscale, dict):
                raise TypeError('Expected databricks.AutoScale() or dict for field autoscale')

class CronSchedule(dict):
    def __init__(self, quartz_cron_expression, timezone_id):
        super(CronSchedule, self).__init__()
        if quartz_cron_expression is not None:
            self['quartz_cron_expression'] = quartz_cron_expression
        if timezone_id is not None:
            self['timezone_id'] = timezone_id

class JobSettings(dict):
    def __init__(self, name=None, existing_cluster_id=None, new_cluster=None, libraries=None,
                 email_notifications=None, timeout_seconds=None, max_retries=None,
                 min_retry_interval_millis=None, retry_on_timeout=None, schedule=None,
                 notebook_task=None, spark_jar_task=None, max_concurrent_runs=None):
        super(JobSettings, self).__init__()
        if name is not None:
            self['name'] = name
        if existing_cluster_id is not None:
            self['existing_cluster_id'] = existing_cluster_id
        if new_cluster is not None:
            self['new_cluster'] = new_cluster
            if not isinstance(new_cluster, dict):
                raise TypeError('Expected databricks.NewCluster() or dict for field new_cluster')
        if libraries is not None:
            self['libraries'] = libraries
        if email_notifications is not None:
            self['email_notifications'] = email_notifications
            if not isinstance(email_notifications, dict):
                raise TypeError('Expected databricks.JobEmailNotifications() or dict for field email_notifications')
        if timeout_seconds is not None:
            self['timeout_seconds'] = timeout_seconds
        if max_retries is not None:
            self['max_retries'] = max_retries
        if min_retry_interval_millis is not None:
            self['min_retry_interval_millis'] = min_retry_interval_millis
        if retry_on_timeout is not None:
            self['retry_on_timeout'] = retry_on_timeout
        if schedule is not None:
            self['schedule'] = schedule
            if not isinstance(schedule, dict):
                raise TypeError('Expected databricks.CronSchedule() or dict for field schedule')
        if notebook_task is not None:
            self['notebook_task'] = notebook_task
            if not isinstance(notebook_task, dict):
                raise TypeError('Expected databricks.NotebookTask() or dict for field notebook_task')
        if spark_jar_task is not None:
            self['spark_jar_task'] = spark_jar_task
            if not isinstance(spark_jar_task, dict):
                raise TypeError('Expected databricks.SparkJarTask() or dict for field spark_jar_task')
        if max_concurrent_runs is not None:
            self['max_concurrent_runs'] = max_concurrent_runs

class JobEmailNotifications(dict):
    def __init__(self, on_start=None, on_success=None, on_failure=None):
        super(JobEmailNotifications, self).__init__()
        if on_start is not None:
            self['on_start'] = on_start
        if on_success is not None:
            self['on_success'] = on_success
        if on_failure is not None:
            self['on_failure'] = on_failure

class NotebookTask(dict):
    def __init__(self, notebook_path, base_parameters=None):
        super(NotebookTask, self).__init__()
        if notebook_path is not None:
            self['notebook_path'] = notebook_path
        if base_parameters is not None:
            self['base_parameters'] = base_parameters

class ParamPair(dict):
    def __init__(self, key=None, value=None):
        super(ParamPair, self).__init__()
        if key is not None:
            self['key'] = key
        if value is not None:
            self['value'] = value

class SparkJarTask(dict):
    def __init__(self, jar_uri=None, main_class_name=None, parameters=None):
        super(SparkJarTask, self).__init__()
        if jar_uri is not None:
            self['jar_uri'] = jar_uri
        if main_class_name is not None:
            self['main_class_name'] = main_class_name
        if parameters is not None:
            self['parameters'] = parameters

class EggSpecification(dict):
    def __init__(self, uri=None):
        super(EggSpecification, self).__init__()
        if uri is not None:
            self['uri'] = uri

class JarSpecification(dict):
    def __init__(self, uri=None):
        super(JarSpecification, self).__init__()
        if uri is not None:
            self['uri'] = uri

class MavenSpecification(dict):
    def __init__(self):
        super(MavenSpecification, self).__init__()
        pass

class PipSpecification(dict):
    def __init__(self, package_name=None, version_specifier=None):
        super(PipSpecification, self).__init__()
        if package_name is not None:
            self['package_name'] = package_name
        if version_specifier is not None:
            self['version_specifier'] = version_specifier

class Library(dict):
    def __init__(self, jar=None, egg=None, pypi=None, maven=None):
        super(Library, self).__init__()
        if jar is not None:
            self['jar'] = jar
        if egg is not None:
            self['egg'] = egg
        if pypi is not None:
            self['pypi'] = pypi
            if not isinstance(pypi, dict):
                raise TypeError('Expected databricks.PythonPyPiLibrary() or dict for field pypi')
        if maven is not None:
            self['maven'] = maven
            if not isinstance(maven, dict):
                raise TypeError('Expected databricks.MavenLibrary() or dict for field maven')

class MavenLibrary(dict):
    def __init__(self, coordinates, repo=None, exclusions=None):
        super(MavenLibrary, self).__init__()
        if coordinates is not None:
            self['coordinates'] = coordinates
        if repo is not None:
            self['repo'] = repo
        if exclusions is not None:
            self['exclusions'] = exclusions

class PythonPyPiLibrary(dict):
    def __init__(self, package, repo=None):
        super(PythonPyPiLibrary, self).__init__()
        if package is not None:
            self['package'] = package
        if repo is not None:
            self['repo'] = repo
