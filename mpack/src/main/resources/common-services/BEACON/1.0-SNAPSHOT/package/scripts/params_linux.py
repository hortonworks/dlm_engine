#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import functools
import os

from ambari_commons.os_check import OSCheck
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import format
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.expect import expect
from status_params import *
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources
from resource_management.libraries.functions.is_empty import is_empty
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.script import Script


# server configurations
java_home = config['hostLevelParams']['java_home']
beacon_cluster_name = config['clusterName']
host_name = config["hostname"]
java_version = expect("/hostLevelParams/java_version", int)
host_sys_prepped = default("/hostLevelParams/host_sys_prepped", False)

config = Script.get_config()
tmp_dir = Script.get_tmp_dir()
stack_root = Script.get_stack_root()
beacon_home_dir = '/user/beacon'
beacon_plugin_staging_dir = '/apps/beacon/plugin/stage'
beacon_root = 'beacon-server'
beacon_webapp_dir = format('{stack_root}/current/{beacon_root}/webapp')
beacon_home = format('{stack_root}/current/{beacon_root}')
beacon_env = config['configurations']['beacon-env']
user_group = config['configurations']['cluster-env']['user_group']
beacon_host_name = format(beacon_env['beacon_host_name'])
beacon_user = beacon_env['beacon_user']
beacon_pid_dir = beacon_env['beacon_pid_dir']
beacon_data_dir = beacon_env['beacon_data_dir']
beacon_log_dir = beacon_env['beacon_log_dir']
beacon_port = beacon_env['beacon_port']
beacon_principal = beacon_env['beacon_principal']
beacon_tls_port = beacon_env['beacon_tls_port']
beacon_tls_enabled = beacon_env['beacon_tls_enabled']
beacon_config_store_uri = beacon_env['beacon_config_store_uri']
beacon_results_per_page = beacon_env['beacon_results_per_page']
beacon_app_path = format('{beacon_webapp_dir}/beacon')
beacon_socket_buffer_size = beacon_env['beacon_socket_buffer_size']
beacon_services = beacon_env['beacon_services']

beacon_store_driver = beacon_env['beacon_store_driver']
beacon_store_url = format(beacon_env['beacon_store_url'])
beacon_store_user = beacon_env['beacon_store_user']
beacon_store_password = beacon_env['beacon_store_password']
beacon_store_schema_dir = format(beacon_env['beacon_store_schema_dir'])

beacon_quartz_prefix = beacon_env['beacon_quartz_prefix']
beacon_quartz_thread_pool = beacon_env['beacon_quartz_thread_pool']
beacon_retired_policy_older_than = beacon_env['beacon_retired_policy_older_than']
beacon_cleanup_service_frequency = beacon_env['beacon_cleanup_service_frequency']
beacon_house_keeping_threads = beacon_env['beacon_house_keeping_threads']
beacon_sync_status_frequency = beacon_env['beacon_sync_status_frequency']

beacon_store_max_connections = beacon_env['beacon_store_max_connections']
etc_prefix_dir = "/etc/beacon"

security_enabled = config['configurations']['cluster-env']['security_enabled']
hostname = config["hostname"]
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
hdfs_principal_name = config['configurations']['hadoop-env']['hdfs_principal_name']
dfs_type = default("/commandParams/dfs_type", "")
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
hadoop_home_dir = stack_select.get_hadoop_dir("home")
hadoop_bin_dir = stack_select.get_hadoop_dir("bin")
hadoop_conf_dir = conf_select.get_hadoop_conf_dir()
hdfs_site = config['configurations']['hdfs-site']
default_fs = config['configurations']['core-site']['fs.defaultFS']

beacon_dbsetup_tool = 'com.hortonworks.beacon.tools.BeaconDBSetup'
beacon_schema_create_command = format("{java_home}/bin/java -cp {beacon_home}/libext/*:{beacon_home}/conf:. -Dbeacon.log.dir={beacon_log_dir} {beacon_dbsetup_tool}")

HdfsResource = functools.partial(
  HdfsResource,
  user=hdfs_user,
  hdfs_resource_ignore_file = "/var/lib/ambari-agent/data/.hdfs_resource_ignore",
  security_enabled = security_enabled,
  keytab = hdfs_user_keytab,
  kinit_path_local = kinit_path_local,
  hadoop_bin_dir = hadoop_bin_dir,
  hadoop_conf_dir = hadoop_conf_dir,
  principal_name = hdfs_principal_name,
  hdfs_site = hdfs_site,
  default_fs = default_fs,
  immutable_paths = get_not_managed_resources(),
  dfs_type = dfs_type
 )

# install repo  - for future.
#yum_repo_type = beacon_env['repo_type']
#if yum_repo_type == 'local':
#    repo_url = 'file:///localrepo'
#else:
#    repo_url = beacon_env['repo_url']

# hadoop params
