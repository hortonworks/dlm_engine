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

import os.path
import traceback

# Local Imports
from resource_management.core.environment import Environment
from resource_management.core.source import InlineTemplate
from resource_management.core.source import Template
from resource_management.core.source import  DownloadSource
from resource_management.core.resources import Execute
from resource_management.core.resources.service import Service
from resource_management.core.resources.service import ServiceConfig
from resource_management.core.resources.system import Directory
from resource_management.core.resources.system import File
from resource_management.libraries.functions import get_user_call_output
from resource_management.libraries.script import Script
from resource_management.libraries.resources import PropertiesFile
from resource_management.libraries.functions import format
from resource_management.libraries.functions.show_logs import show_logs
from resource_management.libraries.functions.setup_atlas_hook import has_atlas_in_cluster, setup_atlas_hook, install_atlas_hook_packages, setup_atlas_jar_symlinks
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions import StackFeature
from ambari_commons.constants import SERVICE
from resource_management.core.logger import Logger

from ambari_commons import OSConst
from ambari_commons.os_family_impl import OsFamilyFuncImpl, OsFamilyImpl

@OsFamilyFuncImpl(os_family = OsFamilyImpl.DEFAULT)
def beacon(type, action = None, upgrade_type=None):
  import params

  if action == 'config':
    Directory(params.beacon_pid_dir,
      owner = params.beacon_user,
      create_parents = True,
      mode = 0755,
      cd_access = "a",
    )

    Directory(params.beacon_log_dir,
      owner = params.beacon_user,
      create_parents = True,
      mode = 0755,
      cd_access = "a",
    )

    Directory(params.beacon_webapp_dir,
      owner = params.beacon_user,
      create_parents = True)

    Directory(params.beacon_home,
      owner = params.beacon_user,
      create_parents = True)

    Directory(params.etc_prefix_dir,
      mode = 0755,
      create_parents = True)

    Directory(params.beacon_conf_dir,
      owner = params.beacon_user,
      create_parents = True)

  environment_dictionary = { "HADOOP_HOME" : params.hadoop_home_dir,
                             "JAVA_HOME" : params.java_home,
                             "BEACON_CLUSTER" : params.cluster_name }
  pid = get_user_call_output.get_user_call_output(format("cat {server_pid_file}"), user=params.beacon_user, is_checked_call=False)[1]
  process_exists = format("ls {server_pid_file} && ps -p {pid}")

  if type == 'server':
    if action == 'start':
      try:
        Execute(format('python {beacon_home}/bin/beacon_start.py'),
          user = params.beacon_user,
          path = params.hadoop_bin_dir,
          environment=environment_dictionary,
          not_if = process_exists,
        )
      except:
        show_logs(params.beacon_log_dir, params.beacon_user)
        raise

    if action == 'stop':
      try:
        Execute(format('python {beacon_home}/bin/beacon_stop.py'),
          user = params.beacon_user,
          path = params.hadoop_bin_dir,
          environment=environment_dictionary)
      except:
        show_logs(params.beacon_log_dir, params.beacon_user)
        raise

      File(params.server_pid_file, action = 'delete')
