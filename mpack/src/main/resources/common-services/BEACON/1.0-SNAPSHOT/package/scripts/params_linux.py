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
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources
from resource_management.libraries.functions.is_empty import is_empty
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.script import Script


# server configurations
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()
stack_root = Script.get_stack_root()
hostname = config['hostname']
beacon_root = 'beacon-server'
beacon_home = format('{stack_root}/current/{beacon_root}')
beacon_user = config['configurations']['beacon-env']['beacon_user']
user_group = config['configurations']['cluster-env']['user_group']
beacon_pid_dir = config['configurations']['beacon-env']['beacon_pid_dir']
beacon_log_dir = config['configurations']['beacon-env']['beacon_log_dir']
beacon_port = config['configurations']['beacon-env']['beacon_port']
beacon_webapp_dir = format('{stack_root}/current/{beacon_root}/webapp')
etc_prefix_dir = "/etc/beacon"


# install repo  - for future.
#yum_repo_type = config['configurations']['beacon-env']['repo_type']
#if yum_repo_type == 'local':
#    repo_url = 'file:///localrepo'
#else:
#    repo_url = config['configurations']['beacon-env']['repo_url']

# hadoop params
