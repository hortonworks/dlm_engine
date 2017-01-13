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
from ambari_commons import OSCheck

from resource_management.libraries.functions import format
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions import StackFeature

# a map of the Ambari role to the component name
# for use with <stack-root>/current/<component>
SERVER_ROLE_DIRECTORY_MAP = {
    'BEACON' : 'beacon-server'
}

component_directory = Script.get_component_from_role(SERVER_ROLE_DIRECTORY_MAP, "BEACON")

config = Script.get_config()
stack_root = Script.get_stack_root()

stack_version_unformatted = config['hostLevelParams']['stack_version']
stack_version_formatted = format_stack_version(stack_version_unformatted)

beacon_pid_dir = config['configurations']['beacon-env']['beacon_pid_dir']
server_pid_file = format('{beacon_pid_dir}/beacon.pid')


beacon_conf_dir = "/etc/beacon/conf"
if stack_version_formatted and check_stack_feature(StackFeature.ROLLING_UPGRADE, stack_version_formatted):
    beacon_conf_dir = format("{stack_root}/current/{component_directory}/conf")

  # Security related/required params
hostname = config['hostname']
security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
tmp_dir = Script.get_tmp_dir()
beacon_user = config['configurations']['beacon-env']['beacon_user']

stack_name = default("/hostLevelParams/stack_name", None)
hadoop_home_dir = stack_select.get_hadoop_dir("home")
hadoop_bin_dir = stack_select.get_hadoop_dir("bin")
