
"""
  HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES

  (c) 2016-2018 Hortonworks, Inc. All rights reserved.

  This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
  Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
  to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
  properly licensed third party, you do not have any rights to this code.

  If this code is provided to you under the terms of the AGPLv3:
  (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
  (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
  (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
    FROM OR RELATED TO THE CODE; AND
  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
    OR LOSS OR CORRUPTION OF DATA.
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
    'BEACON' : 'beacon'
}

component_directory = Script.get_component_from_role(SERVER_ROLE_DIRECTORY_MAP, "BEACON")

config = Script.get_config()
stack_root = "/usr/dlm"

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
