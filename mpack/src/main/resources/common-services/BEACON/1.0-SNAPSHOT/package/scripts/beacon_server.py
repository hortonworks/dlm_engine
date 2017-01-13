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

import beacon_server_upgrade

from resource_management.core.logger import Logger
from resource_management.libraries.script import Script
from resource_management.libraries.functions import check_process_status
from resource_management.libraries.functions import Direction
from resource_management.libraries.functions.security_commons import build_expectations
from resource_management.libraries.functions.security_commons import cached_kinit_executor
from resource_management.libraries.functions.security_commons import get_params_from_filesystem
from resource_management.libraries.functions.security_commons import validate_security_config_properties
from resource_management.libraries.functions.security_commons import FILE_TYPE_PROPERTIES

from beacon import beacon
from ambari_commons import OSConst
from ambari_commons.os_family_impl import OsFamilyFuncImpl, OsFamilyImpl
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions import StackFeature

class BeaconServer(Script):
  def configure(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    beacon('server', action='config', upgrade_type=upgrade_type)

  def start(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    beacon('server', action='start', upgrade_type=upgrade_type)

  def stop(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    beacon('server', action='stop', upgrade_type=upgrade_type)

    # if performing an upgrade (ROLLING / NON_ROLLING), backup some directories after stopping beacon
    if upgrade_type is not None:
      beacon_server_upgrade.post_stop_backup()

@OsFamilyImpl(os_family=OsFamilyImpl.DEFAULT)
class BeaconServerLinux(BeaconServer):
  def get_component_name(self):
    return "beacon-server"

  def install(self, env):
    import params
    env.set_params(params)
    self.install_packages(env)
    self.configure(env)

  def status(self, env):
    import status_params
    env.set_params(status_params)
    check_process_status(status_params.server_pid_file)

  def pre_upgrade_restart(self, env, upgrade_type=None):
    pass

  def security_status(self, env):
      self.put_structured_out({"securityState": "UNSECURED"})

  def get_log_folder(self):
    import params
    return params.beacon_log_dir

  def get_user(self):
    import params
    return params.beacon_user

if __name__ == "__main__":
  BeaconServer().execute()
