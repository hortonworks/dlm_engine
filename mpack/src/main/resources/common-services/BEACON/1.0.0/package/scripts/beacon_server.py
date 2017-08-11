
"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your
company and Hortonworks, Inc. or an authorized affiliate or partner
thereof, any use, reproduction, modification, redistribution, sharing,
lending or other exploitation of all or any part of the contents of this
software is strictly prohibited.
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
    self.configure(env)
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
