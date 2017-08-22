
"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your
company and Hortonworks, Inc. or an authorized affiliate or partner
thereof, any use, reproduction, modification, redistribution, sharing,
lending or other exploitation of all or any part of the contents of this
software is strictly prohibited.
"""

import os.path
import traceback

# Local Imports
from resource_management.core.environment import Environment
from resource_management.core.source import InlineTemplate
from resource_management.core.source import Template
from resource_management.core.source import StaticFile
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
from resource_management.libraries.functions.security_commons import update_credential_provider_path
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.resources.xml_config import XmlConfig
from ambari_commons.constants import SERVICE
from resource_management.core.logger import Logger

from ambari_commons import OSConst
from ambari_commons.os_family_impl import OsFamilyFuncImpl, OsFamilyImpl

@OsFamilyFuncImpl(os_family = OsFamilyImpl.DEFAULT)
def beacon(type, action = None, upgrade_type=None):
  import params

  if action == 'config':
    params.HdfsResource(params.beacon_home_dir,
      type = "directory",
      action = "create_on_execute",
      owner = params.beacon_user,
      mode = 0755)

    params.HdfsResource(params.beacon_plugin_staging_dir,
      type = "directory",
      action = "create_on_execute",
      owner = params.beacon_user,
      mode = 0775)

    if params.is_hive_installed:
      params.HdfsResource(params.hive_repl_cmrootdir,
                          type = "directory",
                          action = "create_on_execute",
                          owner = params.hive_user,
                          mode = 01777)
      params.HdfsResource(params.hive_repl_rootdir,
                          type = "directory",
                          action = "create_on_execute",
                          owner = params.hive_user,
                          mode = 0700)

    params.HdfsResource(None, action = "execute")

    Directory(params.beacon_pid_dir,
      owner = params.beacon_user,
      create_parents = True,
      mode = 0755,
      cd_access = "a",
    )

    Directory(params.beacon_data_dir,
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

  environment_dictionary = {
    "HADOOP_HOME" : params.hadoop_home_dir,
    "JAVA_HOME" : params.java_home,
    "BEACON_LOG_DIR" : params.beacon_log_dir,
    "BEACON_PID_DIR" : params.beacon_pid_dir,
    "BEACON_DATA_DIR" : params.beacon_data_dir,
    "BEACON_CLUSTER" : params.beacon_cluster_name,
    "HDP_VERSION": params.hadoop_stack_version,
    "HADOOP_CONF": params.hadoop_conf_dir
  }
  pid = get_user_call_output.get_user_call_output(format("cat {server_pid_file}"), user=params.beacon_user, is_checked_call=False)[1]
  process_exists = format("ls {server_pid_file} && ps -p {pid}")

  if type == 'server':
    if action == 'start':
      try:

        if params.credential_store_enabled:
          if 'hadoop.security.credential.provider.path' in params.beacon_env:
            credential_provider_path = params.beacon_env['hadoop.security.credential.provider.path']
            credential_provider_src_path = credential_provider_path[len('jceks://file'):]
            File(params.beacon_credential_provider_path[len('jceks://file'):],
              owner = params.beacon_user,
              group = params.user_group,
              mode = 0640,
              content = StaticFile(credential_provider_src_path)
            )
          else:
            Logger.error("hadoop.security.credential.provider.path property not found in beacon-env config-type")

        File(os.path.join(params.beacon_conf_dir, 'beacon.yml'),
           owner='root',
           group='root',
           mode=0644,
           content=Template("beacon.yml.j2")
        )

        params.beacon_security_site = update_credential_provider_path(
          params.beacon_security_site,
          'beacon-security-site',
          os.path.join(params.beacon_conf_dir, 'beacon-security-site.jceks'),
          params.beacon_user,
          params.user_group
        )

        XmlConfig("beacon-security-site.xml",
          conf_dir = params.beacon_conf_dir,
          configurations = params.beacon_security_site,
          configuration_attributes = params.config['configuration_attributes']['beacon-security-site'],
          owner = params.beacon_user,
          group = params.user_group,
          mode = 0644
        )

        Execute( params.beacon_schema_create_command,
           user = params.beacon_user
        )

        Execute(format('{beacon_home}/bin/beacon start'),
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
        Execute(format('{beacon_home}/bin/beacon stop'),
          user = params.beacon_user,
          path = params.hadoop_bin_dir,
          environment=environment_dictionary)
      except:
        show_logs(params.beacon_log_dir, params.beacon_user)
        raise

      File(params.server_pid_file, action = 'delete')
