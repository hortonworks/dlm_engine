
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

import os.path
import traceback
import time

# Local Imports
from resource_management.core.environment import Environment
from resource_management.core.source import InlineTemplate
from resource_management.core.exceptions import Fail
from resource_management.core.source import Template
from resource_management.core.source import StaticFile
from resource_management.core.source import DownloadSource
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
from resource_management.libraries.script.config_dictionary import UnknownConfiguration

import ranger_api_functions

@OsFamilyFuncImpl(os_family = OsFamilyImpl.DEFAULT)
def beacon(type, action = None, upgrade_type=None):
  import params

  if action == 'config':
    create_directory(params.beacon_home_dir)
    create_directory(params.beacon_plugin_staging_dir)

    cloud_cred_provider = params.beacon_cloud_cred_provider_dir.split('://')[1]
    cloud_cred_parts = cloud_cred_provider.split('/', 1)
    create_directory("/" + cloud_cred_parts[1], cloud_cred_parts[0])

    if params.is_hive_installed:
      if not isinstance(params.hive_repl_cmrootdir, UnknownConfiguration):
        params.HdfsResource(params.hive_repl_cmrootdir,
                          type = "directory",
                          action = "create_on_execute",
                          owner = params.hive_user,
                          mode = 01777)
      if not isinstance(params.hive_repl_rootdir, UnknownConfiguration):
        params.HdfsResource(params.hive_repl_rootdir,
                          type = "directory",
                          action = "create_on_execute",
                          owner = params.hive_user,
                          mode = 0700)


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

        Execute(format('{beacon_home}/bin/beacon setup'),
          user = params.beacon_user,
          path = params.hadoop_bin_dir,
          environment=environment_dictionary
        )

        if params.download_mysql_driver:
           download_mysql_driver()

        Execute(format('{beacon_home}/bin/beacon start'),
          user = params.beacon_user,
          path = params.hadoop_bin_dir,
          environment=environment_dictionary,
          not_if = process_exists,
        )

        if params.has_ranger_admin:
          ranger_admin_url = params.config['configurations']['admin-properties']['policymgr_external_url']
          ranger_admin_user = params.config['configurations']['ranger-env']['admin_username']
          ranger_admin_passwd = params.config['configurations']['ranger-env']['admin_password']

          if not params.security_enabled:
            # Creating/Updating beacon.ranger.user with role "ROLE_SYS_ADMIN"
            response_user = ranger_api_functions.get_user(ranger_admin_url, params.beacon_ranger_user, format("{ranger_admin_user}:{ranger_admin_passwd}"))
            if response_user is not None and response_user['name'] == params.beacon_ranger_user:
              response_user_role = response_user['userRoleList'][0]
              Logger.info(format("Beacon Ranger User with username {beacon_ranger_user} exists with role {response_user_role}"))
              if response_user_role != "ROLE_SYS_ADMIN":
                response_user_role = ranger_api_functions.update_user_role(ranger_admin_url, params.beacon_ranger_user, "ROLE_SYS_ADMIN", format("{ranger_admin_user}:{ranger_admin_passwd}"))
            else:
              response_code = ranger_api_functions.create_user(ranger_admin_url, params.beacon_ranger_user, params.beacon_ranger_password, "ROLE_SYS_ADMIN", format("{ranger_admin_user}:{ranger_admin_passwd}"))

          # Updating beacon_user role depending upon cluster environment
          count = 0
          while count < 10:
            beacon_user_get = ranger_api_functions.get_user(ranger_admin_url, params.beacon_user, format("{ranger_admin_user}:{ranger_admin_passwd}"))
            if beacon_user_get is not None:
              break
            else:
              time.sleep(10) # delay for 10 seconds
              count = count + 1
              Logger.error(format('Retrying to fetch {beacon_user} user from Ranger Admin for {count} time(s)'))

          if beacon_user_get is not None and beacon_user_get['name'] == params.beacon_user:
            beacon_user_get_role = beacon_user_get['userRoleList'][0]
            if params.security_enabled and beacon_user_get_role != "ROLE_SYS_ADMIN":
              beacon_service_user = ranger_api_functions.update_user_role(ranger_admin_url, params.beacon_user, "ROLE_SYS_ADMIN", format("{ranger_admin_user}:{ranger_admin_passwd}"))
            elif not params.security_enabled and beacon_user_get_role != "ROLE_USER":
              beacon_service_user = ranger_api_functions.update_user_role(ranger_admin_url, params.beacon_user, "ROLE_USER", format("{ranger_admin_user}:{ranger_admin_passwd}"))

          if params.ranger_hive_plugin_enabled:
            # Get Ranger Hive default policy for resource database, table, column
            response_policy = ranger_api_functions.get_ranger_service_default_policy(ranger_admin_url, params.service_name, format("{ranger_admin_user}:{ranger_admin_passwd}"), ['database', 'table', 'column'])
            if response_policy:
              user_present = ranger_api_functions.check_user_policy(response_policy, params.beacon_user)
              if not user_present and beacon_user_get is not None and beacon_user_get['name'] == params.beacon_user:
                # Updating beacon_user in Ranger Hive default policy for resource database, table, column
                policy_id = response_policy['id']
                beacon_user_policy_item = {'groups': [], 'conditions': [], 'users': [params.beacon_user], 'accesses': [{'isAllowed': True, 'type': 'all'}, {'isAllowed': True, 'type': 'repladmin'}], 'delegateAdmin': False}
                policy_data = ranger_api_functions.update_policy_item(response_policy, beacon_user_policy_item)
                update_policy_response = ranger_api_functions.update_policy(ranger_admin_url, policy_id, policy_data, format("{ranger_admin_user}:{ranger_admin_passwd}"))

            # Get Ranger Hive default policy for resource hiveservice
            response_policy = ranger_api_functions.get_ranger_service_default_policy(ranger_admin_url, params.service_name, format("{ranger_admin_user}:{ranger_admin_passwd}"), ['hiveservice'])
            if response_policy:
              user_present = ranger_api_functions.check_user_policy(response_policy, params.beacon_user)
              if not user_present and beacon_user_get is not None and beacon_user_get['name'] == params.beacon_user:
                # Updating beacon_user in Ranger Hive default policy for resource hiveservice
                policy_id = response_policy['id']
                beacon_user_policy_item = {'groups': [], 'conditions': [], 'users': [params.beacon_user], 'accesses': [{'isAllowed': True, 'type': 'serviceadmin'}], 'delegateAdmin': False}
                policy_data = ranger_api_functions.update_policy_item(response_policy, beacon_user_policy_item)
                update_policy_response = ranger_api_functions.update_policy(ranger_admin_url, policy_id, policy_data, format("{ranger_admin_user}:{ranger_admin_passwd}"))

          if params.ranger_atlas_plugin_enabled:
            # Creating beacon.atlas.user with role "ROLE_USER"
            beacon_atlas_user_response = ranger_api_functions.get_user(ranger_admin_url, params.beacon_atlas_user, format("{ranger_admin_user}:{ranger_admin_passwd}"))
            if beacon_atlas_user_response is not None and beacon_atlas_user_response['name'] == params.beacon_atlas_user:
              beacon_atlas_user_role = beacon_atlas_user_response['userRoleList'][0]
              Logger.info(format("Beacon Atlas User with username {beacon_atlas_user} exists with role {beacon_atlas_user_role}"))
            else:
              beacon_atlas_user_create_response_code = ranger_api_functions.create_user(ranger_admin_url, params.beacon_atlas_user, params.beacon_atlas_password, "ROLE_USER", format("{ranger_admin_user}:{ranger_admin_passwd}"))

            # Get Ranger Atlas default policy for ENTITY resource
            atlas_policy_response = ranger_api_functions.get_ranger_service_default_policy(ranger_admin_url, params.ranger_atlas_service_name, format("{ranger_admin_user}:{ranger_admin_passwd}"), ['entity'])

            if params.security_enabled:
              get_beacon_atlas_user = params.beacon_user
            else:
              get_beacon_atlas_user = params.beacon_atlas_user

            if atlas_policy_response:
              beacon_atlas_user_present = ranger_api_functions.check_user_policy(atlas_policy_response, get_beacon_atlas_user)
              if not beacon_atlas_user_present:
                # Updating beacon atlas user in Ranger Atlas default policy for entity resource
                atlas_policy_id = atlas_policy_response['id']
                beacon_atlas_user_policy_item = {'groups': [], 'conditions': [], 'users': [get_beacon_atlas_user], 'accesses': [{'type': 'read', 'isAllowed': True}, {'type': 'create', 'isAllowed': True}, {'type': 'update', 'isAllowed': True}, {'type':'delete', 'isAllowed': True}, {'type':'all', 'isAllowed': True}]}
                atlas_policy_data = ranger_api_functions.update_policy_item(atlas_policy_response, beacon_atlas_user_policy_item)
                atlas_update_policy_response = ranger_api_functions.update_policy(ranger_admin_url, atlas_policy_id, atlas_policy_data, format("{ranger_admin_user}:{ranger_admin_passwd}"))

            # Get Ranger Atlas default policy for OPERATION resource
            atlas_operation_policy_response = ranger_api_functions.get_ranger_service_default_policy(ranger_admin_url, params.ranger_atlas_service_name, format("{ranger_admin_user}:{ranger_admin_passwd}"), ['operation'])
            if atlas_operation_policy_response:
              beacon_atlas_user_present = ranger_api_functions.check_user_policy(atlas_operation_policy_response, get_beacon_atlas_user)
              if not beacon_atlas_user_present:
                # Updating beacon atlas user in Ranger Atlas default policy for operation resource
                atlas_operation_policy_id = atlas_operation_policy_response['id']
                beacon_atlas_user_policy_item = {'groups': [], 'conditions': [], 'users': [get_beacon_atlas_user], 'accesses': [{'type': 'read', 'isAllowed': True}, {'type': 'create', 'isAllowed': True}, {'type': 'update', 'isAllowed': True}, {'type':'delete', 'isAllowed': True}, {'type':'all', 'isAllowed': True}]}
                atlas_operation_policy_data = ranger_api_functions.update_policy_item(atlas_operation_policy_response, beacon_atlas_user_policy_item)
                atlas_operation_policy_update_response = ranger_api_functions.update_policy(ranger_admin_url, atlas_operation_policy_id, atlas_operation_policy_data, format("{ranger_admin_user}:{ranger_admin_passwd}"))
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


def create_directory(directory, scheme = None):
  import params

  if (scheme is None or scheme == ''):
    if params.is_hdfs_installed:
      scheme = 'hdfs'
    else:
      scheme = 'file'

  Logger.info("Creating directory {0}:/{1}".format(scheme, directory))
  if scheme == 'file':
    Directory(directory,
              owner = params.beacon_user,
              create_parents = True,
              mode = 0755,
              cd_access = "a")
  elif scheme == 'hdfs':
    params.HdfsResource(directory,
                        type = "directory",
                        action = "create_on_execute",
                        owner = params.beacon_user,
                        mode = 0775)
    params.HdfsResource(None, action = "execute")

def download_mysql_driver():
  import params

  if params.jdbc_jar_name is None:
    raise Fail("Mysql JDBC driver not installed on ambari-server")

  File(
    params.mysql_driver_target,
    content=DownloadSource(params.driver_source),
    mode=0644
  )
