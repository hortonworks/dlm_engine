#!/usr/bin/env ambari-python-wrap
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
import os
import fnmatch
import imp
import socket
import sys
import traceback

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STACKS_DIR = os.path.join(SCRIPT_DIR, '../../../../../stacks/')
PARENT_FILE = os.path.join(STACKS_DIR, 'service_advisor.py')

try:
  with open(PARENT_FILE, 'rb') as fp:
    service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
  traceback.print_exc()
  print "Failed to load parent"

class BEACON100ServiceAdvisor(service_advisor.ServiceAdvisor):

  def __init__(self, *args, **kwargs):
    self.as_super = super(BEACON100ServiceAdvisor, self)
    self.as_super.__init__(*args, **kwargs)

  def getServiceComponentLayoutValidations(self, services, hosts):

    componentsListList = [service["components"] for service in services["services"]]
    componentsList = [item["StackServiceComponents"] for sublist in componentsListList for item in sublist]

    items = []

    # Metron Must Co-locate with KAFKA_BROKER and STORM_SUPERVISOR
    return items

  def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):
    servicesList = [service['StackServices']['service_name'] for service in services['services']]
    putBeaconSecurityProperty = self.putProperty(configurations, 'beacon-security-site', services)
    putbeaconEnvProperty = self.putProperty(configurations, 'beacon-env', services)

    beacon_server_hosts = self.getComponentHostNames(services, 'BEACON', 'BEACON_SERVER')
    beacon_server_host = None
    if len(beacon_server_hosts) > 0:
      beacon_server_host = beacon_server_hosts[0]

    if 'forced-configurations' not in services:
      services["forced-configurations"] = []

    if 'beacon-env' in services['configurations'] and ('beacon_database' in services['configurations']['beacon-env']['properties']) \
      and ('beacon_store_driver' in services['configurations']['beacon-env']['properties']):
      beacon_database_type = services['configurations']['beacon-env']['properties']['beacon_database']
      putbeaconEnvProperty('beacon_store_driver', self.getBeaconDBDriver(beacon_database_type))

      if ('beacon_store_db_name' in services['configurations']['beacon-env']['properties']) \
        and ('beacon_store_url' in services['configurations']['beacon-env']['properties']):
        beacon_db_connection_url = services['configurations']['beacon-env']['properties']['beacon_store_url']
        beacon_store_db_name = services['configurations']['beacon-env']['properties']['beacon_store_db_name']
        protocol = self.getDBProtocol(beacon_database_type)
        old_schema_name = self.getOldPropertyValue(services, 'beacon-env', 'beacon_store_db_name')
        old_db_type = self.getOldPropertyValue(services, 'beacon-env', 'beacon_database')
        # under these if constructions we are checking if beacon server hostname available,
        # if it's default db connection url with "localhost" or if schema name was changed or if db type was changed (only for db type change from default mysql to existing mysql)
        # or if protocol according to current db type differs with protocol in db connection url(other db types changes)
        if beacon_server_host is not None:
          if (beacon_db_connection_url and "//localhost" in beacon_db_connection_url) \
            or old_schema_name or old_db_type or (protocol and beacon_db_connection_url \
            and not beacon_db_connection_url.startswith(protocol)):
            db_connection = self.getBeaconDBConnectionString(beacon_database_type).format(beacon_server_host, beacon_store_db_name)
            putbeaconEnvProperty('beacon_store_url', db_connection)

    knox_host = 'localhost'
    knox_port = '8443'
    if 'KNOX' in servicesList:
      knox_hosts = self.getComponentHostNames(services, "KNOX", "KNOX_GATEWAY")
      if len(knox_hosts) > 0:
        knox_hosts.sort()
        knox_host = knox_hosts[0]
      if 'gateway-site' in services['configurations'] and 'gateway.port' in services['configurations']['gateway-site']['properties']:
        knox_port = services['configurations']['gateway-site']['properties']['gateway.port']
      putBeaconSecurityProperty('beacon.sso.knox.providerurl', 'https://{0}:{1}/gateway/knoxsso/api/v1/websso'.format(knox_host, knox_port))

    beacon_user = 'beacon'
    if 'beacon-env' in services['configurations'] and 'beacon_user' in services['configurations']['beacon-env']['properties']:
      beacon_user = services['configurations']['beacon-env']['properties']['beacon_user']

    if 'HDFS' in servicesList and 'core-site' in services['configurations']:
      putHdfsCoreSiteProperty = self.putProperty(configurations, 'core-site', services)
      putHdfsCoreSitePropertyAttribute = self.putPropertyAttribute(configurations, 'core-site')
      beacon_old_user = self.getOldPropertyValue(services, 'beacon-env', 'beacon_user')

      putHdfsCoreSiteProperty('hadoop.proxyuser.{0}.hosts'.format(beacon_user), '*')
      putHdfsCoreSiteProperty('hadoop.proxyuser.{0}.groups'.format(beacon_user), '*')
      putHdfsCoreSiteProperty('hadoop.proxyuser.{0}.users'.format(beacon_user), '*')

      if beacon_old_user is not None and beacon_user != beacon_old_user:
        putHdfsCoreSitePropertyAttribute('hadoop.proxyuser.{0}.hosts'.format(beacon_old_user), 'delete', 'true')
        services['forced-configurations'].append({'type' : 'core-site', 'name' : 'hadoop.proxyuser.{0}.hosts'.format(beacon_old_user)})
        services['forced-configurations'].append({'type' : 'core-site', 'name' : 'hadoop.proxyuser.{0}.hosts'.format(beacon_user)})

        putHdfsCoreSitePropertyAttribute('hadoop.proxyuser.{0}.groups'.format(beacon_old_user), 'delete', 'true')
        services['forced-configurations'].append({'type' : 'core-site', 'name' : 'hadoop.proxyuser.{0}.groups'.format(beacon_old_user)})
        services['forced-configurations'].append({'type' : 'core-site', 'name' : 'hadoop.proxyuser.{0}.groups'.format(beacon_user)})

        putHdfsCoreSitePropertyAttribute('hadoop.proxyuser.{0}.users'.format(beacon_old_user), 'delete', 'true')
        services['forced-configurations'].append({'type' : 'core-site', 'name' : 'hadoop.proxyuser.{0}.users'.format(beacon_old_user)})
        services['forced-configurations'].append({'type' : 'core-site', 'name' : 'hadoop.proxyuser.{0}.users'.format(beacon_user)})

  def getOldPropertyValue(self, services, configType, propertyName):
    if services:
      if 'changed-configurations' in services.keys():
        changedConfigs = services["changed-configurations"]
        for changedConfig in changedConfigs:
          if changedConfig["type"] == configType and changedConfig["name"]== propertyName and "old_value" in changedConfig:
            return changedConfig["old_value"]
    return None

  def getBeaconDBDriver(self, databaseType):
    driverDict = {
      'NEW MYSQL DATABASE': 'com.mysql.jdbc.Driver',
      'NEW DERBY DATABASE': 'org.apache.derby.jdbc.EmbeddedDriver',
      'EXISTING MYSQL DATABASE': 'com.mysql.jdbc.Driver',
      'EXISTING MYSQL / MARIADB DATABASE': 'com.mysql.jdbc.Driver',
      'EXISTING POSTGRESQL DATABASE': 'org.postgresql.Driver',
      'EXISTING ORACLE DATABASE': 'oracle.jdbc.driver.OracleDriver',
      'EXISTING SQL ANYWHERE DATABASE': 'sap.jdbc4.sqlanywhere.IDriver'
    }
    return driverDict.get(databaseType.upper())

  def getBeaconDBConnectionString(self, databaseType):
    driverDict = {
      'NEW DERBY DATABASE': 'jdbc:derby:${{beacon.data.dir}}/${{beacon.store.db.name}}-db;create=true',
      'EXISTING MYSQL DATABASE': 'jdbc:mysql://{0}/{1}',
      'EXISTING MYSQL / MARIADB DATABASE': 'jdbc:mysql://{0}/{1}',
      'EXISTING ORACLE DATABASE': 'jdbc:oracle:thin:@//{0}:1521/{1}'
    }
    return driverDict.get(databaseType.upper())

  def getDBProtocol(self, databaseType):
    first_parts_of_connection_string = {
      'NEW MYSQL DATABASE': 'jdbc:mysql',
      'NEW DERBY DATABASE': 'jdbc:derby',
      'EXISTING MYSQL DATABASE': 'jdbc:mysql',
      'EXISTING MYSQL / MARIADB DATABASE': 'jdbc:mysql',
      'EXISTING POSTGRESQL DATABASE': 'jdbc:postgresql',
      'EXISTING ORACLE DATABASE': 'jdbc:oracle',
      'EXISTING SQL ANYWHERE DATABASE': 'jdbc:sqlanywhere'
    }
    return first_parts_of_connection_string.get(databaseType.upper())
