
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
import ConfigParser
import os

from resource_management.core.logger import Logger

# is ambari major version 2.6 or not
def is_ambari_2_6():
    ambari_version = get_ambari_version()
    return ambari_version.startswith('2.6')

# find ambari version
def get_ambari_version():
    ambari_version = None
    AMBARI_AGENT_CONF = '/etc/ambari-agent/conf/ambari-agent.ini'
    ambari_agent_config = ConfigParser.RawConfigParser()
    if os.path.exists(AMBARI_AGENT_CONF):
        try:
            ambari_agent_config.read(AMBARI_AGENT_CONF)
            data_dir = ambari_agent_config.get('agent', 'prefix')
            ver_file = os.path.join(data_dir, 'version')
            f = open(ver_file, "r")
            ambari_version = f.read().strip()
            f.close()
        except Exception, e:
            Logger.info('Unable to determine ambari version from version file.')
            Logger.debug('Exception: %s' % str(e))
            # No hostname script identified in the ambari agent conf
            pass
        pass
    print ambari_version
    return ambari_version

# create hdfs directory
def create_hdfs_directory(directory, owner, mode):
    import params
    if is_ambari_2_6():
        params.HdfsResource(directory,
                            type = "directory",
                            action = "create_on_execute",
                            owner = owner,
                            mode = mode)
    else:
        params.HdfsResource(directory,
                            type = "directory",
                            action = "create_on_execute",
                            owner = owner,
                            mode = mode,
                            dfs_type=params.default_fs)
