
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

import os
from resource_management.libraries.functions.check_process_status import check_process_status

RESULT_CODE_OK = 'OK'
RESULT_CODE_CRITICAL = 'CRITICAL'
RESULT_CODE_UNKNOWN = 'UNKNOWN'

BEACON_PID_DIR_KEY = '{{beacon-env/beacon_pid_dir}}'


def get_tokens():
    """
    Returns a tuple of tokens in the format {{site/property}} that will be used
    to build the dictionary passed into execute
    """
    return (BEACON_PID_DIR_KEY,)


def execute(configurations={}, parameters={}, host_name=None):
    """
    Returns a tuple containing the result code and a pre-formatted result label

    Keyword arguments:
    configurations (dictionary): a mapping of configuration key to value
    parameters (dictionary): a mapping of script parameter key to value
    host_name (string): the name of this host where the alert is running
    """

    try:
        beacon_pid_dir = configurations[BEACON_PID_DIR_KEY]
        beacon_pid_file = os.path.join(beacon_pid_dir, "beacon.pid")
        check_process_status(beacon_pid_file)
        return (RESULT_CODE_OK, ["Beacon Server process is running"])
    except:
        return (RESULT_CODE_CRITICAL, ['Beacon Server process is not running'])
