#!/usr/bin/env python

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

# resolve links - $0 may be a soft link
import time
import os
import sys
import subprocess
import beacon_config as bc

def launch_java_process(java_bin, java_class, class_path, jdk_options, out_file):
    with open(out_file, 'w', 0) as out_file_f:
        cmd = [java_bin]
        cmd.extend(jdk_options)
        cmd.extend(['-cp', class_path, java_class])

        process = subprocess.Popen(filter(None,cmd), stdout = out_file_f, stderr=out_file_f, shell=False)
        process.communicate()
        if process.poll() == False:
            print "Beacon schema initialisation succeeded"
        else:
            log_file = os.path.join(bc.log_dir, log_filename)
            print "Beacon schema initialisation failed. See " + out_file + ", " + log_file + ' for details'
            sys.exit(1)

#        print process.returncode

print "Beacon - setting up schema"
cmd = sys.argv[0]
bc.init_config(cmd, 'server')

bc.mkdir_p(bc.log_dir)
bc.mkdir_p(bc.data_dir)

jdk_options =  [bc.heap]
jdk_options.extend(bc.options)
log_filename = 'beacon-schematool-' + bc.hostname + '.log'
jdk_options.extend([os.getenv('BEACON_PROPERTIES'),
                '-Dlog4j.configuration=beacon-log4j.xml',
                '-Dbeacon.log.dir=' + bc.log_dir,
                '-Dbeacon.log.appender=FILE',
                '-Dbeacon.log.level=info',
                '-Dbeacon.log.filename=' + log_filename,
                '-Dbeacon.data=' + bc.data_dir ])

java_class = 'com.hortonworks.beacon.tools.BeaconDBSetup'
out_file = os.path.join(bc.log_dir, 'beacon-schematool-' + bc.hostname + '.out.' + time.strftime('%Y%m%d%H%M%S'))

launch_java_process(bc.java_bin, java_class, bc.class_path, jdk_options, out_file)
