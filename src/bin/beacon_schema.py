#!/usr/bin/env python
#
# Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between you or your
# company and Hortonworks, Inc. or an authorized affiliate or partner
# thereof, any use, reproduction, modification, redistribution, sharing,
# lending or other exploitation of all or any part of the contents of this
# software is strictly prohibited.
#

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
                '-Dbeacon.log.dir=' + bc.log_dir,
                '-Dbeacon.log.appender=FILE',
                '-Dbeacon.log.level=info',
                '-Dbeacon.log.filename=' + log_filename,
                '-Dbeacon.data=' + bc.data_dir ])

java_class = 'com.hortonworks.beacon.tools.BeaconDBSetup'
out_file = os.path.join(bc.log_dir, 'beacon-schematool-' + bc.hostname + '.out.' + time.strftime('%Y%m%d%H%M%S'))

launch_java_process(bc.java_bin, java_class, bc.class_path, jdk_options, out_file)