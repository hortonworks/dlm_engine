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
import os
import sys
import subprocess
import beacon_config as bc

def launch_java_process(java_bin, java_class, class_path, jdk_options, arguments):
    cmd = [java_bin]
    cmd.extend(jdk_options)
    cmd.extend(['-cp', class_path, java_class])
    cmd.extend(arguments)

    process = subprocess.Popen(filter(None,cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    out,err = process.communicate()
    if process.poll() == False:
        print out
    else:
        print err
        sys.exit(1)

cmd = sys.argv[0]
bc.init_config(cmd, 'client')


jdk_options =  [bc.heap]
jdk_options.extend(bc.options)
jdk_options.extend([os.getenv('BEACON_PROPERTIES'),
                    '-Dlog4j.configuration=beacon-log4j.xml',
                    '-Dbeacon.log.appender=console',
                    '-Dbeacon.log.level=error'])

java_class = 'com.hortonworks.beacon.client.cli.BeaconCLI'
launch_java_process(bc.java_bin, java_class, bc.class_path, jdk_options, sys.argv[1:len(sys.argv)])







