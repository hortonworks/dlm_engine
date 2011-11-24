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
import sys
import os
import subprocess
import time
import beacon_config as bc


def check_running(pid_file):
    if os.path.exists(pid_file):
        pid_file = open(pid_file)
        pid_file.seek(0)
        pid = int(pid_file.readline())
        try:
            os.kill(pid, 0)
            os.sys.exit('beacon is running as process ' + str(pid) + '. stop it first.')
        except OSError:
        	return False

def launch_java_process(java_bin, java_class, class_path, jdk_options,
                        beacon_app_arg, beacon_app_war,
                        beacon_cluster_arg, beacon_cluster, other_args,
                        out_file, pid_file):
    with open(out_file, 'w', 0) as out_file_f:
        cmd = [java_bin]
        cmd.extend(jdk_options)
        cmd.extend(['-cp', class_path, java_class, beacon_app_arg, beacon_app_war, beacon_cluster_arg, beacon_cluster])
        cmd.extend(other_args)
        process = subprocess.Popen(filter(None,cmd), stdout = out_file_f, stderr=subprocess.STDOUT, shell=False, bufsize=0)
        pid_f = open(pid_file, 'w')
        pid_f.write(str(process.pid))
        pid_f.close()


def get_hadoop_version(java_bin, class_path):
    FNULL = open(os.devnull, 'w')
    cmd = [java_bin, '-cp', class_path, 'org.apache.hadoop.util.VersionInfo']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=FNULL)
    lines = process.communicate()[0]
    return lines.splitlines()[0]  # return only the first line

cmd = sys.argv[0]

print os.environ

bc.init_config(cmd, 'server')
service_entry = '--service' in sys.argv
if not service_entry:
   check_running(bc.pid_file)
bc.mkdir_p(bc.log_dir)
bc.mkdir_p(bc.pid_dir)
bc.mkdir_p(bc.data_dir)

heapDumpPath = '-XX:HeapDumpPath=' + bc.log_dir
jdk_def_options = ['-server', '-XX:+HeapDumpOnOutOfMemoryError', heapDumpPath, '-XX:OnOutOfMemoryError=kill -9 %p']
jdk_options = []
jdk_options.extend(jdk_def_options)
jdk_options.append(bc.heap)
jdk_options.extend(bc.options)
jdk_options.extend([os.getenv('BEACON_PROPERTIES'),
     '-Dlog4j.configuration=beacon-log4j.xml',
     '-Dbeacon.log.dir=' + bc.log_dir,
     '-Dbeacon.data=' + bc.data_dir,
     '-Dbeacon.home=' + bc.home_dir,
     '-Dbeacon.app.type=beacon',
     '-Dconfig.location=' + bc.conf,
     '-Dbeacon.log.appender=FILE',
     '-Dbeacon.log.level=info',
     '-Dbeacon.log.filename=beacon-application-' + bc.hostname + '.log',
     '-Dderby.stream.error.file=' + bc.log_dir + '/derby.log'])

# Add all the JVM command line options
other_args=[]
for arg in sys.argv[3:]:
    if arg.startswith('-D'):
        jdk_options.extend(arg.split(' '))
    else:
        other_args.append(arg)

war_file = os.path.join(bc.webapp_dir, "beacon")
out_file = os.path.join(bc.log_dir, 'beacon-' + bc.hostname + '.out.' + time.strftime('%Y%m%d%H%M%S'))
java_class = 'com.hortonworks.beacon.main.Beacon'
beacon_app_arg = '-app'
beacon_app_war = war_file
beacon_cluster_arg = '-localcluster'
beacon_cluster = os.getenv("BEACON_CLUSTER")

if beacon_cluster == None :
   print "BEACON_CLUSTER env variable not set"
   print "Defaulting beacon_cluster to cluster-local"
   beacon_cluster = "cluster-local"


if service_entry:
    from xml.dom.minidom import getDOMImplementation
    dom = getDOMImplementation()
    xmlDoc = dom.createDocument(None, 'service', None)
    xmlDocRoot = xmlDoc.documentElement

    def appendTextElement(name, value):
        elem = xmlDoc.createElement(name)
        elem.appendChild(xmlDoc.createTextNode(value))
        xmlDocRoot.appendChild(elem)

    appendTextElement('id', app_type)
    appendTextElement('name', app_type)
    appendTextElement('description', 'This service runs ' + app_type)
    appendTextElement('executable', bc.java_bin)
    arguments = ' '.join([' '.join(jdk_options), '-cp', fc
    .class_path, java_class, class_arguments])
    appendTextElement('arguments', arguments)

    print xmlDoc.toprettyxml(indent='  ')
    sys.exit()

launch_java_process(bc.java_bin, java_class,
                    bc.class_path, jdk_options,
                    beacon_app_arg, beacon_app_war,
                    beacon_cluster_arg, beacon_cluster, other_args,
                    out_file, bc.pid_file)

print 'beacon started using hadoop version: ' + \
      get_hadoop_version(bc.java_bin, bc.class_path)
