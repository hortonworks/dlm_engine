#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

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
                        beacon_cluster_arg, beacon_cluster,
                        out_file, pid_file):
    with open(out_file, 'w') as out_file_f:
        cmd = [java_bin]
        cmd.extend(jdk_options)
        cmd.extend(['-cp', class_path, java_class, beacon_app_arg, beacon_app_war, beacon_cluster_arg, beacon_cluster])
        process = subprocess.Popen(filter(None,cmd),stdout=out_file_f, stderr=out_file_f, shell=False)
        #process = subprocess.Popen(' '.join(filter(None, cmd)),
        #                           stdout=out_file_f, stderr=out_file_f,
	#			   shell=False)
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

bc.init_config(cmd, 'server')
service_entry = '--service' in sys.argv
if not service_entry:
   check_running(bc.pid_file)
bc.mkdir_p(bc.log_dir)


jdk_options =  [bc.options, os.getenv('BEACON_PROPERTIES'),
     '-Dbeacon.log.dir=' + bc.log_dir,
     '-Dbeacon.embeddedmq.data=' + bc.data_dir,
     '-Dbeacon.home=' + bc.home_dir,
     '-Dbeacon.app.type=beacon',
     '-Dconfig.location=' + bc.conf]

# Add all the JVM command line options
jdk_options.extend([arg for arg in sys.argv if arg.startswith('-D')])
other_args = ' '.join([arg for arg in sys.argv[3:] if not arg.startswith('-D')])

war_file = os.path.join(bc.webapp_dir, "beacon")
out_file = os.path.join(bc.log_dir, 'beacon.out.' + time.strftime('%Y%m%d%H%M%S'))
java_class = 'com.hortonworks.beacon.main.Main'
beacon_app_arg = '-app'
beacon_app_war = war_file
beacon_cluster_arg = '-localcluster'
beacon_cluster = os.getenv("BEACON_CLUSTER")

#if beacon_cluster == None :
#   print "BEACON_CLUSTER env variable must be set to local cluster name"
#   sys.exit(1)

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
                    beacon_cluster_arg, beacon_cluster,
                    out_file, bc.pid_file)

print 'beacon started using hadoop version: ' + \
      get_hadoop_version(bc.java_bin, bc.class_path)
