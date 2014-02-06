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
import falcon_config


def check_running(app_type, pid_file):
    if os.path.exists(pid_file):
        pid_file = open(pid_file)
        pid_file.seek(0)
        pid = int(pid_file.readline())
        try:
            os.kill(pid, 0)
            sys.exit(app_type + ' is running as process ' +
                     str(pid) + '. stop it first.')
        except:
            pass


def launch_java_process(java_bin, java_class, class_path, jdk_options,
                        class_arguments, out_file, pid_file):
    with open(out_file, 'w') as out_file_f:
        cmd = [java_bin, jdk_options, '-cp', class_path,
               java_class, class_arguments]
        process = subprocess.Popen(' '.join(cmd), stdout=out_file_f,
                                   stderr=out_file_f, shell=True)
        pid_f = open(pid_file, 'w')
        pid_f.write(str(process.pid))
        pid_f.close()


def get_hadoop_version(java_bin, class_path):
    cmd = [java_bin, '-cp', class_path, 'org.apache.hadoop.util.VersionInfo']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    lines = process.communicate()[0]
    return lines.splitlines()[0]  # return only the first line

cmd = sys.argv[0]
app_type = sys.argv[1]

falcon_config.init_config(cmd, 'server', app_type)
check_running(app_type, falcon_config.pid_file)
falcon_config.mkdir_p(falcon_config.log_dir)

jdk_options = ' '.join(
    [falcon_config.options, os.getenv('FALCON_PROPERTIES', ''),
     '-Dfalcon.log.dir=' + falcon_config.log_dir,
     '-Dfalcon.embeddedmq.data=' + falcon_config.data_dir,
     '-Dfalcon.home=' + falcon_config.home_dir,
     '-Dconfig.location=' + falcon_config.conf,
     '-Dfalcon.app.type=' + app_type,
     '-Dfalcon.catalog.service.enabled=' + os.getenv('CATALOG_ENABLED', '')])

# Add all the JVM command line options
jdk_options += ' '.join([arg for arg in sys.argv if arg.startswith('-D')])
other_args = ' '.join([arg for arg in sys.argv[3:] if not arg.startswith('-D')])

war_file = os.path.join(falcon_config.webapp_dir, app_type + '.war')
out_file = os.path.join(falcon_config.log_dir,
                        app_type + '.out.' + time.strftime('%Y%m%d%H%M%s'))
java_class = 'org.apache.falcon.Main'
class_arguments = '-app ' + war_file + ' ' + other_args

launch_java_process(falcon_config.java_bin, java_class,
                    falcon_config.class_path,
                    jdk_options, class_arguments, out_file,
                    falcon_config.pid_file)
print app_type + ' started using hadoop version: ' + \
      get_hadoop_version(falcon_config.java_bin, falcon_config.class_path)
