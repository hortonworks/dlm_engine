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

import os
import sys
import falcon_config as fc
import subprocess


def get_admin_status(base_dir, app_type, pid):
    admin_status_cmd = os.path.join(base_dir, 'bin', 'falcon')
    cmd = ['python', admin_status_cmd, 'admin', '-status']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    output = process.communicate()[0]
    if 'is running' in output:
        print app_type, 'process: ', pid
        sys.exit(pid)


cmd = sys.argv[0]
app_type = sys.argv[1]
prg, base_dir = fc.resolve_sym_link(os.path.abspath(cmd))
fc.init_config(cmd, 'server', app_type)

service_status_cmd = os.path.join(base_dir, 'bin', 'service_status.py')
subprocess.call(['python', service_status_cmd, 'falcon'])

if os.path.exists(fc.pid_file):
    pid_file = open(fc.pid_file)
    pid_file.seek(0)
    pid = int(pid_file.readline())
    try:
        os.kill(pid, 0)
        get_admin_status(base_dir, app_type, pid)
    except:
        print app_type + ' with pid ', pid, ' is not running'
        sys.exit(-2)
else:
    print app_type + ' is not running'
    sys.exit(-1)
