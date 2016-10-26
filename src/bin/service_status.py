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
import beacon_config as bc
import subprocess


def get_admin_status(base_dir, pid):
    admin_status_cmd = os.path.join(base_dir, 'bin', 'beacon')
    cmd = ['python', admin_status_cmd, 'admin', '-status']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    output = process.communicate()[0]
    if 'is running' in output:
        print 'beacon process: ', pid
        sys.exit(pid)


cmd = sys.argv[0]
prg, base_dir = bc.resolve_sym_link(os.path.abspath(cmd))
bc.init_config(cmd, 'server')

service_status_cmd = os.path.join(base_dir, 'bin', 'service_status.py')
subprocess.call(['python', service_status_cmd, 'beacon'])

if os.path.exists(bc.pid_file):
    pid_file = open(bc.pid_file)
    pid_file.seek(0)
    pid = int(pid_file.readline())
    try:
        os.kill(pid, 0)
        get_admin_status(base_dir, pid)
    except:
        print 'beacon with pid ', pid, ' is not running'
        sys.exit(-2)
else:
    print 'beacon is not running'
    sys.exit(-1)
