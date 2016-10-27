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


def stop_process(pid_file):
    if not os.path.exists(pid_file):
        os.sys.exit('pid file ' + pid_file + ' not present')

    pid_file = open(pid_file)
    pid = int(pid_file.readline().strip())
    try:
        os.kill(pid, 0)
    except OSError:
        print app_type + ' server pid : '+ str(pid) + ' is not running'
    else:
	print 'Stopping beacon process with pid: '+str(pid)
        os.kill(pid,15)

cmd = sys.argv[0]
app_type = sys.argv[1]
prg, base_dir = bc.resolve_sym_link(os.path.abspath(cmd))
bc.init_config(cmd, 'server', app_type)
stop_process(bc.pid_file)
