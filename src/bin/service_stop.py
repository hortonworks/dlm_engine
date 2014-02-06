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
import falcon_config


def stop_process(pid_file):
    if os.path.exists(pid_file):
        pid_file = open(pid_file)
        pid_file.seek(0)
        pid = int(pid_file.readline())
        os.kill(pid, 15)
    else:
        os.sys.exit('pid file ' + pid_file + ' not present')

cmd = sys.argv[0]
app_type = sys.argv[1]
prg, base_dir = falcon_config.resolve_sym_link(os.path.abspath(cmd))
falcon_config.init_config(cmd, 'server', app_type)
stop_process(falcon_config.pid_file)
