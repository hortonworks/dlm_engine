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
cmd = sys.argv[0]
prg, base_dir = fc.resolve_sym_link(os.path.abspath(cmd))
service_stop_cmd = os.path.join(base_dir, 'bin', 'service_stop.py')
subprocess.call(['python', service_stop_cmd, 'prism'])
