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

import os
import sys
import beacon_config as bc
import subprocess

cmd = sys.argv[0]
prg, base_dir = bc.resolve_sym_link(os.path.abspath(cmd))
service_stop_cmd = os.path.join(base_dir, 'bin', 'service_stop.py')
subprocess.call(['python', service_stop_cmd, 'beacon'])
