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

if os.path.exists(bc.pid_file):
    pid_file = open(bc.pid_file)
    pid_file.seek(0)
    pid = int(pid_file.readline())
    try:
        os.kill(pid, 0)
	print 'beacon with pid ', pid, ' is running'
        #get_admin_status(base_dir, pid)
    except :
        print 'beacon is not running'
        sys.exit(-2)
else:
    print 'beacon is not running'
    sys.exit(-1)
