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


def stop_process(pid_file):
    if not os.path.exists(pid_file):
        os.sys.exit('pid file ' + pid_file + ' not present')

    pid_file = open(pid_file)
    pid = int(pid_file.readline().strip())
    try:
        os.kill(pid, 0)
    except OSError:
        print ' beacon server pid : '+ str(pid) + ' is not running'
    else:
	print 'Stopping beacon process with pid: '+str(pid)
        os.kill(pid,15)

cmd = sys.argv[0]
prg, base_dir = bc.resolve_sym_link(os.path.abspath(cmd))
bc.init_config(cmd, 'server')
stop_process(bc.pid_file)
