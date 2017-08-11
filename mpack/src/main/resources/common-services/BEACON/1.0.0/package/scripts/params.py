#!/usr/bin/env python

"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your
company and Hortonworks, Inc. or an authorized affiliate or partner
thereof, any use, reproduction, modification, redistribution, sharing,
lending or other exploitation of all or any part of the contents of this
software is strictly prohibited.
"""
from ambari_commons import OSCheck
from resource_management.libraries.functions.default import default

if OSCheck.is_windows_family():
    from params_windows import *
else:
    from params_linux import *
