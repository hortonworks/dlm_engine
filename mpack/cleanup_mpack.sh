#!/bin/bash

#
# Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
# 
# Except as expressly permitted in a written agreement between you or your
# company and Hortonworks, Inc. or an authorized affiliate or partner
# thereof, any use, reproduction, modification, redistribution, sharing,
# lending or other exploitation of all or any part of the contents of this
# software is strictly prohibited.
#

echo "Removing beacon management pack"
pushd /var/lib/ambari-server/resources
echo "Removing mpack artifacts"
rm -rf mpacks/beacon-engine.mpack-1.0-SNAPSHOT mpacks/cache/beacon-engine-*
echo "Removing common service entries"
rm -rf common-services/BEACON
echo "Removing stack entries"
rm -rf stacks/HDP/2.6/services/BEACON
echo "Removing ambari agent cache entries"
cd /var/lib/ambari-agent/cache
rm -rf  common-services/BEACON
echo "Removed beacon management pack"
popd
