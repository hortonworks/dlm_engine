#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
