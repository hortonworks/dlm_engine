#!/usr/bin/env ambari-python-wrap
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os
import fnmatch
import imp
import socket
import sys
import traceback

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STACKS_DIR = os.path.join(SCRIPT_DIR, '../../../../../stacks/')
PARENT_FILE = os.path.join(STACKS_DIR, 'service_advisor.py')

try:
    with open(PARENT_FILE, 'rb') as fp:
        service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
    traceback.print_exc()
    print "Failed to load parent"

class Beacon10ServiceAdvisor(service_advisor.ServiceAdvisor):

    def getServiceComponentLayoutValidations(self, services, hosts):

        componentsListList = [service["components"] for service in services["services"]]
        componentsList = [item["StackServiceComponents"] for sublist in componentsListList for item in sublist]


        items = []

        # Metron Must Co-locate with KAFKA_BROKER and STORM_SUPERVISOR
        return items
