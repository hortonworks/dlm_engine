/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.beacon;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.service.Services;

/**
 * Abstract Class to initialize the Services required for test cases.
 *
 */
public abstract class XTestCase {
    private static final BeaconLog LOG = BeaconLog.getLog(XTestCase.class);
    private static final List<String> DEFAULTSERVICES = new ArrayList<String>() {
        {
            // ResourceBundleService is to access the resourceBundle which is
            // accessed by all modules for i18n. This should be the last entry.
            if (!Services.get().isRegistered(ResourceBundleService.SERVICE_NAME)) {
                add(ResourceBundleService.SERVICE_NAME);
            }
        }
    };

    protected static void initializeServices(List<String> services) throws BeaconException {
        try {
            if (CollectionUtils.isEmpty(services)) {
                if (!Services.get().isRegistered(ResourceBundleService.SERVICE_NAME)) {
                    services = DEFAULTSERVICES;
                }
            } else if (!(services.contains(ResourceBundleService.SERVICE_NAME)
                    && Services.get().isRegistered(ResourceBundleService.SERVICE_NAME))) {
                services.add(ResourceBundleService.SERVICE_NAME);
            }
            ServiceManager.getInstance().initialize(services, null);
        } catch (BeaconException e) {
            LOG.error("Exception occurred while initializing service : {}", e.getMessage());
            throw e;
        }
    }
}
