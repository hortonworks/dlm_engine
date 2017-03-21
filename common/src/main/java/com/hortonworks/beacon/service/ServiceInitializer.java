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

package com.hortonworks.beacon.service;

import com.hortonworks.beacon.exceptions.BeaconException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Initializer that Beacon uses at startup to bring up all the Beacon startup services.
 */
public class ServiceInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceInitializer.class);
    private ServiceLoader<BeaconService> beaconServiceLoader;

    public void initialize() throws BeaconException {
        Class serviceClassName = BeaconService.class;
        beaconServiceLoader = ServiceLoader.load(serviceClassName);
        Iterator<BeaconService> services = beaconServiceLoader.iterator();
        if (!services.hasNext()) {
            LOG.info("Cannot find implementation for: {}", serviceClassName);
            return;
        }

        while (services.hasNext()) {
            BeaconService service = services.next();
            String serviceName = service.getName();
            LOG.info("Initializing service: {}", serviceName);
            try {
                service.init();
            } catch (Throwable t) {
                LOG.error("Failed to initialize service {}", serviceName, t);
                throw new BeaconException(t);
            }
            LOG.info("Service initialized: {}", serviceName);
        }
    }

    public void destroy() throws BeaconException {
        Iterator<BeaconService> services = beaconServiceLoader.iterator();
        while (services.hasNext()) {
            BeaconService service = services.next();
            LOG.info("Destroying service: {}", service.getName());
            try {
                service.destroy();
            } catch (Throwable t) {
                LOG.error("Failed to destroy service {}", service.getName(), t);
                throw new BeaconException(t);
            }
            LOG.info("Service destroyed: {}", service.getName());
        }
    }


    public boolean isRegistered(String serviceName) {
        boolean isRegistered = false;
        Iterator<BeaconService> services = beaconServiceLoader.iterator();
        while (services.hasNext()) {
            BeaconService service = services.next();
            isRegistered = service.getName().equalsIgnoreCase(serviceName);
        }
        return isRegistered;
    }
}
