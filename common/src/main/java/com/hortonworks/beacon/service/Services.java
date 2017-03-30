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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Repository of services initialized at startup.
 */
public final class Services implements Iterable<BeaconService> {

    private static final Services INSTANCE = new Services();

    private Services() {
    }

    public static Services get() {
        return INSTANCE;
    }

    private final Map<String, BeaconService> services =
            new LinkedHashMap<>();

    public synchronized void register(BeaconService service)
            throws BeaconException {

        if (services.containsKey(service.getName())) {
            throw new BeaconException("Service " + service.getName() + " already registered");
        } else {
            services.put(service.getName(), service);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends BeaconService> T getService(String serviceName) {
        if (services.containsKey(serviceName)) {
            return (T) services.get(serviceName);
        } else {
            throw new NoSuchElementException("Service " + serviceName + " not registered with registry");
        }
    }

    public boolean isRegistered(String serviceName) {
        return services.containsKey(serviceName);
    }

    @Override
    public Iterator<BeaconService> iterator() {
        return services.values().iterator();
    }

    public void reset() {
        services.clear();
    }
}
