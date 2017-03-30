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

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Initializer that Beacon uses at startup to bring up all the Beacon startup services.
 */
public final class ServiceInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceInitializer.class);
    private final Services services = Services.get();

    private ServiceInitializer() {
    }

    public static ServiceInitializer getInstance() {
        return Holder.INSTANCE;
    }

    private static class Holder {
        private static final ServiceInitializer INSTANCE = new ServiceInitializer();
    }

    public void initialize() throws BeaconException {
        String serviceClassNames = BeaconConfig.getInstance().getEngine().getServices();
        for (String serviceClassName : serviceClassNames.split(",")) {
            serviceClassName = serviceClassName.trim();
            if (serviceClassName.isEmpty()) {
                continue;
            }
            BeaconService service = getInstanceByClassName(serviceClassName);
            services.register(service);
            LOG.info("Initializing service: {}", serviceClassName);
            try {
                service.init();
            } catch (Throwable t) {
                LOG.error("Failed to initialize service {}", serviceClassName, t);
                throw new BeaconException(t);
            }
            LOG.info("Service initialized: {}", serviceClassName);
        }
    }

    public void destroy() throws BeaconException {
        for (BeaconService service : services) {
            LOG.info("Destroying service: {}", service.getClass().getName());
            try {
                service.destroy();
            } catch (Throwable t) {
                LOG.error("Failed to destroy service {}", service.getClass().getName(), t);
                throw new BeaconException(t);
            }
            LOG.info("Service destroyed: {}", service.getClass().getName());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getInstanceByClassName(String clazzName) throws BeaconException {
        try {
            Class<T> clazz = (Class<T>) ReflectionUtils.class.getClassLoader().loadClass(clazzName);
            try {
                return clazz.newInstance();
            } catch (IllegalAccessException e) {
                Method method = clazz.getMethod("get");
                return (T) method.invoke(null);
            }
        } catch (Exception e) {
            throw new BeaconException("Unable to get instance for " + clazzName, e);
        }
    }
}
