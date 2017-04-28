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
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Initializer that Beacon uses at startup to bring up all the Beacon startup services.
 */
public final class ServiceManager {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceManager.class);
    private final Services services = Services.get();


    private ServiceManager() {
    }

    public static ServiceManager getInstance() {
        return Holder.INSTANCE;
    }

    private static class Holder {
        private static final ServiceManager INSTANCE = new ServiceManager();
    }

    public void initialize(List<String> defaultServices, List<String> dependentServices) throws BeaconException {
        String serviceClassNames = BeaconConfig.getInstance().getEngine().getServices();
        String[] serviceNames = null;
        if (StringUtils.isNotBlank(serviceClassNames)) {
            serviceNames = serviceClassNames.split(BeaconConstants.COMMA_SEPARATOR);
        }

        List<String> serviceList = new LinkedList<>();
        if (serviceNames != null && serviceNames.length > 0) {
            serviceList.addAll(Arrays.asList(serviceNames));
        }


        // Add default services to beginning of list
        if (defaultServices != null && !defaultServices.isEmpty()) {
            for (String defaultService : defaultServices) {
                if (!serviceList.contains(defaultService)) {
                    serviceList.add(0, defaultService);
                }
            }
        }
        // Add dependent services at the end i.e. {@link SchedulerStartService}
        if (dependentServices != null && !dependentServices.isEmpty()) {
            for (String service : dependentServices) {
                assert !serviceList.contains(service) : "Dependent service " + service + " is already present.";
                serviceList.add(service);
            }
        }

        for (String serviceClassName : serviceList) {
            serviceClassName = serviceClassName.trim();
            if (serviceClassName.isEmpty()) {
                continue;
            }
            BeaconService service = getInstanceByClassName(serviceClassName);
            LOG.info("Initializing service: {}", serviceClassName);
            try {
                service.init();
            } catch (Throwable t) {
                LOG.error("Failed to initialize service {}", serviceClassName, t);
                throw new BeaconException(t);
            }
            services.register(service);
            LOG.info("Service initialized: {}", serviceClassName);
        }
    }

    public void destroy() throws BeaconException {
        Iterator<BeaconService> iterator = services.iterator();
        while (iterator.hasNext()) {
            BeaconService service = iterator.next();
            LOG.info("Destroying service: {}", service.getClass().getName());
            try {
                service.destroy();
                iterator.remove();
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
