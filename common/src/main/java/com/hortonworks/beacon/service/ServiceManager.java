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
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.ReflectionUtils;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Initializer that Beacon uses at startup to bring up all the Beacon startup services.
 */
public final class ServiceManager {
    private static final BeaconLog LOG = BeaconLog.getLog(ServiceManager.class);
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
            if (serviceClassName.isEmpty() || Services.get().isRegistered(serviceClassName)) {
                continue;
            }
            BeaconService service = getInstanceByClassName(serviceClassName);
            LOG.info(MessageCode.COMM_000033.name(), serviceClassName);
            try {
                service.init();
            } catch (Throwable t) {
                LOG.error(MessageCode.COMM_000034.name(), serviceClassName, t);
                throw new BeaconException(t);
            }
            services.register(service);
            LOG.info(ResourceBundleService.getService().getString(MessageCode.COMM_000026.name(), serviceClassName));
        }
    }

    public void destroy() throws BeaconException {
        Iterator<String> iterator = services.reverseIterator();
        while (iterator.hasNext()) {
            BeaconService service = services.getService(iterator.next());
            LOG.info(MessageCode.COMM_000035.name(), service.getClass().getName());
            try {
                service.destroy();
                services.deregister(service.getName());
            } catch (Throwable t) {
                LOG.error(MessageCode.COMM_000025.name(), service.getClass().getName(), t);
                throw new BeaconException(t);
            }
            LOG.info(MessageCode.COMM_000039.name(), service.getClass().getName());
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
            throw new BeaconException(MessageCode.COMM_000036.name(), e, clazzName);
        }
    }
}
