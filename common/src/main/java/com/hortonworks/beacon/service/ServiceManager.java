/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.service;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;

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
                LOG.error("Failed to initialize service: {}", serviceClassName, t);
                throw new BeaconException(t);
            }
            services.register(service);
            LOG.info("Service initialized: {}", serviceClassName);
        }
    }

    public void destroy() throws BeaconException {
        Iterator<String> iterator = services.reverseIterator();
        while (iterator.hasNext()) {
            BeaconService service = services.getService(iterator.next());
            LOG.info("Destroying service: {}", service.getClass().getName());
            try {
                service.destroy();
                services.deregister(service.getName());
            } catch (Throwable t) {
                LOG.error("Failed to destroy service: {}", service.getClass().getName(), t);
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
            throw new BeaconException("Unable to get instance for: ", e);
        }
    }
}
