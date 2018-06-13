/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
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
        List<String> serviceList = new LinkedList<>();
        // Add default services to beginning of list
        if (defaultServices != null && !defaultServices.isEmpty()) {
            for (String defaultService : defaultServices) {
                if (!serviceList.contains(defaultService)) {
                    serviceList.add(defaultService);
                }
            }
        }

        String serviceClassNames = BeaconConfig.getInstance().getEngine().getServices();
        String[] serviceNames = null;
        if (StringUtils.isNotBlank(serviceClassNames)) {
            serviceNames = serviceClassNames.split(BeaconConstants.COMMA_SEPARATOR);
        }

        // Add dependent services at the end i.e. {@link SchedulerStartService}
        if (dependentServices != null && !dependentServices.isEmpty()) {
            for (String service : dependentServices) {
                assert !serviceList.contains(service) : "Dependent service " + service + " is already present.";
                serviceList.add(service);
            }
        }

        if (serviceNames != null && serviceNames.length > 0) {
            serviceList.addAll(Arrays.asList(serviceNames));
        }

        LOG.debug("Services to be initialised: {}", serviceList);
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
        Iterator<BeaconService> iterator = services.reverseIterator();
        while (iterator.hasNext()) {
            BeaconService service = iterator.next();
            LOG.info("Destroying service: {}", service.getClass().getName());
            try {
                service.destroy();
                services.deregister(service.getClass().getName());
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
            throw new BeaconException(e, "Unable to get instance for: ", clazzName);
        }
    }
}
