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

import com.hortonworks.beacon.exceptions.BeaconException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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

    synchronized void deregister(String serviceName) throws BeaconException {

        if (!services.containsKey(serviceName)) {
            throw new BeaconException("Service " + serviceName + " is not registered");
        } else {
            services.remove(serviceName);
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

    public Iterator<String> reverseIterator() {
        List<String> list = new ArrayList<String>(services.keySet());
        Collections.reverse(list);
        return list.iterator();
    }

    public void reset() {
        services.clear();
    }
}
